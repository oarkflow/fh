export type EnvelopeType =
  | 'hello'
  | 'emit'
  | 'ack'
  | 'error'
  | 'subscribe'
  | 'unsubscribe'
  | 'presence'
  | 'ping'
  | 'pong';

export interface Envelope<T = unknown> {
  type?: EnvelopeType;
  id?: string;
  replyTo?: string;
  event?: string;
  topic?: string;
  channel?: string;
  payload?: T;
  error?: string;
  code?: string;
  ts?: number;
}

export type EventHandler<T = unknown> = (payload: T, envelope: Envelope<T>) => void | Promise<void>;
export type StatusHandler = (socket: EventSocket) => void;
export type ErrorHandler = (error: unknown, envelope?: Envelope) => void;

export interface EventSocketOptions {
  url: string;
  token?: string | (() => string | Promise<string>);
  tokenParam?: string;
  protocols?: string | string[];
  reconnect?: boolean;
  reconnectMinMs?: number;
  reconnectMaxMs?: number;
  ackTimeoutMs?: number;
  heartbeatMs?: number;
  maxQueue?: number;
  maxMessageBytes?: number;
  autoAckServerRequests?: boolean;
  debug?: boolean;
}

type Pending = {
  resolve: (value: Envelope) => void;
  reject: (reason: unknown) => void;
  timer: ReturnType<typeof setTimeout>;
};

const SEP = '\u0000';

export class EventSocket {
  private ws?: WebSocket;
  private readonly handlers = new Map<string, Set<EventHandler>>();
  private readonly anyHandlers = new Set<EventHandler>();
  private readonly pending = new Map<string, Pending>();
  private readonly queue: Envelope[] = [];
  private readonly subscribed = new Set<string>();
  private readonly openHandlers = new Set<StatusHandler>();
  private readonly closeHandlers = new Set<(ev: CloseEvent) => void>();
  private readonly errorHandlers = new Set<ErrorHandler>();
  private closedByUser = false;
  private connecting?: Promise<void>;
  private reconnectAttempt = 0;
  private reconnectTimer?: ReturnType<typeof setTimeout>;
  private heartbeatTimer?: ReturnType<typeof setInterval>;

  constructor(private readonly options: EventSocketOptions) {
    this.options.tokenParam ??= 'token';
    this.options.reconnect ??= true;
    this.options.reconnectMinMs ??= 500;
    this.options.reconnectMaxMs ??= 15000;
    this.options.ackTimeoutMs ??= 10000;
    this.options.heartbeatMs ??= 25000;
    this.options.maxQueue ??= 512;
    this.options.maxMessageBytes ??= 1024 * 1024;
    this.options.autoAckServerRequests ??= true;
  }

  get readyState(): number | undefined { return this.ws?.readyState; }
  get isOpen(): boolean { return this.ws?.readyState === WebSocket.OPEN; }

  connect(): Promise<void> {
    if (this.isOpen) return Promise.resolve();
    if (this.connecting) return this.connecting;
    this.closedByUser = false;

    this.connecting = new Promise<void>(async (resolve, reject) => {
      let settled = false;
      try {
        const url = await this.buildURL();
        const ws = new WebSocket(url, this.options.protocols);
        this.ws = ws;

        ws.onopen = () => {
          settled = true;
          this.connecting = undefined;
          this.reconnectAttempt = 0;
          this.flush();
          this.restoreSubscriptions();
          this.startHeartbeat();
          for (const h of [...this.openHandlers]) h(this);
          resolve();
        };

        ws.onmessage = (ev) => this.handleMessage(ev.data);

        ws.onerror = () => {
          const err = new Error('websocket error');
          this.emitError(err);
          if (!settled) {
            settled = true;
            this.connecting = undefined;
            reject(err);
          }
        };

        ws.onclose = (ev) => {
          this.stopHeartbeat();
          this.rejectAll(new Error(`websocket closed: ${ev.code} ${ev.reason}`));
          for (const h of [...this.closeHandlers]) h(ev);
          if (!settled) {
            settled = true;
            this.connecting = undefined;
            reject(new Error(`websocket closed: ${ev.code} ${ev.reason}`));
          }
          if (!this.closedByUser && this.options.reconnect) this.scheduleReconnect();
        };
      } catch (err) {
        this.connecting = undefined;
        reject(err);
      }
    });

    return this.connecting;
  }

  close(code = 1000, reason = 'client closed'): void {
    this.closedByUser = true;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    this.stopHeartbeat();
    this.rejectAll(new Error('client closed'));
    this.ws?.close(code, reason);
  }

  onOpen(handler: StatusHandler): () => void {
    this.openHandlers.add(handler);
    return () => this.openHandlers.delete(handler);
  }

  onClose(handler: (ev: CloseEvent) => void): () => void {
    this.closeHandlers.add(handler);
    return () => this.closeHandlers.delete(handler);
  }

  onError(handler: ErrorHandler): () => void {
    this.errorHandlers.add(handler);
    return () => this.errorHandlers.delete(handler);
  }

  onAny(handler: EventHandler): () => void {
    this.anyHandlers.add(handler);
    return () => this.anyHandlers.delete(handler);
  }

  on<T = unknown>(event: string, handler: EventHandler<T>): () => void {
    const key = normalize(event);
    if (!this.handlers.has(key)) this.handlers.set(key, new Set());
    this.handlers.get(key)!.add(handler as EventHandler);
    return () => this.off(key, handler as EventHandler);
  }

  off(event: string, handler?: EventHandler): void {
    const key = normalize(event);
    if (!handler) { this.handlers.delete(key); return; }
    this.handlers.get(key)?.delete(handler);
  }

  emit<T = unknown>(event: string, payload?: T, opts: { topic?: string; channel?: string } = {}): void {
    this.send({ type: 'emit', event: normalize(event), topic: opts.topic, channel: opts.channel, payload });
  }

  request<TReq = unknown, TRes = unknown>(
    event: string,
    payload?: TReq,
    opts: { topic?: string; channel?: string; timeoutMs?: number } = {},
  ): Promise<Envelope<TRes>> {
    const id = id16();
    const timeout = opts.timeoutMs ?? this.options.ackTimeoutMs!;
    const env: Envelope<TReq> = { type: 'emit', id, event: normalize(event), topic: opts.topic, channel: opts.channel, payload, ts: Date.now() };
    const p = new Promise<Envelope<TRes>>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new Error(`ack timeout for ${event}`));
      }, timeout);
      this.pending.set(id, { resolve: resolve as (value: Envelope) => void, reject, timer });
    });
    this.send(env);
    return p;
  }

  subscribe(topic?: string, channel?: string): void {
    const key = subKey(topic, channel);
    this.subscribed.add(key);
    this.send({ type: 'subscribe', topic, channel });
  }

  async subscribeAck(topic?: string, channel?: string, timeoutMs?: number): Promise<Envelope> {
    const key = subKey(topic, channel);
    this.subscribed.add(key);
    return this.requestEnvelope({ type: 'subscribe', topic, channel }, timeoutMs);
  }

  unsubscribe(topic?: string, channel?: string): void {
    const key = subKey(topic, channel);
    this.subscribed.delete(key);
    this.send({ type: 'unsubscribe', topic, channel });
  }

  async unsubscribeAck(topic?: string, channel?: string, timeoutMs?: number): Promise<Envelope> {
    const key = subKey(topic, channel);
    this.subscribed.delete(key);
    return this.requestEnvelope({ type: 'unsubscribe', topic, channel }, timeoutMs);
  }

  ack<T = unknown>(replyTo: string, payload?: T): void {
    this.send({ type: 'ack', replyTo, payload, ts: Date.now() });
  }

  error(replyTo: string, code: string, message: string): void {
    this.send({ type: 'error', replyTo, code, error: message, ts: Date.now() });
  }

  private requestEnvelope<T = unknown>(env: Envelope<T>, timeoutMs?: number): Promise<Envelope> {
    const id = id16();
    env.id = id;
    env.ts ??= Date.now();
    const timeout = timeoutMs ?? this.options.ackTimeoutMs!;
    const p = new Promise<Envelope>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new Error(`ack timeout for ${env.type ?? env.event ?? 'envelope'}`));
      }, timeout);
      this.pending.set(id, { resolve, reject, timer });
    });
    this.send(env);
    return p;
  }

  private async buildURL(): Promise<string> {
    const token = typeof this.options.token === 'function' ? await this.options.token() : this.options.token;
    if (!token) return this.options.url;
    const u = new URL(this.options.url, typeof location !== 'undefined' ? location.href : undefined);
    u.searchParams.set(this.options.tokenParam!, token);
    return u.toString();
  }

  private send(env: Envelope): void {
    env.ts ??= Date.now();
    const raw = JSON.stringify(env);
    if (raw.length > this.options.maxMessageBytes!) throw new Error('event websocket message too large');
    if (this.isOpen) {
      this.ws!.send(raw);
      return;
    }
    if (this.queue.length >= this.options.maxQueue!) this.queue.shift();
    this.queue.push(env);
  }

  private flush(): void {
    while (this.isOpen && this.queue.length) this.ws!.send(JSON.stringify(this.queue.shift()));
  }

  private restoreSubscriptions(): void {
    for (const key of [...this.subscribed]) {
      const [topic, channel] = key.split(SEP);
      this.send({ type: 'subscribe', topic: topic || undefined, channel: channel || undefined });
    }
  }

  private handleMessage(data: unknown): void {
    let env: Envelope;
    try { env = JSON.parse(String(data)); } catch (err) { this.emitError(err); return; }

    if (env.type === 'ack' || env.type === 'error') {
      const id = env.replyTo || env.id;
      if (id && this.pending.has(id)) {
        const p = this.pending.get(id)!;
        clearTimeout(p.timer);
        this.pending.delete(id);
        env.type === 'error' ? p.reject(new Error(`${env.code ?? 'error'}: ${env.error ?? ''}`)) : p.resolve(env);
      }
      if (env.type === 'error') this.emitError(new Error(`${env.code ?? 'error'}: ${env.error ?? ''}`), env);
      return;
    }

    if (env.type === 'hello') {
      void this.dispatch('hello', env.payload, env);
      return;
    }

    if (env.type === 'ping') {
      this.send({ type: 'pong', replyTo: env.id, payload: env.payload, ts: Date.now() });
      return;
    }

    if (env.id && this.options.autoAckServerRequests) {
      Promise.resolve(this.dispatch(env.event ?? env.type ?? '', env.payload, env))
        .then(() => this.ack(env.id!, { ok: true }))
        .catch((e) => this.error(env.id!, 'handler_error', String(e?.message ?? e)));
      return;
    }

    void this.dispatch(env.event ?? env.type ?? '', env.payload, env);
  }

  private async dispatch<T>(event: string, payload: T, env: Envelope<T>): Promise<void> {
    const key = normalize(event);
    const exact = this.handlers.get(key);
    const wildcard = this.findWildcardHandlers(key);
    for (const h of [...(exact ?? []), ...wildcard, ...this.anyHandlers]) await h(payload, env);
  }

  private findWildcardHandlers(event: string): EventHandler[] {
    const out: EventHandler[] = [];
    for (const [pattern, set] of this.handlers) {
      if (!pattern.endsWith('.*')) continue;
      const prefix = pattern.slice(0, -1);
      if (event.startsWith(prefix)) out.push(...set);
    }
    return out;
  }

  private scheduleReconnect(): void {
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    const min = this.options.reconnectMinMs!;
    const max = this.options.reconnectMaxMs!;
    const exp = Math.min(max, min * 2 ** this.reconnectAttempt++);
    const jitter = Math.floor(Math.random() * Math.max(1, exp / 3));
    this.reconnectTimer = setTimeout(() => void this.connect().catch((e) => this.emitError(e)), exp + jitter);
  }

  private startHeartbeat(): void {
    this.stopHeartbeat();
    if (!this.options.heartbeatMs) return;
    this.heartbeatTimer = setInterval(() => {
      if (this.isOpen) this.send({ type: 'ping', event: 'ping', payload: { t: Date.now() } });
    }, this.options.heartbeatMs);
  }

  private stopHeartbeat(): void {
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer);
    this.heartbeatTimer = undefined;
  }

  private rejectAll(reason: unknown): void {
    for (const [id, p] of this.pending) {
      clearTimeout(p.timer);
      p.reject(reason);
      this.pending.delete(id);
    }
  }

  private emitError(error: unknown, env?: Envelope): void {
    if (this.errorHandlers.size === 0 && this.options.debug) console.error(error, env);
    for (const h of [...this.errorHandlers]) h(error, env);
  }
}

function normalize(s: string): string { return String(s || '').trim(); }
function subKey(topic?: string, channel?: string): string { return `${topic ?? ''}${SEP}${channel ?? ''}`; }

function id16(): string {
  const arr = new Uint8Array(16);
  crypto.getRandomValues(arr);
  return [...arr].map((b) => b.toString(16).padStart(2, '0')).join('');
}
