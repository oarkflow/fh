const SEP = '\u0000';
export class EventSocket {
  constructor(options) {
    this.options = options;
    this.ws = undefined;
    this.handlers = new Map();
    this.anyHandlers = new Set();
    this.pending = new Map();
    this.queue = [];
    this.subscribed = new Set();
    this.openHandlers = new Set();
    this.closeHandlers = new Set();
    this.errorHandlers = new Set();
    this.closedByUser = false;
    this.connecting = undefined;
    this.reconnectAttempt = 0;
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
  get readyState() { return this.ws?.readyState; }
  get isOpen() { return this.ws?.readyState === WebSocket.OPEN; }
  connect() {
    if (this.isOpen) return Promise.resolve();
    if (this.connecting) return this.connecting;
    this.closedByUser = false;
    this.connecting = new Promise(async (resolve, reject) => {
      let settled = false;
      try {
        const ws = new WebSocket(await this.buildURL(), this.options.protocols);
        this.ws = ws;
        ws.onopen = () => { settled = true; this.connecting = undefined; this.reconnectAttempt = 0; this.flush(); this.restoreSubscriptions(); this.startHeartbeat(); for (const h of [...this.openHandlers]) h(this); resolve(); };
        ws.onmessage = (ev) => this.handleMessage(ev.data);
        ws.onerror = () => { const err = new Error('websocket error'); this.emitError(err); if (!settled) { settled = true; this.connecting = undefined; reject(err); } };
        ws.onclose = (ev) => { this.stopHeartbeat(); this.rejectAll(new Error(`websocket closed: ${ev.code} ${ev.reason}`)); for (const h of [...this.closeHandlers]) h(ev); if (!settled) { settled = true; this.connecting = undefined; reject(new Error(`websocket closed: ${ev.code} ${ev.reason}`)); } if (!this.closedByUser && this.options.reconnect) this.scheduleReconnect(); };
      } catch (err) { this.connecting = undefined; reject(err); }
    });
    return this.connecting;
  }
  close(code = 1000, reason = 'client closed') { this.closedByUser = true; if (this.reconnectTimer) clearTimeout(this.reconnectTimer); this.stopHeartbeat(); this.rejectAll(new Error('client closed')); this.ws?.close(code, reason); }
  onOpen(handler) { this.openHandlers.add(handler); return () => this.openHandlers.delete(handler); }
  onClose(handler) { this.closeHandlers.add(handler); return () => this.closeHandlers.delete(handler); }
  onError(handler) { this.errorHandlers.add(handler); return () => this.errorHandlers.delete(handler); }
  onAny(handler) { this.anyHandlers.add(handler); return () => this.anyHandlers.delete(handler); }
  on(event, handler) { const key = normalize(event); if (!this.handlers.has(key)) this.handlers.set(key, new Set()); this.handlers.get(key).add(handler); return () => this.off(key, handler); }
  off(event, handler) { const key = normalize(event); if (!handler) { this.handlers.delete(key); return; } this.handlers.get(key)?.delete(handler); }
  emit(event, payload, opts = {}) { this.send({ type: 'emit', event: normalize(event), topic: opts.topic, channel: opts.channel, payload }); }
  request(event, payload, opts = {}) { const id = id16(); const timeout = opts.timeoutMs ?? this.options.ackTimeoutMs; const env = { type: 'emit', id, event: normalize(event), topic: opts.topic, channel: opts.channel, payload, ts: Date.now() }; const p = new Promise((resolve, reject) => { const timer = setTimeout(() => { this.pending.delete(id); reject(new Error(`ack timeout for ${event}`)); }, timeout); this.pending.set(id, { resolve, reject, timer }); }); this.send(env); return p; }
  subscribe(topic, channel) { this.subscribed.add(subKey(topic, channel)); this.send({ type: 'subscribe', topic, channel }); }
  subscribeAck(topic, channel, timeoutMs) { this.subscribed.add(subKey(topic, channel)); return this.requestEnvelope({ type: 'subscribe', topic, channel }, timeoutMs); }
  unsubscribe(topic, channel) { this.subscribed.delete(subKey(topic, channel)); this.send({ type: 'unsubscribe', topic, channel }); }
  unsubscribeAck(topic, channel, timeoutMs) { this.subscribed.delete(subKey(topic, channel)); return this.requestEnvelope({ type: 'unsubscribe', topic, channel }, timeoutMs); }
  ack(replyTo, payload) { this.send({ type: 'ack', replyTo, payload, ts: Date.now() }); }
  error(replyTo, code, message) { this.send({ type: 'error', replyTo, code, error: message, ts: Date.now() }); }
  requestEnvelope(env, timeoutMs) { const id = id16(); env.id = id; env.ts ??= Date.now(); const timeout = timeoutMs ?? this.options.ackTimeoutMs; const p = new Promise((resolve, reject) => { const timer = setTimeout(() => { this.pending.delete(id); reject(new Error(`ack timeout for ${env.type ?? env.event ?? 'envelope'}`)); }, timeout); this.pending.set(id, { resolve, reject, timer }); }); this.send(env); return p; }
  async buildURL() { const token = typeof this.options.token === 'function' ? await this.options.token() : this.options.token; if (!token) return this.options.url; const u = new URL(this.options.url, location.href); u.searchParams.set(this.options.tokenParam, token); return u.toString(); }
  send(env) { env.ts ??= Date.now(); const raw = JSON.stringify(env); if (raw.length > this.options.maxMessageBytes) throw new Error('event websocket message too large'); if (this.isOpen) { this.ws.send(raw); return; } if (this.queue.length >= this.options.maxQueue) this.queue.shift(); this.queue.push(env); }
  flush() { while (this.isOpen && this.queue.length) this.ws.send(JSON.stringify(this.queue.shift())); }
  restoreSubscriptions() { for (const key of [...this.subscribed]) { const [topic, channel] = key.split(SEP); this.send({ type: 'subscribe', topic: topic || undefined, channel: channel || undefined }); } }
  handleMessage(data) { let env; try { env = JSON.parse(String(data)); } catch (err) { this.emitError(err); return; } if (env.type === 'ack' || env.type === 'error') { const id = env.replyTo || env.id; if (id && this.pending.has(id)) { const p = this.pending.get(id); clearTimeout(p.timer); this.pending.delete(id); env.type === 'error' ? p.reject(new Error(`${env.code ?? 'error'}: ${env.error ?? ''}`)) : p.resolve(env); } if (env.type === 'error') this.emitError(new Error(`${env.code ?? 'error'}: ${env.error ?? ''}`), env); return; } if (env.type === 'hello') { void this.dispatch('hello', env.payload, env); return; } if (env.type === 'ping') { this.send({ type: 'pong', replyTo: env.id, payload: env.payload, ts: Date.now() }); return; } if (env.id && this.options.autoAckServerRequests) { Promise.resolve(this.dispatch(env.event ?? env.type ?? '', env.payload, env)).then(() => this.ack(env.id, { ok: true })).catch((e) => this.error(env.id, 'handler_error', String(e?.message ?? e))); return; } void this.dispatch(env.event ?? env.type ?? '', env.payload, env); }
  async dispatch(event, payload, env) { const key = normalize(event); const exact = this.handlers.get(key); const wildcard = this.findWildcardHandlers(key); for (const h of [...(exact ?? []), ...wildcard, ...this.anyHandlers]) await h(payload, env); }
  findWildcardHandlers(event) { const out = []; for (const [pattern, set] of this.handlers) { if (!pattern.endsWith('.*')) continue; const prefix = pattern.slice(0, -1); if (event.startsWith(prefix)) out.push(...set); } return out; }
  scheduleReconnect() { if (this.reconnectTimer) clearTimeout(this.reconnectTimer); const min = this.options.reconnectMinMs; const max = this.options.reconnectMaxMs; const exp = Math.min(max, min * 2 ** this.reconnectAttempt++); const jitter = Math.floor(Math.random() * Math.max(1, exp / 3)); this.reconnectTimer = setTimeout(() => void this.connect().catch((e) => this.emitError(e)), exp + jitter); }
  startHeartbeat() { this.stopHeartbeat(); if (!this.options.heartbeatMs) return; this.heartbeatTimer = setInterval(() => { if (this.isOpen) this.send({ type: 'ping', event: 'ping', payload: { t: Date.now() } }); }, this.options.heartbeatMs); }
  stopHeartbeat() { if (this.heartbeatTimer) clearInterval(this.heartbeatTimer); this.heartbeatTimer = undefined; }
  rejectAll(reason) { for (const [id, p] of this.pending) { clearTimeout(p.timer); p.reject(reason); this.pending.delete(id); } }
  emitError(error, env) { if (this.errorHandlers.size === 0 && this.options.debug) console.error(error, env); for (const h of [...this.errorHandlers]) h(error, env); }
}
function normalize(s) { return String(s || '').trim(); }
function subKey(topic, channel) { return `${topic ?? ''}${SEP}${channel ?? ''}`; }
function id16() { const arr = new Uint8Array(16); crypto.getRandomValues(arr); return [...arr].map((b) => b.toString(16).padStart(2, '0')).join(''); }
