/** MIRA-T/1 browser bridge: concurrent fetch POST and authenticated NDJSON pull.
 *  Native clients use the custom UDP transport; browser clients cannot.
 *  Configure CORS deliberately at your trusted HTTPS reverse proxy; no wildcard.
 */
export type MiraEvent = {cursor: number; data: Uint8Array};
export type MiraError = {status: number; message: string};

function toBase64(data: Uint8Array): string {
  let text = "";
  for (let i = 0; i < data.length; i += 8192) {
    text += String.fromCharCode(...data.subarray(i, i + 8192));
  }
  return btoa(text);
}
function fromBase64(encoded: string): Uint8Array {
  const raw = atob(encoded);
  const out = new Uint8Array(raw.length);
  for (let i = 0; i < raw.length; i++) out[i] = raw.charCodeAt(i);
  return out;
}

export class MiraBrowserClient {
  private session = "";
  private cursor = 0;
  private stop = new AbortController();
  private running = false;
  constructor(
    private readonly endpoint: string,
    private readonly bearerToken: string,
    private readonly onEvent: (e: MiraEvent) => void,
  ) {
    if (!endpoint.startsWith("https://")) throw new Error("HTTPS is mandatory");
  }
  private headers(extra?: Record<string, string>): HeadersInit {
    return { Authorization: `Bearer ${this.bearerToken}`, ...extra };
  }
  private async checked(response: Response): Promise<Response> {
    if (!response.ok) throw { status: response.status, message: `HTTP ${response.status}` } as MiraError;
    return response;
  }
  async connect(): Promise<void> {
    if (this.running) throw new Error("already connected");
    this.stop = new AbortController();
    const res = await this.checked(await fetch(`${this.endpoint}/v1/connect`, {
      method: "POST", headers: this.headers({"Content-Type":"application/json"}),
      body: "{}", signal: this.stop.signal, cache: "no-store",
    }));
    const data = await res.json();
    if (!data.session || typeof data.session !== "string") throw new Error("missing session");
    this.session = data.session;
    this.cursor = 0;
    this.running = true;
    void this.streamLoop().catch((error) => {
      this.running = false;
      console.error("MIRA gateway stream ended; reconnect and replay required", error);
    });
  }
  async send(payload: Uint8Array): Promise<number> {
    if (!this.session) throw new Error("not connected");
    if (payload.length > 256 * 1024) throw new Error("payload too large");
    const res = await this.checked(await fetch(`${this.endpoint}/v1/send`, {
      method: "POST", headers: this.headers({"Content-Type":"application/json"}),
      body: JSON.stringify({session:this.session, data:toBase64(payload)}),
      signal:this.stop.signal, cache: "no-store",
    }));
    return (await res.json()).message_id as number;
  }
  private async streamLoop(): Promise<void> {
    let retryMs=250;
    while(this.running && !this.stop.signal.aborted) {
      try {
        const u = new URL(`${this.endpoint}/v1/stream`);
        u.searchParams.set("session",this.session);
        u.searchParams.set("cursor",String(this.cursor));
        const res=await this.checked(await fetch(u,{
          headers:this.headers(),signal:this.stop.signal,cache:"no-store",referrerPolicy:"no-referrer",
        }));
        if (!res.body) throw new Error("stream body unavailable");
        const reader=res.body.getReader();const decoder=new TextDecoder();let pending="";
        while(this.running) {
          const {done,value}=await reader.read();if(done)break;
          pending+=decoder.decode(value,{stream:true});
          if (pending.length>1024*1024) throw new Error("oversized event line");
          for(;;) {
            const newline=pending.indexOf("\n");if(newline<0)break;
            const line=pending.slice(0,newline);pending=pending.slice(newline+1);
            if(!line)continue;
            const event=JSON.parse(line);
            if(event.type==="heartbeat")continue;
            if(typeof event.cursor!=="number" || typeof event.data!=="string")continue;
            if(event.cursor<=this.cursor)continue;
            if(event.cursor>this.cursor+1)throw new Error("cursor gap: application replay required");
            this.cursor=event.cursor;
            this.onEvent({cursor:event.cursor,data:fromBase64(event.data)});
          }
        }
        retryMs=250;
      } catch(e) {
        if(this.stop.signal.aborted)break;
        if(typeof e==="object" && e!==null && "status" in e && [404,409,410].includes((e as MiraError).status)) {
          this.running=false;
          throw e; // application must reconnect with a durable replay cursor
        }
        await new Promise(resolve=>setTimeout(resolve,retryMs));
        retryMs=Math.min(10000,retryMs*2);
      }
    }
  }
  async close(): Promise<void> {
    const old=this.session;
    this.running=false;
    this.stop.abort();
    this.session="";
    if(old) {
      const u = new URL(`${this.endpoint}/v1/connect`);u.searchParams.set("session",old);
      try {await fetch(u,{method:"DELETE",headers:this.headers(),cache:"no-store"});}catch { /* server expiry is bounded */ }
    }
  }
}
