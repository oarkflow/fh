export function isBodyInit(value: unknown): value is BodyInit {
  if (typeof value === "string") return true;
  if (value instanceof ArrayBuffer) return true;
  if (ArrayBuffer.isView(value)) return true;
  if (typeof Blob !== "undefined" && value instanceof Blob) return true;
  if (typeof FormData !== "undefined" && value instanceof FormData) return true;
  if (typeof URLSearchParams !== "undefined" && value instanceof URLSearchParams) return true;
  if (typeof ReadableStream !== "undefined" && value instanceof ReadableStream) return true;
  return false;
}

export function responseHasNoBody(method: string, status: number): boolean {
  return method === "HEAD" || status === 204 || status === 205 || status === 304 || (status >= 100 && status < 200);
}
