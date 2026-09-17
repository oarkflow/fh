import { ApiError, type Validator } from './types.ts';

// One small, reusable JSON request helper for the plain same-origin
// endpoints (/auth/*, /secure-config.json). Every call is same-origin,
// credentialed, and never cached, matching what the server's origin checks
// require; callers only supply a path, method, and body.
export interface JSONRequestInit extends Omit<RequestInit, 'body'> {
  body?: unknown;
}

export async function requestJSON<T>(path: string, validate: Validator<T>, init: JSONRequestInit = {}): Promise<T> {
  const { body, headers, ...rest } = init;
  const response = await fetch(path, {
    credentials: 'same-origin',
    cache: 'no-store',
    ...rest,
    headers: { 'content-type': 'application/json', ...(headers as Record<string, string> | undefined) },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  return parseJSONResponse<T>(response, validate);
}

// Shared between plain fetch responses and the responses secure-fetch
// returns from an encrypted call - both are standard Response objects once
// decrypted, so the same parsing/error contract applies to either.
//
// `validate` is required, not optional: the transport/signature layer only
// proves a response is genuinely from this server and unmodified in
// transit, not that its *content* is the shape this client expects. Every
// call site owns a validator for its own response type (see types.ts) so a
// malformed or unexpected payload is rejected here instead of flowing into
// the UI as an unchecked `any`.
export async function parseJSONResponse<T>(response: Response, validate: Validator<T>): Promise<T> {
  const text = await response.text();
  let body: unknown = {};
  if (text) {
    try {
      body = JSON.parse(text);
    } catch {
      body = { detail: text };
    }
  }
  if (!response.ok) {
    const record = (body ?? {}) as Record<string, unknown>;
    const detail = typeof record.detail === 'string' ? record.detail : undefined;
    const code = typeof record.code === 'string' ? record.code : undefined;
    throw new ApiError(detail || `request failed with status ${response.status}`, response.status, code, detail);
  }
  if (!validate(body)) {
    throw new ApiError('server response did not match the expected shape', response.status, 'INVALID_RESPONSE_SHAPE');
  }
  return body;
}
