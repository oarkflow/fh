// Shared, framework-independent types for the FH secure API client.
// Keeping these separate from the components that consume them is what
// makes the client reusable: any panel/page can import from here without
// pulling in UI code.

export interface SessionStatus {
  authenticated: boolean;
  userID?: string;
}

export interface SecureBootstrapConfig {
  baseURL: string;
  pinnedServerKey: string;
  pinnedServerKeyID: string;
  responseSigningPublicKey: string;
  responseSigningKeyID: string;
  requireResponseSignature: boolean;
  requireEmbeddedTrust: boolean;
  registrationToken: string;
  wasmURL: string;
  wasmExecURL: string;
  wasmIntegrity: string;
  wasmExecIntegrity: string;
  requireAssetIntegrity: boolean;
}

// The shape of the object `createSecureFetch()` resolves to, from
// web/wasm/secure-fetch.js. Declared narrowly here so the rest of the app
// depends on a small, typed contract instead of `any`.
export interface SecureFetchClient {
  fetch(input: string, init?: RequestInit): Promise<Response>;
  sessionInfo(): Record<string, unknown>;
  revokeSession(): Promise<void>;
}

export interface WasmBridge {
  createSecureFetch(config: Record<string, unknown>): Promise<SecureFetchClient>;
}

export interface ProfileResponse {
  userID: string;
  roles: string[];
  securePrincipal?: unknown;
}

export interface EchoResponse {
  accepted: boolean;
  body: unknown;
}

// A normalized error every API call in this app throws, whether it came
// from a plain fetch to /auth/* or an encrypted call through secure-fetch.
export class ApiError extends Error {
  readonly status: number;
  readonly code?: string;
  readonly detail?: string;

  constructor(message: string, status: number, code?: string, detail?: string) {
    super(message);
    this.name = 'ApiError';
    this.status = status;
    this.code = code;
    this.detail = detail;
  }
}

export function describeError(error: unknown): string {
  if (error instanceof ApiError) {
    return error.code ? `${error.code}: ${error.message}` : error.message;
  }
  if (error instanceof Error) return error.message;
  return String(error);
}

// --- Response shape validation -------------------------------------------
//
// The server's transport/signing layer (securetransport + httpsignature)
// proves a response wasn't tampered with or forged by a third party, but it
// says nothing about whether the *decrypted, verified* payload actually has
// the shape this client expects. A compromised/misbehaving/rolled-back
// server, a proxy that rewrites a 200 body, or a future API change could all
// return a validly-signed response this app should still refuse to trust
// blindly. Every parsed response is checked against one of these guards
// before a single `as T` cast happens - see parseJSONResponse/requestJSON in
// ./http.ts, which require one for every call site.
export type Validator<T> = (value: unknown) => value is T;

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function isStringArray(value: unknown): value is string[] {
  return Array.isArray(value) && value.every((item) => typeof item === 'string');
}

export const isSessionStatus: Validator<SessionStatus> = (value): value is SessionStatus =>
  isRecord(value) &&
  typeof value.authenticated === 'boolean' &&
  (value.userID === undefined || typeof value.userID === 'string');

export const isSecureBootstrapConfig: Validator<SecureBootstrapConfig> = (value): value is SecureBootstrapConfig =>
  isRecord(value) &&
  typeof value.baseURL === 'string' &&
  typeof value.pinnedServerKey === 'string' &&
  typeof value.pinnedServerKeyID === 'string' &&
  typeof value.responseSigningPublicKey === 'string' &&
  typeof value.responseSigningKeyID === 'string' &&
  typeof value.requireResponseSignature === 'boolean' &&
  typeof value.requireEmbeddedTrust === 'boolean' &&
  typeof value.registrationToken === 'string' &&
  typeof value.wasmURL === 'string' &&
  typeof value.wasmExecURL === 'string' &&
  typeof value.wasmIntegrity === 'string' &&
  typeof value.wasmExecIntegrity === 'string' &&
  typeof value.requireAssetIntegrity === 'boolean';

export const isProfileResponse: Validator<ProfileResponse> = (value): value is ProfileResponse =>
  isRecord(value) && typeof value.userID === 'string' && isStringArray(value.roles);

export const isEchoResponse: Validator<EchoResponse> = (value): value is EchoResponse =>
  isRecord(value) && typeof value.accepted === 'boolean' && 'body' in value;

export const isLoginResponse: Validator<{ authenticated: boolean }> = (value): value is { authenticated: boolean } =>
  isRecord(value) && typeof value.authenticated === 'boolean';

// /auth/logout returns an empty JSON object on success; nothing more to
// check beyond "the server returned a JSON object, not something else".
export const isEmptyResponse: Validator<Record<string, unknown>> = (value): value is Record<string, unknown> =>
  isRecord(value);
