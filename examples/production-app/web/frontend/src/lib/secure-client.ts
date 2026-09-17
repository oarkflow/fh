import { loadWasmBridge } from './wasm-bridge.ts';
import { requestJSON, parseJSONResponse } from './http.ts';
import {
  ApiError,
  isEchoResponse,
  isEmptyResponse,
  isLoginResponse,
  isProfileResponse,
  isSecureBootstrapConfig,
  isSessionStatus,
} from './types.ts';
import type {
  EchoResponse,
  ProfileResponse,
  SecureBootstrapConfig,
  SecureFetchClient,
  SessionStatus,
} from './types.ts';

// A single, structured, reusable client for every call this app makes.
// It is deliberately the only module that knows about /auth/*,
// /secure-config.json, /api/*, and the WASM secure transport - every
// component below calls through here instead of touching `fetch` or the
// WASM bridge directly, so adding a new page or a new API only ever means
// adding one function to this file plus one call site.

let secure: SecureFetchClient | undefined;

/** Plain, unauthenticated/session-cookie endpoints. */
export const authApi = {
  session: () => requestJSON<SessionStatus>('/auth/session', isSessionStatus, { method: 'GET' }),

  login: (username: string, password: string) =>
    requestJSON<{ authenticated: boolean }>('/auth/login', isLoginResponse, {
      method: 'POST',
      body: { username, password },
    }),

  logout: async () => {
    await requestJSON('/auth/logout', isEmptyResponse, { method: 'POST' });
  },
};

/**
 * Bootstraps the encrypted WASM transport: fetches a one-use registration
 * grant and the server's public trust pins, loads the secure-fetch WASM
 * runtime, and registers/derives an encrypted session bound to the current
 * login. Safe to call more than once - later calls reuse the live session.
 */
export async function ensureSecureSession(): Promise<SecureFetchClient> {
  if (secure) return secure;
  const config = await requestJSON<SecureBootstrapConfig>('/secure-config.json', isSecureBootstrapConfig, { method: 'POST' });
  const { createSecureFetch } = await loadWasmBridge();
  secure = await createSecureFetch({
    ...config,
    credentials: 'same-origin',
    clientBuild: 'fh-control-center-1',
    deviceName: deviceLabel(),
  });
  return secure;
}

export function secureSessionInfo(): Record<string, unknown> | undefined {
  return secure?.sessionInfo();
}

export async function revokeSecureSession(): Promise<void> {
  if (!secure) return;
  await secure.revokeSession().catch(() => undefined);
  secure = undefined;
}

export function hasSecureSession(): boolean {
  return secure !== undefined;
}

/** Encrypted, authenticated `/api/*` calls - every one goes through secure-fetch. */
export const secureApi = {
  async me(): Promise<ProfileResponse> {
    const client = await requireSecureClient();
    const response = await client.fetch('/api/me');
    return parseJSONResponse<ProfileResponse>(response, isProfileResponse);
  },

  async echo(payload: unknown): Promise<EchoResponse> {
    const client = await requireSecureClient();
    const response = await client.fetch('/api/echo', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify(payload),
    });
    return parseJSONResponse<EchoResponse>(response, isEchoResponse);
  },
};

async function requireSecureClient(): Promise<SecureFetchClient> {
  if (secure) return secure;
  return ensureSecureSession();
}

function deviceLabel(): string {
  if (typeof navigator === 'undefined') return 'Browser';
  const platform = (navigator as { userAgentData?: { platform?: string } }).userAgentData?.platform;
  return platform || navigator.platform || 'Browser';
}

export { ApiError };
