import { computed, signal } from '@oarkflow/lithe/core';
import {
  authApi,
  ensureSecureSession,
  revokeSecureSession,
  secureApi,
  secureSessionInfo,
} from '../lib/secure-client.ts';
import { describeError, type EchoResponse, type ProfileResponse } from '../lib/types.ts';

// The application's whole client state lives in this module as a handful
// of signals plus the actions that mutate them. Components only ever read
// signals and call actions - none of them talk to `secure-client` directly,
// which keeps the data flow in one direction and the UI layer replaceable.

export type Phase = 'checking' | 'guest' | 'signing-in' | 'authenticated';
export type Panel = 'overview' | 'api' | 'activity';

export interface ActivityEntry {
  id: number;
  at: number;
  label: string;
  status: 'ok' | 'error' | 'pending';
  detail?: string;
}

export const phase = signal<Phase>('checking', { name: 'phase' });
export const userID = signal<string | undefined>(undefined, { name: 'userID' });
export const profile = signal<ProfileResponse | undefined>(undefined, { name: 'profile' });
export const lastEcho = signal<EchoResponse | undefined>(undefined, { name: 'lastEcho' });
export const secureInfo = signal<Record<string, unknown> | undefined>(undefined, { name: 'secureInfo' });
export const activity = signal<ActivityEntry[]>([], { name: 'activity' });
export const formError = signal<string | undefined>(undefined, { name: 'formError' });
export const busy = signal(false, { name: 'busy' });

export const isAuthenticated = computed(() => phase.value === 'authenticated');
export const isChecking = computed(() => phase.value === 'checking');

// Which dashboard panel is showing. A plain signal rather than a router:
// this app has one authenticated surface with a few facets, not distinct
// navigable pages - see web/frontend/README.md for how to graduate this to
// @oarkflow/lithe/router if/when the app grows real routes.
export const activePanel = signal<Panel>('overview', { name: 'activePanel' });

export function selectPanel(panel: Panel): void {
  activePanel.value = panel;
}

let activitySeq = 0;
function record(label: string, status: ActivityEntry['status'], detail?: string) {
  const entry: ActivityEntry = { id: ++activitySeq, at: Date.now(), label, status, detail };
  activity.value = [entry, ...activity.value].slice(0, 12);
}

async function withBusy<T>(fn: () => Promise<T>): Promise<T> {
  busy.value = true;
  try {
    return await fn();
  } finally {
    busy.value = false;
  }
}

/** Called once on boot: restores an existing web login, if any, and re-establishes the secure session. */
export async function restoreSession(): Promise<void> {
  try {
    const status = await authApi.session();
    if (!status.authenticated) {
      phase.value = 'guest';
      return;
    }
    userID.value = status.userID;
    await ensureSecureSession();
    secureInfo.value = secureSessionInfo();
    phase.value = 'authenticated';
    record('Restored existing session', 'ok', status.userID);
  } catch (error) {
    phase.value = 'guest';
    record('Session restore failed', 'error', describeError(error));
  }
}

export async function signIn(username: string, password: string): Promise<void> {
  formError.value = undefined;
  phase.value = 'signing-in';
  await withBusy(async () => {
    try {
      await authApi.login(username, password);
      await ensureSecureSession();
      userID.value = username;
      secureInfo.value = secureSessionInfo();
      phase.value = 'authenticated';
      record('Signed in', 'ok', username);
    } catch (error) {
      phase.value = 'guest';
      formError.value = describeError(error);
      record('Sign-in failed', 'error', describeError(error));
    }
  });
}

export async function signOut(): Promise<void> {
  await withBusy(async () => {
    try {
      await revokeSecureSession();
      await authApi.logout();
      record('Signed out', 'ok');
    } catch (error) {
      record('Sign-out request failed', 'error', describeError(error));
    } finally {
      userID.value = undefined;
      profile.value = undefined;
      lastEcho.value = undefined;
      secureInfo.value = undefined;
      phase.value = 'guest';
    }
  });
}

export async function loadProfile(): Promise<void> {
  await withBusy(async () => {
    try {
      profile.value = await secureApi.me();
      record('Loaded protected profile', 'ok', '/api/me');
    } catch (error) {
      record('Profile request failed', 'error', describeError(error));
    }
  });
}

export async function sendEcho(payload: unknown): Promise<void> {
  await withBusy(async () => {
    try {
      lastEcho.value = await secureApi.echo(payload);
      record('Sent encrypted echo', 'ok', '/api/echo');
    } catch (error) {
      record('Echo request failed', 'error', describeError(error));
    }
  });
}
