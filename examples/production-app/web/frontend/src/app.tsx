import { onMount } from '@oarkflow/lithe/core';
import { Show } from '@oarkflow/lithe/dom';
import { AppShell } from './components/layout/AppShell.tsx';
import { Spinner } from './components/ui/Spinner.tsx';
import { ThemeEffect } from './components/ui/ThemeToggle.tsx';
import { LoginPanel } from './features/auth/LoginPanel.tsx';
import { DashboardPanels } from './features/dashboard/DashboardPanels.tsx';
import { isAuthenticated, isChecking, restoreSession } from './state/session.ts';

/** Composition root: one shell, three phases (checking / signed out / signed in). */
export function App() {
  onMount(() => {
    void restoreSession();
  });

  return (
    <>
      <ThemeEffect />
      <AppShell>
        <Show when={() => isChecking.value}>
          <div class="fh-splash">
            <Spinner />
            <p>Checking for an existing session…</p>
          </div>
        </Show>
        <Show when={() => !isChecking.value && !isAuthenticated.value}>
          <LoginPanel />
        </Show>
        <Show when={() => isAuthenticated.value}>
          <DashboardPanels />
        </Show>
      </AppShell>
    </>
  );
}
