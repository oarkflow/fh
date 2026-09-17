import { Show } from '@oarkflow/lithe/dom';
import { Badge } from '../ui/Badge.tsx';
import { Button } from '../ui/Button.tsx';
import { ThemeToggle } from '../ui/ThemeToggle.tsx';
import { activePanel, busy, isAuthenticated, signOut, userID, type Panel } from '../../state/session.ts';

const PANEL_TITLE: Record<Panel, string> = {
  overview: 'Overview',
  api: 'Secure API console',
  activity: 'Activity',
};

export function Topbar() {
  return (
    <header class="fh-topbar">
      <div>
        <p class="fh-topbar__eyebrow">FH application</p>
        <h1 class="fh-topbar__title">{() => (isAuthenticated.value ? PANEL_TITLE[activePanel.value] : 'Welcome')}</h1>
      </div>
      <div class="fh-topbar__end">
        <Show
          when={() => isAuthenticated.value}
          fallback={<Badge tone="neutral" dot>Guest</Badge>}
        >
          <Badge tone="ok" dot>
            Signed in as {userID}
          </Badge>
        </Show>
        <ThemeToggle />
        <Show when={() => isAuthenticated.value}>
          <Button variant="ghost" busy={busy} onClick={() => void signOut()}>
            Sign out
          </Button>
        </Show>
      </div>
    </header>
  );
}
