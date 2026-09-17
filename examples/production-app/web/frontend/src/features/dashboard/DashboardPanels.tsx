import { Show } from '@oarkflow/lithe/dom';
import { activePanel } from '../../state/session.ts';
import { ActivityLog } from './ActivityLog.tsx';
import { ApiConsole } from './ApiConsole.tsx';
import { ProfileCard } from './ProfileCard.tsx';
import { SessionCard } from './SessionCard.tsx';

/** Swaps between the three authenticated panels driven by the sidebar. Add a panel by adding one Show block here. */
export function DashboardPanels() {
  return (
    <div class="fh-panels">
      <Show when={() => activePanel.value === 'overview'}>
        <div class="fh-grid">
          <ProfileCard />
          <SessionCard />
        </div>
      </Show>
      <Show when={() => activePanel.value === 'api'}>
        <ApiConsole />
      </Show>
      <Show when={() => activePanel.value === 'activity'}>
        <ActivityLog />
      </Show>
    </div>
  );
}
