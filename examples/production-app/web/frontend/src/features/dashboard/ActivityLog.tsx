import { For } from '@oarkflow/lithe/dom';
import { Badge, type BadgeTone } from '../../components/ui/Badge.tsx';
import { Card } from '../../components/ui/Card.tsx';
import { activity, type ActivityEntry } from '../../state/session.ts';

const TONE: Record<ActivityEntry['status'], BadgeTone> = {
  ok: 'ok',
  error: 'error',
  pending: 'warn',
};

function timeLabel(at: number): string {
  return new Date(at).toLocaleTimeString(undefined, { hour12: false });
}

/** A running, client-side log of what this session has done - login, profile loads, echo calls, sign-out. */
export function ActivityLog() {
  return (
    <Card eyebrow="This browser session" title="Activity">
      <ul class="fh-activity">
        <For each={() => activity.value} key={(entry) => entry.id} fallback={<li class="fh-muted">Nothing yet.</li>}>
          {(entry: ActivityEntry) => (
            <li class="fh-activity__row">
              <Badge tone={TONE[entry.status]} dot />
              <div class="fh-activity__body">
                <p class="fh-activity__label">{entry.label}</p>
                {entry.detail ? <p class="fh-activity__detail">{entry.detail}</p> : null}
              </div>
              <time class="fh-activity__time">{timeLabel(entry.at)}</time>
            </li>
          )}
        </For>
      </ul>
    </Card>
  );
}
