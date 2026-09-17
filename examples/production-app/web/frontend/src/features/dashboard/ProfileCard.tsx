import { Button } from '../../components/ui/Button.tsx';
import { Card } from '../../components/ui/Card.tsx';
import { CodeBlock } from '../../components/ui/CodeBlock.tsx';
import { StatTile } from '../../components/ui/StatTile.tsx';
import { busy, loadProfile, profile, userID } from '../../state/session.ts';

/** Calls the encrypted GET /api/me and renders the RBAC-bound principal it returns. */
export function ProfileCard() {
  return (
    <Card
      eyebrow="GET /api/me"
      title="Protected profile"
      actions={
        <Button variant="primary" busy={busy} onClick={() => void loadProfile()}>
          Load profile
        </Button>
      }
    >
      <div class="fh-stat-row">
        <StatTile label="Login principal" value={userID} />
        <StatTile label="Roles" value={() => profile.value?.roles?.join(', ') || '—'} />
      </div>
      <CodeBlock value={() => profile.value} empty="// select “Load profile” to call /api/me" />
    </Card>
  );
}
