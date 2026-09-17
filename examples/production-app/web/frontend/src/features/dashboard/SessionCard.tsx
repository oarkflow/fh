import { Badge } from '../../components/ui/Badge.tsx';
import { Card } from '../../components/ui/Card.tsx';
import { CodeBlock } from '../../components/ui/CodeBlock.tsx';
import { secureInfo } from '../../state/session.ts';

/** Read-only view of the live encrypted transport session (device key, sequence, expiry). */
export function SessionCard() {
  return (
    <Card
      eyebrow="Secure WASM transport"
      title="Session"
      actions={
        <Badge tone="accent" dot>
          AES-256-GCM
        </Badge>
      }
    >
      <p class="fh-muted">
        Established once after login via <code>createSecureFetch</code>; every field below comes straight from the
        active client, not a server round-trip.
      </p>
      <CodeBlock value={() => secureInfo.value} empty="// no secure session yet" />
    </Card>
  );
}
