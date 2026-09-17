import { signal } from '@oarkflow/lithe/core';
import { Show } from '@oarkflow/lithe/dom';
import { Alert } from '../../components/ui/Alert.tsx';
import { Button } from '../../components/ui/Button.tsx';
import { Card } from '../../components/ui/Card.tsx';
import { CodeBlock } from '../../components/ui/CodeBlock.tsx';
import { busy, lastEcho, sendEcho } from '../../state/session.ts';

const SAMPLE = '{\n  "message": "hello from the console"\n}';

const draft = signal(SAMPLE, { name: 'echoDraft' });
const parseError = signal<string | undefined>(undefined, { name: 'echoParseError' });

function submit(event: SubmitEvent) {
  event.preventDefault();
  parseError.value = undefined;
  try {
    const payload = JSON.parse(draft.value);
    void sendEcho(payload);
  } catch {
    parseError.value = 'That is not valid JSON.';
  }
}

/**
 * A small, reusable pattern for extending this app: any new encrypted
 * endpoint only needs a function in lib/secure-client.ts, an action in
 * state/session.ts, and a panel like this one.
 */
export function ApiConsole() {
  return (
    <Card eyebrow="POST /api/echo" title="Secure API console">
      <p class="fh-muted">Edit the JSON body below and send it through the encrypted transport.</p>
      <form class="fh-form fh-form--console" onSubmit={submit}>
        <textarea
          class="fh-textarea"
          rows={6}
          spellcheck={false}
          value={draft.value}
          onInput={(event: InputEvent) => {
            draft.value = (event.currentTarget as HTMLTextAreaElement).value;
          }}
        />
        <Show when={() => Boolean(parseError.value)}>
          <Alert tone="error">{parseError}</Alert>
        </Show>
        <Button type="submit" busy={busy} class="fh-form__submit">
          Send encrypted request
        </Button>
      </form>
      <CodeBlock value={() => lastEcho.value} empty="// the server's decrypted echo will appear here" />
    </Card>
  );
}
