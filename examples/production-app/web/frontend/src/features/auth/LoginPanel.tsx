import { Show } from '@oarkflow/lithe/dom';
import { Alert } from '../../components/ui/Alert.tsx';
import { Button } from '../../components/ui/Button.tsx';
import { Card } from '../../components/ui/Card.tsx';
import { busy, formError, phase, signIn } from '../../state/session.ts';

function handleSubmit(event: SubmitEvent) {
  event.preventDefault();
  const form = event.currentTarget as HTMLFormElement;
  const data = new FormData(form);
  const username = String(data.get('username') ?? '');
  const password = String(data.get('password') ?? '');
  void signIn(username, password).then(() => {
    if (phase.value === 'authenticated') form.reset();
  });
}

/** The signed-out landing view: credentials go straight to /auth/login, then the encrypted transport bootstraps automatically. */
export function LoginPanel() {
  return (
    <div class="fh-auth-gate">
      <Card class="fh-auth-card" eyebrow="Authentication required" title="Sign in to continue">
        <p class="fh-auth-card__lede">
          Sessions are HttpOnly-cookie based; every <code>/api/*</code> call afterwards runs over the encrypted WASM
          transport, not plain fetch.
        </p>
        <form class="fh-form" onSubmit={handleSubmit}>
          <label class="fh-field">
            <span>Username</span>
            <input name="username" autocomplete="username" required autofocus />
          </label>
          <label class="fh-field">
            <span>Password</span>
            <input name="password" type="password" autocomplete="current-password" required />
          </label>
          <Show when={() => Boolean(formError.value)}>
            <Alert tone="error">{formError}</Alert>
          </Show>
          <Button type="submit" busy={() => busy.value && phase.value === 'signing-in'} class="fh-form__submit">
            Sign in
          </Button>
        </form>
      </Card>
    </div>
  );
}
