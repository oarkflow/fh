import { Show } from '@oarkflow/lithe/dom';
import { resolve, type MaybeReactive } from '../../lib/reactive.ts';
import { Spinner } from './Spinner.tsx';

export type ButtonVariant = 'primary' | 'ghost' | 'danger' | 'subtle';

export interface ButtonProps {
  children?: unknown;
  variant?: ButtonVariant;
  type?: 'button' | 'submit';
  busy?: MaybeReactive<boolean>;
  disabled?: MaybeReactive<boolean>;
  onClick?: (event: MouseEvent) => void;
  class?: string;
}

/** The one button implementation the whole app shares. `busy`/`disabled` accept a signal, a getter, or a plain boolean. */
export function Button(props: ButtonProps) {
  const variant = props.variant ?? 'primary';
  return (
    <button
      type={props.type ?? 'button'}
      class={`fh-btn fh-btn--${variant}${props.class ? ` ${props.class}` : ''}`}
      disabled={() => resolve(props.disabled) || resolve(props.busy)}
      onClick={props.onClick}
    >
      <Show when={() => resolve(props.busy)}>
        <Spinner size="sm" />
      </Show>
      <span>{props.children}</span>
    </button>
  );
}
