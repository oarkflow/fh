export type BadgeTone = 'neutral' | 'ok' | 'warn' | 'error' | 'accent';

export interface BadgeProps {
  tone?: BadgeTone;
  children?: unknown;
  dot?: boolean;
}

export function Badge(props: BadgeProps) {
  const tone = props.tone ?? 'neutral';
  return (
    <span class={`fh-badge fh-badge--${tone}`}>
      {props.dot ? <span class="fh-badge__dot" aria-hidden="true" /> : null}
      {props.children}
    </span>
  );
}
