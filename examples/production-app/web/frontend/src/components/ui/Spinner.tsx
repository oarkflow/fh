export interface SpinnerProps {
  size?: 'sm' | 'md';
}

export function Spinner(props: SpinnerProps) {
  return <span class={`fh-spinner fh-spinner--${props.size ?? 'md'}`} aria-hidden="true" />;
}
