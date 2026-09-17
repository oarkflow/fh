export type AlertTone = 'error' | 'info' | 'ok';

export interface AlertProps {
  tone?: AlertTone;
  children?: unknown;
}

export function Alert(props: AlertProps) {
  const tone = props.tone ?? 'info';
  return (
    <p class={`fh-alert fh-alert--${tone}`} role={tone === 'error' ? 'alert' : 'status'}>
      {props.children}
    </p>
  );
}
