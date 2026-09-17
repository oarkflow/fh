export interface StatTileProps {
  label: string;
  /** A plain value, or a getter for a value that changes over time. */
  value: unknown | (() => unknown);
  hint?: unknown;
}

export function StatTile(props: StatTileProps) {
  return (
    <div class="fh-stat">
      <p class="fh-stat__label">{props.label}</p>
      <p class="fh-stat__value">{props.value}</p>
      {props.hint ? <p class="fh-stat__hint">{props.hint}</p> : null}
    </div>
  );
}
