export interface CardProps {
  title?: unknown;
  eyebrow?: unknown;
  actions?: unknown;
  children?: unknown;
  class?: string;
}

/** The base surface every panel in the dashboard is built from. */
export function Card(props: CardProps) {
  return (
    <section class={`fh-card${props.class ? ` ${props.class}` : ''}`}>
      {props.title || props.eyebrow || props.actions ? (
        <header class="fh-card__head">
          <div>
            {props.eyebrow ? <p class="fh-card__eyebrow">{props.eyebrow}</p> : null}
            {props.title ? <h2 class="fh-card__title">{props.title}</h2> : null}
          </div>
          {props.actions ? <div class="fh-card__actions">{props.actions}</div> : null}
        </header>
      ) : null}
      <div class="fh-card__body">{props.children}</div>
    </section>
  );
}
