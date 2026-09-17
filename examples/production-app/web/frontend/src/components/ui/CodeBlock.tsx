export interface CodeBlockProps {
  /** A getter so the block stays live as the underlying signal changes. */
  value: () => unknown;
  empty?: string;
}

/** Pretty-printed JSON viewer - the structured, reusable replacement for a raw <pre>. */
export function CodeBlock(props: CodeBlockProps) {
  const text = () => {
    const current = props.value();
    if (current === undefined) return props.empty ?? '// nothing yet';
    try {
      return JSON.stringify(current, null, 2);
    } catch {
      return String(current);
    }
  };
  return (
    <pre class="fh-code" aria-live="polite">
      <code>{text}</code>
    </pre>
  );
}
