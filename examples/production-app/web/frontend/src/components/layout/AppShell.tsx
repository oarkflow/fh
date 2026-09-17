import { Sidebar } from './Sidebar.tsx';
import { Topbar } from './Topbar.tsx';

export interface AppShellProps {
  children?: unknown;
}

/** The page frame: a fixed sidebar rail plus a scrolling topbar+content column. */
export function AppShell(props: AppShellProps) {
  return (
    <div class="fh-shell">
      <div class="fh-shell__backdrop" aria-hidden="true" />
      <Sidebar />
      <div class="fh-shell__main">
        <Topbar />
        <main class="fh-shell__content">{props.children}</main>
      </div>
    </div>
  );
}
