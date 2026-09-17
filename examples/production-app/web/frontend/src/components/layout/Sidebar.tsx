import { Show } from '@oarkflow/lithe/dom';
import { activePanel, isAuthenticated, selectPanel, type Panel } from '../../state/session.ts';

const NAV_ITEMS: Array<{ id: Panel; label: string; icon: string; hint: string }> = [
  { id: 'overview', label: 'Overview', icon: '◈', hint: 'Profile & session' },
  { id: 'api', label: 'Secure API', icon: '⚙', hint: 'Encrypted console' },
  { id: 'activity', label: 'Activity', icon: '≡', hint: 'Recent events' },
];

/** The left rail: brand mark plus panel navigation once signed in. */
export function Sidebar() {
  return (
    <aside class="fh-sidebar">
      <div class="fh-brand">
        <span class="fh-brand__mark" aria-hidden="true">
          FH
        </span>
        <div>
          <p class="fh-brand__name">Control Center</p>
          <p class="fh-brand__tag">secure transport console</p>
        </div>
      </div>
      <Show when={() => isAuthenticated.value}>
        <nav class="fh-nav" aria-label="Panels">
          {NAV_ITEMS.map((item) => (
            <button
              type="button"
              class={() => `fh-nav__item${activePanel.value === item.id ? ' is-active' : ''}`}
              onClick={() => selectPanel(item.id)}
            >
              <span class="fh-nav__icon" aria-hidden="true">
                {item.icon}
              </span>
              <span class="fh-nav__text">
                <span class="fh-nav__label">{item.label}</span>
                <span class="fh-nav__hint">{item.hint}</span>
              </span>
            </button>
          ))}
        </nav>
      </Show>
      <p class="fh-sidebar__foot">fh secure WASM transport</p>
    </aside>
  );
}
