import { effect, onMount, signal } from '@oarkflow/lithe/core';

export type Theme = 'light' | 'dark';

const STORAGE_KEY = 'fh-control-center:theme';

const theme = signal<Theme>('dark', { name: 'theme' });

function preferredTheme(): Theme {
  try {
    const stored = window.localStorage.getItem(STORAGE_KEY);
    if (stored === 'light' || stored === 'dark') return stored;
  } catch {
    // Private-mode/blocked storage: fall back to the media query below.
  }
  return window.matchMedia?.('(prefers-color-scheme: light)').matches ? 'light' : 'dark';
}

/** Applies the theme to <html data-theme> and persists it. Call once, near the app root. */
export function ThemeEffect() {
  onMount(() => {
    theme.value = preferredTheme();
  });
  effect(() => {
    document.documentElement.dataset.theme = theme.value;
    try {
      window.localStorage.setItem(STORAGE_KEY, theme.value);
    } catch {
      // Ignored: theme still applies for this session.
    }
  });
  return null;
}

function toggle() {
  theme.value = theme.value === 'dark' ? 'light' : 'dark';
}

export function ThemeToggle() {
  return (
    <button
      type="button"
      class="fh-theme-toggle"
      onClick={toggle}
      aria-label="Toggle color theme"
      title="Toggle color theme"
    >
      <span aria-hidden="true">{() => (theme.value === 'dark' ? '☀' : '☽')}</span>
    </button>
  );
}
