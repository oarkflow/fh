// Guards the architecture documented in lib/secure-client.ts: every
// protected `/api/*` call must go through the WASM secure-fetch client, and
// `lib/secure-client.ts` is the only module allowed to touch the network
// primitives that could bypass it (the raw `fetch()` global and
// `requestJSON`/`http.ts`, which is same-origin plain fetch used only for
// the pre-auth `/auth/*` and `/secure-config.json` bootstrap endpoints).
//
// Run via `npm run check` (wired into `make check` too), so a component
// that starts calling `fetch(...)` or imports `http.ts` directly - instead
// of going through `secureApi`/`authApi` - fails the build instead of
// silently shipping a request that skips the encrypted transport.
import { readdirSync, readFileSync, statSync } from 'node:fs';
import { join, relative } from 'node:path';

const SRC_ROOT = join(import.meta.dirname, '..', 'src');

// Files allowed to reference the raw primitives, relative to src/.
// lib/types.ts is declarations only (e.g. the SecureFetchClient interface's
// `fetch(...)` method signature) - it never executes a call itself.
const ALLOWED = new Set(['lib/secure-client.ts', 'lib/http.ts', 'lib/types.ts']);

const RAW_FETCH = /(?<![.\w])fetch\s*\(/;
const HTTP_IMPORT = /from\s+['"][^'"]*\/http\.ts['"]/;
const REQUEST_JSON_IMPORT = /\brequestJSON\b/;

function walk(dir: string, out: string[] = []): string[] {
  for (const entry of readdirSync(dir)) {
    const full = join(dir, entry);
    const info = statSync(full);
    if (info.isDirectory()) walk(full, out);
    else if (/\.(ts|tsx)$/.test(entry)) out.push(full);
  }
  return out;
}

const violations: string[] = [];

for (const file of walk(SRC_ROOT)) {
  const rel = relative(SRC_ROOT, file).replaceAll('\\', '/');
  if (ALLOWED.has(rel)) continue;

  const lines = readFileSync(file, 'utf8').split('\n');
  lines.forEach((line, i) => {
    const lineNo = i + 1;
    if (RAW_FETCH.test(line)) {
      violations.push(`${rel}:${lineNo}: raw fetch() call outside lib/secure-client.ts - protected /api/* requests must go through secureApi (WASM secure-fetch)`);
    }
    if (HTTP_IMPORT.test(line) || REQUEST_JSON_IMPORT.test(line)) {
      violations.push(`${rel}:${lineNo}: imports the plain-fetch helper (requestJSON/http.ts) outside lib/secure-client.ts - use authApi/secureApi from lib/secure-client.ts instead`);
    }
  });
}

if (violations.length) {
  console.error('secure-fetch check failed:\n');
  for (const v of violations) console.error(`  ${v}`);
  console.error('\nEvery protected /api/* call must go through secureApi in lib/secure-client.ts, which routes through the WASM secure-fetch client.');
  process.exit(1);
}

console.log(`secure-fetch check passed (${walk(SRC_ROOT).length} files scanned, no bypass of lib/secure-client.ts found)`);
