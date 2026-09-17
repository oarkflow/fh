import type { WasmBridge } from './types.ts';

// The secure-fetch runtime (web/wasm/index.js, secure-fetch.js,
// wasm_exec.js, securefetch.wasm) is a hand-authored, unbundled set of
// browser-native ES modules served as-is by the Go server. It is
// deliberately outside this project's compiled module graph: it carries
// its own subresource-integrity check against web/wasm/asset-manifest.json,
// and the server refuses to start if that manifest is incomplete, so it
// must stay a plain runtime asset, not something this build inlines.
//
// A plain string-literal `import('/wasm/index.js')` here would be rewritten
// by Lithe's single-file production bundler into a lookup against its own
// internal module registry, which has no entry for a path outside `src/`,
// and would fail silently at runtime. A template-literal specifier is not
// string-literal syntax to that rewriter, so it passes through untouched
// as a genuine native dynamic import, resolved by the browser against the
// current origin exactly like the previous hand-written app.js did.
let bridge: Promise<WasmBridge> | undefined;

export function loadWasmBridge(): Promise<WasmBridge> {
  if (!bridge) {
    bridge = import(`/wasm/index.js`) as Promise<WasmBridge>;
  }
  return bridge;
}
