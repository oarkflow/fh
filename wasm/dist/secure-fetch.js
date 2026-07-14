import { clearDevice, installSecureStorageBridge } from "./storage.js";
const DEFAULT_WASM = "/wasm/securefetch.wasm";
const DEFAULT_WASM_EXEC = "/wasm/wasm_exec.js";
let runtimePromise;
let runtimeAssetConfig;
function validateAssetURL(raw, integrity, label) {
    const asset = new URL(raw, location.href);
    if (asset.protocol !== "https:" && asset.origin !== location.origin) {
        throw new Error(`${label} must use HTTPS outside same-origin loopback development`);
    }
    if (asset.origin !== location.origin && !integrity) {
        throw new Error(`${label} requires an integrity pin when loaded cross-origin`);
    }
    if (asset.username || asset.password || asset.hash) {
        throw new Error(`${label} URL must not contain credentials or a fragment`);
    }
    return asset;
}
function decodeBase64(value) {
    let raw;
    try {
        raw = atob(value);
    }
    catch (error) {
        throw new Error("FH secure asset integrity contains invalid base64", { cause: error });
    }
    const out = new Uint8Array(raw.length);
    for (let i = 0; i < raw.length; i += 1)
        out[i] = raw.charCodeAt(i);
    return out;
}
async function verifySHA256Integrity(bytes, integrity) {
    const candidates = integrity
        .trim()
        .split(/\s+/)
        .filter((token) => token.startsWith("sha256-"))
        .map((token) => decodeBase64(token.slice("sha256-".length)));
    if (candidates.length === 0) {
        throw new Error("FH secure asset integrity must contain a sha256 SRI token");
    }
    const actual = new Uint8Array(await crypto.subtle.digest("SHA-256", bytes));
    let matched = 0;
    for (const expected of candidates) {
        if (expected.byteLength !== actual.byteLength)
            continue;
        let difference = 0;
        for (let i = 0; i < actual.byteLength; i += 1)
            difference |= actual[i] ^ expected[i];
        matched |= Number(difference === 0);
    }
    actual.fill(0);
    for (const candidate of candidates)
        candidate.fill(0);
    if (matched === 0)
        throw new Error("FH secure asset integrity verification failed");
}
async function loadScript(url, integrity) {
    if (globalThis.Go) {
        if (globalThis.__fhGoRuntimeIntegrity === undefined || globalThis.__fhGoRuntimeIntegrity !== (integrity ?? "")) {
            throw new Error("A Go WASM runtime was already loaded outside the configured secure loader");
        }
        return;
    }
    if (typeof document === "undefined") {
        throw new Error("FH secure fetch currently requires a browser Window to load wasm_exec.js");
    }
    await new Promise((resolve, reject) => {
        const script = document.createElement("script");
        script.src = url;
        script.async = true;
        script.crossOrigin = "anonymous";
        script.referrerPolicy = "no-referrer";
        if (integrity)
            script.integrity = integrity;
        script.onload = () => resolve();
        script.onerror = () => reject(new Error(`Unable to load or verify Go WASM runtime: ${url}`));
        document.head.append(script);
    });
    if (!globalThis.Go)
        throw new Error("Go WASM runtime did not initialize");
    Object.defineProperty(globalThis, "__fhGoRuntimeIntegrity", {
        configurable: false,
        enumerable: false,
        writable: false,
        value: integrity ?? "",
    });
}
async function instantiate(url, imports, integrity) {
    const response = await globalThis.__fhNativeFetch(url, {
        credentials: "same-origin",
        cache: "no-store",
        redirect: "error",
        referrerPolicy: "no-referrer",
    });
    if (!response.ok)
        throw new Error(`Unable to load secure fetch WASM: HTTP ${response.status}`);
    const contentType = response.headers.get("content-type")?.split(";", 1)[0].trim().toLowerCase();
    if (contentType !== "application/wasm") {
        throw new Error("FH secure fetch WASM must be served as application/wasm");
    }
    if (integrity) {
        const bytes = await response.arrayBuffer();
        await verifySHA256Integrity(bytes, integrity);
        const result = await WebAssembly.instantiate(bytes, imports);
        return result.instance;
    }
    if (WebAssembly.instantiateStreaming) {
        try {
            const result = await WebAssembly.instantiateStreaming(response.clone(), imports);
            return result.instance;
        }
        catch {
            // Servers without application/wasm MIME support use the byte fallback.
        }
    }
    const bytes = await response.arrayBuffer();
    const result = await WebAssembly.instantiate(bytes, imports);
    return result.instance;
}
async function waitForAPI() {
    for (let i = 0; i < 500; i += 1) {
        if (globalThis.FHSecureWasm)
            return globalThis.FHSecureWasm;
        await new Promise((resolve) => setTimeout(resolve, 2));
    }
    throw new Error("FH secure WASM API did not become ready");
}
async function startRuntime(config) {
    if (!globalThis.crypto?.subtle || !globalThis.indexedDB || !globalThis.WebAssembly) {
        throw new Error("FH secure fetch requires WebCrypto, IndexedDB, and WebAssembly");
    }
    if (typeof globalThis.fetch !== "function")
        throw new Error("FH secure fetch requires native Fetch");
    if (!globalThis.isSecureContext && location.hostname !== "localhost" && location.hostname !== "127.0.0.1") {
        throw new Error("FH secure fetch requires a secure browser context");
    }
    if (config.requireAssetIntegrity && (!config.wasmIntegrity || !config.wasmExecIntegrity)) {
        throw new Error("FH secure fetch requires integrity pins for securefetch.wasm and wasm_exec.js");
    }
    const wasmURL = validateAssetURL(config.wasmURL ?? DEFAULT_WASM, config.wasmIntegrity, "FH secure WASM");
    const wasmExecURL = validateAssetURL(config.wasmExecURL ?? DEFAULT_WASM_EXEC, config.wasmExecIntegrity, "FH secure WASM runtime");
    if (globalThis.__fhNativeFetch !== undefined) {
        throw new Error("FH secure native Fetch reference was unexpectedly initialized before the secure loader");
    }
    Object.defineProperty(globalThis, "__fhNativeFetch", {
        configurable: false,
        enumerable: false,
        writable: false,
        value: globalThis.fetch.bind(globalThis),
    });
    installSecureStorageBridge();
    await loadScript(wasmExecURL.toString(), config.wasmExecIntegrity);
    const go = new globalThis.Go();
    const instance = await instantiate(wasmURL.toString(), go.importObject, config.wasmIntegrity);
    void go.run(instance).catch((error) => {
        console.error("FH secure WASM runtime terminated", error);
    });
    return waitForAPI();
}
async function runtime(config) {
    const assetConfig = JSON.stringify([
        new URL(config.wasmURL ?? DEFAULT_WASM, location.href).toString(),
        new URL(config.wasmExecURL ?? DEFAULT_WASM_EXEC, location.href).toString(),
        config.wasmIntegrity ?? "",
        config.wasmExecIntegrity ?? "",
        config.requireAssetIntegrity === true,
    ]);
    if (runtimeAssetConfig !== undefined && runtimeAssetConfig !== assetConfig) {
        throw new Error("FH secure WASM runtime is already initialized with different asset security settings");
    }
    runtimeAssetConfig = assetConfig;
    runtimePromise ??= startRuntime(config);
    return runtimePromise;
}
function responseHasNoBody(method, status) {
    return method === "HEAD" || status === 204 || status === 205 || status === 304 || (status >= 100 && status < 200);
}
function exactTarget(url) {
    return `${url.pathname}${url.search}`;
}
function isBodyInit(value) {
    if (typeof value === "string")
        return true;
    if (value instanceof ArrayBuffer)
        return true;
    if (ArrayBuffer.isView(value))
        return true;
    if (typeof Blob !== "undefined" && value instanceof Blob)
        return true;
    if (typeof FormData !== "undefined" && value instanceof FormData)
        return true;
    if (typeof URLSearchParams !== "undefined" && value instanceof URLSearchParams)
        return true;
    if (typeof ReadableStream !== "undefined" && value instanceof ReadableStream)
        return true;
    return false;
}
function normalizeInit(init) {
    if (!init || init.body === undefined || isBodyInit(init.body))
        return init;
    const headers = new Headers(init.headers);
    if (!headers.has("content-type"))
        headers.set("content-type", "application/json");
    return { ...init, headers, body: JSON.stringify(init.body) };
}
export async function createSecureFetch(config = {}) {
    const api = await runtime(config);
    await api.initialize(config);
    const secureFetch = async (input, init) => {
        const request = new Request(input, normalizeInit(init));
        const method = request.method.toUpperCase();
        const url = new URL(request.url, config.baseURL ?? location.href);
        const headers = Array.from(request.headers.entries());
        const body = method === "GET" || method === "HEAD" ? new Uint8Array() : new Uint8Array(await request.arrayBuffer());
        const credentials = init?.credentials ??
            (input instanceof Request ? input.credentials : config.credentials ?? request.credentials);
        const mode = init?.mode ?? (input instanceof Request ? input.mode : request.mode);
        if (mode !== "cors" && mode !== "same-origin") {
            body.fill(0);
            throw new Error("FH secure fetch only permits cors or same-origin request modes");
        }
        let result;
        try {
            result = await api.request({
                url: url.toString(),
                target: exactTarget(url),
                method,
                headers,
                body,
                credentials,
                mode,
                signal: request.signal,
            });
        }
        finally {
            body.fill(0);
        }
        const responseHeaders = new Headers(result.headers);
        responseHeaders.set("x-fh-request-id", result.requestId);
        const responseBody = responseHasNoBody(method, result.status) ? null : result.body;
        const response = new Response(responseBody, { status: result.status, headers: responseHeaders });
        try {
            Object.defineProperty(response, "url", { configurable: true, value: result.url });
        }
        catch {
            // Some browser implementations do not allow an own url property.
        }
        return response;
    };
    const handle = {
        fetch: secureFetch,
        sessionInfo: () => api.sessionInfo(),
        revokeSession: async () => {
            await api.revokeSession();
        },
        resetDevice: async () => {
            await api.revokeSession().catch(() => undefined);
            await clearDevice();
            location.reload();
        },
    };
    if (config.installGlobal) {
        globalThis.fetch = secureFetch;
    }
    return handle;
}
