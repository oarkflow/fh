import { isBodyInit, responseHasNoBody } from "./body.js";
import { defaultValidateStatus, FHError } from "./errors.js";
function createInterceptorManager() {
    const entries = new Map();
    let nextId = 0;
    return {
        entries,
        manager: {
            use(onFulfilled, onRejected) {
                const id = nextId++;
                entries.set(id, { onFulfilled, onRejected });
                return id;
            },
            eject(id) {
                entries.delete(id);
            },
        },
    };
}
function combineURLs(baseURL, url) {
    return url.match(/^([a-z][a-z\d+\-.]*:)?\/\//i)
        ? url
        : `${baseURL.replace(/\/+$/, "")}/${url.replace(/^\/+/, "")}`;
}
function defaultParamsSerializer(params) {
    const search = new URLSearchParams();
    for (const [key, value] of Object.entries(params)) {
        if (value === null || value === undefined)
            continue;
        if (Array.isArray(value)) {
            for (const item of value)
                search.append(key, serializeParamValue(item));
            continue;
        }
        search.append(key, serializeParamValue(value));
    }
    return search.toString();
}
function serializeParamValue(value) {
    if (value instanceof Date)
        return value.toISOString();
    if (typeof value === "object" && value !== null)
        return JSON.stringify(value);
    return String(value);
}
function buildURL(baseURL, url, params, serializer = defaultParamsSerializer) {
    let full = baseURL ? combineURLs(baseURL, url) : url;
    if (params !== undefined) {
        const query = params instanceof URLSearchParams ? params.toString() : serializer(params);
        if (query)
            full += (full.includes("?") ? "&" : "?") + query;
    }
    return full;
}
function mergeHeaders(base, override) {
    const headers = new Headers(base);
    if (override) {
        for (const [key, value] of new Headers(override).entries())
            headers.set(key, value);
    }
    return headers;
}
function paramsToRecord(params) {
    if (!(params instanceof URLSearchParams))
        return params;
    const record = {};
    for (const key of new Set(params.keys())) {
        const values = params.getAll(key);
        record[key] = values.length > 1 ? values : values[0];
    }
    return record;
}
function mergeParams(base, override) {
    if (!base)
        return override;
    if (!override)
        return base;
    return { ...paramsToRecord(base), ...paramsToRecord(override) };
}
function mergeConfig(defaults, config) {
    return {
        ...defaults,
        ...config,
        headers: mergeHeaders(defaults.headers, config.headers),
        params: mergeParams(defaults.params, config.params),
    };
}
async function defaultTransformRequest(data, headers) {
    if (data === undefined || isBodyInit(data))
        return data;
    if (!headers.has("content-type"))
        headers.set("content-type", "application/json");
    return JSON.stringify(data);
}
async function defaultTransformResponse(data, _headers, config, _status) {
    const response = data;
    if (config.responseType === "text")
        return response.text();
    if (config.responseType === "blob")
        return response.blob();
    if (config.responseType === "arraybuffer")
        return response.arrayBuffer();
    if (config.responseType === "formdata")
        return response.formData();
    const contentType = response.headers.get("content-type") ?? "";
    if (config.responseType === "json" || contentType.includes("json")) {
        const text = await response.text();
        if (!text)
            return null;
        try {
            return JSON.parse(text);
        }
        catch {
            return text;
        }
    }
    return response.text();
}
async function runTransformers(transformers, fallback, args) {
    if (!transformers || transformers.length === 0) {
        return fallback(...args);
    }
    let value = args[0];
    for (const transformer of transformers) {
        value = await transformer(value, ...args.slice(1));
    }
    return value;
}
const TIMEOUT_REASON = Symbol("fh-timeout");
function combineSignals(...signals) {
    const present = signals.filter((s) => s !== undefined);
    if (present.length === 0) {
        const controller = new AbortController();
        return { signal: controller.signal, cleanup: () => { } };
    }
    if (present.length === 1 && present[0]) {
        return { signal: present[0], cleanup: () => { } };
    }
    const controller = new AbortController();
    const abort = (signal) => controller.abort(signal.reason);
    const listeners = present.map((signal) => {
        if (signal.aborted)
            controller.abort(signal.reason);
        const listener = () => abort(signal);
        signal.addEventListener("abort", listener);
        return { signal, listener };
    });
    return {
        signal: controller.signal,
        cleanup: () => {
            for (const { signal, listener } of listeners)
                signal.removeEventListener("abort", listener);
        },
    };
}
function defaultRetryDelay(attempt) {
    return Math.min(30000, 250 * 2 ** (attempt - 1)) + Math.random() * 100;
}
const IDEMPOTENT_METHODS = new Set(["GET", "HEAD", "OPTIONS", "PUT", "DELETE"]);
function defaultRetryCondition(error) {
    const method = (error.config.method ?? "GET").toUpperCase();
    if (!IDEMPOTENT_METHODS.has(method))
        return false;
    if (error.code === "ERR_NETWORK")
        return true;
    if (error.response && error.response.status >= 500)
        return true;
    return false;
}
function normalizeRetryConfig(retries) {
    const normalized = typeof retries === "number" ? { retries } : (retries ?? {});
    return {
        retries: normalized.retries ?? 0,
        retryDelay: normalized.retryDelay ?? defaultRetryDelay,
        retryCondition: normalized.retryCondition ?? defaultRetryCondition,
    };
}
export function createClient(fetchImpl, config = {}) {
    const defaults = { method: "GET", validateStatus: defaultValidateStatus, ...config };
    const requestInterceptors = createInterceptorManager();
    const responseInterceptors = createInterceptorManager();
    async function dispatch(rawConfig) {
        let cfg = mergeConfig(defaults, rawConfig);
        for (const { onFulfilled, onRejected } of requestInterceptors.entries.values()) {
            try {
                if (onFulfilled)
                    cfg = (await onFulfilled(cfg));
            }
            catch (error) {
                if (onRejected)
                    return (await onRejected(error));
                throw error;
            }
        }
        const method = (cfg.method ?? "GET").toUpperCase();
        const url = buildURL(cfg.baseURL, cfg.url ?? "", cfg.params, cfg.paramsSerializer);
        const headers = new Headers(cfg.headers);
        const credentials = cfg.credentials ?? (cfg.withCredentials ? "include" : undefined);
        const transformRequest = Array.isArray(cfg.transformRequest)
            ? cfg.transformRequest
            : cfg.transformRequest
                ? [cfg.transformRequest]
                : undefined;
        const body = (await runTransformers(transformRequest, defaultTransformRequest, [
            cfg.data,
            headers,
            cfg,
        ]));
        const { retries, retryDelay, retryCondition } = normalizeRetryConfig(cfg.retries);
        const totalBytes = typeof body === "string"
            ? new TextEncoder().encode(body).byteLength
            : body instanceof Blob
                ? body.size
                : body instanceof ArrayBuffer
                    ? body.byteLength
                    : ArrayBuffer.isView(body)
                        ? body.byteLength
                        : undefined;
        cfg.onUploadProgress?.({ loaded: 0, total: totalBytes, lengthComputable: totalBytes !== undefined, upload: true, synthetic: true });
        let attempt = 0;
        for (;;) {
            attempt += 1;
            const timeoutController = cfg.timeout ? new AbortController() : undefined;
            const timeoutHandle = timeoutController
                ? setTimeout(() => timeoutController.abort(TIMEOUT_REASON), cfg.timeout)
                : undefined;
            const { signal, cleanup } = combineSignals(cfg.signal, timeoutController?.signal);
            const request = new Request(url, { method, headers, body, credentials, signal });
            try {
                const response = await fetchImpl(url, { method, headers, body, credentials, signal });
                cfg.onUploadProgress?.({ loaded: totalBytes ?? 0, total: totalBytes, lengthComputable: totalBytes !== undefined, upload: true, synthetic: true });
                cfg.onDownloadProgress?.({ loaded: 0, total: undefined, lengthComputable: false, upload: false, synthetic: true });
                const attemptResult = await buildAttemptResult(response, request, cfg);
                cfg.onDownloadProgress?.({
                    loaded: Number(response.headers.get("content-length") ?? 0),
                    total: response.headers.get("content-length") ? Number(response.headers.get("content-length")) : undefined,
                    lengthComputable: response.headers.has("content-length"),
                    upload: false,
                    synthetic: true,
                });
                if (attemptResult.error && attempt <= retries && retryCondition(attemptResult.error, attempt)) {
                    await new Promise((resolve) => setTimeout(resolve, retryDelay(attempt, attemptResult.error)));
                    continue;
                }
                return finalizeResponse(attemptResult.response, attemptResult.error, responseInterceptors.entries);
            }
            catch (error) {
                const aborted = signal.aborted;
                const code = !aborted
                    ? "ERR_NETWORK"
                    : signal.reason === TIMEOUT_REASON
                        ? "ECONNABORTED"
                        : "ERR_CANCELED";
                const fhError = new FHError(aborted ? (code === "ECONNABORTED" ? "Request timed out" : "Request canceled") : "Network error", code, cfg, request);
                if (code !== "ERR_CANCELED" && attempt <= retries && retryCondition(fhError, attempt)) {
                    await new Promise((resolve) => setTimeout(resolve, retryDelay(attempt, fhError)));
                    continue;
                }
                throw fhError;
            }
            finally {
                if (timeoutHandle !== undefined)
                    clearTimeout(timeoutHandle);
                cleanup();
            }
        }
    }
    return {
        defaults,
        interceptors: { request: requestInterceptors.manager, response: responseInterceptors.manager },
        request: (config) => dispatch(config),
        get: (url, config) => dispatch({ ...config, url, method: "GET" }),
        delete: (url, config) => dispatch({ ...config, url, method: "DELETE" }),
        head: (url, config) => dispatch({ ...config, url, method: "HEAD" }),
        options: (url, config) => dispatch({ ...config, url, method: "OPTIONS" }),
        post: (url, data, config) => dispatch({ ...config, url, method: "POST", data }),
        put: (url, data, config) => dispatch({ ...config, url, method: "PUT", data }),
        patch: (url, data, config) => dispatch({ ...config, url, method: "PATCH", data }),
        getUri: (config) => {
            const cfg = config ? mergeConfig(defaults, config) : defaults;
            return buildURL(cfg.baseURL, cfg.url ?? "", cfg.params, cfg.paramsSerializer);
        },
        create: (childConfig) => createClient(fetchImpl, mergeConfig(defaults, childConfig ?? {})),
    };
}
async function buildAttemptResult(response, request, cfg) {
    const method = (cfg.method ?? "GET").toUpperCase();
    let data = null;
    if (!responseHasNoBody(method, response.status)) {
        const transformResponse = Array.isArray(cfg.transformResponse)
            ? cfg.transformResponse
            : cfg.transformResponse
                ? [cfg.transformResponse]
                : undefined;
        data = await runTransformers(transformResponse, defaultTransformResponse, [
            response,
            response.headers,
            cfg,
            response.status,
        ]);
    }
    const fhResponse = buildResponse(data, response, request, cfg);
    const validateStatus = cfg.validateStatus ?? defaultValidateStatus;
    const error = !validateStatus(response.status)
        ? new FHError(`Request failed with status ${response.status}`, response.status >= 500 ? "ERR_BAD_RESPONSE" : "ERR_BAD_REQUEST", cfg, request, fhResponse)
        : undefined;
    return { response: fhResponse, error };
}
function buildResponse(data, response, request, config) {
    return {
        data,
        status: response.status,
        statusText: response.statusText,
        headers: Object.fromEntries(response.headers.entries()),
        config,
        request,
        raw: response,
    };
}
async function finalizeResponse(response, error, responseInterceptors) {
    if (!error) {
        let result = response;
        for (const { onFulfilled } of responseInterceptors.values()) {
            if (onFulfilled)
                result = await onFulfilled(result);
        }
        return result;
    }
    for (const { onRejected } of responseInterceptors.values()) {
        if (onRejected) {
            try {
                return (await onRejected(error));
            }
            catch (nextError) {
                if (nextError !== error)
                    throw nextError;
            }
        }
    }
    throw error;
}
export async function createSecureClient(handleOrConfig, clientConfig = {}) {
    const handle = "fetch" in handleOrConfig
        ? handleOrConfig
        : await (await import("./secure-fetch.js")).createSecureFetch(handleOrConfig);
    return createClient(handle.fetch, clientConfig);
}
export function all(values) {
    return Promise.all(values);
}
