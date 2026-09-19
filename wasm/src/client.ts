import { isBodyInit, responseHasNoBody } from "./body.js";
import { defaultValidateStatus, FHError, type FHErrorCode } from "./errors.js";
import type { SecureFetchConfig, SecureFetchHandle } from "./secure-fetch.js";

export type FetchLike = (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>;

export type HttpMethod = "GET" | "POST" | "PUT" | "PATCH" | "DELETE" | "HEAD" | "OPTIONS";

export type RequestTransformer = (
  data: unknown,
  headers: Headers,
  config: FHRequestConfig,
) => unknown | Promise<unknown>;

export type ResponseTransformer = (
  data: unknown,
  headers: Headers,
  config: FHRequestConfig,
  status: number,
) => unknown | Promise<unknown>;

export interface FHProgressEvent {
  loaded: number;
  total?: number;
  lengthComputable: boolean;
  upload: boolean;
  /** Always true today: the underlying transport buffers whole bodies, so this is a start/end signal, not real streaming progress. */
  synthetic: true;
}

export interface RetryConfig {
  retries?: number;
  retryDelay?: (attempt: number, error: FHError) => number;
  retryCondition?: (error: FHError, attempt: number) => boolean;
}

export interface FHRequestConfig<TData = unknown> {
  url?: string;
  method?: HttpMethod;
  baseURL?: string;
  headers?: HeadersInit;
  params?: Record<string, unknown> | URLSearchParams;
  paramsSerializer?: (params: Record<string, unknown>) => string;
  data?: TData;
  timeout?: number;
  signal?: AbortSignal;
  withCredentials?: boolean;
  credentials?: RequestCredentials;
  validateStatus?: (status: number) => boolean;
  transformRequest?: RequestTransformer | RequestTransformer[];
  transformResponse?: ResponseTransformer | ResponseTransformer[];
  retries?: number | RetryConfig;
  onUploadProgress?: (event: FHProgressEvent) => void;
  onDownloadProgress?: (event: FHProgressEvent) => void;
  responseType?: "json" | "text" | "blob" | "arraybuffer" | "formdata";
  meta?: Record<string, unknown>;
}

export interface FHResponse<T = unknown, TData = unknown> {
  data: T;
  status: number;
  statusText: string;
  headers: Record<string, string>;
  config: FHRequestConfig<TData>;
  request: Request;
  raw: Response;
}

interface InterceptorEntry<V> {
  onFulfilled?: (value: V) => V | Promise<V>;
  onRejected?: (error: unknown) => unknown;
}

export interface InterceptorManager<V> {
  use(onFulfilled?: (value: V) => V | Promise<V>, onRejected?: (error: unknown) => unknown): number;
  eject(id: number): void;
}

function createInterceptorManager<V>(): { manager: InterceptorManager<V>; entries: Map<number, InterceptorEntry<V>> } {
  const entries = new Map<number, InterceptorEntry<V>>();
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

function combineURLs(baseURL: string, url: string): string {
  return url.match(/^([a-z][a-z\d+\-.]*:)?\/\//i)
    ? url
    : `${baseURL.replace(/\/+$/, "")}/${url.replace(/^\/+/, "")}`;
}

function defaultParamsSerializer(params: Record<string, unknown>): string {
  const search = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (value === null || value === undefined) continue;
    if (Array.isArray(value)) {
      for (const item of value) search.append(key, serializeParamValue(item));
      continue;
    }
    search.append(key, serializeParamValue(value));
  }
  return search.toString();
}

function serializeParamValue(value: unknown): string {
  if (value instanceof Date) return value.toISOString();
  if (typeof value === "object" && value !== null) return JSON.stringify(value);
  return String(value);
}

function buildURL(
  baseURL: string | undefined,
  url: string,
  params: FHRequestConfig["params"],
  serializer: (params: Record<string, unknown>) => string = defaultParamsSerializer,
): string {
  let full = baseURL ? combineURLs(baseURL, url) : url;
  if (params !== undefined) {
    const query =
      params instanceof URLSearchParams ? params.toString() : serializer(params as Record<string, unknown>);
    if (query) full += (full.includes("?") ? "&" : "?") + query;
  }
  return full;
}

function mergeHeaders(base: HeadersInit | undefined, override: HeadersInit | undefined): Headers {
  const headers = new Headers(base);
  if (override) {
    for (const [key, value] of new Headers(override).entries()) headers.set(key, value);
  }
  return headers;
}

function paramsToRecord(params: URLSearchParams | Record<string, unknown>): Record<string, unknown> {
  if (!(params instanceof URLSearchParams)) return params;
  const record: Record<string, unknown> = {};
  for (const key of new Set(params.keys())) {
    const values = params.getAll(key);
    record[key] = values.length > 1 ? values : values[0];
  }
  return record;
}

function mergeParams(
  base: FHRequestConfig["params"],
  override: FHRequestConfig["params"],
): FHRequestConfig["params"] {
  if (!base) return override;
  if (!override) return base;
  return { ...paramsToRecord(base), ...paramsToRecord(override) };
}

function mergeConfig<D>(defaults: FHRequestConfig, config: FHRequestConfig<D>): FHRequestConfig<D> {
  return {
    ...defaults,
    ...config,
    headers: mergeHeaders(defaults.headers, config.headers),
    params: mergeParams(defaults.params, config.params),
  } as FHRequestConfig<D>;
}

async function defaultTransformRequest(data: unknown, headers: Headers): Promise<unknown> {
  if (data === undefined || isBodyInit(data)) return data;
  if (!headers.has("content-type")) headers.set("content-type", "application/json");
  return JSON.stringify(data);
}

async function defaultTransformResponse(
  data: unknown,
  _headers: Headers,
  config: FHRequestConfig,
  _status: number,
): Promise<unknown> {
  const response = data as Response;
  if (config.responseType === "text") return response.text();
  if (config.responseType === "blob") return response.blob();
  if (config.responseType === "arraybuffer") return response.arrayBuffer();
  if (config.responseType === "formdata") return response.formData();
  const contentType = response.headers.get("content-type") ?? "";
  if (config.responseType === "json" || contentType.includes("json")) {
    const text = await response.text();
    if (!text) return null;
    try {
      return JSON.parse(text);
    } catch {
      return text;
    }
  }
  return response.text();
}

async function runTransformers(
  transformers: RequestTransformer[] | ResponseTransformer[] | undefined,
  fallback: (...args: never[]) => Promise<unknown>,
  args: unknown[],
): Promise<unknown> {
  if (!transformers || transformers.length === 0) {
    return (fallback as (...a: unknown[]) => Promise<unknown>)(...args);
  }
  let value = args[0];
  for (const transformer of transformers) {
    value = await (transformer as (...a: unknown[]) => unknown)(value, ...args.slice(1));
  }
  return value;
}

const TIMEOUT_REASON = Symbol("fh-timeout");

function combineSignals(...signals: (AbortSignal | undefined)[]): { signal: AbortSignal; cleanup: () => void } {
  const present = signals.filter((s): s is AbortSignal => s !== undefined);
  if (present.length === 0) {
    const controller = new AbortController();
    return { signal: controller.signal, cleanup: () => {} };
  }
  if (present.length === 1 && present[0]) {
    return { signal: present[0], cleanup: () => {} };
  }
  const controller = new AbortController();
  const abort = (signal: AbortSignal) => controller.abort(signal.reason);
  const listeners = present.map((signal) => {
    if (signal.aborted) controller.abort(signal.reason);
    const listener = () => abort(signal);
    signal.addEventListener("abort", listener);
    return { signal, listener };
  });
  return {
    signal: controller.signal,
    cleanup: () => {
      for (const { signal, listener } of listeners) signal.removeEventListener("abort", listener);
    },
  };
}

function defaultRetryDelay(attempt: number): number {
  return Math.min(30000, 250 * 2 ** (attempt - 1)) + Math.random() * 100;
}

const IDEMPOTENT_METHODS: ReadonlySet<HttpMethod> = new Set(["GET", "HEAD", "OPTIONS", "PUT", "DELETE"]);

function defaultRetryCondition(error: FHError): boolean {
  const method = (error.config.method ?? "GET").toUpperCase() as HttpMethod;
  if (!IDEMPOTENT_METHODS.has(method)) return false;
  if (error.code === "ERR_NETWORK") return true;
  if (error.response && error.response.status >= 500) return true;
  return false;
}

function normalizeRetryConfig(retries: FHRequestConfig["retries"]): Required<RetryConfig> {
  const normalized = typeof retries === "number" ? { retries } : (retries ?? {});
  return {
    retries: normalized.retries ?? 0,
    retryDelay: normalized.retryDelay ?? defaultRetryDelay,
    retryCondition: normalized.retryCondition ?? defaultRetryCondition,
  };
}

export interface FHClient {
  defaults: FHRequestConfig;
  interceptors: {
    request: InterceptorManager<FHRequestConfig>;
    response: InterceptorManager<FHResponse>;
  };
  request<T = unknown, D = unknown>(config: FHRequestConfig<D>): Promise<FHResponse<T, D>>;
  get<T = unknown>(url: string, config?: FHRequestConfig): Promise<FHResponse<T>>;
  delete<T = unknown>(url: string, config?: FHRequestConfig): Promise<FHResponse<T>>;
  head<T = unknown>(url: string, config?: FHRequestConfig): Promise<FHResponse<T>>;
  options<T = unknown>(url: string, config?: FHRequestConfig): Promise<FHResponse<T>>;
  post<T = unknown, D = unknown>(url: string, data?: D, config?: FHRequestConfig<D>): Promise<FHResponse<T, D>>;
  put<T = unknown, D = unknown>(url: string, data?: D, config?: FHRequestConfig<D>): Promise<FHResponse<T, D>>;
  patch<T = unknown, D = unknown>(url: string, data?: D, config?: FHRequestConfig<D>): Promise<FHResponse<T, D>>;
  getUri(config?: FHRequestConfig): string;
  create(config?: FHRequestConfig): FHClient;
}

export function createClient(fetchImpl: FetchLike, config: FHRequestConfig = {}): FHClient {
  const defaults: FHRequestConfig = { method: "GET", validateStatus: defaultValidateStatus, ...config };
  const requestInterceptors = createInterceptorManager<FHRequestConfig>();
  const responseInterceptors = createInterceptorManager<FHResponse>();

  async function dispatch<T, D>(rawConfig: FHRequestConfig<D>): Promise<FHResponse<T, D>> {
    let cfg = mergeConfig(defaults, rawConfig);
    for (const { onFulfilled, onRejected } of requestInterceptors.entries.values()) {
      try {
        if (onFulfilled) cfg = (await onFulfilled(cfg)) as FHRequestConfig<D>;
      } catch (error) {
        if (onRejected) return (await onRejected(error)) as FHResponse<T, D>;
        throw error;
      }
    }

    const method = (cfg.method ?? "GET").toUpperCase() as HttpMethod;
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
    ])) as BodyInit | undefined;

    const { retries, retryDelay, retryCondition } = normalizeRetryConfig(cfg.retries);

    const totalBytes =
      typeof body === "string"
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
        const attemptResult = await buildAttemptResult<T, D>(response, request, cfg);
        cfg.onDownloadProgress?.({
          loaded: Number(response.headers.get("content-length") ?? 0),
          total: response.headers.get("content-length") ? Number(response.headers.get("content-length")) : undefined,
          lengthComputable: response.headers.has("content-length"),
          upload: false,
          synthetic: true,
        });
        if (attemptResult.error && attempt <= retries && retryCondition(attemptResult.error, attempt)) {
          await new Promise((resolve) => setTimeout(resolve, retryDelay(attempt, attemptResult.error!)));
          continue;
        }
        return finalizeResponse(attemptResult.response, attemptResult.error, responseInterceptors.entries);
      } catch (error) {
        const aborted = signal.aborted;
        const code: FHErrorCode = !aborted
          ? "ERR_NETWORK"
          : signal.reason === TIMEOUT_REASON
            ? "ECONNABORTED"
            : "ERR_CANCELED";
        const fhError = new FHError(
          aborted ? (code === "ECONNABORTED" ? "Request timed out" : "Request canceled") : "Network error",
          code,
          cfg,
          request,
        );
        if (code !== "ERR_CANCELED" && attempt <= retries && retryCondition(fhError, attempt)) {
          await new Promise((resolve) => setTimeout(resolve, retryDelay(attempt, fhError)));
          continue;
        }
        throw fhError;
      } finally {
        if (timeoutHandle !== undefined) clearTimeout(timeoutHandle);
        cleanup();
      }
    }
  }

  return {
    defaults,
    interceptors: { request: requestInterceptors.manager, response: responseInterceptors.manager },
    request: <T, D>(config: FHRequestConfig<D>) => dispatch<T, D>(config),
    get: <T>(url: string, config?: FHRequestConfig) => dispatch<T, unknown>({ ...config, url, method: "GET" }),
    delete: <T>(url: string, config?: FHRequestConfig) => dispatch<T, unknown>({ ...config, url, method: "DELETE" }),
    head: <T>(url: string, config?: FHRequestConfig) => dispatch<T, unknown>({ ...config, url, method: "HEAD" }),
    options: <T>(url: string, config?: FHRequestConfig) => dispatch<T, unknown>({ ...config, url, method: "OPTIONS" }),
    post: <T, D>(url: string, data?: D, config?: FHRequestConfig<D>) =>
      dispatch<T, D>({ ...config, url, method: "POST", data }),
    put: <T, D>(url: string, data?: D, config?: FHRequestConfig<D>) =>
      dispatch<T, D>({ ...config, url, method: "PUT", data }),
    patch: <T, D>(url: string, data?: D, config?: FHRequestConfig<D>) =>
      dispatch<T, D>({ ...config, url, method: "PATCH", data }),
    getUri: (config?: FHRequestConfig) => {
      const cfg = config ? mergeConfig(defaults, config) : defaults;
      return buildURL(cfg.baseURL, cfg.url ?? "", cfg.params, cfg.paramsSerializer);
    },
    create: (childConfig?: FHRequestConfig) => createClient(fetchImpl, mergeConfig(defaults, childConfig ?? {})),
  };
}

interface AttemptResult<T, D> {
  response: FHResponse<T, D>;
  error?: FHError<T>;
}

async function buildAttemptResult<T, D>(
  response: Response,
  request: Request,
  cfg: FHRequestConfig<D>,
): Promise<AttemptResult<T, D>> {
  const method = (cfg.method ?? "GET").toUpperCase();
  let data: unknown = null;
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
  const fhResponse = buildResponse<T, D>(data as T, response, request, cfg);
  const validateStatus = cfg.validateStatus ?? defaultValidateStatus;
  const error = !validateStatus(response.status)
    ? new FHError<T>(
        `Request failed with status ${response.status}`,
        response.status >= 500 ? "ERR_BAD_RESPONSE" : "ERR_BAD_REQUEST",
        cfg,
        request,
        fhResponse,
      )
    : undefined;
  return { response: fhResponse, error };
}

function buildResponse<T, D>(data: T, response: Response, request: Request, config: FHRequestConfig<D>): FHResponse<T, D> {
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

async function finalizeResponse<T, D>(
  response: FHResponse<T, D>,
  error: FHError<T> | undefined,
  responseInterceptors: Map<number, InterceptorEntry<FHResponse>>,
): Promise<FHResponse<T, D>> {
  if (!error) {
    let result: FHResponse = response as FHResponse;
    for (const { onFulfilled } of responseInterceptors.values()) {
      if (onFulfilled) result = await onFulfilled(result);
    }
    return result as FHResponse<T, D>;
  }
  for (const { onRejected } of responseInterceptors.values()) {
    if (onRejected) {
      try {
        return (await onRejected(error)) as FHResponse<T, D>;
      } catch (nextError) {
        if (nextError !== error) throw nextError;
      }
    }
  }
  throw error;
}

export async function createSecureClient(
  handleOrConfig: SecureFetchHandle | SecureFetchConfig,
  clientConfig: FHRequestConfig = {},
): Promise<FHClient> {
  const handle: SecureFetchHandle =
    "fetch" in handleOrConfig
      ? handleOrConfig
      : await (await import("./secure-fetch.js")).createSecureFetch(handleOrConfig);
  return createClient(handle.fetch, clientConfig);
}

export function all<T extends readonly unknown[] | []>(
  values: T,
): Promise<{ -readonly [P in keyof T]: Awaited<T[P]> }> {
  return Promise.all(values) as Promise<{ -readonly [P in keyof T]: Awaited<T[P]> }>;
}
