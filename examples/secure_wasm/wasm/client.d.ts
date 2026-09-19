import { FHError } from "./errors.js";
import type { SecureFetchConfig, SecureFetchHandle } from "./secure-fetch.js";
export type FetchLike = (input: RequestInfo | URL, init?: RequestInit) => Promise<Response>;
export type HttpMethod = "GET" | "POST" | "PUT" | "PATCH" | "DELETE" | "HEAD" | "OPTIONS";
export type RequestTransformer = (data: unknown, headers: Headers, config: FHRequestConfig) => unknown | Promise<unknown>;
export type ResponseTransformer = (data: unknown, headers: Headers, config: FHRequestConfig, status: number) => unknown | Promise<unknown>;
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
export interface InterceptorManager<V> {
    use(onFulfilled?: (value: V) => V | Promise<V>, onRejected?: (error: unknown) => unknown): number;
    eject(id: number): void;
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
export declare function createClient(fetchImpl: FetchLike, config?: FHRequestConfig): FHClient;
export declare function createSecureClient(handleOrConfig: SecureFetchHandle | SecureFetchConfig, clientConfig?: FHRequestConfig): Promise<FHClient>;
export declare function all<T extends readonly unknown[] | []>(values: T): Promise<{
    -readonly [P in keyof T]: Awaited<T[P]>;
}>;
