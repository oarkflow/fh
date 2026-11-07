import type { FHRequestConfig, FHResponse } from "./client.js";
export type FHErrorCode = "ERR_BAD_REQUEST" | "ERR_BAD_RESPONSE" | "ERR_NETWORK" | "ECONNABORTED" | "ERR_CANCELED" | "ERR_INVALID_CONFIG";
export declare class FHError<T = unknown> extends Error {
    readonly config: FHRequestConfig;
    readonly code?: FHErrorCode;
    readonly request?: Request;
    readonly response?: FHResponse<T>;
    constructor(message: string, code: FHErrorCode | undefined, config: FHRequestConfig, request?: Request, response?: FHResponse<T>);
    toJSON(): {
        message: string;
        code?: string;
        status?: number;
    };
}
export declare function isFHError(value: unknown): value is FHError;
export declare function isCancel(value: unknown): boolean;
export declare function defaultValidateStatus(status: number): boolean;
