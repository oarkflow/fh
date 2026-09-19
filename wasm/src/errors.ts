import type { FHRequestConfig, FHResponse } from "./client.js";

export type FHErrorCode =
  | "ERR_BAD_REQUEST"
  | "ERR_BAD_RESPONSE"
  | "ERR_NETWORK"
  | "ECONNABORTED"
  | "ERR_CANCELED"
  | "ERR_INVALID_CONFIG";

export class FHError<T = unknown> extends Error {
  readonly config: FHRequestConfig;
  readonly code?: FHErrorCode;
  readonly request?: Request;
  readonly response?: FHResponse<T>;

  constructor(
    message: string,
    code: FHErrorCode | undefined,
    config: FHRequestConfig,
    request?: Request,
    response?: FHResponse<T>,
  ) {
    super(message);
    this.name = "FHError";
    this.code = code;
    this.config = config;
    this.request = request;
    this.response = response;
  }

  toJSON(): { message: string; code?: string; status?: number } {
    return {
      message: this.message,
      code: this.code,
      status: this.response?.status,
    };
  }
}

export function isFHError(value: unknown): value is FHError {
  return value instanceof FHError;
}

export function isCancel(value: unknown): boolean {
  if (isFHError(value)) return value.code === "ERR_CANCELED";
  return value instanceof DOMException && value.name === "AbortError";
}

export function defaultValidateStatus(status: number): boolean {
  return status >= 200 && status < 300;
}
