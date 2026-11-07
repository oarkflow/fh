export class FHError extends Error {
    config;
    code;
    request;
    response;
    constructor(message, code, config, request, response) {
        super(message);
        this.name = "FHError";
        this.code = code;
        this.config = config;
        this.request = request;
        this.response = response;
    }
    toJSON() {
        return {
            message: this.message,
            code: this.code,
            status: this.response?.status,
        };
    }
}
export function isFHError(value) {
    return value instanceof FHError;
}
export function isCancel(value) {
    if (isFHError(value))
        return value.code === "ERR_CANCELED";
    return value instanceof DOMException && value.name === "AbortError";
}
export function defaultValidateStatus(status) {
    return status >= 200 && status < 300;
}
