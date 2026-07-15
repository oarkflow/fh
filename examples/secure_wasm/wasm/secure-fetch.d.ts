export interface SecureFetchConfig {
    baseURL?: string;
    prefix?: string;
    wasmURL?: string;
    wasmExecURL?: string;
    /** SRI value such as `sha256-...` for securefetch.wasm. */
    wasmIntegrity?: string;
    /** SRI value such as `sha256-...` for wasm_exec.js. */
    wasmExecIntegrity?: string;
    /** Fail closed unless both WASM assets have explicit integrity pins. */
    requireAssetIntegrity?: boolean;
    pinnedServerKey?: string;
    /** Optional key identifier expected in the authenticated server handshake. */
    pinnedServerKeyID?: string;
    /** Trusted base64url Ed25519 key for RFC 9421 response verification. */
    responseSigningPublicKey?: string;
    /** Required RFC 9421 response-signing key identifier. */
    responseSigningKeyID?: string;
    /** Fail closed during initialization unless response-signing pins are set. */
    requireResponseSignature?: boolean;
    /** Require the origin and both public keys to be embedded in the WASM build. */
    requireEmbeddedTrust?: boolean;
    /** Development-only escape hatch. Production clients should always pin. */
    allowUnpinnedServerKey?: boolean;
    clientBuild?: string;
    deviceName?: string;
    credentials?: RequestCredentials;
    /** One-time token issued by an already-authenticated FH bootstrap endpoint. */
    registrationToken?: string;
    handshakeTTL?: number;
    requestTTL?: number;
    /** Maximum accepted lifetime of a server-created secure session. */
    maxSessionTTL?: number;
    clockSkew?: number;
    maxBody?: number;
    maxHeaders?: number;
    installGlobal?: boolean;
}
export interface SecureSessionInfo {
    deviceId?: string;
    deviceName?: string;
    sessionId?: string;
    expiresAt?: number;
    sequence?: number;
    trustMode?: "embedded" | "loopback-development";
}
export type SecureFetchBody = BodyInit | Record<string, unknown> | readonly unknown[] | number | boolean | null;
export interface SecureFetchInit extends Omit<RequestInit, "body"> {
    body?: SecureFetchBody;
}
interface WasmResponse {
    status: number;
    headers: Array<[string, string]>;
    body: Uint8Array;
    url: string;
    requestId: string;
}
interface FHSecureWasmAPI {
    initialize(config: SecureFetchConfig): Promise<SecureSessionInfo>;
    request(request: Record<string, unknown>): Promise<WasmResponse>;
    revokeSession(): Promise<WasmResponse>;
    sessionInfo(): SecureSessionInfo;
}
interface GoRuntime {
    importObject: WebAssembly.Imports;
    run(instance: WebAssembly.Instance): Promise<void>;
}
declare global {
    var Go: {
        new (): GoRuntime;
    } | undefined;
    var FHSecureWasm: FHSecureWasmAPI | undefined;
    var __fhNativeFetch: typeof fetch | undefined;
    var __fhGoRuntimeIntegrity: string | undefined;
    var __fhSecureStorage: {
        loadDevice(): Promise<unknown>;
        createDeviceKey(): Promise<unknown>;
        saveDevice(device: unknown): Promise<void>;
        signClientHello(data: Uint8Array): Promise<Uint8Array>;
        clearDevice(): Promise<void>;
    } | undefined;
}
export interface SecureFetchHandle {
    fetch(input: RequestInfo | URL, init?: SecureFetchInit): Promise<Response>;
    sessionInfo(): SecureSessionInfo;
    revokeSession(): Promise<void>;
    resetDevice(): Promise<void>;
}
export declare function createSecureFetch(config?: SecureFetchConfig): Promise<SecureFetchHandle>;
export {};
