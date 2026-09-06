export interface StoredDevice {
    id: Uint8Array;
    publicKey: Uint8Array;
    name: string;
}
export declare function createDeviceKey(): Promise<{
    publicKey: Uint8Array;
}>;
export declare function saveDevice(device: StoredDevice): Promise<void>;
export declare function loadDevice(): Promise<StoredDevice | null>;
export declare function signClientHello(data: Uint8Array): Promise<Uint8Array>;
export declare function clearDevice(): Promise<void>;
export declare function installSecureStorageBridge(): void;
