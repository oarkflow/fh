const DB_NAME = "fh-secure-fetch";
const DB_VERSION = 2;
const KEY_STORE = "keys";
const DEVICE_STORE = "device";
const SIGNING_KEY_ID = "device-ed25519-key-v1";
const DEVICE_ID = "current-device-v2";
const LEGACY_DEVICE_ID = "current-device-v1";
const LEGACY_WRAP_KEY_ID = "device-wrap-key-v1";
let dbPromise;
function openDB() {
    if (dbPromise)
        return dbPromise;
    dbPromise = new Promise((resolve, reject) => {
        const request = indexedDB.open(DB_NAME, DB_VERSION);
        request.onupgradeneeded = () => {
            const db = request.result;
            if (!db.objectStoreNames.contains(KEY_STORE))
                db.createObjectStore(KEY_STORE);
            if (!db.objectStoreNames.contains(DEVICE_STORE))
                db.createObjectStore(DEVICE_STORE);
        };
        request.onsuccess = () => resolve(request.result);
        request.onerror = () => reject(request.error ?? new Error("Unable to open FH secure storage"));
        request.onblocked = () => reject(new Error("FH secure storage upgrade is blocked"));
    });
    return dbPromise;
}
async function readStore(storeName, key) {
    const db = await openDB();
    return new Promise((resolve, reject) => {
        const tx = db.transaction(storeName, "readonly");
        const request = tx.objectStore(storeName).get(key);
        request.onsuccess = () => resolve(request.result);
        request.onerror = () => reject(request.error ?? tx.error ?? new Error("FH secure storage read failed"));
        tx.onabort = () => reject(tx.error ?? new Error("FH secure storage transaction aborted"));
    });
}
async function writeStore(storeName, key, value) {
    const db = await openDB();
    await new Promise((resolve, reject) => {
        const tx = db.transaction(storeName, "readwrite");
        tx.objectStore(storeName).put(value, key);
        tx.oncomplete = () => resolve();
        tx.onerror = () => reject(tx.error ?? new Error("FH secure storage write failed"));
        tx.onabort = () => reject(tx.error ?? new Error("FH secure storage transaction aborted"));
    });
}
async function deleteStore(storeName, key) {
    const db = await openDB();
    await new Promise((resolve, reject) => {
        const tx = db.transaction(storeName, "readwrite");
        tx.objectStore(storeName).delete(key);
        tx.oncomplete = () => resolve();
        tx.onerror = () => reject(tx.error ?? new Error("FH secure storage delete failed"));
        tx.onabort = () => reject(tx.error ?? new Error("FH secure storage transaction aborted"));
    });
}
function copyBuffer(value) {
    return value.slice().buffer;
}
function equalBytes(left, right) {
    if (left.byteLength !== right.byteLength)
        return false;
    let difference = 0;
    for (let i = 0; i < left.byteLength; i += 1)
        difference |= left[i] ^ right[i];
    return difference === 0;
}
function validClientHello(data) {
    // FHC1 + version + fixed fields + build length. The Go protocol parser and
    // server still perform full validation; this prevents a generic signing oracle.
    return (data.byteLength >= 4 + 1 + 16 + 32 + 8 + 8 + 16 + 2 &&
        data[0] === 0x46 &&
        data[1] === 0x48 &&
        data[2] === 0x43 &&
        data[3] === 0x31 &&
        data[4] === 1);
}
async function signingKeyRecord() {
    const record = await readStore(KEY_STORE, SIGNING_KEY_ID);
    if (!record || record.version !== 1 || !(record.privateKey instanceof CryptoKey))
        return undefined;
    const publicKey = new Uint8Array(record.publicKey);
    if (publicKey.byteLength !== 32 || record.privateKey.type !== "private" || record.privateKey.extractable)
        return undefined;
    if (record.privateKey.algorithm.name !== "Ed25519" || !record.privateKey.usages.includes("sign"))
        return undefined;
    return record;
}
export async function createDeviceKey() {
    const existing = await signingKeyRecord();
    if (existing)
        return { publicKey: new Uint8Array(existing.publicKey).slice() };
    let pair;
    try {
        pair = (await crypto.subtle.generateKey({ name: "Ed25519" }, false, ["sign", "verify"]));
    }
    catch (error) {
        throw new Error("FH secure fetch requires WebCrypto Ed25519 support", { cause: error });
    }
    if (pair.privateKey.extractable || pair.privateKey.type !== "private") {
        throw new Error("Browser returned an extractable FH device private key");
    }
    const publicKey = new Uint8Array(await crypto.subtle.exportKey("raw", pair.publicKey));
    if (publicKey.byteLength !== 32)
        throw new Error("Browser returned an invalid Ed25519 public key");
    const record = {
        version: 1,
        privateKey: pair.privateKey,
        publicKey: copyBuffer(publicKey),
    };
    await writeStore(KEY_STORE, SIGNING_KEY_ID, record);
    return { publicKey: publicKey.slice() };
}
export async function saveDevice(device) {
    if (device.id.byteLength !== 16 || device.publicKey.byteLength !== 32 || device.name.length > 256) {
        throw new Error("FH secure device record is invalid");
    }
    const keyRecord = await signingKeyRecord();
    if (!keyRecord || !equalBytes(new Uint8Array(keyRecord.publicKey), device.publicKey)) {
        throw new Error("FH secure device identity does not match its non-extractable signing key");
    }
    const record = {
        version: 2,
        id: copyBuffer(device.id),
        publicKey: copyBuffer(device.publicKey),
        name: device.name,
    };
    await writeStore(DEVICE_STORE, DEVICE_ID, record);
    await Promise.all([
        deleteStore(DEVICE_STORE, LEGACY_DEVICE_ID).catch(() => undefined),
        deleteStore(KEY_STORE, LEGACY_WRAP_KEY_ID).catch(() => undefined),
    ]);
}
export async function loadDevice() {
    const record = await readStore(DEVICE_STORE, DEVICE_ID);
    if (!record || record.version !== 2)
        return null;
    const id = new Uint8Array(record.id);
    const publicKey = new Uint8Array(record.publicKey);
    const keyRecord = await signingKeyRecord();
    if (id.byteLength !== 16 ||
        publicKey.byteLength !== 32 ||
        !keyRecord ||
        !equalBytes(publicKey, new Uint8Array(keyRecord.publicKey))) {
        await clearDevice();
        return null;
    }
    return { id: id.slice(), publicKey: publicKey.slice(), name: record.name };
}
export async function signClientHello(data) {
    if (!validClientHello(data))
        throw new Error("FH secure signing accepts only a versioned ClientHello transcript");
    const record = await signingKeyRecord();
    if (!record)
        throw new Error("FH secure device signing key is unavailable");
    const signature = await crypto.subtle.sign({ name: "Ed25519" }, record.privateKey, data);
    const out = new Uint8Array(signature);
    if (out.byteLength !== 64) {
        out.fill(0);
        throw new Error("Browser returned an invalid Ed25519 signature");
    }
    return out;
}
export async function clearDevice() {
    await Promise.all([
        deleteStore(DEVICE_STORE, DEVICE_ID),
        deleteStore(DEVICE_STORE, LEGACY_DEVICE_ID),
        deleteStore(KEY_STORE, SIGNING_KEY_ID),
        deleteStore(KEY_STORE, LEGACY_WRAP_KEY_ID),
    ]);
}
export function installSecureStorageBridge() {
    Object.defineProperty(globalThis, "__fhSecureStorage", {
        configurable: false,
        enumerable: false,
        writable: false,
        value: Object.freeze({ loadDevice, createDeviceKey, saveDevice, signClientHello, clearDevice }),
    });
}
