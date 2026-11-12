const encoder = new TextEncoder();
const dbPromise = openKeyDB();
const sessionKey = "fh-deviceauth-session";
const registrationRequestKey = "fh-deviceauth-registration-request";
let current = null;

const byId = (id) => document.getElementById(id);
const forms = ["register-form", "login-form", "recover-form"];

function openKeyDB() {
  return new Promise((resolve, reject) => {
    const request = indexedDB.open("fh-deviceauth", 1);
    request.onupgradeneeded = () => request.result.createObjectStore("devices", { keyPath: "userId" });
    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error);
  });
}

async function deviceFor(userId) {
  const db = await dbPromise;
  return new Promise((resolve, reject) => {
    const request = db.transaction("devices", "readonly").objectStore("devices").get(userId);
    request.onsuccess = () => resolve(request.result ?? null);
    request.onerror = () => reject(request.error);
  });
}

async function saveDevice(device) {
  const db = await dbPromise;
  return new Promise((resolve, reject) => {
    const tx = db.transaction("devices", "readwrite");
    tx.objectStore("devices").put(device);
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error);
    tx.onabort = () => reject(tx.error);
  });
}

async function newDevice(userId, deviceId = "") {
  const pair = await crypto.subtle.generateKey({ name: "Ed25519" }, false, ["sign", "verify"]);
  const rawPublic = new Uint8Array(await crypto.subtle.exportKey("raw", pair.publicKey));
  return { userId, deviceId, privateKey: pair.privateKey, publicKey: toBase64URL(rawPublic) };
}

function toBase64URL(bytes) {
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary).replaceAll("+", "-").replaceAll("/", "_").replace(/=+$/, "");
}

function fromBase64URL(value) {
  const normalized = value.replaceAll("-", "+").replaceAll("_", "/");
  const binary = atob(normalized + "=".repeat((4 - normalized.length % 4) % 4));
  return Uint8Array.from(binary, (char) => char.charCodeAt(0));
}

async function api(path, { method = "POST", body = null, session = "", device = null, headers: extraHeaders = {} } = {}) {
  const headers = new Headers(extraHeaders);
  let bytes = new Uint8Array();
  if (body !== null) {
    const json = JSON.stringify(body);
    bytes = encoder.encode(json);
    headers.set("content-type", "application/json");
  }
  if (session) {
    if (!device?.privateKey) throw new Error("No device signing key is available in this browser");
    const url = new URL(path, location.href);
    const timestamp = Math.floor(Date.now() / 1000).toString();
    const nonceBytes = crypto.getRandomValues(new Uint8Array(24));
    const nonce = toBase64URL(nonceBytes);
    const digest = new Uint8Array(await crypto.subtle.digest("SHA-256", bytes));
    const hashHex = [...digest].map((byte) => byte.toString(16).padStart(2, "0")).join("");
    const canonical = `FH-DEVICE-AUTH-V1\n${method}\n${url.pathname}${url.search}\n${url.host}\n${hashHex}\n${timestamp}\n${nonce}\n${session}`;
    const signature = await crypto.subtle.sign("Ed25519", device.privateKey, encoder.encode(canonical));
    headers.set("authorization", `Device ${session}`);
    headers.set("X-Device-Time", timestamp);
    headers.set("X-Device-Nonce", nonce);
    headers.set("X-Device-Proof", toBase64URL(new Uint8Array(signature)));
  }
  const response = await fetch(path, {
    method,
    headers,
    body: body === null ? undefined : bytes,
    credentials: "same-origin",
    cache: "no-store",
    redirect: "error",
  });
  const text = await response.text();
  let data = {};
  try { data = text ? JSON.parse(text) : {}; } catch { data = { error: text || response.statusText }; }
  if (!response.ok) throw new Error(data.error || data.reason || `Request failed (${response.status})`);
  return data;
}

async function challengeSignature(challenge, privateKey) {
  const challengeBytes = fromBase64URL(challenge);
  const prefix = encoder.encode("FH-DEVICE-AUTH-CHALLENGE-V1\n");
  const message = new Uint8Array(prefix.length + challengeBytes.length);
  message.set(prefix);
  message.set(challengeBytes, prefix.length);
  return toBase64URL(new Uint8Array(await crypto.subtle.sign("Ed25519", privateKey, message)));
}

async function finishEnrollment(start, device, endpoint) {
  const signature = await challengeSignature(start.challenge, device.privateKey);
  return api(endpoint, {
    body: { challenge_id: start.challenge_id, signature, public_key: device.publicKey },
  });
}

async function beginLogin(device) {
  const start = await api("/auth/login/begin", { body: { user_id: device.userId, device_id: device.deviceId } });
  const signature = await challengeSignature(start.challenge, device.privateKey);
  const result = await api("/auth/login/finish", { body: { challenge_id: start.challenge_id, signature } });
  sessionStorage.setItem(sessionKey, result.session);
  current = device;
  showAccount();
  return result.session;
}

function showAccount() {
  byId("account-panel").hidden = false;
  byId("signed-in-as").textContent = current.userId;
  byId("device-status").textContent = "This browser holds the registered device key. The private key is non-extractable and stored in IndexedDB.";
}

async function showRecoveryCode(code) {
  byId("recovery-output").textContent = code;
  byId("recovery-dialog").showModal();
}

function notify(message, error = false) {
  const notice = byId("notice");
  notice.textContent = message;
  notice.className = `notice show${error ? " error" : ""}`;
  clearTimeout(notify.timer);
  notify.timer = setTimeout(() => { notice.className = "notice"; }, 3500);
}

async function withForm(form, action) {
  const button = form.querySelector("button[type=submit]");
  button.disabled = true;
  try {
    await action(new FormData(form));
  } catch (error) {
    notify(error instanceof Error ? error.message : String(error), true);
  } finally {
    button.disabled = false;
  }
}

async function completeRegistration(userId, inviteCode, reason = "Email verified and registration checks passed.") {
  const device = await newDevice(userId);
  const start = await api("/auth/register/begin", { body: { user_id: userId, invite_code: inviteCode } });
  device.deviceId = start.device_id;
  const result = await finishEnrollment(start, device, "/auth/register/finish");
  await saveDevice(device);
  await beginLogin(device);
  byId("protected-result").textContent = JSON.stringify(await api("/api/me", { method: "GET", session: sessionStorage.getItem(sessionKey), device }), null, 2);
  localStorage.removeItem(registrationRequestKey);
  byId("request-status").hidden = true;
  byId("email-verification-form").hidden = true;
  await showRecoveryCode(result.recovery_code);
  notify(`${reason} Account registered and signed in.`);
}

byId("register-form").addEventListener("submit", (event) => {
  event.preventDefault();
  withForm(event.currentTarget, async (values) => {
    const userId = String(values.get("user_id")).trim();
    const email = String(values.get("email")).trim();
    const result = await api("/auth/registration-requests", {
      body: { user_id: userId, email },
    });
    localStorage.setItem(registrationRequestKey, result.request_id);
    byId("request-status").hidden = false;
    byId("request-status").textContent = "We sent a six-digit verification code to your email. Enter it below to complete the security check. Eligible requests are approved automatically.";
    byId("email-verification-form").hidden = false;
    notify("Check your email for the verification code");
  });
});

byId("email-verification-form").addEventListener("submit", (event) => {
  event.preventDefault();
  withForm(event.currentTarget, async (values) => {
    const requestID = localStorage.getItem(registrationRequestKey);
    if (!requestID) throw new Error("Submit a registration request first");
    const result = await api(`/auth/registration-requests/${encodeURIComponent(requestID)}/verify`, {
      body: { code: String(values.get("code")).trim() },
    });
    await completeRegistration(result.user_id, result.invite_code, result.reason);
  });
});

byId("login-form").addEventListener("submit", (event) => {
  event.preventDefault();
  withForm(event.currentTarget, async (values) => {
    const device = await deviceFor(String(values.get("user_id")).trim());
    if (!device) throw new Error("No device key for this account in this browser. Use account recovery.");
    await beginLogin(device);
    const result = await api("/api/me", { method: "GET", session: sessionStorage.getItem(sessionKey), device });
    byId("protected-result").textContent = JSON.stringify(result, null, 2);
    notify("Signed in");
  });
});

byId("recover-form").addEventListener("submit", (event) => {
  event.preventDefault();
  withForm(event.currentTarget, async (values) => {
    const userId = String(values.get("user_id")).trim();
    const device = await newDevice(userId);
    const start = await api("/auth/recovery/begin", { body: { user_id: userId, recovery_code: values.get("recovery_code") } });
    device.deviceId = start.device_id;
    const result = await finishEnrollment(start, device, "/auth/recovery/finish");
    await saveDevice(device);
    await beginLogin(device);
    byId("protected-result").textContent = JSON.stringify(await api("/api/me", { method: "GET", session: sessionStorage.getItem(sessionKey), device }), null, 2);
    await showRecoveryCode(result.recovery_code);
    notify("Account recovered; the old device was removed");
  });
});

byId("check-session").addEventListener("click", async () => {
  try {
    const session = sessionStorage.getItem(sessionKey);
    if (!session || !current) throw new Error("Sign in again to create a fresh session");
    const result = await api("/api/me", { method: "GET", session, device: current });
    byId("protected-result").textContent = JSON.stringify(result, null, 2);
  } catch (error) { notify(error instanceof Error ? error.message : String(error), true); }
});

byId("change-device").addEventListener("click", async () => {
  const button = byId("change-device");
  button.disabled = true;
  try {
    const session = sessionStorage.getItem(sessionKey);
    if (!session || !current) throw new Error("Sign in again before replacing the device key");
    const start = await api("/auth/change/begin", { session, device: current, body: {} });
    const replacement = await newDevice(current.userId, start.device_id);
    await api("/auth/change/finish", {
      session,
      device: current,
      body: { challenge_id: start.challenge_id, signature: await challengeSignature(start.challenge, replacement.privateKey), public_key: replacement.publicKey },
    });
    await saveDevice(replacement);
    current = replacement;
    const newSession = await beginLogin(replacement);
    byId("protected-result").textContent = JSON.stringify(await api("/api/me", { method: "GET", session: newSession, device: replacement }), null, 2);
    notify("Device key replaced; old key revoked");
  } catch (error) { notify(error instanceof Error ? error.message : String(error), true); }
  finally { button.disabled = false; }
});

byId("logout").addEventListener("click", () => {
  sessionStorage.removeItem(sessionKey);
  current = null;
  byId("account-panel").hidden = true;
  byId("protected-result").textContent = "Sign in to check the protected endpoint.";
  notify("Signed out; device key remains on this browser");
});

for (const id of forms) {
  const form = byId(id);
  form.addEventListener("submit", (event) => {
    const userId = form.elements.user_id?.value;
    if (userId && !/^[A-Za-z0-9_.@-]{3,128}$/.test(userId)) {
      event.preventDefault();
      notify("Account ID must be 3–128 letters, digits, or . _ - @", true);
    }
  }, { capture: true });
}

if (!globalThis.crypto?.subtle || !globalThis.indexedDB) {
  notify("This browser needs WebCrypto and IndexedDB; use localhost or HTTPS", true);
}

if (localStorage.getItem(registrationRequestKey)) {
  byId("request-status").hidden = false;
  byId("request-status").textContent = "Enter the six-digit verification code sent to your email to finish signup.";
  byId("email-verification-form").hidden = false;
}
