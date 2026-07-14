import { createSecureFetch } from "/wasm/index.js";

const output = document.querySelector("#output");
const sessionState = document.querySelector("#session-state");
const loginForm = document.querySelector("#login-form");
const logoutButton = document.querySelector("#logout");
const securePanel = document.querySelector("#secure-panel");
let secure;
let currentUser;

function show(value) {
  output.textContent = typeof value === "string" ? value : JSON.stringify(value, null, 2);
}

async function jsonFetch(url, init) {
  const response = await fetch(url, {
    cache: "no-store",
    credentials: "same-origin",
    headers: { "content-type": "application/json", ...(init?.headers ?? {}) },
    ...init,
  });
  if (!response.ok) {
    let detail = "";
    try {
      detail = ` ${JSON.stringify(await response.json())}`;
    } catch {
      detail = ` ${await response.text()}`;
    }
    throw new Error(`HTTP ${response.status}${detail}`);
  }
  if (response.status === 204) return null;
  return response.json();
}

function renderSession(auth) {
  currentUser = auth.authenticated ? auth.user : undefined;
  loginForm.hidden = auth.authenticated;
  logoutButton.hidden = !auth.authenticated;
  securePanel.hidden = !auth.authenticated;
  sessionState.textContent = auth.authenticated
    ? `Logged in as ${auth.user.name} (${auth.user.role}) with the ${auth.session_cookie} session cookie.`
    : "Not logged in. Use demo / demo.";
}

async function initializeSecureFetch() {
  const config = await jsonFetch("/secure-config.json");
  secure = await createSecureFetch({
    ...config,
    wasmURL: "/wasm/securefetch.wasm",
    wasmExecURL: "/wasm/wasm_exec.js",
    credentials: "same-origin",
    clientBuild: "fh-secure-wasm-session-auth-example-v1",
    deviceName: navigator.userAgentData?.platform ?? navigator.platform ?? "Browser device",
  });
  show({ user: currentUser, secure_session: secure.sessionInfo() });
}

async function refresh() {
  const auth = await jsonFetch("/auth/me");
  renderSession(auth);
  if (auth.authenticated) {
    await initializeSecureFetch();
  } else {
    secure = undefined;
    show("Log in to initialize secure fetch.");
  }
}

loginForm.addEventListener("submit", async (event) => {
  event.preventDefault();
  const data = Object.fromEntries(new FormData(loginForm).entries());
  await jsonFetch("/auth/login", { method: "POST", body: JSON.stringify(data) });
  await refresh();
});

logoutButton.addEventListener("click", async () => {
  if (secure) await secure.revokeSession().catch(() => undefined);
  await fetch("/auth/logout", { method: "POST", credentials: "same-origin", cache: "no-store" });
  secure = undefined;
  await refresh();
});

document.querySelector("#profile").addEventListener("click", async () => {
  const response = await secure.fetch("/api/profile");
  show(await response.json());
});

document.querySelector("#echo").addEventListener("click", async () => {
  const response = await secure.fetch("/api/echo", {
    method: "POST",
    headers: { "content-type": "application/json", authorization: "Bearer encrypted-demo-token" },
    body: { message: "hello from the logged-in session", at: new Date().toISOString() },
  });
  show({
    status: response.status,
    requestId: response.headers.get("x-fh-request-id"),
    encryptedApplicationMetadata: response.headers.get("x-application-metadata"),
    body: await response.json(),
  });
});

document.querySelector("#revoke").addEventListener("click", async () => {
  await secure.revokeSession();
  show("Secure transport session revoked. The next protected request creates a new one using the same app login session.");
});

refresh().catch((error) => {
  sessionState.textContent = "Initialization failed.";
  show(error.message);
});
