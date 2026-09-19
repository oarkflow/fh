import { createSecureFetch, createSecureClient, isFHError } from "/wasm/index.js";

const login = document.querySelector("#login");
const transfer = document.querySelector("#transfer");
const clientSmoke = document.querySelector("#client-smoke");
const output = document.querySelector("#output");
let secure;

function show(value) {
  output.textContent = typeof value === "string" ? value : JSON.stringify(value, null, 2);
}

async function responseJSON(response) {
  const body = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(body.detail ?? body.message ?? `HTTP ${response.status}`);
  return body;
}

login.addEventListener("submit", async (event) => {
  event.preventDefault();
  try {
    const values = new FormData(login);
    await responseJSON(await fetch("/auth/login", {
      method: "POST",
      credentials: "same-origin",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ username: values.get("username"), password: values.get("password") }),
    }));
    const config = await responseJSON(await fetch("/secure-config.json", {
      credentials: "same-origin",
      cache: "no-store",
    }));
    secure = await createSecureFetch({
      ...config,
      credentials: "same-origin",
      clientBuild: "secure-wasm-example-1",
      deviceName: "Example browser",
    });
    const me = await responseJSON(await secure.fetch("/api/me"));
    transfer.hidden = false;
    clientSmoke.hidden = false;
    show({ session: secure.sessionInfo(), response: me });
  } catch (error) {
    show(error instanceof Error ? error.message : String(error));
  }
});

// Manual smoke check for the axios-style client layer (wasm/src/client.ts)
// against the real WASM binary: one interceptor, one retried request, and
// one deliberately-failing request to confirm the FHError shape end-to-end.
// Not part of any automated test -- run this example and click the button.
clientSmoke.addEventListener("click", async () => {
  try {
    const api = await createSecureClient(secure);
    const seen = [];
    api.interceptors.request.use((config) => {
      seen.push(`${config.method} ${config.url}`);
      return config;
    });

    const me = await api.get("/api/me");
    let retryAttempts = 0;
    let retryError;
    try {
      await api.get("/api/does-not-exist", {
        retries: { retries: 2, retryCondition: () => (retryAttempts += 1) < 3 },
      });
    } catch (error) {
      retryError = error;
    }

    show({
      interceptorLog: seen,
      getMeStatus: me.status,
      getMeData: me.data,
      retryAttempts,
      retryFailureIsFHError: isFHError(retryError),
      retryFailureStatus: retryError?.response?.status,
      retryFailureCode: retryError?.code,
    });
  } catch (error) {
    show(error instanceof Error ? error.message : String(error));
  }
});

transfer.addEventListener("submit", async (event) => {
  event.preventDefault();
  try {
    const values = new FormData(transfer);
    const response = await secure.fetch("/api/transfer", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: { to: values.get("to"), amount: Number(values.get("amount")) },
    });
    show({ status: response.status, requestId: response.headers.get("x-fh-request-id"), body: await responseJSON(response) });
  } catch (error) {
    show(error instanceof Error ? error.message : String(error));
  }
});
