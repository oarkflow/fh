import { createSecureFetch } from "/wasm/index.js";

const login = document.querySelector("#login");
const transfer = document.querySelector("#transfer");
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
    show({ session: secure.sessionInfo(), response: me });
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
