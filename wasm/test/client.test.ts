import assert from "node:assert/strict";
import { test } from "node:test";
import { createClient, all, type FetchLike } from "../dist/client.js";
import { isFHError, isCancel } from "../dist/errors.js";

function jsonResponse(body: unknown, init: ResponseInit = {}): Response {
  return new Response(JSON.stringify(body), {
    status: 200,
    headers: { "content-type": "application/json" },
    ...init,
  });
}

test("get/post build request and parse JSON response", async () => {
  const calls: Array<{ url: string; init?: RequestInit }> = [];
  const fetchImpl: FetchLike = async (input, init) => {
    calls.push({ url: input.toString(), init });
    return jsonResponse({ ok: true });
  };
  const client = createClient(fetchImpl, { baseURL: "https://api.example.com" });

  const res = await client.get<{ ok: boolean }>("/things", { params: { a: 1, b: ["x", "y"] } });
  assert.equal(res.data.ok, true);
  assert.equal(res.status, 200);
  assert.equal(calls[0].url, "https://api.example.com/things?a=1&b=x&b=y");

  const postRes = await client.post<{ ok: boolean }, { name: string }>("/things", { name: "hi" });
  assert.equal(postRes.data.ok, true);
  const postInit = calls[1].init!;
  assert.equal(postInit.method, "POST");
  assert.equal(postInit.body, JSON.stringify({ name: "hi" }));
  assert.equal(new Headers(postInit.headers).get("content-type"), "application/json");
});

test("request interceptors mutate config, response interceptors transform response", async () => {
  const fetchImpl: FetchLike = async () => jsonResponse({ value: 1 });
  const client = createClient(fetchImpl, { baseURL: "https://api.example.com" });

  client.interceptors.request.use((config) => ({ ...config, headers: { ...config.headers, "x-trace": "abc" } }));
  let sawTraceHeader = false;
  const fetchImplWithCheck: FetchLike = async (_input, init) => {
    sawTraceHeader = new Headers(init?.headers).get("x-trace") === "abc";
    return jsonResponse({ value: 1 });
  };
  const client2 = createClient(fetchImplWithCheck, { baseURL: "https://api.example.com" });
  client2.interceptors.request.use((config) => ({ ...config, headers: { ...config.headers, "x-trace": "abc" } }));
  await client2.get("/x");
  assert.equal(sawTraceHeader, true);

  const id = client.interceptors.response.use((res) => ({ ...res, data: { ...res.data as object, patched: true } }));
  const res = await client.get<Record<string, unknown>>("/x");
  assert.equal(res.data.patched, true);

  client.interceptors.response.eject(id);
  const res2 = await client.get<Record<string, unknown>>("/x");
  assert.equal(res2.data.patched, undefined);
});

test("response interceptor rejection handler can recover from a validateStatus error", async () => {
  const fetchImpl: FetchLike = async () => new Response("nope", { status: 500 });
  const client = createClient(fetchImpl, { baseURL: "https://api.example.com" });
  client.interceptors.response.use(undefined, async (error) => {
    assert.ok(isFHError(error));
    return { data: "recovered", status: 200, statusText: "OK", headers: {}, config: (error as any).config, request: (error as any).request, raw: (error as any).response.raw };
  });
  const res = await client.get("/fails");
  assert.equal(res.data, "recovered");
});

test("config merge: per-request headers/params/baseURL override instance defaults", async () => {
  let seenURL = "";
  let seenHeaders: Headers | undefined;
  const fetchImpl: FetchLike = async (input, init) => {
    seenURL = input.toString();
    seenHeaders = new Headers(init?.headers);
    return jsonResponse({});
  };
  const client = createClient(fetchImpl, {
    baseURL: "https://api.example.com",
    headers: { "x-a": "1", "x-b": "1" },
    params: { shared: "base" },
  });
  await client.get("/y", { headers: { "x-b": "2" }, params: { shared: "override", extra: "z" } });
  assert.equal(seenHeaders?.get("x-a"), "1");
  assert.equal(seenHeaders?.get("x-b"), "2");
  assert.equal(seenURL, "https://api.example.com/y?shared=override&extra=z");
});

test("config merge: merging a URLSearchParams with repeated keys preserves every value", async () => {
  let seenURL = "";
  const fetchImpl: FetchLike = async (input) => {
    seenURL = input.toString();
    return jsonResponse({});
  };
  const client = createClient(fetchImpl, {
    baseURL: "https://api.example.com",
    params: new URLSearchParams("tag=a&tag=b"),
  });
  await client.get("/x", { params: { extra: "1" } });
  assert.equal(seenURL, "https://api.example.com/x?tag=a&tag=b&extra=1");
});

test("default validateStatus throws FHError with response for non-2xx", async () => {
  const fetchImpl: FetchLike = async () => new Response(JSON.stringify({ message: "bad" }), {
    status: 404,
    headers: { "content-type": "application/json" },
  });
  const client = createClient(fetchImpl, { baseURL: "https://api.example.com" });
  await assert.rejects(
    () => client.get("/missing"),
    (error: unknown) => {
      assert.ok(isFHError(error));
      assert.equal(error.code, "ERR_BAD_REQUEST");
      assert.equal(error.response?.status, 404);
      assert.deepEqual(error.response?.data, { message: "bad" });
      return true;
    },
  );
});

test("timeout aborts with ECONNABORTED", async () => {
  const fetchImpl: FetchLike = (_input, init) =>
    new Promise((resolve, reject) => {
      init?.signal?.addEventListener("abort", () => reject(new DOMException("aborted", "AbortError")));
    });
  const client = createClient(fetchImpl, { baseURL: "https://api.example.com" });
  await assert.rejects(
    () => client.get("/slow", { timeout: 20 }),
    (error: unknown) => {
      assert.ok(isFHError(error));
      assert.equal(error.code, "ECONNABORTED");
      return true;
    },
  );
});

test("a caller-supplied retryCondition can opt in to retrying ECONNABORTED timeouts", async () => {
  let attempts = 0;
  const fetchImpl: FetchLike = (_input, init) => {
    attempts += 1;
    if (attempts < 3) {
      return new Promise((_resolve, reject) => {
        init?.signal?.addEventListener("abort", () => reject(new DOMException("aborted", "AbortError")));
      });
    }
    return Promise.resolve(jsonResponse({ ok: true }));
  };
  const client = createClient(fetchImpl, { baseURL: "https://api.example.com" });
  const res = await client.get<{ ok: boolean }>("/slow", {
    timeout: 20,
    retries: { retries: 5, retryCondition: (error) => error.code === "ECONNABORTED" },
  });
  assert.equal(attempts, 3);
  assert.equal(res.data.ok, true);
});

test("caller-provided AbortSignal yields ERR_CANCELED and isCancel() is true", async () => {
  const controller = new AbortController();
  const fetchImpl: FetchLike = (_input, init) =>
    new Promise((_resolve, reject) => {
      const signal = init?.signal;
      if (signal?.aborted) {
        reject(new DOMException("aborted", "AbortError"));
        return;
      }
      signal?.addEventListener("abort", () => reject(new DOMException("aborted", "AbortError")));
    });
  const client = createClient(fetchImpl, { baseURL: "https://api.example.com" });
  const promise = client.get("/slow", { signal: controller.signal });
  await new Promise((resolve) => setTimeout(resolve, 0));
  controller.abort();
  await assert.rejects(promise, (error: unknown) => {
    assert.ok(isFHError(error));
    assert.equal(error.code, "ERR_CANCELED");
    assert.equal(isCancel(error), true);
    return true;
  });
});

test("retries on 5xx for idempotent methods up to the configured count, then succeeds", async () => {
  let attempts = 0;
  const fetchImpl: FetchLike = async () => {
    attempts += 1;
    if (attempts < 3) return new Response("err", { status: 503 });
    return jsonResponse({ ok: true });
  };
  const client = createClient(fetchImpl, { baseURL: "https://api.example.com" });
  const res = await client.get<{ ok: boolean }>("/retry", { retries: 5 });
  assert.equal(attempts, 3);
  assert.equal(res.data.ok, true);
});

test("does not retry POST (non-idempotent) by default", async () => {
  let attempts = 0;
  const fetchImpl: FetchLike = async () => {
    attempts += 1;
    return new Response("err", { status: 503 });
  };
  const client = createClient(fetchImpl, { baseURL: "https://api.example.com" });
  await assert.rejects(() => client.post("/retry", {}, { retries: 5 }));
  assert.equal(attempts, 1);
});

test("all() resolves a tuple of settled values", async () => {
  const [a, b] = await all([Promise.resolve(1), Promise.resolve("two")] as const);
  assert.equal(a, 1);
  assert.equal(b, "two");
});

test("HEAD / 204 responses short-circuit to null data without invoking transformResponse", async () => {
  const fetchImpl: FetchLike = async () => new Response(null, { status: 204 });
  const client = createClient(fetchImpl, { baseURL: "https://api.example.com" });
  const res = await client.delete("/x");
  assert.equal(res.data, null);
});

test("create() derives a child client that inherits and can override defaults", async () => {
  let seenURL = "";
  const fetchImpl: FetchLike = async (input) => {
    seenURL = input.toString();
    return jsonResponse({});
  };
  const base = createClient(fetchImpl, { baseURL: "https://api.example.com", headers: { "x-a": "1" } });
  const child = base.create({ baseURL: "https://api.example.com/v2" });
  await child.get("/z");
  assert.equal(seenURL, "https://api.example.com/v2/z");
});
