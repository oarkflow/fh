import assert from "node:assert/strict";
import test from "node:test";
import { createRequestCoalescer, secureRequestKey } from "./coalesce.mjs";

test("overlapping identical requests share one operation and completed work is not reused", async () => {
    const coalescer = createRequestCoalescer();
    let calls = 0;
    let release;
    const gate = new Promise((resolve) => { release = resolve; });
    const operation = () => {
        calls += 1;
        return gate.then(() => ({ body: new Uint8Array([1, 2, 3]) }));
    };
    const first = coalescer.run("same", undefined, operation);
    const second = coalescer.run("same", undefined, operation);
    await Promise.resolve();
    release();
    assert.deepEqual(await first, await second);
    assert.equal(calls, 1);
    await coalescer.run("same", undefined, operation);
    assert.equal(calls, 2);
});

test("request identity includes URL, credentials, mode, and normalized headers", () => {
    const base = secureRequestKey("GET", "/api/notes?page=1", "include", "same-origin", [["Accept", "application/json"]]);
    assert.notEqual(base, secureRequestKey("HEAD", "/api/notes?page=1", "include", "same-origin", [["Accept", "application/json"]]));
    assert.notEqual(base, secureRequestKey("GET", "/api/notes?page=2", "include", "same-origin", [["Accept", "application/json"]]));
    assert.notEqual(base, secureRequestKey("GET", "/api/notes?page=1", "omit", "same-origin", [["Accept", "application/json"]]));
    assert.notEqual(base, secureRequestKey("GET", "/api/notes?page=1", "include", "cors", [["Accept", "application/json"]]));
    assert.notEqual(base, secureRequestKey("GET", "/api/notes?page=1", "include", "same-origin", [["Accept", "text/plain"]]));
    assert.equal(base, secureRequestKey("GET", "/api/notes?page=1", "include", "same-origin", [["accept", "application/json"]]));
    assert.notEqual(base, secureRequestKey("GET", "/api/notes?page=1#top", "include", "same-origin", [["Accept", "application/json"]]));
    assert.notEqual(base, secureRequestKey("GET", "/api/notes?page=1", "include", "same-origin", [["Accept", "application/json"]], ["tenant-a"]));
});

test("aborting any coalesced caller aborts the shared operation", async () => {
    const coalescer = createRequestCoalescer();
    const controllerA = new AbortController();
    const controllerB = new AbortController();
    const operation = (signal) => new Promise((resolve, reject) => {
        signal.addEventListener("abort", () => reject(signal.reason), { once: true });
    });
    const first = coalescer.run("same", controllerA.signal, operation);
    const second = coalescer.run("same", controllerB.signal, operation);
    await Promise.resolve();
    const reason = new Error("caller canceled");
    controllerB.abort(reason);
    const results = await Promise.allSettled([first, second]);
    assert.deepEqual(results.map(({ status }) => status), ["rejected", "rejected"]);
    assert.equal(results[0].reason, reason);
    assert.equal(results[1].reason, reason);
});
