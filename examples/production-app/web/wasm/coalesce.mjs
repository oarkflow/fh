export function secureRequestKey(method, url, credentials, mode, headers, requestContext = []) {
    const normalizedHeaders = headers
        .map(([name, value]) => [name.toLowerCase(), value])
        .sort(([leftName, leftValue], [rightName, rightValue]) =>
            leftName.localeCompare(rightName) || leftValue.localeCompare(rightValue));
    return JSON.stringify([method, url, credentials, mode, normalizedHeaders, requestContext]);
}

export function createRequestCoalescer() {
    const inFlight = new Map();
    return {
        run(key, signal, operation) {
            let entry = inFlight.get(key);
            if (!entry) {
                entry = { controller: new AbortController(), listeners: [], promise: null };
                inFlight.set(key, entry);
                entry.promise = Promise.resolve()
                    .then(() => operation(entry.controller.signal))
                    .finally(() => {
                        if (inFlight.get(key) === entry)
                            inFlight.delete(key);
                        for (const [callerSignal, listener] of entry.listeners)
                            callerSignal.removeEventListener("abort", listener);
                        entry.listeners.length = 0;
                    });
            }
            if (signal) {
                const abortShared = () => entry.controller.abort(signal.reason);
                if (signal.aborted)
                    abortShared();
                else {
                    signal.addEventListener("abort", abortShared, { once: true });
                    entry.listeners.push([signal, abortShared]);
                }
            }
            return entry.promise;
        },
    };
}
