import { isSignal } from '@oarkflow/lithe/core';

// Lithe components run once: a component function returns its tree a
// single time, and only values it hands to the DOM layer as a signal or a
// plain closure stay live afterwards (see src/dom/dom.js's `__mountChild`,
// which special-cases `isSignal(value) || typeof value === 'function'` for
// both children and attributes). A prop read as `props.count.value` inside
// a component body is a one-time snapshot; forwarding `props.count` itself,
// or a closure that reads it, keeps it reactive.
//
// `MaybeReactive<T>` documents that contract for a small shared component's
// prop, and `resolve` reads whichever form was passed - a plain value, a
// signal, or a getter - so a component only has to write the read once.
export type MaybeReactive<T> = T | (() => T) | { value: T };

export function resolve<T>(input: MaybeReactive<T>): T {
  if (typeof input === 'function') return (input as () => T)();
  if (isSignal(input)) return (input as { value: T }).value;
  return input as T;
}
