// Copyright (c) 2026 Kenneth Stott
// Canary: 5b1e7c02-93d4-4e18-b6f2-0c8a4d2e9f13
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/** A `Storage` that holds what fits in `capacity` characters, as a browser's does. */
export function fullStorage(capacity: number, seed: Record<string, string> = {}): Storage {
  const held = new Map(Object.entries(seed));
  const used = () => [...held].reduce((n, [k, v]) => n + k.length + v.length, 0);
  return {
    get length() {
      return held.size;
    },
    key: (i) => [...held.keys()][i] ?? null,
    getItem: (k) => held.get(k) ?? null,
    setItem: (k, v) => {
      const room = capacity - used() + (held.has(k) ? k.length + held.get(k)!.length : 0);
      if (k.length + v.length > room) {
        throw new DOMException(`Setting the value of '${k}' exceeded the quota`, "QuotaExceededError");
      }
      held.set(k, v);
    },
    removeItem: (k) => void held.delete(k),
    clear: () => held.clear(),
  };
}

/** A `Storage` that throws on every call, as blocked site data does. */
export function throwingStorage(): Storage {
  const refuse = (): never => {
    throw new DOMException("The operation is insecure.", "SecurityError");
  };
  return {
    get length() {
      return refuse();
    },
    key: refuse,
    getItem: refuse,
    setItem: refuse,
    removeItem: refuse,
    clear: refuse,
  };
}

/** Make `name` the window's storage until the returned function puts the real one back. */
export function installStorage(
  name: "localStorage" | "sessionStorage",
  storage: Storage,
): () => void {
  const real = window[name];
  Object.defineProperty(window, name, { configurable: true, get: () => storage });
  return () => Object.defineProperty(window, name, { configurable: true, value: real });
}
