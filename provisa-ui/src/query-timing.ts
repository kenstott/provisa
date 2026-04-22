// Copyright (c) 2026 Kenneth Stott
// Canary: b3c4d5e6-f7a8-9012-bcde-f01234567890
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

/** Shared timing state between createProvisaFetch and ResponseTableOverlay. */
export let lastQueryElapsedMs: number | null = null;

type Listener = (ms: number | null) => void;
const listeners: Set<Listener> = new Set();

export function subscribeQueryTiming(fn: Listener): () => void {
  listeners.add(fn);
  return () => listeners.delete(fn);
}

export function setLastQueryElapsedMs(ms: number | null): void {
  lastQueryElapsedMs = ms;
  listeners.forEach((fn) => fn(ms));
}
