// Copyright (c) 2026 Kenneth Stott
// Canary: 7f3a2b1c-4d5e-6f7a-8b9c-0d1e2f3a4b5c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

export interface SourceStat {
  field: string;
  source: string;
  strategy: string;
  elapsed_ms: number;
  rows: number;
  cache_hit?: boolean;
}

export interface QueryStats {
  total_elapsed_ms: number;
  sources: SourceStat[];
  mermaid?: string;
}

type Listener = (stats: QueryStats | null) => void;

let current: QueryStats | null = null;
const listeners = new Set<Listener>();

export function setCurrentQueryStats(stats: QueryStats | null): void {
  current = stats;
  listeners.forEach((fn) => fn(stats));
}

export function subscribeQueryStats(fn: Listener): () => void {
  listeners.add(fn);
  fn(current);
  return () => listeners.delete(fn);
}
