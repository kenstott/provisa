// Copyright (c) 2026 Kenneth Stott
// Canary: b37ddef2-bc5b-493e-8ce7-a52fec8d12b2
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/** The explorers that run every query they are handed: the Graph page, as Cypher and as SPARQL
 * (maintainer 2026-11-28). They take no autoRun. */
const RUNS_EVERY_HANDOFF = new Set(["/graph", "/sparql"]);

/** ``payload`` as router state for the explorer at ``route``: the explorers that run only when
 * asked are asked (``run``); the Graph page runs it whatever it is told. */
export function explorerHandoff(
  route: string,
  payload: Record<string, unknown>,
  run: boolean,
): Record<string, unknown> {
  return RUNS_EVERY_HANDOFF.has(route) ? payload : { ...payload, autoRun: run };
}
