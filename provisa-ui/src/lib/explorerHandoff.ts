// Copyright (c) 2026 Kenneth Stott
// Canary: b37ddef2-bc5b-493e-8ce7-a52fec8d12b2
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/** Why a handed-off query opens unrun, waiting for Run (REQ-1159, amended 2026-11-28): it is a
 * command that writes, or it still carries parameter placeholders the user must fill
 * (REQ-2252). */
export type ExplorerHold = "write" | "parameters";

/** The explorers that run every query they are handed unless it is held: the Graph page, as
 * Cypher and as SPARQL (REQ-1159). They take no autoRun. */
const RUNS_UNLESS_HELD = new Set(["/graph", "/sparql"]);

/** ``payload`` as router state for the explorer at ``route``, held for ``hold`` or, when null,
 * to run. The Graph page runs it unless it carries ``hold``; the other explorers run it only
 * when told ``autoRun``. */
export function explorerHandoff(
  route: string,
  payload: Record<string, unknown>,
  hold: ExplorerHold | null,
): Record<string, unknown> {
  if (RUNS_UNLESS_HELD.has(route)) return hold === null ? payload : { ...payload, hold };
  return { ...payload, autoRun: hold === null };
}
