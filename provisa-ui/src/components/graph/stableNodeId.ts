// Copyright (c) 2026 Kenneth Stott
// Canary: f836cd9f-b296-4b99-832c-f29f497d295b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * A node's stable id, the number the Inspector shows: one per node identity (its label and primary
 * key), kept in this browser's storage. Apart from graph-model, which the server-side renderer
 * (graph-render) bundles for Node, where there is no browser storage.
 */
import { readBulk, writeBulk } from "../../lib/bulkStorage";
import { getStored, setStored, storageKey } from "../../lib/browserStorage";

// ── Stable node ID ────────────────────────────────────────────────────────────
const LS_IDS = "provisa_stable_node_ids";
const LS_CTR = "provisa_stable_node_id_counter";

/** The ids of the bound scope, in memory: the map grows with every node seen, so it is saved in
 * IndexedDB. The counter stays a small setting, read at once, so an id given before the saved map
 * has loaded still takes a fresh number. */
let held: { scope: string; ids: Record<string, number>; loaded: Promise<void> } | null = null;

function heldIds(): NonNullable<typeof held> {
  const scope = storageKey("");
  if (held?.scope === scope) return held;
  const ids: Record<string, number> = {};
  const loaded = readBulk<Record<string, number>>(LS_IDS).then((saved) => {
    if (saved) Object.assign(ids, saved);
  });
  held = { scope, ids, loaded };
  return held;
}

/** Resolves once the saved ids of the bound scope are held. */
export function stableNodeIdsLoaded(): Promise<void> {
  return heldIds().loaded;
}

function _stableKey(
  node: { label: string; properties: Record<string, unknown> },
  pkMap: Record<string, string[]>,
): string {
  const pkCols = pkMap[node.label] ?? [];
  if (pkCols.length > 0) {
    const pkVal = pkCols.map((c) => String(node.properties[c] ?? "")).join(":");
    return `${node.label}::${pkVal}`;
  }
  // Fallback: hash all property values deterministically
  const sorted = Object.keys(node.properties)
    .sort()
    .map((k) => `${k}=${String(node.properties[k])}`)
    .join("|");
  return `${node.label}::${sorted}`;
}

export function getStableNodeId(
  node: { label: string; properties: Record<string, unknown> },
  pkMap: Record<string, string[]>,
): number {
  const key = _stableKey(node, pkMap);
  const { ids } = heldIds();
  if (key in ids) return ids[key];
  const ctr = (parseInt(getStored(LS_CTR) ?? "0", 10) || 0) + 1;
  ids[key] = ctr;
  setStored(LS_CTR, String(ctr));
  void writeBulk(LS_IDS, ids);
  return ctr;
}
