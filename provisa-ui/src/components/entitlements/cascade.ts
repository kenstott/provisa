// Copyright (c) 2026 Kenneth Stott
// Canary: d23127ca-5918-480c-8420-90958069c61e
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2219: the entries of a domains, tables or columns list as picked in cascading pickers, one
// per level of the names tree: domains, then the tables of the chosen domains, then the columns of
// the chosen tables. A pick with nothing chosen beneath it stands for all of it, as a shorter
// identifier selects everything beneath it on the server (matches() in
// provisa/entitlements/index.py); the identifiers stored are the same as before. A listed value
// the tree does not hold is kept, never dropped.

import type { NamesNode } from "../../api/entitlements";
import { WILDCARD } from "./kinds";

/** A node of the names tree with its level (0 domain, 1 table, 2 column) and its parent's id. */
export interface IndexedNode {
  node: NamesNode;
  level: number;
  parent: string | null;
}

/** The names tree by lower-cased id, as a listed identifier is matched (case ignored). */
export type NamesIndex = ReadonlyMap<string, IndexedNode>;

export function indexNames(roots: readonly NamesNode[]): NamesIndex {
  const out = new Map<string, IndexedNode>();
  const walk = (nodes: readonly NamesNode[], level: number, parent: string | null) => {
    for (const n of nodes) {
      out.set(n.id.toLowerCase(), { node: n, level, parent });
      walk(n.children, level + 1, n.id);
    }
  };
  walk(roots, 0, null);
  return out;
}

/** What is chosen at each level, by the model's spelling, and the values the tree lacks. */
export interface Selection {
  /** Chosen ids by level: [domains, tables, columns]. */
  levels: string[][];
  /** Listed values the tree does not hold (a pattern, or a name the model no longer has). */
  unknown: string[];
}

export const emptySelection = (depth: number): Selection => ({
  levels: Array.from({ length: depth }, () => []),
  unknown: [],
});

/**
 * The pickers' choice for a stored list. A listed table shows its domain chosen, a listed column
 * its table and domain, so editing starts from where the entries are. An entry another entry
 * already covers (a table under a listed domain) is not shown on its own: picking the domain
 * with a table beneath it would narrow the list to the table.
 */
export function selectionOf(value: readonly string[], index: NamesIndex, depth: number): Selection {
  const sel = emptySelection(depth);
  const found: IndexedNode[] = [];
  for (const v of value) {
    if (v === WILDCARD) continue;
    const hit = index.get(v.toLowerCase());
    if (hit === undefined || hit.level >= depth) {
      if (!sel.unknown.includes(v)) sel.unknown.push(v);
    } else if (!found.includes(hit)) {
      found.push(hit);
    }
  }
  const ancestors = (n: IndexedNode): IndexedNode[] => {
    const out: IndexedNode[] = [];
    for (let p = n.parent; p !== null; ) {
      const up = index.get(p.toLowerCase());
      if (up === undefined) break;
      out.push(up);
      p = up.parent;
    }
    return out;
  };
  for (const hit of found) {
    if (ancestors(hit).some((a) => found.includes(a))) continue;
    for (const n of [...ancestors(hit).reverse(), hit]) {
      if (!sel.levels[n.level].includes(n.node.id)) sel.levels[n.level].push(n.node.id);
    }
  }
  return sel;
}

/**
 * The stored list for the pickers' choice: each chosen id with nothing chosen beneath it, deepest
 * level last, then the values the tree lacks.
 */
export function valueOf(sel: Selection, index: NamesIndex): string[] {
  const parentOf = (id: string) => index.get(id.toLowerCase())?.parent ?? null;
  const out: string[] = [];
  sel.levels.forEach((ids, level) => {
    const below = new Set((sel.levels[level + 1] ?? []).map(parentOf));
    for (const id of ids) if (!below.has(id)) out.push(id);
  });
  return [...out, ...sel.unknown];
}

/** Whether `id` is `ancestor` or lies beneath it in the tree. */
function within(id: string, ancestor: string, index: NamesIndex): boolean {
  for (let p: string | null = id; p !== null; p = index.get(p.toLowerCase())?.parent ?? null) {
    if (p === ancestor) return true;
  }
  return false;
}

/** Choose `id` at its level, or un-choose it with everything chosen beneath it. */
export function toggle(sel: Selection, id: string, index: NamesIndex): Selection {
  const hit = index.get(id.toLowerCase());
  if (hit === undefined) return sel;
  const chosen = sel.levels[hit.level].includes(hit.node.id);
  if (!chosen) {
    return {
      ...sel,
      levels: sel.levels.map((ids, l) => (l === hit.level ? [...ids, hit.node.id] : ids)),
    };
  }
  return {
    ...sel,
    levels: sel.levels.map((ids, l) =>
      l < hit.level ? ids : ids.filter((x) => !within(x, hit.node.id, index)),
    ),
  };
}

/** Drop a value the tree does not hold. */
export function dropUnknown(sel: Selection, value: string): Selection {
  return { ...sel, unknown: sel.unknown.filter((v) => v !== value) };
}

/** The level a value the tree lacks is shown at: by its dotted parts, at most the kind's depth. */
export function levelOfUnknown(value: string, depth: number): number {
  return Math.min(value.split(".").length, depth) - 1;
}

/** A group of choices: the parent they belong to (null for domains) and its children. */
export interface ChoiceGroup {
  parent: string | null;
  options: NamesNode[];
}

/**
 * The choices at `level`: every domain; the tables of the chosen domains (of every domain when
 * none is chosen); the columns of the chosen tables (none when no table is chosen). Grouped by
 * the parent they belong to.
 */
export function choicesAt(
  level: number,
  roots: readonly NamesNode[],
  sel: Selection,
  index: NamesIndex,
): ChoiceGroup[] {
  if (level === 0) return [{ parent: null, options: [...roots] }];
  const above = sel.levels[level - 1];
  let parents: NamesNode[];
  if (above.length > 0) {
    parents = above.flatMap((id) => {
      const hit = index.get(id.toLowerCase());
      return hit === undefined ? [] : [hit.node];
    });
  } else if (level === 1) {
    parents = [...roots];
  } else {
    parents = [];
  }
  return parents
    .map((p) => ({ parent: p.id, options: p.children }))
    .filter((g) => g.options.length > 0);
}

/** The choices whose id or label holds `needle` (case ignored), at most `limit` of them, with
 *  the count of all that match. */
export function filterChoices(
  groups: readonly ChoiceGroup[],
  needle: string,
  limit: number,
): { groups: ChoiceGroup[]; shown: number; total: number } {
  const n = needle.trim().toLowerCase();
  let shown = 0;
  let total = 0;
  const out: ChoiceGroup[] = [];
  for (const g of groups) {
    // An id is the dotted path, so a search for a domain or table also finds what is beneath it.
    const hits = g.options.filter((o) => n === "" || o.id.toLowerCase().includes(n));
    total += hits.length;
    const room = Math.max(0, limit - shown);
    if (room > 0 && hits.length > 0) {
      const taken = hits.slice(0, room);
      shown += taken.length;
      out.push({ parent: g.parent, options: taken });
    }
  }
  return { groups: out, shown, total };
}
