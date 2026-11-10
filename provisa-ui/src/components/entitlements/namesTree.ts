// Copyright (c) 2026 Kenneth Stott
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2219: the semantic objects a list names, as a tree of the names table's client-facing SQL
// names: domain, then table (its alias where it has one), then column. An identifier is the dotted
// path of a node; a shorter one selects everything beneath it (matches() in
// provisa/entitlements/index.py), so checking a node stands for its children.

import { domainToSqlName } from "../../naming";

export interface NamesNode {
  /** The identifier this node is listed as. */
  id: string;
  /** The last segment, which the tree shows. */
  label: string;
  children: NamesNode[];
}

export interface NamedDomain {
  id: string;
}

export interface NamedTable {
  domainId: string;
  tableName: string;
  alias?: string | null;
  columns: { columnName: string; alias?: string | null }[];
}

const byLabel = (a: NamesNode, b: NamesNode) => a.label.localeCompare(b.label);

/** The tree down to `depth` levels: 1 domains, 2 their tables, 3 their columns. */
export function buildNamesTree(
  domains: readonly NamedDomain[],
  tables: readonly NamedTable[],
  depth: 1 | 2 | 3,
): NamesNode[] {
  const roots = new Map<string, NamesNode>();
  for (const d of domains) {
    const name = domainToSqlName(d.id);
    roots.set(name, { id: name, label: name, children: [] });
  }
  if (depth >= 2) {
    for (const t of tables) {
      const domain = roots.get(domainToSqlName(t.domainId));
      // A table's domain is registered before the table; one the domain list lacks is not in
      // this model's names table either.
      if (domain === undefined) continue;
      const tableName = t.alias || t.tableName;
      const table: NamesNode = {
        id: `${domain.id}.${tableName}`,
        label: tableName,
        children:
          depth === 3
            ? t.columns.map((c) => {
                const columnName = c.alias || c.columnName;
                return {
                  id: `${domain.id}.${tableName}.${columnName}`,
                  label: columnName,
                  children: [],
                };
              })
            : [],
      };
      table.children.sort(byLabel);
      domain.children.push(table);
    }
  }
  const out = [...roots.values()];
  for (const d of out) d.children.sort(byLabel);
  return out.sort(byLabel);
}

/** Whether `identifier` selects `id`: the same path, or a shorter one above it (case ignored). */
export function covers(identifier: string, id: string): boolean {
  const wanted = identifier.toLowerCase().split(".");
  const path = id.toLowerCase().split(".");
  return wanted.length <= path.length && wanted.every((w, i) => w === "*" || w === path[i]);
}

/** Every node id of the tree. */
export function allIds(nodes: readonly NamesNode[]): Set<string> {
  const out = new Set<string>();
  const walk = (ns: readonly NamesNode[]) => {
    for (const n of ns) {
      out.add(n.id.toLowerCase());
      walk(n.children);
    }
  };
  walk(nodes);
  return out;
}

/** Check `node`: list it, and drop what it now covers. */
export function checkNode(value: readonly string[], node: NamesNode): string[] {
  return [...value.filter((v) => !covers(node.id, v)), node.id];
}

/**
 * Uncheck `node`. When an ancestor listed covers it, the ancestor is replaced by its other
 * children, so unchecking one table of a checked domain keeps the rest of the domain.
 */
export function uncheckNode(
  value: readonly string[],
  node: NamesNode,
  roots: readonly NamesNode[],
): string[] {
  const listed = value.find((v) => covers(v, node.id) && v.toLowerCase() !== node.id.toLowerCase());
  const own = value.filter((v) => !covers(node.id, v));
  if (listed === undefined) return own;
  // Replace the listed ancestor by what it covered, less the node: at each level from the
  // ancestor down to the node's parent, the siblings of the path's next node.
  const out = own.filter((v) => v !== listed);
  const same = (a: string, b: string) => a.toLowerCase() === b.toLowerCase();
  const path = node.id.split(".");
  const from = listed.split(".").length;
  let level: readonly NamesNode[] = roots;
  for (let i = 0; i < path.length - 1; i++) {
    const here = level.find((n) => same(n.id, path.slice(0, i + 1).join(".")));
    if (here === undefined) return out;
    if (i + 1 >= from) {
      const next = path.slice(0, i + 2).join(".");
      for (const c of here.children) if (!same(c.id, next)) out.push(c.id);
    }
    level = here.children;
  }
  return out;
}
