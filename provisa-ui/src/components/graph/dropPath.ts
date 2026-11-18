// Copyright (c) 2026 Kenneth Stott
// Canary: 7c2e9a41-d38b-4f06-a5e1-9b4d0c7f2e83
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2135: the one path finder a drop onto a result uses, in the Cypher and the SPARQL view alike;
// each view writes the path in its own language. The path keeps to the selected domains: it never
// passes through, starts at or ends on a label of a domain the Domains picker leaves out, nor one of
// a system domain (meta, ontology, ops) unless the query or the drop is of that domain. It is one
// path from one of the result's labels, so the query never holds two branches with no condition
// relating them. Where that path is one hop, every relationship joining the same two labels is a hop
// of it (REQ-1586: a junction backs several types between the very same two tables).

import type { DropNotice } from "./graph-model";
import type { SchemaNodeLabel } from "./graph-schema-types";
import {
  MAX_SCHEMA_HOPS,
  shortestSchemaPaths,
  type SchemaEdge,
  type SchemaStep,
} from "./schemaPath";

/** What /data/graph-schema says of a label that a path's way through depends on. */
export type ScopedLabel = Pick<
  SchemaNodeLabel,
  "domainLabel" | "domainId" | "tableLabel" | "systemDomain"
>;

/**
 * Where a path may go: the role's labels, and the domains the Domains picker selects (none selected
 * reads as every domain, as the sidebar reads it).
 */
export interface PathScope {
  nodeLabels: readonly ScopedLabel[];
  checkedDomains: ReadonlySet<string>;
}

/** A label as a query writes it: ``Domain:Table``, or the table's alone where it has no domain. */
export function compoundLabelOf(n: Pick<ScopedLabel, "domainLabel" | "tableLabel">): string {
  return n.domainLabel ? `${n.domainLabel}:${n.tableLabel}` : n.tableLabel;
}

/** The labels of the selected domains: the sidebar's list. */
export function inSelectedDomains<L extends ScopedLabel>(
  nodeLabels: readonly L[],
  checkedDomains: ReadonlySet<string>,
): L[] {
  return checkedDomains.size === 0
    ? [...nodeLabels]
    : nodeLabels.filter((n) => !n.domainId || checkedDomains.has(n.domainId));
}

/**
 * The labels a path between ``ends`` may use: those of the selected domains, less a system domain's
 * unless one of ``ends`` is of that domain. The model's own metadata joins every table to every
 * other (HAS_TABLE through its registration), and is no relationship of the data the reader asked
 * about.
 */
export function pathThrough(scope: PathScope, ends: readonly string[]): Set<string> {
  const endDomains = new Set(
    scope.nodeLabels.filter((n) => ends.includes(compoundLabelOf(n))).map((n) => n.domainId),
  );
  return new Set(
    inSelectedDomains(scope.nodeLabels, scope.checkedDomains)
      .filter((n) => !n.systemDomain || endDomains.has(n.domainId))
      .map(compoundLabelOf),
  );
}

/** The path a drop adds: from ``start``, one label sequence; at one hop, each relationship of it. */
export interface DropPath<E extends SchemaEdge> {
  start: string;
  paths: SchemaStep<E>[][];
}

/** No path within the selected domains, and the hops of the nearest at any distance (null: none). */
export interface NoDropPath {
  hops: number | null;
}

/**
 * The path from a label of ``held`` (the result's, in the query's order) to a label of ``targets``
 * along ``edges``, within ``scope`` and never through a label of ``parameterized``.
 */
export function dropPath<E extends SchemaEdge>(
  edges: readonly E[],
  held: readonly string[],
  targets: readonly string[],
  parameterized: ReadonlySet<string>,
  scope: PathScope,
): DropPath<E> | NoDropPath {
  const through = pathThrough(scope, [...held, ...targets]);
  const { paths, hops } = shortestSchemaPaths(
    edges,
    held.filter((l) => through.has(l)),
    targets.filter((l) => through.has(l)),
    parameterized,
    through,
  );
  if (paths.length === 0) return { hops };
  const [first] = paths;
  const start = first[0].from;
  if (hops !== 1) return { start, paths: [first] };
  const end = first[0].to;
  return { start, paths: paths.filter(([step]) => step.from === start && step.to === end) };
}

/** The refusal of a drop no path within the selected domains joins, by name. */
export function noDropPath(name: string, classes: string, hops: number | null): DropNotice {
  return hops === null
    ? { key: "graphFrame.sparqlDropNoPath", values: { name, classes } }
    : {
        key: "graphFrame.sparqlDropTooFar",
        values: { name, classes, hops, most: MAX_SCHEMA_HOPS },
      };
}
