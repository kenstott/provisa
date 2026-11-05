// Copyright (c) 2026 Kenneth Stott
// Canary: b7d3e1a6-52c4-4e09-8f36-1c9a6d0f4e72
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2135: how a dropped class reaches a result. The role's relationships as a graph of labels: a
// Cypher frame's schema relationships, a SPARQL frame's vocabulary edges, one walk for both. A
// parameterized table is a function of its arguments, with no rows for a path to walk: the walk
// goes round one and never ends on one. Two relationships with one join are one edge declared
// twice (SUBMITTED and SUBMITTED_BY), which the walk takes once: the two as tied paths double the
// edges and square the rows.

/** The furthest a drop walks; a class further off has no short path. */
export const MAX_SCHEMA_HOPS = 4;

/** The tied paths a drop adds; the rest are said to be left out. */
export const MAX_SCHEMA_PATHS = 5;

export interface SchemaEdge {
  source: string;
  target: string;
  /** What it joins, the same for a join declared from both ends. */
  join: string;
}

/** One relationship of a path, walked ``from`` one of its ends ``to`` the other. */
export interface SchemaStep<E extends SchemaEdge> {
  edge: E;
  from: string;
  to: string;
}

export interface SchemaPaths<E extends SchemaEdge> {
  /** Every shortest path within MAX_SCHEMA_HOPS, in the order of ``edges``. */
  paths: SchemaStep<E>[][];
  /** The hops of the shortest path at any distance; null where none joins them. */
  hops: number | null;
}

/**
 * Every shortest path from a label of ``from`` to a label of ``to``, either way along a relationship,
 * as a Cypher drop's undirected hop reads one. A path has at least one hop: a label in both reaches
 * itself through a relationship, not by standing still. ``parameterized`` are the labels no step may
 * reach; a label of ``from`` is one the query already holds, with its arguments.
 */
export function shortestSchemaPaths<E extends SchemaEdge>(
  edges: readonly E[],
  from: readonly string[],
  to: readonly string[],
  parameterized: ReadonlySet<string>,
): SchemaPaths<E> {
  const ends = new Map<string, { edge: E; other: string }[]>();
  const add = (label: string, edge: E, other: string) =>
    ends.set(label, [...(ends.get(label) ?? []), { edge, other }]);
  const once = new Map<string, E>();
  for (const edge of edges) if (!once.has(edge.join)) once.set(edge.join, edge);
  for (const edge of once.values()) {
    add(edge.target, edge, edge.source);
    if (edge.source !== edge.target) add(edge.source, edge, edge.target);
  }
  const distance = new Map(from.map((label) => [label, 0]));
  for (let frontier = [...from]; frontier.length > 0; ) {
    const next: string[] = [];
    for (const label of frontier)
      for (const { other } of ends.get(label) ?? [])
        if (!distance.has(other) && !parameterized.has(other)) {
          distance.set(other, (distance.get(label) as number) + 1);
          next.push(other);
        }
    frontier = next;
  }
  const arrivals = to
    .filter((label) => !parameterized.has(label))
    .flatMap((label) =>
      (ends.get(label) ?? []).flatMap(({ edge, other }) => {
        const before = distance.get(other);
        return before === undefined ? [] : [{ edge, from: other, to: label, before }];
      }),
    );
  if (arrivals.length === 0) return { paths: [], hops: null };
  const nearest = Math.min(...arrivals.map((a) => a.before));
  const hops = nearest + 1;
  if (hops > MAX_SCHEMA_HOPS) return { paths: [], hops };
  /** The steps from a label of ``from`` up to ``label``, each a hop nearer than the last. */
  const reaching = (label: string, walked: number): SchemaStep<E>[][] =>
    walked === 0
      ? [[]]
      : (ends.get(label) ?? [])
          .filter(({ other }) => distance.get(other) === walked - 1)
          .flatMap(({ edge, other }) =>
            reaching(other, walked - 1).map((path) => [...path, { edge, from: other, to: label }]),
          );
  const paths = arrivals
    .filter((a) => a.before === nearest)
    .flatMap(({ edge, from: last, to: label }) =>
      reaching(last, nearest).map((path) => [...path, { edge, from: last, to: label }]),
    );
  return { paths, hops };
}
