// Copyright (c) 2026 Kenneth Stott
// Canary: 8a668514-61f7-49cd-87cb-ee62bef33d0c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { TFunction } from "i18next";
import { ownColumnName, type FrameData, type GNode } from "../graph-model";

/** A done frame's header count: its nodes and relationships when it draws any, else the rows of a
 * table answer (a read of values only), never "0 nodes · 0 rels" over rows. */
export function frameMetaText(frame: FrameData, t: TFunction): string {
  const drawn = frame.nodes.size > 0 || frame.edges.size > 0;
  if (!drawn && frame.rows.length > 0) {
    return frame.elapsed !== undefined
      ? t("graphFrame.metaRowsElapsed", { count: frame.rows.length, elapsed: frame.elapsed })
      : t("graphFrame.metaRows", { count: frame.rows.length });
  }
  return frame.elapsed !== undefined
    ? t("graphFrame.metaCountElapsed", {
        nodes: frame.nodes.size,
        edges: frame.edges.size,
        elapsed: frame.elapsed,
      })
    : t("graphFrame.metaCount", { nodes: frame.nodes.size, edges: frame.edges.size });
}

/** What a class has, by the key a node holds each property under, before its nodes were read. */
export interface ClassProperties {
  properties: string[];
  propertyNames: Record<string, string>;
}

const NOT_GROUPABLE = new Set([
  "scl1",
  "scl2",
  "scl3",
  "l1Cluster",
  "l2Cluster",
  "l3Cluster",
  "degIn",
  "degOut",
  "degTotal",
]);

/** A frame's Group list, alphabetically: the domain, the schema levels and degrees that differ, and
 * every property any node has or its class declares, named as the Inspector names it. A name two
 * properties share stays as written, so each can be told apart. */
export function groupOptions(
  nodes: Map<string, GNode>,
  classProperties: Record<string, ClassProperties> = {},
): { value: string; label: string }[] {
  if (nodes.size === 0) return [];
  const differs = (prop: string): boolean => {
    const vals = new Set<string>();
    nodes.forEach((n) => {
      const v = n.properties[prop];
      if (v !== null && v !== undefined) vals.add(String(v));
    });
    return vals.size > 1;
  };
  const named = new Map<string, string>();
  nodes.forEach((n) => {
    for (const k of Object.keys(n.properties))
      if (!NOT_GROUPABLE.has(k)) named.set(k, ownColumnName(n, k) ?? k);
    const declared = classProperties[n.label];
    if (!declared) return;
    for (const k of declared.properties) named.set(k, declared.propertyNames[k] ?? k);
  });
  const shared = new Map<string, number>();
  for (const name of named.values()) shared.set(name, (shared.get(name) ?? 0) + 1);
  const virtuals = [
    "domain",
    ...(["schema_L1", "schema_L2", "schema_L3"] as const).filter((_, i) => differs(`scl${i + 1}`)),
    ...(["degIn", "degOut", "degTotal"] as const).filter(differs),
  ];
  return [
    ...virtuals.map((value) => ({ value, label: value })),
    ...[...named].map(([value, name]) => ({
      value,
      label: (shared.get(name) ?? 0) > 1 ? value : name,
    })),
  ].sort((a, b) => a.label.localeCompare(b.label));
}

/** A frame's Code view: its query, what it answered, the SQL it ran and how long each part took. */
export function frameCode(frame: FrameData): Record<string, unknown> {
  type SourceEntry = {
    field?: string;
    source?: string;
    strategy?: string;
    elapsed_ms?: number;
    rows?: number;
    physical_sql?: string;
    cache_hit?: boolean;
  };
  const stats = frame.queryStats as
    | { total_elapsed_ms?: number; sources?: SourceEntry[]; mermaid?: string }
    | undefined;
  const sqlQueries = (stats?.sources ?? [])
    .filter((s) => s.physical_sql)
    .map((s) => ({
      field: s.field,
      source: s.source,
      strategy: s.strategy,
      elapsed_ms: s.elapsed_ms,
      rows: s.rows,
      ...(s.cache_hit ? { cache_hit: true } : {}),
      sql: s.physical_sql,
    }));
  return {
    query: frame.query,
    summary: {
      nodes: frame.nodes.size,
      relationships: frame.edges.size,
      rows: frame.rows.length,
      columns: frame.columns,
      ...(frame.elapsed !== undefined ? { elapsed_ms: frame.elapsed } : {}),
    },
    ...(sqlQueries.length > 0 ? { sql: sqlQueries } : {}),
    ...(stats
      ? {
          stats: {
            total_elapsed_ms: stats.total_elapsed_ms,
            sources: (stats.sources ?? []).map(({ physical_sql: _, ...rest }) => rest),
          },
        }
      : {}),
  };
}
