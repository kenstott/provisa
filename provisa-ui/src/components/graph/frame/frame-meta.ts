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
import type { FrameData, GNode } from "../graph-model";

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

/** Properties available for grouping: virtual schema_L1/L2/L3 (mapped to scl1/scl2/scl3) followed
 * by any scalar property with more than one distinct value. */
export function groupableAttributes(nodes: Map<string, GNode>): string[] {
  if (nodes.size === 0) return [];
  const SKIP = new Set([
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
  const schemaVirtuals: string[] = [];
  for (const [virtName, prop] of [
    ["schema_L1", "scl1"],
    ["schema_L2", "scl2"],
    ["schema_L3", "scl3"],
  ] as const) {
    const vals = new Set<string>();
    nodes.forEach((n) => {
      const v = n.properties[prop];
      if (v !== null && v !== undefined) vals.add(String(v));
    });
    if (vals.size > 1) schemaVirtuals.push(virtName);
  }
  const degreeVirtuals: string[] = [];
  for (const key of ["degIn", "degOut", "degTotal"] as const) {
    const vals = new Set<string>();
    nodes.forEach((n) => {
      const v = n.properties[key];
      if (v !== null && v !== undefined) vals.add(String(v));
    });
    if (vals.size > 1) degreeVirtuals.push(key);
  }
  const counts = new Map<string, Set<string>>();
  nodes.forEach((n) => {
    Object.entries(n.properties).forEach(([k, v]) => {
      if (SKIP.has(k) || v === null || v === undefined) return;
      if (typeof v === "object") return;
      if (!counts.has(k)) counts.set(k, new Set());
      counts.get(k)!.add(String(v));
    });
  });
  const regularAttrs = [...counts.entries()].filter(([, vals]) => vals.size > 1).map(([k]) => k);
  return ["domain", ...schemaVirtuals, ...degreeVirtuals, ...regularAttrs].sort((a, b) =>
    a.localeCompare(b),
  );
}
