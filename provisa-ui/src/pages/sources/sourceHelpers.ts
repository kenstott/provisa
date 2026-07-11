// Copyright (c) 2026 Kenneth Stott
// Canary: 55ecea78-4131-4bb4-8a93-71e1bdacea84
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { FederationEngineState } from "../../api/admin";
import { SOURCE_TYPES, TYPE_ALIAS } from "./constants";

export function getCategory(type: string) {
  return SOURCE_TYPES.find((s) => s.value === type)?.category ?? "RDBMS";
}

export const backendType = (uiValue: string) => TYPE_ALIAS[uiValue] ?? uiValue;

export type ReachTag = "live" | "replica" | "unreachable";
export interface ReachInfo {
  tag: ReachTag;
  /** LIVE (attach) and REPLICA (landed) are selectable on the current engine; unreachable is not. */
  selectable: boolean;
  /** Engine labels that read this type LIVE — the hint for a type unreachable on the current engine. */
  liveEngines: string[];
}

// Classify a source type against the CURRENTLY-configured engine, so the dropdown can annotate and
// gate it — LIVE (engine attaches it in place), REPLICA (Provisa lands a refreshed copy the engine
// reads), or unreachable here but LIVE on another engine (REQ-947). Educates on the engine choice.
export function reachInfoFor(uiValue: string, engineState: FederationEngineState | null): ReachInfo {
  if (!engineState) return { tag: "live", selectable: true, liveEngines: [] };
  const t = backendType(uiValue);
  const current = engineState.engines.find((e) => e.key === engineState.current);
  const liveEngines = engineState.engines
    .filter((e) => (e.live_source_types ?? []).includes(t))
    .map((e) => e.label);
  if (current) {
    if ((current.live_source_types ?? []).includes(t))
      return { tag: "live", selectable: true, liveEngines };
    if ((current.reachable_source_types ?? []).includes(t))
      return { tag: "replica", selectable: true, liveEngines };
  }
  return { tag: "unreachable", selectable: false, liveEngines };
}

export function reachSuffix(info: ReachInfo): string {
  if (info.tag === "live") return " · LIVE";
  if (info.tag === "replica") return " · REPLICA";
  return info.liveEngines.length ? ` · LIVE w/ ${info.liveEngines.join(", ")}` : " · unreachable";
}

export function getDefaultPort(type: string) {
  return SOURCE_TYPES.find((s) => s.value === type)?.defaultPort ?? 5432;
}

export const parseFilesPath = (fullPath: string): { transport: string; path: string } => {
  for (const t of ["ftp://", "sftp://", "s3a://", "s3://", "http://", "https://", "sharepoint://"]) {
    if (fullPath.startsWith(t)) return { transport: t, path: fullPath.slice(t.length) };
  }
  return { transport: "file://", path: fullPath };
};
