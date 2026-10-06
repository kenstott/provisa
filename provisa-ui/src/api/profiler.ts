// Copyright (c) 2026 Kenneth Stott
// Canary: 58e2c0a7-4b19-4d63-8f2e-a1c7d9b3e604
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1934: the Data Profiler's admin routes (provisa/api/admin/profiler_router.py).

import { serverMessage, requestFailed } from "../i18n/serverMessage";

const API_BASE = import.meta.env.VITE_API_BASE || "";

export interface Profiler {
  id: string;
  cron: string;
  sampleAboveCells: number | null;
  lowCardinalityMax: number;
  members: string[];
}

// A result table's field with the grants and mask the registration form starts from, derived from
// the profiled table's rules as they are now.
export interface ProfilerCatalogColumn {
  name: string;
  dataType: string;
  description: string;
  visibleTo: string[];
  unmaskedTo: string[];
  maskType: string | null;
}

// A row rule a result table is registered with: the described columns ``roleId`` may see.
export interface ProfilerRowRule {
  roleId: string;
  filter: string;
}

export interface ProfilerCatalogTable {
  kind: string;
  tableName: string;
  columns: ProfilerCatalogColumn[];
  rowRules: ProfilerRowRule[];
}

export interface ProfilerCatalogEntry {
  member: string;
  memberId: number;
  tables: ProfilerCatalogTable[];
}

export interface ProfileRun {
  run_id: string;
  run_time: string;
  region: string | null;
  profiled_table: string;
  row_count: number | null;
  sampled: boolean | null;
  sample_fraction: number | null;
  profiled_rows: number | null;
  duration_ms: number;
  status: "succeeded" | "failed";
  error: string | null;
}

export type ProfileRunResults = Record<string, Record<string, unknown>[]>;

export interface SourceRunOutcome {
  table: string;
  run_id: string | null;
  error: string | null;
}

async function call<T>(
  op: string,
  path: string,
  method: "GET" | "POST" = "GET",
  headers: Record<string, string> = {},
): Promise<T> {
  const resp = await fetch(`${API_BASE}${path}`, { method, headers });
  if (!resp.ok) {
    const body = await resp.json().catch(() => ({ detail: resp.statusText }));
    throw new Error(serverMessage(body, requestFailed(op, resp.status)));
  }
  return resp.json() as Promise<T>;
}

export const fetchProfilers = () => call<Profiler[]>("fetchProfilers", "/admin/profilers");

export const runProfiler = (sourceId: string) =>
  call<SourceRunOutcome[]>(
    "runProfiler",
    `/admin/profilers/${encodeURIComponent(sourceId)}/run`,
    "POST",
  );

export const fetchProfilerCatalog = (sourceId: string) =>
  call<ProfilerCatalogEntry[]>(
    "fetchProfilerCatalog",
    `/admin/profilers/${encodeURIComponent(sourceId)}/catalog`,
  );

export const runProfileNow = (tableId: number) =>
  call<{ runId: string; rowCount: number | null; profiledRows: number | null }>(
    "runProfileNow",
    `/admin/tables/${tableId}/profile-runs`,
    "POST",
  );

export const fetchProfileRuns = (tableId: number) =>
  call<ProfileRun[]>("fetchProfileRuns", `/admin/tables/${tableId}/profile-runs`);

// The run as ``role`` may see it: the server makes it safe for its viewer (REQ-1934).
export const fetchProfileRun = (tableId: number, runId: string, role: string) =>
  call<ProfileRunResults>(
    "fetchProfileRun",
    `/admin/tables/${tableId}/profile-runs/${encodeURIComponent(runId)}`,
    "GET",
    { "X-Provisa-Role": role },
  );
