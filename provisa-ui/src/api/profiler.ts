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
  driftWindow: number;
  driftSeason: string;
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
  /** REQ-1934: how the rows were read; null when the run failed before reading them. */
  sample_method: "whole" | "block" | "key_range" | "random" | null;
  target_fraction: number | null;
  /** The realised share: profiled_rows / row_count. */
  sample_fraction: number | null;
  /** Each sample read, as JSON [{percent, rows}]. */
  sample_attempts: string | null;
  profiled_rows: number | null;
  /** REQ-1934: the previous successful run this run is compared with; null for the first. */
  previous_run_id: string | null;
  /** Rows repeating an earlier row in every profiled column, and their share of profiled_rows. */
  duplicate_rows: number | null;
  duplicate_share: number | null;
  /** Key values held by more than one row; null where the table declares no key. */
  key_duplicates: number | null;
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
  method: "GET" | "POST" | "DELETE" = "GET",
  headers: Record<string, string> = {},
  body?: unknown,
): Promise<T> {
  const resp = await fetch(`${API_BASE}${path}`, {
    method,
    headers: body === undefined ? headers : { ...headers, "Content-Type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
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

/** One measure of a run, as its drift row identifies it (REQ-1934 DRIFT ACROSS RUNS). */
export interface MeasureKey {
  scope: string;
  measure: string;
  column: string | null;
  subject: string | null;
}

export interface MeasurePoint {
  runId: string;
  runTime: string;
  value: number | null;
  current: boolean;
}

export interface MeasureHistory {
  drift: Record<string, unknown>;
  points: MeasurePoint[];
}

// One measure across the run's drift window, oldest first, as ``role`` may see it.
export const fetchMeasureHistory = (
  tableId: number,
  runId: string,
  role: string,
  key: MeasureKey,
) => {
  const q = new URLSearchParams({ scope: key.scope, measure: key.measure });
  if (key.column != null) q.set("column", key.column);
  if (key.subject != null) q.set("subject", key.subject);
  return call<MeasureHistory>(
    "fetchMeasureHistory",
    `/admin/tables/${tableId}/profile-runs/${encodeURIComponent(runId)}/history?${q}`,
    "GET",
    { "X-Provisa-Role": role },
  );
};

// The run as ``role`` may see it: the server makes it safe for its viewer (REQ-1934).
export const fetchProfileRun = (tableId: number, runId: string, role: string) =>
  call<ProfileRunResults>(
    "fetchProfileRun",
    `/admin/tables/${tableId}/profile-runs/${encodeURIComponent(runId)}`,
    "GET",
    { "X-Provisa-Role": role },
  );

// -- constraints and checker exports (REQ-1934 PROPOSED CONSTRAINTS; EXCEPTIONS COME FROM CHECKERS)

/** A checker table (Soda or Great Expectations) a check can be added to. */
export interface CheckerTable {
  id: number;
  tableName: string;
  sourceId: string;
  checker: "soda" | "great_expectations";
}

/** The operator's decision on a constraint, as profiler_constraints stores it. */
export interface ConstraintDecision {
  id: string;
  kind: string;
  column_name: string;
  other_column: string | null;
  definition: string;
  evidence: string;
  share: number | null;
  sampled: boolean;
  status: "accepted" | "dismissed";
  run_id: string;
}

export interface ConstraintDecisionInput {
  kind: string;
  column: string;
  otherColumn: string | null;
  definition: Record<string, unknown>;
  evidence: string;
  share: number | null;
  sampled: boolean;
  status: "accepted" | "dismissed";
  runId: string;
}

export interface ExportResult {
  checkerTable: CheckerTable;
  added: boolean;
}

export interface ProfileCheckCandidates {
  driftTableRegistered: boolean;
  checkers: CheckerTable[];
  expectationTables: { id: number; tableName: string; published: string }[];
}

const constraintsPath = (tableId: number) => `/admin/tables/${tableId}/profile-constraints`;

export const fetchConstraints = (tableId: number) =>
  call<{ decisions: ConstraintDecision[]; checkers: CheckerTable[] }>(
    "fetchConstraints",
    constraintsPath(tableId),
  );

export const decideConstraint = (tableId: number, input: ConstraintDecisionInput) =>
  call<{ id: string }>("decideConstraint", constraintsPath(tableId), "POST", {}, input);

export const forgetConstraint = (tableId: number, constraintId: string) =>
  call<{ id: string }>(
    "forgetConstraint",
    `${constraintsPath(tableId)}/${encodeURIComponent(constraintId)}`,
    "DELETE",
  );

export const exportConstraint = (
  tableId: number,
  constraintId: string,
  checkerTableId: number | null,
) =>
  call<ExportResult>(
    "exportConstraint",
    `${constraintsPath(tableId)}/${encodeURIComponent(constraintId)}/export`,
    "POST",
    {},
    { checkerTableId },
  );

export const fetchProfileChecks = (tableId: number) =>
  call<ProfileCheckCandidates>("fetchProfileChecks", `/admin/tables/${tableId}/profile-checks`);

export const createDriftCheck = (tableId: number, checkerTableId: number | null) =>
  call<ExportResult>(
    "createDriftCheck",
    `/admin/tables/${tableId}/profile-checks/drift`,
    "POST",
    {},
    { checkerTableId },
  );

export const createExpectationCheck = (
  tableId: number,
  expectationsTableId: number,
  checkerTableId: number | null,
) =>
  call<ExportResult>(
    "createExpectationCheck",
    `/admin/tables/${tableId}/profile-checks/expectation`,
    "POST",
    {},
    { expectationsTableId, checkerTableId },
  );
