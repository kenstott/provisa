// Copyright (c) 2026 Kenneth Stott
// Canary: 81a82953-b7ce-4df5-8b91-31c32e4644ed
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2098: a code source's image and remote host -- building the image of a dev-mode source
// (pushed to a container registry, or downloaded), connecting the remote host running it (which
// switches the source to built mode), its host's status, re-pinning, and going back to dev.

import { requestFailed, serverMessage } from "../i18n/serverMessage";

const API_BASE = import.meta.env.VITE_API_BASE || "";

export type Arch = "amd64" | "arm64";

export interface Build {
  id: string;
  source_id: string;
  code_hash: string;
  arch: Arch;
  registry_id: string | null;
  status: "building" | "done" | "failed";
  image_ref: string | null;
  digest: string | null;
  node: string | null;
  error: string | null;
  started_at: string;
  finished_at: string | null;
  /** The compose service to run the image, once it is built. */
  compose: string | null;
}

export interface Connected {
  mode: "built";
  remote: Record<string, unknown>;
  content_hash: string;
}

export interface HostStatus {
  mode: "dev" | "built";
  host?: string;
  pinned_hash?: string;
  answering?: boolean;
  /** The code hash the host reports. */
  code_hash?: string;
  error?: string;
  /** What the last health poll of a connector's host found; absent until one has run. */
  health?: { state: "healthy" | "unhealthy"; detail: string | null; checked_at: string } | null;
}

async function ok<T>(res: Response, op: string): Promise<T> {
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(serverMessage(data, requestFailed(op, res.status)));
  }
  return res.json() as Promise<T>;
}

const base = (orgId: string, sourceId: string) =>
  `${API_BASE}/admin/orgs/${encodeURIComponent(orgId)}/code-sources/${encodeURIComponent(sourceId)}`;

const post = (url: string, body?: unknown) =>
  fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });

export async function startBuild(
  orgId: string,
  sourceId: string,
  body: { arch: Arch; registry_id: string | null; download: boolean },
): Promise<Build> {
  return ok(await post(`${base(orgId, sourceId)}/builds`, body), "startBuild");
}

export async function fetchBuild(orgId: string, sourceId: string, buildId: string): Promise<Build> {
  return ok(
    await fetch(`${base(orgId, sourceId)}/builds/${encodeURIComponent(buildId)}`),
    "fetchBuild",
  );
}

export async function fetchBuilds(orgId: string, sourceId: string): Promise<Build[]> {
  return (
    await ok<{ builds: Build[] }>(await fetch(`${base(orgId, sourceId)}/builds`), "fetchBuilds")
  ).builds;
}

export const archiveUrl = (orgId: string, sourceId: string, buildId: string) =>
  `${base(orgId, sourceId)}/builds/${encodeURIComponent(buildId)}/archive`;

export async function connectHost(
  orgId: string,
  sourceId: string,
  body: { host: string; port: number; pairing_code: string },
): Promise<Connected> {
  return ok(await post(`${base(orgId, sourceId)}/connect`, body), "connectHost");
}

export async function backToDev(orgId: string, sourceId: string): Promise<void> {
  await ok(await post(`${base(orgId, sourceId)}/dev`), "backToDev");
}

export async function fetchHostStatus(orgId: string, sourceId: string): Promise<HostStatus> {
  return ok(await fetch(`${base(orgId, sourceId)}/host`), "fetchHostStatus");
}

export async function repin(
  orgId: string,
  sourceId: string,
  codeHash: string,
): Promise<{ content_hash: string }> {
  return ok(await post(`${base(orgId, sourceId)}/repin`, { code_hash: codeHash }), "repin");
}

/** A function the source's code exports, and what it is. */
export interface CodeExport {
  name: string;
  kind: "table" | "command" | "stream";
}

export async function fetchExports(orgId: string, sourceId: string): Promise<CodeExport[]> {
  return (
    await ok<{ exports: CodeExport[] }>(
      await fetch(`${base(orgId, sourceId)}/exports`),
      "fetchExports",
    )
  ).exports;
}

// REQ-2225: a Spark source's script validated by its host in three steps.

/** A step's outcome: `incomplete` when an input or a key is not named yet. */
export type StepStatus = "ok" | "failed" | "incomplete" | "not_run";
/** One function's outcome within a step: a @command's dry run is `not_run_by_design`. */
export type FunctionStatus =
  | "ok"
  | "empty"
  | "mismatch"
  | "error"
  | "needs_input"
  | "not_run"
  | "not_run_by_design";

/** A table, query or command of the script, as its static check read it. */
export interface ScriptFunction {
  name: string;
  kind: string;
  /** Its table parameters, each given a governed table to dry-run on. */
  tables: string[];
  arguments: string[];
  columns: string[];
  /** A two-table @query used to match rows: its two keys are checked. */
  resolver: boolean;
}

export interface ColumnType {
  name: string;
  type: string;
}

export interface DryRunResult {
  function: string;
  status: FunctionStatus;
  rows?: number;
  error?: string;
  declared?: ColumnType[];
  actual?: ColumnType[];
  mismatches?: string[];
  /** The table parameters not given a table. */
  missing?: string[];
  /** Why a function was not run. */
  reason?: string;
}

export interface KeyResult {
  function: string;
  status: FunctionStatus;
  source_key?: string;
  target_key?: string;
  rows?: number;
  problems?: string[];
  missing?: string[];
  error?: string;
}

interface Step<T> {
  status: StepStatus;
  functions: T[];
  reason?: string;
}

export interface ValidationReport {
  /** The cluster the dry run ran on; null when there is none. */
  cluster: { address: string; origin: "source" | "organisation" | "local_test" } | null;
  static: Step<ScriptFunction> & { problems: string[]; hash?: string };
  dry_run: Step<DryRunResult>;
  keys: Step<KeyResult>;
}

export interface ValidateBody {
  /** The script in the form; null validates the one the source serves. */
  script: string | null;
  /** Each function's governed table for each of its table parameters. */
  inputs: Record<string, Record<string, string>>;
  /** Each resolver's source key and target key. */
  keys: Record<string, [string, string]>;
}

export async function validateScript(
  orgId: string,
  sourceId: string,
  body: ValidateBody,
): Promise<ValidationReport> {
  return ok(await post(`${base(orgId, sourceId)}/validate`, body), "validateScript");
}
