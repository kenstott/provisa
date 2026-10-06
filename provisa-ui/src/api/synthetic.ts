// Copyright (c) 2026 Kenneth Stott
// Canary: 3c7e1a92-4f08-4d65-b2e9-a8d5c0f17b43
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1939: an environment's synthetic datasets (provisa/api/admin/synthetic_router.py). Every call
// names the environment it acts in; it is sent as the environment header, never the browser's
// selection, because the panel manages one environment while the browser may be reading another.

import { ENV_HEADER } from "../lib/authFetch";
import { serverMessage, requestFailed } from "../i18n/serverMessage";

const API_BASE = import.meta.env.VITE_API_BASE || "";
const BASE = `${API_BASE}/admin/synthetic-datasets`;

export interface DatasetTable {
  tableId: number;
  profileEnv: string;
  runId: string;
  scale: number | null;
}

export interface SyntheticDataset {
  id: string;
  seed: number;
  scale: number;
  status: "defined" | "generating" | "generated" | "failed";
  storeSchema: string;
  error: string | null;
  generatedAt: string | null;
  tables: DatasetTable[];
}

export interface ProfiledTableRuns {
  tableId: number;
  tableName: string;
  runs: { runId: string; runTime: string; rowCount: number | null }[];
}

export interface ReportRow {
  table: string;
  column: string | null;
  measure: string;
  source: number | null;
  synthetic: number | null;
  delta: number | null;
  note: string | null;
}

async function call<T>(op: string, env: string, path: string, init: RequestInit = {}): Promise<T> {
  const headers = new Headers(init.headers);
  headers.set(ENV_HEADER, env);
  if (init.body) headers.set("Content-Type", "application/json");
  const res = await fetch(`${BASE}${path}`, { ...init, headers });
  if (!res.ok) {
    const body = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(serverMessage(body, requestFailed(op, res.status)));
  }
  return res.json() as Promise<T>;
}

export const fetchDatasets = (env: string) => call<SyntheticDataset[]>("fetchDatasets", env, "");

export const fetchProfileRuns = (env: string, profileEnv: string) =>
  call<ProfiledTableRuns[]>(
    "fetchProfileRuns",
    env,
    `/-/profile-runs?env=${encodeURIComponent(profileEnv)}`,
  );

export const defineDataset = (
  env: string,
  id: string,
  body: { seed: number; scale: number; tables: DatasetTable[] },
) =>
  call<{ id: string; storeSchema: string }>("defineDataset", env, `/${encodeURIComponent(id)}`, {
    method: "PUT",
    body: JSON.stringify(body),
  });

export const generateDataset = (env: string, id: string) =>
  call<{ id: string; status: string }>(
    "generateDataset",
    env,
    `/${encodeURIComponent(id)}/generate`,
    {
      method: "POST",
    },
  );

export const fetchReport = (env: string, id: string) =>
  call<ReportRow[]>("fetchReport", env, `/${encodeURIComponent(id)}/report`);

export const dropDataset = (env: string, id: string) =>
  call<{ id: string; dropped: boolean }>("dropDataset", env, `/${encodeURIComponent(id)}`, {
    method: "DELETE",
  });
