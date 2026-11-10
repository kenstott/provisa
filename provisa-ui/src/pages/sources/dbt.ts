// Copyright (c) 2026 Kenneth Stott
// Canary: 4a8d21f6-73e0-49ab-8c15-e6f03b7a92d1
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2228: the setup of a dbt source. Its spec is dbt's manifest; adding it registers nothing.

import { useState } from "react";
import { requestFailed, serverMessage } from "../../i18n/serverMessage";

export type DbtSpecMode = "location" | "upload" | "cloud";

export const DBT_CLOUD_HOST = "https://cloud.getdbt.com";

export interface DbtWarehouseOffer {
  database: string;
  source_type: string | null;
  source_id: string | null;
}

export interface DbtPreview {
  project: string;
  adapter: string;
  models: number;
  commands: number;
  warehouses: DbtWarehouseOffer[];
}

/** A model the manifest marks public with an enforced contract: a steward decides. */
export interface DbtCandidate {
  unique_id: string;
  name: string;
  description: string | null;
  version: string | null;
  suggested_id: string;
  owner: { name: string | null; email: string | null } | null;
  owner_role: string | null;
  columns: {
    name: string;
    data_type: string | null;
    description: string | null;
    required: boolean;
  }[];
  tests: { name: string; column: string | null }[];
  table_id: number | null;
  products: string[];
}

/** What creating a warehouse source asks for: the manifest knows the rest. */
export interface DbtNewWarehouse {
  sourceId: string;
  host: string;
  username: string;
  password: string;
}

export interface DbtSeed {
  spec: DbtSpecMode;
  location: string;
  commandLine: string;
  host: string;
  accountId: string;
  jobId: string;
}

export const newWarehouse = (id: string, database: string): DbtNewWarehouse => ({
  sourceId: `${id || "dbt"}-${database}`.replace(/[^A-Za-z0-9_-]/g, "-"),
  host: "",
  username: "",
  password: "",
});

/** What to create, one source per database no registered source reads and that was filled in. */
export function warehousesToCreate(
  preview: DbtPreview | null,
  fills: Record<string, DbtNewWarehouse>,
): Record<string, unknown>[] {
  const out: Record<string, unknown>[] = [];
  for (const w of preview?.warehouses ?? []) {
    const fill = fills[w.database];
    if (w.source_id || !w.source_type || !fill?.host.trim()) continue;
    const file = w.source_type === "duckdb";
    out.push({
      id: fill.sourceId,
      type: w.source_type,
      host: file ? "" : fill.host.trim(),
      database: w.database,
      path: file ? fill.host.trim() : undefined,
      username: fill.username,
      password: fill.password,
    });
  }
  return out;
}

export function useDbtSourceForm() {
  const [spec, setSpec] = useState<DbtSpecMode>("location");
  const [location, setLocation] = useState("");
  const [content, setContent] = useState("");
  const [host, setHost] = useState(DBT_CLOUD_HOST);
  const [accountId, setAccountId] = useState("");
  const [jobId, setJobId] = useState("");
  const [token, setToken] = useState("");
  const [commandLine, setCommandLine] = useState("dbt");
  const [preview, setPreview] = useState<DbtPreview | null>(null);
  const [previewing, setPreviewing] = useState(false);
  const [previewError, setPreviewError] = useState<string | null>(null);
  const [fills, setFills] = useState<Record<string, DbtNewWarehouse>>({});

  const specBody = () => ({
    spec,
    location,
    content,
    host,
    account_id: accountId,
    job_id: jobId,
    token,
  });

  const runPreview = async (sourceId: string) => {
    setPreviewing(true);
    setPreviewError(null);
    setPreview(null);
    try {
      const resp = await fetch("/admin/dbt/preview", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(specBody()),
      });
      if (!resp.ok) {
        const body = await resp.json().catch(() => ({ detail: resp.statusText }));
        throw new Error(serverMessage(body, requestFailed("dbt preview", resp.status)));
      }
      const data = (await resp.json()) as DbtPreview;
      setPreview(data);
      setFills(
        Object.fromEntries(
          data.warehouses
            .filter((w) => !w.source_id)
            .map((w) => [w.database, newWarehouse(sourceId, w.database)]),
        ),
      );
    } catch (err) {
      setPreviewError(err instanceof Error ? err.message : String(err));
    } finally {
      setPreviewing(false);
    }
  };

  const seed = (s: DbtSeed) => {
    setSpec(s.spec);
    setLocation(s.location);
    setContent("");
    setHost(s.host || DBT_CLOUD_HOST);
    setAccountId(s.accountId);
    setJobId(s.jobId);
    setToken("");
    setCommandLine(s.commandLine || "dbt");
    setPreview(null);
    setPreviewError(null);
    setFills({});
  };

  return {
    spec,
    setSpec,
    location,
    setLocation,
    content,
    setContent,
    host,
    setHost,
    accountId,
    setAccountId,
    jobId,
    setJobId,
    token,
    setToken,
    commandLine,
    setCommandLine,
    preview,
    previewing,
    previewError,
    runPreview,
    fills,
    setFills,
    seed,
    specBody,
  };
}

export type DbtSourceForm = ReturnType<typeof useDbtSourceForm>;
