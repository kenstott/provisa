// Copyright (c) 2026 Kenneth Stott
// Canary: 7f4a2c93-6b18-4d05-9e37-c05a81d6f4b2
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { requestFailed } from "../i18n/serverMessage";

const API_BASE = import.meta.env.VITE_API_BASE || "";

// --- Metadata export admin surface (REQ-1068, REQ-1072, REQ-1073, REQ-1074, REQ-2118, REQ-2122) ---

/** One catalog the org publishes to (REQ-2118: an org may have several, two of one provider
 *  among them). It names a catalog of the Catalogs panel, which holds the catalog's address and
 *  credentials, and carries only the export's own settings for it (REQ-2122). */
export interface MetadataExportTarget {
  catalog: string;
  datazone_project_id: string;
  delta_share: boolean;
}

export interface MetadataExportConfig {
  enabled: boolean;
  reconcile_cron: string;
  targets: MetadataExportTarget[];
}

export interface TargetOutcome {
  id: string;
  provider: string;
  ok: boolean;
  published: Record<string, number>;
  total_published: number;
  errors: { asset: string; message: string }[];
}

/** One publish, to every target. */
export interface PublishOutcome {
  ok: boolean;
  targets: TargetOutcome[];
}

export interface HealthOutcome {
  ok: boolean;
  targets: { id: string; provider: string; ok: boolean; error?: string }[];
}

export interface MetadataExportState {
  entitled: boolean;
  required_tier: string;
  providers: string[];
  /** The providers not yet proven against a live instance, which are labelled "(beta)". */
  beta_providers: string[];
  config: MetadataExportConfig;
  last_publish: PublishOutcome | null;
}

/** The whole list is sent: a target left out of it is removed. */
export interface MetadataExportUpdate {
  enabled?: boolean;
  reconcile_cron?: string;
  targets?: MetadataExportTarget[];
}

export async function fetchMetadataExport(): Promise<MetadataExportState> {
  const resp = await fetch(`${API_BASE}/admin/metadata-export`);
  if (!resp.ok) throw new Error(requestFailed("Metadata export fetch", resp.status));
  return resp.json();
}

export async function setMetadataExport(
  body: MetadataExportUpdate,
): Promise<{ success: boolean; enabled: boolean; targets: string[] }> {
  const resp = await fetch(`${API_BASE}/admin/metadata-export`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!resp.ok) throw new Error(requestFailed("Metadata export update", resp.status));
  return resp.json();
}

export async function checkMetadataExport(): Promise<HealthOutcome> {
  const resp = await fetch(`${API_BASE}/admin/metadata-export/health`, { method: "POST" });
  if (!resp.ok) throw new Error(requestFailed("Metadata export health check", resp.status));
  return resp.json();
}

export async function publishMetadataExport(): Promise<PublishOutcome> {
  const resp = await fetch(`${API_BASE}/admin/metadata-export/publish`, { method: "POST" });
  if (!resp.ok) throw new Error(requestFailed("Metadata export publish", resp.status));
  return resp.json();
}
