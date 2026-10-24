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

// --- Metadata export admin surface (REQ-1068, REQ-1072, REQ-1073, REQ-1074, REQ-2118) ---

/** One catalog the org publishes to, as the server reports it (REQ-2118: an org may have several,
 *  two of one provider among them).
 *
 *  Credentials appear only as `*_set` booleans: the server never returns a stored secret, so
 *  the form has nothing to render into a password field and nothing to send back unchanged.
 */
export interface MetadataExportTarget {
  id: string;
  provider: string;
  endpoint: string;
  auth_mode: string;
  username: string;
  entra_tenant_id: string;
  entra_client_id: string;
  gcp_project: string;
  gcp_location: string;
  aws_region: string;
  datazone_domain_id: string;
  datazone_project_id: string;
  delta_share: boolean;
  timeout_seconds: number;
  api_key_set: boolean;
  token_set: boolean;
  entra_client_secret_set: boolean;
  google_service_account_json_set: boolean;
  aws_access_key_id_set: boolean;
  aws_secret_access_key_set: boolean;
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

/** A target as saved: its settings, and only the credentials the administrator typed. A
 *  credential omitted keeps the one stored for that target; sent empty, it is cleared. */
export type MetadataExportTargetUpdate = Omit<
  MetadataExportTarget,
  | "api_key_set"
  | "token_set"
  | "entra_client_secret_set"
  | "google_service_account_json_set"
  | "aws_access_key_id_set"
  | "aws_secret_access_key_set"
> & {
  api_key?: string;
  token?: string;
  entra_client_secret?: string;
  google_service_account_json?: string;
  aws_access_key_id?: string;
  aws_secret_access_key?: string;
};

/** The whole list is sent: a target left out of it is removed. */
export interface MetadataExportUpdate {
  enabled?: boolean;
  reconcile_cron?: string;
  targets?: MetadataExportTargetUpdate[];
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
