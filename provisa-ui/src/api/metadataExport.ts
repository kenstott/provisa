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

// --- Metadata export admin surface (REQ-1068, REQ-1072, REQ-1073, REQ-1074) ---

/** The org's export settings as the server reports them.
 *
 *  Credentials appear only as `*_set` booleans: the server never returns a stored secret, so
 *  the form has nothing to render into a password field and nothing to send back unchanged.
 */
export interface MetadataExportConfig {
  enabled: boolean;
  provider: string;
  endpoint: string;
  auth_mode: string;
  username: string;
  entra_tenant_id: string;
  entra_client_id: string;
  reconcile_cron: string;
  timeout_seconds: number;
  api_key_set: boolean;
  token_set: boolean;
  entra_client_secret_set: boolean;
}

export interface PublishOutcome {
  provider: string;
  ok: boolean;
  published: Record<string, number>;
  total_published: number;
  errors: { asset: string; message: string }[];
}

export interface MetadataExportState {
  entitled: boolean;
  required_tier: string;
  providers: string[];
  config: MetadataExportConfig;
  last_publish: PublishOutcome | null;
}

/** A credential omitted from the update keeps its stored value; sent empty, it is cleared. */
export interface MetadataExportUpdate {
  enabled?: boolean;
  provider?: string;
  endpoint?: string;
  auth_mode?: string;
  username?: string;
  entra_tenant_id?: string;
  entra_client_id?: string;
  reconcile_cron?: string;
  timeout_seconds?: number;
  api_key?: string;
  token?: string;
  entra_client_secret?: string;
}

export async function fetchMetadataExport(): Promise<MetadataExportState> {
  const resp = await fetch(`${API_BASE}/admin/metadata-export`);
  if (!resp.ok) throw new Error(requestFailed("Metadata export fetch", resp.status));
  return resp.json();
}

export async function setMetadataExport(
  body: MetadataExportUpdate,
): Promise<{ success: boolean; provider: string; enabled: boolean }> {
  const resp = await fetch(`${API_BASE}/admin/metadata-export`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!resp.ok) throw new Error(requestFailed("Metadata export update", resp.status));
  return resp.json();
}

export async function checkMetadataExport(): Promise<{
  ok: boolean;
  provider: string;
  error?: string;
}> {
  const resp = await fetch(`${API_BASE}/admin/metadata-export/health`, { method: "POST" });
  if (!resp.ok) throw new Error(requestFailed("Metadata export health check", resp.status));
  return resp.json();
}

export async function publishMetadataExport(): Promise<PublishOutcome> {
  const resp = await fetch(`${API_BASE}/admin/metadata-export/publish`, { method: "POST" });
  if (!resp.ok) throw new Error(requestFailed("Metadata export publish", resp.status));
  return resp.json();
}
