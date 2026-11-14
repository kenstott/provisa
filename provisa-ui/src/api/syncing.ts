// Copyright (c) 2026 Kenneth Stott
// Canary: 2197dc14-b082-48c8-9ce0-ef7c66a227f4
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { requestFailed, serverMessage } from "../i18n/serverMessage";

const API_BASE = import.meta.env.VITE_API_BASE || "";

// --- The Syncing page (REQ-2247, REQ-2122, REQ-1072, REQ-1074) ---

/** The kinds of outside system an entry is. */
export type SyncEntryType = "data_catalog" | "ontology" | "custom";
export const SYNC_ENTRY_TYPES: SyncEntryType[] = ["data_catalog", "ontology", "custom"];

/** Which way an entry syncs: Export is Provisa to the system, Import the system to Provisa. */
export type SyncDirection = "export" | "import" | "both";
export const SYNC_DIRECTIONS: SyncDirection[] = ["export", "import", "both"];

export const exportsTo = (direction: SyncDirection) =>
  direction === "export" || direction === "both";

/** What one publish did to one entry. */
export interface PublishOutcome {
  id: string;
  provider: string;
  ok: boolean;
  published: Record<string, number>;
  total_published: number;
  errors: { asset: string; message: string }[];
}

/** One entry as the server reports it.
 *
 *  Credentials appear only as `*_set` booleans: the server never returns a stored secret, so
 *  the form has nothing to render into a password field and nothing to send back unchanged.
 *  Only a data catalog has a provider, a connection and export settings.
 */
export interface SyncEntry {
  id: string;
  type: SyncEntryType;
  direction: SyncDirection;
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
  timeout_seconds: number;
  /** The domains that work with this entry. */
  domains: string[];
  /** REQ-2152: the role a data catalog is published as -- it is sent what that role reads. */
  role: string | null;
  /** The DataZone project that owns the data products published to a DataZone catalog. */
  datazone_project_id: string;
  /** Whether a Unity Catalog catalog also gets each product as a Delta Share. */
  delta_share: boolean;
  api_key_set: boolean;
  token_set: boolean;
  entra_client_secret_set: boolean;
  google_service_account_json_set: boolean;
  aws_access_key_id_set: boolean;
  aws_secret_access_key_set: boolean;
  /** The last publish this server ran to it, or null when it has not published to it. */
  last_run: PublishOutcome | null;
}

export interface SyncingState {
  /** Whether the organization's plan includes publishing. */
  entitled: boolean;
  required_tier: string;
  providers: string[];
  /** The providers not yet proven against a live instance, which are labelled "(beta)". */
  beta_providers: string[];
  /** REQ-2152: the roles an entry may be published as -- those whose reads the caller holds. */
  publishable_roles: string[];
  /** The organisation-wide schedule of the full publish to every entry that exports. */
  reconcile_cron: string;
  entries: SyncEntry[];
}

export type SyncSecretField =
  | "api_key"
  | "token"
  | "entra_client_secret"
  | "google_service_account_json"
  | "aws_access_key_id"
  | "aws_secret_access_key";

export const SYNC_SECRET_FIELDS: SyncSecretField[] = [
  "api_key",
  "token",
  "entra_client_secret",
  "google_service_account_json",
  "aws_access_key_id",
  "aws_secret_access_key",
];

/** An entry as saved: its settings, and only the credentials the administrator typed. A
 *  credential omitted keeps the one stored for that entry; sent empty, it is cleared. */
export type SyncEntryUpdate = Omit<SyncEntry, `${SyncSecretField}_set` | "last_run"> &
  Partial<Record<SyncSecretField, string>>;

/** An entry as the server holds it, ready to send back unchanged. */
export function entryUpdate(entry: SyncEntry): SyncEntryUpdate {
  const kept: Record<string, unknown> = { ...entry };
  delete kept.last_run;
  for (const field of SYNC_SECRET_FIELDS) delete kept[`${field}_set`];
  return kept as SyncEntryUpdate;
}

/** A new entry of `type`, empty but for what every entry starts with. */
export const blankEntry = (type: SyncEntry["type"]): SyncEntry => ({
  id: "",
  type,
  direction: type === "data_catalog" ? "export" : "import",
  provider: "",
  endpoint: "",
  auth_mode: "api_key",
  username: "",
  entra_tenant_id: "",
  entra_client_id: "",
  gcp_project: "",
  gcp_location: "",
  aws_region: "",
  datazone_domain_id: "",
  timeout_seconds: 30,
  domains: [],
  role: null,
  datazone_project_id: "",
  delta_share: false,
  api_key_set: false,
  token_set: false,
  entra_client_secret_set: false,
  google_service_account_json_set: false,
  aws_access_key_id_set: false,
  aws_secret_access_key_set: false,
  last_run: null,
});

export interface EntryHealth {
  id: string;
  provider: string;
  ok: boolean;
  error?: string;
}

/** The server's refusal, worded by its code where this build has the wording. */
async function ok<T>(res: Response, op: string): Promise<T> {
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(serverMessage(data, requestFailed(op, res.status)));
  }
  return res.json() as Promise<T>;
}

export async function fetchSyncing(): Promise<SyncingState> {
  return ok(await fetch(`${API_BASE}/admin/syncing`), "Syncing fetch");
}

/** The whole list is sent: an entry left out of it is removed. The schedule is kept when sent. */
export async function setSyncing(body: {
  entries: SyncEntryUpdate[];
  reconcile_cron?: string;
}): Promise<{ success: boolean; entries: string[] }> {
  return ok(
    await fetch(`${API_BASE}/admin/syncing`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    }),
    "Syncing update",
  );
}

export async function checkEntry(id: string): Promise<EntryHealth> {
  return ok(
    await fetch(`${API_BASE}/admin/syncing/${encodeURIComponent(id)}/health`, {
      method: "POST",
    }),
    "Connection test",
  );
}

export async function publishEntry(
  id: string,
): Promise<{ ok: boolean; entries: PublishOutcome[] }> {
  return ok(
    await fetch(`${API_BASE}/admin/syncing/${encodeURIComponent(id)}/publish`, {
      method: "POST",
    }),
    "Publish",
  );
}
