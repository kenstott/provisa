// Copyright (c) 2026 Kenneth Stott
// Canary: 4b9e1d72-0c35-4a8f-9e61-d27a3f85c0b4
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { requestFailed, serverMessage } from "../i18n/serverMessage";

const API_BASE = import.meta.env.VITE_API_BASE || "";

// --- The Catalogs panel (REQ-2122) ---

/** One catalog the org connects to, for export and import alike, as the server reports it.
 *
 *  Credentials appear only as `*_set` booleans: the server never returns a stored secret, so
 *  the form has nothing to render into a password field and nothing to send back unchanged.
 */
export interface CatalogConnection {
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
  timeout_seconds: number;
  /** The domains that work with this catalog. */
  domains: string[];
  api_key_set: boolean;
  token_set: boolean;
  entra_client_secret_set: boolean;
  google_service_account_json_set: boolean;
  aws_access_key_id_set: boolean;
  aws_secret_access_key_set: boolean;
}

export interface CatalogsState {
  providers: string[];
  /** The providers not yet proven against a live instance, which are labelled "(beta)". */
  beta_providers: string[];
  catalogs: CatalogConnection[];
}

export type CatalogSecretField =
  | "api_key"
  | "token"
  | "entra_client_secret"
  | "google_service_account_json"
  | "aws_access_key_id"
  | "aws_secret_access_key";

/** A catalog as saved: its settings, and only the credentials the administrator typed. A
 *  credential omitted keeps the one stored for that catalog; sent empty, it is cleared. */
export type CatalogConnectionUpdate = Omit<CatalogConnection, `${CatalogSecretField}_set`> &
  Partial<Record<CatalogSecretField, string>>;

export interface CatalogHealth {
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

export async function fetchCatalogs(): Promise<CatalogsState> {
  return ok(await fetch(`${API_BASE}/admin/catalogs`), "Catalogs fetch");
}

/** The whole list is sent: a catalog left out of it is removed. */
export async function setCatalogs(
  catalogs: CatalogConnectionUpdate[],
): Promise<{ success: boolean; catalogs: string[] }> {
  return ok(
    await fetch(`${API_BASE}/admin/catalogs`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ catalogs }),
    }),
    "Catalogs update",
  );
}

export async function checkCatalog(id: string): Promise<CatalogHealth> {
  return ok(
    await fetch(`${API_BASE}/admin/catalogs/${encodeURIComponent(id)}/health`, {
      method: "POST",
    }),
    "Catalog connection test",
  );
}
