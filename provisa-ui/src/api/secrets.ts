// Copyright (c) 2026 Kenneth Stott
// Canary: ccd48753-8303-4b67-a537-249ef7d51516
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1557, REQ-1558: the org's secrets. There is deliberately no read call here — nothing in the
// API returns a stored value, so nothing in the browser can be handed one. A `Secret` is a name,
// what it is for, who last set it and the reference to paste; a lost value is REPLACED.

import { serverMessage, requestFailed } from "../i18n/serverMessage";

const API_BASE = import.meta.env.VITE_API_BASE || "";

function base(orgId: string): string {
  return `${API_BASE}/admin/orgs/${encodeURIComponent(orgId)}/secrets`;
}

async function ok<T>(res: Response, op: string): Promise<T> {
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(serverMessage(data, requestFailed(op, res.status)));
  }
  return res.json() as Promise<T>;
}

export interface Secret {
  name: string;
  description: string | null;
  created_at: string | null;
  updated_at: string | null;
  updated_by: string | null;
  /** What to paste into a connection field: `${secret:NAME}`. */
  reference: string;
}

export interface SecretsProvider {
  key: string;
  label: string;
  /** Whether this deployment's secrets service lets Provisa create and delete names in it. */
  writable: boolean;
}

/** A backend the build knows about, whether or not this deployment can use it. */
export interface SecretsProviderChoice {
  key: string;
  label: string;
  description: string;
  /** Whether the client library is installed. An unavailable backend is shown, not hidden. */
  available: boolean;
  /** The distribution to install to make it available — named in the greyed-out row. */
  requires: string | null;
  writable: boolean;
  config_fields: SecretsConfigField[];
}

export interface SecretsConfigField {
  config_key: string;
  label: string;
  type: string;
  required: boolean;
  secret?: boolean;
  placeholder?: string;
}

/** The deployment-wide secrets service: which backend, and what else it could be. */
export interface SecretsServiceState {
  provider: string;
  providers: SecretsProviderChoice[];
  config: Record<string, Record<string, string>>;
}

export interface SecretsState {
  provider: SecretsProvider;
  secrets: Secret[];
}

export async function fetchSecrets(orgId: string): Promise<SecretsState> {
  const res = await fetch(base(orgId));
  return ok<SecretsState>(res, "load secrets");
}

/** Create or replace one secret — the same call, because the name is the identity. */
export async function putSecret(
  orgId: string,
  name: string,
  body: { value: string; description?: string | null },
): Promise<Secret> {
  const res = await fetch(`${base(orgId)}/${encodeURIComponent(name)}`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  return ok<Secret>(res, "save secret");
}

export async function deleteSecret(orgId: string, name: string): Promise<{ deleted: string }> {
  const res = await fetch(`${base(orgId)}/${encodeURIComponent(name)}`, { method: "DELETE" });
  return ok<{ deleted: string }>(res, "delete secret");
}

// The SERVICE is the deployment's (platform_settings); the NAMES above are the org's
// (org_settings). Two rights, two endpoints, one page.

export async function fetchSecretsService(): Promise<SecretsServiceState> {
  const res = await fetch(`${API_BASE}/admin/secrets-service`);
  return ok<SecretsServiceState>(res, "load secrets service");
}

export async function setSecretsService(body: {
  provider: string;
  config: Record<string, string>;
}): Promise<{ success: boolean; provider: string }> {
  const res = await fetch(`${API_BASE}/admin/secrets-service`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  return ok<{ success: boolean; provider: string }>(res, "save secrets service");
}
