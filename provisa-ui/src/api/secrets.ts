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
//
// REQ-1560: every call names the VAULT it is addressing. The personal endpoints take their owner
// from the authenticated identity on the server, so there is no user id to pass here and no way
// for the browser to ask for anybody else's — "whose" is not a parameter, it is the caller.

import { serverMessage, requestFailed } from "../i18n/serverMessage";

const API_BASE = import.meta.env.VITE_API_BASE || "";

/** Which vault a call addresses: the org's shared one, or the caller's own (REQ-1560). */
export type Vault = "org" | "user";

function base(orgId: string, vault: Vault): string {
  const path = vault === "org" ? "secrets" : "my-secrets";
  return `${API_BASE}/admin/orgs/${encodeURIComponent(orgId)}/${path}`;
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
  /** Whose vault this row is in — the grammar of `reference` follows it (REQ-1560). */
  scope: Vault;
  /** What to paste into a connection field: `${secret:NAME}` or `${user:NAME}`. */
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
  /** Per-provider config, minus every field the registry marks secret (REQ-1575). */
  config: Record<string, Record<string, string>>;
  /** REQ-1575: per provider, per secret field, whether a value is stored. Never the value. */
  secret_set: Record<string, Record<string, boolean>>;
}

export interface SecretsState {
  provider: SecretsProvider;
  secrets: Secret[];
}

export async function fetchSecrets(orgId: string, vault: Vault): Promise<SecretsState> {
  const res = await fetch(base(orgId, vault));
  return ok<SecretsState>(res, "load secrets");
}

/** Create or replace one secret — the same call, because the name is the identity. */
export async function putSecret(
  orgId: string,
  vault: Vault,
  name: string,
  body: { value: string; description?: string | null },
): Promise<Secret> {
  const res = await fetch(`${base(orgId, vault)}/${encodeURIComponent(name)}`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  return ok<Secret>(res, "save secret");
}

export async function deleteSecret(
  orgId: string,
  vault: Vault,
  name: string,
): Promise<{ deleted: string }> {
  const res = await fetch(`${base(orgId, vault)}/${encodeURIComponent(name)}`, {
    method: "DELETE",
  });
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
