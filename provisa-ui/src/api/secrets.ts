// Copyright (c) 2026 Kenneth Stott
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
