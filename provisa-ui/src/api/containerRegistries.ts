// Copyright (c) 2026 Kenneth Stott
// Canary: ddf4d3fe-5c8b-471e-9222-04d1fc178e9a
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2098: the organisation's container registries -- where a code source's built image is pushed,
// entered by an organisation administrator beside its package registries. Each field is answered
// as it was entered (a value, or the reference typed for it), to the administrator who may enter it.

import { requestFailed, serverMessage } from "../i18n/serverMessage";

const API_BASE = import.meta.env.VITE_API_BASE || "";

export interface ContainerRegistry {
  id: string;
  /** A registry host and repository namespace: ghcr.io/acme. */
  address: string;
  username: string;
  /** As entered: the value, or a reference someone typed. */
  credential: string;
  /** Whether a build pushes here unless it names another. */
  default: boolean;
  /** As entered: the PEM certificate authority, or a reference to it; null for none. */
  ca_bundle: string | null;
}

export interface ContainerRegistryBody {
  address: string;
  username: string;
  /** What the field holds; left out on a replace, the entry keeps what it has. */
  credential?: string;
  default: boolean;
  ca_bundle: string | null;
}

async function ok<T>(res: Response, op: string): Promise<T> {
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(serverMessage(data, requestFailed(op, res.status)));
  }
  return res.json() as Promise<T>;
}

const base = (orgId: string) =>
  `${API_BASE}/admin/orgs/${encodeURIComponent(orgId)}/container-registries`;

export async function fetchContainerRegistries(orgId: string): Promise<ContainerRegistry[]> {
  const answered = await ok<{ registries: ContainerRegistry[] }>(
    await fetch(base(orgId)),
    "fetchContainerRegistries",
  );
  return answered.registries;
}

export async function addContainerRegistry(
  orgId: string,
  body: ContainerRegistryBody,
): Promise<ContainerRegistry> {
  return ok(
    await fetch(base(orgId), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    }),
    "addContainerRegistry",
  );
}

export async function replaceContainerRegistry(
  orgId: string,
  id: string,
  body: ContainerRegistryBody,
): Promise<ContainerRegistry> {
  return ok(
    await fetch(`${base(orgId)}/${encodeURIComponent(id)}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    }),
    "replaceContainerRegistry",
  );
}

export async function deleteContainerRegistry(orgId: string, id: string): Promise<void> {
  await ok(
    await fetch(`${base(orgId)}/${encodeURIComponent(id)}`, { method: "DELETE" }),
    "deleteContainerRegistry",
  );
}
