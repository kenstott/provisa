// Copyright (c) 2026 Kenneth Stott
// Canary: e06ab91e-ee03-46de-9ea5-9270a12b3f6b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1984, REQ-1993: the organisation's package registries -- the npm registries, PyPI indexes and
// Git hosts every Node and Python install of the organisation goes through, entered by an organisation administrator. Each field
// is answered as it was entered (a value, or the reference typed for it), to the administrator
// who may enter it. REQ-2088: an entry may carry the PEM certificate authority its TLS chains to.

import { requestFailed, serverMessage } from "../i18n/serverMessage";

const API_BASE = import.meta.env.VITE_API_BASE || "";

export type RegistryKind = "npm" | "pypi" | "git";

export interface PackageRegistry {
  id: string;
  kind: RegistryKind;
  /** An npm registry's URL, a PyPI index's simple-index URL, or a Git host. */
  address: string;
  /** An npm registry's scope ("@acme"), or null. */
  scope: string | null;
  /** As entered: the value, or a reference someone typed. */
  credential: string;
  /** Whether this npm registry (or PyPI index) is the mirror every npm (or Python) install
   * goes through. */
  mirror: boolean;
  /** As entered: the PEM certificate authority, or a reference to it; null for none. */
  ca_bundle: string | null;
}

export interface PackageRegistryBody {
  kind: RegistryKind;
  address: string;
  scope?: string | null;
  /** What the field holds; left out on a replace, the entry keeps what it has. */
  credential?: string;
  mirror: boolean;
  /** The PEM certificate authority, or a reference; null holds none. */
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
  `${API_BASE}/admin/orgs/${encodeURIComponent(orgId)}/package-registries`;

export async function fetchPackageRegistries(orgId: string): Promise<PackageRegistry[]> {
  const answered = await ok<{ registries: PackageRegistry[] }>(
    await fetch(base(orgId)),
    "fetchPackageRegistries",
  );
  return answered.registries;
}

export async function addPackageRegistry(
  orgId: string,
  body: PackageRegistryBody,
): Promise<PackageRegistry> {
  return ok(
    await fetch(base(orgId), {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    }),
    "addPackageRegistry",
  );
}

export async function replacePackageRegistry(
  orgId: string,
  id: string,
  body: PackageRegistryBody,
): Promise<PackageRegistry> {
  return ok(
    await fetch(`${base(orgId)}/${encodeURIComponent(id)}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    }),
    "replacePackageRegistry",
  );
}

export async function deletePackageRegistry(orgId: string, id: string): Promise<void> {
  await ok(
    await fetch(`${base(orgId)}/${encodeURIComponent(id)}`, { method: "DELETE" }),
    "deletePackageRegistry",
  );
}
