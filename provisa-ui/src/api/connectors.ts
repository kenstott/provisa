// Copyright (c) 2026 Kenneth Stott
// Canary: 3b7e2c91-6f40-4d8a-92e1-c5a0f7d3b164
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The connectors an organization offers (its model's connectors block), which Sources lists beside
// the built-in source types. How one is built is never here: the server does not read that.

import { requestFailed, serverMessage } from "../i18n/serverMessage";

const API_BASE = import.meta.env.VITE_API_BASE || "";

export interface ConnectorRequirement {
  name: string;
  secret?: boolean;
  /** Entered as multi-line text. */
  multiline?: boolean;
  /** What the form starts with; a steward can change it. */
  default_value?: string;
  /** Shown read-only; a save that changes it is refused. */
  fixed_value?: string;
}

/** A standard source setting the connector's file sets: one of the two values. */
export interface ConnectorSetting {
  name: string;
  default_value?: boolean | number | string;
  fixed_value?: boolean | number | string;
}

export interface Connector {
  name: string;
  git: string;
  ref: string;
  commit: string;
  language: "node" | "python" | "go" | "rust";
  image: string;
  digest: string;
  code_hash: string;
  arch: "amd64" | "arm64";
  requires: ConnectorRequirement[];
  /** What the store shows; empty when the project has no standard file. */
  description: string;
  /** A data: URI. */
  icon: string;
  settings: ConnectorSetting[];
}

export async function fetchConnectors(orgId: string): Promise<Connector[]> {
  const res = await fetch(`${API_BASE}/admin/orgs/${encodeURIComponent(orgId)}/connectors`);
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(serverMessage(data, requestFailed("fetchConnectors", res.status)));
  }
  return ((await res.json()) as { connectors: Connector[] }).connectors;
}
