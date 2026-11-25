// Copyright (c) 2026 Kenneth Stott
// Canary: 8d2f4b61-7a39-4e05-b1c8-3e9a6d0f5c27
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2225: the organisation's default Spark cluster -- where a Spark script runs and is validated
// when its source names no cluster of its own -- set by an organisation administrator, with a
// connection test.

import { requestFailed, serverMessage } from "../i18n/serverMessage";
import type { Connector } from "./connectors";

const API_BASE = import.meta.env.VITE_API_BASE || "";

export interface SparkCluster {
  /** A Spark Connect address: sc://host:15002. */
  address: string;
  /** A ${secret:NAME} reference to the token, or null when the cluster takes none. */
  credential: string | null;
}

/** The cluster a source naming none runs on now. */
export interface SparkClusterInUse {
  /** "organisation", "local_test", or null when none is configured. */
  origin: "organisation" | "local_test" | null;
  /** "local test cluster" for the demo's one-node Spark. */
  label?: string | null;
  /** host:port. */
  target?: string;
  /** Why there is none. */
  detail?: string;
}

export interface SparkClusterState {
  cluster: SparkCluster | null;
  in_use: SparkClusterInUse;
}

export interface SparkClusterTest {
  ok: true;
  spark_version: string;
  target: string;
}

async function ok<T>(res: Response, op: string): Promise<T> {
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(serverMessage(data, requestFailed(op, res.status)));
  }
  return res.json() as Promise<T>;
}

const base = (orgId: string) => `${API_BASE}/admin/orgs/${encodeURIComponent(orgId)}/spark-cluster`;

const json = (method: string, body: unknown): RequestInit => ({
  method,
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify(body),
});

export async function fetchSparkCluster(orgId: string): Promise<SparkClusterState> {
  return ok(await fetch(base(orgId)), "fetchSparkCluster");
}

export async function saveSparkCluster(
  orgId: string,
  body: SparkCluster,
): Promise<SparkClusterState> {
  return ok(await fetch(base(orgId), json("PUT", body)), "saveSparkCluster");
}

export async function deleteSparkCluster(orgId: string): Promise<void> {
  await ok(await fetch(base(orgId), { method: "DELETE" }), "deleteSparkCluster");
}

/** Call the cluster as entered; an empty address tests the one a source naming none uses. */
export async function testSparkCluster(
  orgId: string,
  body: SparkCluster,
): Promise<SparkClusterTest> {
  return ok(await fetch(`${base(orgId)}/test`, json("POST", body)), "testSparkCluster");
}

/** REQ-2225 (amended 2026-11-24): what a Spark source joins -- Spark is a built-in source type,
 * not a connector an organisation offers -- and the cluster a source naming none runs on.
 * ``host`` is null when this installation carries no Spark host (e.g. e2e, CI without a release
 * package); the UI shows Spark as unavailable in that case. */
export interface SparkSource {
  host: Connector | null;
  default_cluster: SparkClusterInUse;
}

export async function fetchSparkSource(orgId: string): Promise<SparkSource> {
  return ok(
    await fetch(`${API_BASE}/admin/orgs/${encodeURIComponent(orgId)}/builtin-sources/spark`),
    "fetchSparkSource",
  );
}
