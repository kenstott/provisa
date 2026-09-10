// Copyright (c) 2026 Kenneth Stott
// Canary: 0cea7b2b-a366-4bb2-b37c-63267ff59349
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The live sources the source-to-query e2e (REQ-1671) configures through the UI. Each is the same
// demo/sources/<name> unit the --source flag of start-ui-install.sh provisions, through the same
// entry point (demo/sources/provision.py), under the provisa-e2e project prefix on e2e-only ports so
// a running demo and a running e2e never collide. Started in globalSetup and removed in globalTeardown, like the
// Neo4j export target (neo4j-container.ts): a container start mid-run tears down a veth while
// browsers are open, which is what that file exists to avoid.

import { execSync } from "child_process";
import path from "path";
import { fileURLToPath } from "url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");

export const E2E_NEO4J_HTTP_PORT = 37474;
export const E2E_NEO4J_BOLT_PORT = 37687;
export const E2E_MONGO_PORT = 37117;
export const E2E_ES_PORT = 39200;
export const E2E_REDIS_PORT = 36379;
export const E2E_CASSANDRA_PORT = 39042;
export const E2E_SPARQL_PORT = 33030;
export const E2E_PROMETHEUS_PORT = 39090;
export const E2E_CHINOOK_PORT = 35433;
export const E2E_SPLUNK_PORT = 38089;
export const E2E_SPLUNK_HEC_PORT = 38088;

export const DEMO_SOURCE_ENV: Record<string, string> = {
  PROVISA_DEMO_NEO4J_HTTP_PORT: String(E2E_NEO4J_HTTP_PORT),
  PROVISA_DEMO_NEO4J_BOLT_PORT: String(E2E_NEO4J_BOLT_PORT),
  PROVISA_DEMO_MONGO_PORT: String(E2E_MONGO_PORT),
  PROVISA_DEMO_ES_PORT: String(E2E_ES_PORT),
  PROVISA_DEMO_REDIS_PORT: String(E2E_REDIS_PORT),
  PROVISA_DEMO_CASSANDRA_PORT: String(E2E_CASSANDRA_PORT),
  PROVISA_DEMO_SPARQL_PORT: String(E2E_SPARQL_PORT),
  PROVISA_DEMO_PROMETHEUS_PORT: String(E2E_PROMETHEUS_PORT),
  PROVISA_DEMO_CHINOOK_PORT: String(E2E_CHINOOK_PORT),
  PROVISA_DEMO_SPLUNK_PORT: String(E2E_SPLUNK_PORT),
  PROVISA_DEMO_SPLUNK_HEC_PORT: String(E2E_SPLUNK_HEC_PORT),
};

// Every one of these is read by the native engine over localhost (REQ-1672 made Elasticsearch
// engine-independent), so they all belong to the core lane.
export const DEMO_SOURCES = [
  "neo4j",
  "mongodb",
  "elasticsearch",
  "redis",
  "cassandra",
  "sparql",
  "chinook", // a Postgres for the Hasura v2 import e2e (hasura-import.spec.ts)
  // Read through the bundled Calcite pgwire server the native engine ATTACHes (REQ-1690/1694).
  // Slowest of the set by far: a full Splunk init under amd64 emulation, ~3 minutes to a healthy
  // container before prime.py can seed its index and Data Model.
  "splunk",
] as const;
export type DemoSource = (typeof DEMO_SOURCES)[number];

const PROVISION = path.join(ROOT, "demo", "sources", "provision.py");
const PYTHON = path.join(ROOT, ".venv", "bin", "python");
const ENV_ARGS = Object.entries(DEMO_SOURCE_ENV)
  .map(([k, v]) => `--env ${k}=${v}`)
  .join(" ");

function provision(cmd: "up" | "down"): void {
  execSync(
    `"${PYTHON}" "${PROVISION}" ${cmd} --prefix provisa-e2e ${ENV_ARGS} ${DEMO_SOURCES.join(" ")}`,
    { stdio: "pipe", env: { ...process.env, ...DEMO_SOURCE_ENV } },
  );
}

/** Start and prime every demo source. Blocks until each is healthy. Core lane only. */
export function startDemoSources(): void {
  provision("up");
}

/** Remove the containers and their volumes; a project that was never started removes nothing. */
export function removeDemoSources(): void {
  try {
    provision("down");
  } catch {
    // Nothing to remove.
  }
}
