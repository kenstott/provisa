// Copyright (c) 2026 Kenneth Stott
// Canary: 28ec90e3-f56d-47df-81ca-eed6b47465a2
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { defineConfig } from "@playwright/test";
import { execFileSync } from "child_process";
import * as fs from "fs";
import * as path from "path";
import { fileURLToPath } from "url";

// Load root .env so live-backend tests receive AWS credentials
const __dirname = path.dirname(fileURLToPath(import.meta.url));
const rootEnv = path.resolve(__dirname, "../.env");
if (fs.existsSync(rootEnv)) {
  for (const line of fs.readFileSync(rootEnv, "utf8").split("\n")) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#") || !trimmed.includes("=")) continue;
    const [k, ...rest] = trimmed.split("=");
    const key = k.trim();
    if (!process.env[key]) process.env[key] = rest.join("=").trim();
  }
}

// REQ-1363: the e2e harness must never share ports, ports, or on-disk state with the
// interactive dev environment (start-ui.sh). Every service the backend binds is
// isolated: HTTP, UI, gRPC, Arrow Flight, bolt, MCP, pgwire, and the DuckDB
// materialize store (a single-writer file — two backends attaching it concurrently
// deadlock/error). All defaults are distinct from start-ui.sh's and from each other.
const E2E_UI_PORT = Number(process.env.PROVISA_E2E_UI_PORT ?? 3901);
const E2E_API_PORT = Number(process.env.PROVISA_E2E_API_PORT ?? 8901);
const E2E_GRPC_PORT = Number(process.env.PROVISA_E2E_GRPC_PORT ?? 8902);
const E2E_FLIGHT_PORT = Number(process.env.PROVISA_E2E_FLIGHT_PORT ?? 8903);
const E2E_BOLT_PORT = Number(process.env.PROVISA_E2E_BOLT_PORT ?? 8904);
const E2E_MCP_PORT = Number(process.env.PROVISA_E2E_MCP_PORT ?? 8905);
const E2E_PGWIRE_PORT = Number(process.env.PROVISA_E2E_PGWIRE_PORT ?? 8906);
// provisa-install.yaml (the config this harness boots from) statically registers a
// graphql-demo and a petstore-mock source. Both default to compose-network hostnames
// (graphql-demo:4000, petstore-mock:8080) that don't resolve outside docker-compose, so
// any cross-source query touching them fails with a connection error unless this harness
// runs the same host-process demo servers run-demo-servers.sh uses and points the API
// server's env at them.
const E2E_GRAPHQL_DEMO_PORT = Number(process.env.PROVISA_E2E_GRAPHQL_DEMO_PORT ?? 8907);
const E2E_PETSTORE_PORT = Number(process.env.PROVISA_E2E_PETSTORE_PORT ?? 8908);
const E2E_DATA_DIR = process.env.PROVISA_E2E_DATA_DIR ?? path.resolve(__dirname, "../.playwright-data");
// Control-plane isolation: state.tenant_db scopes every org's tables/domains/relationships to a
// Postgres schema named org_<ORG_ID> on the SHARED control-plane database (provisa/core/models.py
// resolves org_id from this env var). Without an override the e2e backend lands in org_default —
// the same schema the dev backend (start-ui.sh) already writes to — and their two configs'
// differing `pets` table sources collide as a duplicate domain+table registration. A distinct
// ORG_ID isolates the e2e run to its own schema with zero new infrastructure.
const E2E_ORG_ID = process.env.PROVISA_E2E_ORG_ID ?? "e2e";

// The e2e backend must also boot from its own config file, not config/provisa.yaml (the
// shared dev config, auth.provider: basic — booting against it makes every admin call,
// including this harness's own config bootstrap, require credentials that don't exist
// yet). Seed an isolated writable copy from config/provisa-install.yaml (auth.provider:
// none) so PUT /admin/config below can run unauthenticated, matching a fresh install.
const E2E_CONFIG_PATH =
  process.env.PROVISA_E2E_CONFIG ?? path.resolve(E2E_DATA_DIR, "provisa.yaml");
fs.mkdirSync(path.dirname(E2E_CONFIG_PATH), { recursive: true });
fs.copyFileSync(
  path.resolve(__dirname, "../config/provisa-install.yaml"),
  E2E_CONFIG_PATH,
);

// global-setup.ts/global-teardown.ts run in this same process — exporting these lets
// them derive the backend URL from the single source of truth above instead of
// hardcoding it a second time.
process.env.PROVISA_E2E_API_PORT = String(E2E_API_PORT);

// The control-plane postgres (see E2E_ORG_ID comment above) is the shared `provisa`
// compose project's `postgres` service. Its host port is docker-assigned (`${PG_PORT:-5432}`
// in docker-compose.core.yml) and can drift from the literal 5432 default baked into
// ControlPlaneConfig (and from whatever stale value .env's PLATFORM_DATABASE_URL carries) —
// asking the running container is the single source of truth, so the e2e backend's env
// always overrides both URLs with it rather than duplicating a port number that can go stale.
function resolveControlPlanePort(): string {
  const output = execFileSync(
    "docker",
    ["compose", "-f", path.resolve(__dirname, "../docker-compose.core.yml"), "port", "postgres", "5432"],
    { cwd: path.resolve(__dirname, ".."), encoding: "utf8" },
  ).trim();
  const port = output.split(":").pop();
  if (!port) {
    throw new Error(`Could not resolve control-plane postgres port from docker output: ${output}`);
  }
  return port;
}

const pgPort = resolveControlPlanePort();
const pgPassword = process.env.PG_PASSWORD ?? "provisa";
const controlPlaneUrl = `postgresql+asyncpg://provisa:${pgPassword}@localhost:${pgPort}/provisa`;
const controlPlaneEnv: Record<string, string> = {
  TENANT_DATABASE_URL: controlPlaneUrl,
  PLATFORM_DATABASE_URL: controlPlaneUrl,
};

export default defineConfig({
  globalSetup: "./e2e/global-setup.ts",
  globalTeardown: "./e2e/global-teardown.ts",
  testDir: "./e2e",
  testMatch: /[/\\][^.][^/\\]*\.spec\.ts$/,
  timeout: 30000,
  retries: 1,
  use: {
    baseURL: `http://localhost:${E2E_UI_PORT}`,
    headless: true,
  },
  webServer: [
    {
      command: "npm run dev",
      port: E2E_UI_PORT,
      env: {
        PROVISA_UI_PORT: String(E2E_UI_PORT),
        PROVISA_API_PORT: String(E2E_API_PORT),
      },
      reuseExistingServer: !process.env.CI,
      timeout: 15000,
    },
    {
      command: `bash -c 'cd .. && .venv/bin/uvicorn server:app --app-dir demo/graphql_server --host 0.0.0.0 --port ${E2E_GRAPHQL_DEMO_PORT}'`,
      url: `http://localhost:${E2E_GRAPHQL_DEMO_PORT}/graphql?query=%7B__typename%7D`,
      reuseExistingServer: !process.env.CI,
      timeout: 15000,
    },
    {
      command: `bash -c 'cd .. && .venv/bin/uvicorn server:app --app-dir demo/petstore_server --host 0.0.0.0 --port ${E2E_PETSTORE_PORT}'`,
      url: `http://localhost:${E2E_PETSTORE_PORT}/api/v3/pet/findByStatus?status=available`,
      reuseExistingServer: !process.env.CI,
      timeout: 15000,
    },
    {
      command: `bash -c 'cd .. && .venv/bin/uvicorn main:app --host 0.0.0.0 --port ${E2E_API_PORT}'`,
      url: `http://localhost:${E2E_API_PORT}/health`,
      env: {
        GRPC_PORT: String(E2E_GRPC_PORT),
        FLIGHT_PORT: String(E2E_FLIGHT_PORT),
        PROVISA_BOLT_PORT: String(E2E_BOLT_PORT),
        PROVISA_MCP_PORT: String(E2E_MCP_PORT),
        PROVISA_PGWIRE_PORT: String(E2E_PGWIRE_PORT),
        PROVISA_DATA_DIR: E2E_DATA_DIR,
        PROVISA_CONFIG: E2E_CONFIG_PATH,
        ORG_ID: E2E_ORG_ID,
        GRAPHQL_DEMO_URL: `http://localhost:${E2E_GRAPHQL_DEMO_PORT}/graphql`,
        PETSTORE_BASE_URL: `http://localhost:${E2E_PETSTORE_PORT}/api/v3`,
        ...controlPlaneEnv,
      },
      reuseExistingServer: !process.env.CI,
      timeout: 30000,
    },
  ],
});
