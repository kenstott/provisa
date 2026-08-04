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
// Trino-backed E2E backend — a second isolated backend instance for sharepoint/splunk
// tests that require Trino catalog creation (TrinoBackend.register_source creates the
// catalog; NativeBackend.register_source is a no-op so the schema dropdown never
// populates). Uses a minimal config (domains only, no pre-registered sources) to
// avoid the TrinoPgBackedConnector PG-host networking complexity at startup.
const E2E_TRINO_API_PORT = Number(process.env.PROVISA_E2E_TRINO_API_PORT ?? 8910);
const E2E_TRINO_GRPC_PORT = Number(process.env.PROVISA_E2E_TRINO_GRPC_PORT ?? 8911);
const E2E_TRINO_FLIGHT_PORT = Number(process.env.PROVISA_E2E_TRINO_FLIGHT_PORT ?? 8912);
const E2E_TRINO_BOLT_PORT = Number(process.env.PROVISA_E2E_TRINO_BOLT_PORT ?? 8913);
const E2E_TRINO_MCP_PORT = Number(process.env.PROVISA_E2E_TRINO_MCP_PORT ?? 8914);
const E2E_TRINO_PGWIRE_PORT = Number(process.env.PROVISA_E2E_TRINO_PGWIRE_PORT ?? 8915);
// provisa-install.yaml (the config this harness boots from) statically registers a
// graphql-demo and a petstore-mock source. Both default to compose-network hostnames
// (graphql-demo:4000, petstore-mock:8080) that don't resolve outside docker-compose, so
// any cross-source query touching them fails with a connection error unless this harness
// runs the same host-process demo servers run-demo-servers.sh uses and points the API
// server's env at them.
const E2E_GRAPHQL_DEMO_PORT = Number(process.env.PROVISA_E2E_GRAPHQL_DEMO_PORT ?? 8907);
const E2E_PETSTORE_PORT = Number(process.env.PROVISA_E2E_PETSTORE_PORT ?? 8908);
const E2E_DATA_DIR = process.env.PROVISA_E2E_DATA_DIR ?? path.resolve(__dirname, "../.playwright-data");
// Lane selection. `core` is every spec that runs on the DuckDB (NativeBackend) backend — 37 of the
// 39 specs — and needs no container at all: the control plane runs on SQLite (see
// E2E_CONTROL_PLANE below) and the cache is embedded fakeredis (neither config sets
// cache.redis_url and no REDIS_URL is exported, so app.py resolves state.redis_url to None).
// `trino` is the two connector specs (sharepoint, splunk) that require TrinoBackend.register_source
// to create a catalog. `all` runs both and is the default so a bare `npx playwright test` on a dev
// box behaves exactly as before.
// The specs that address the Trino backend (TRINO_BACKEND_URL from e2e/coverage.ts). Kept as one
// literal so the project split and the lane's server list cannot drift apart.
const TRINO_SPECS = ["**/sharepoint-connector.spec.ts", "**/splunk-connector.spec.ts"];
const LANE = process.env.PROVISA_E2E_LANE ?? "all";
if (!["core", "trino", "all"].includes(LANE)) {
  throw new Error(`PROVISA_E2E_LANE must be core|trino|all, got: ${LANE}`);
}
const RUNS_CORE = LANE === "core" || LANE === "all";
const RUNS_TRINO = LANE === "trino" || LANE === "all";
// Control-plane store. SQLite is a first-class metadata home (REQ-889 / capabilities.yaml presets
// declare control_plane_store: sqlite): db.py::_init_schema_portable bootstraps from the
// dialect-neutral schema_org metadata instead of the PostgreSQL-only schema.sql, and OrgRouter
// gives file-per-org isolation where PG uses org_<id> schemas. It is the default for the core
// lane — no Docker, no compose project, no port discovery.
//
// The Trino lane cannot use it: Trino reaches the control plane over JDBC from inside its own
// container (PROVISA_ENGINE_CONTROL_PLANE_HOST below), which a local SQLite file cannot serve.
// That lane therefore requires the compose Postgres, and asking for the pair explicitly is an
// error rather than a silent downgrade.
const E2E_CONTROL_PLANE =
  process.env.PROVISA_E2E_CONTROL_PLANE ?? (LANE === "core" ? "sqlite" : "postgres");
if (!["sqlite", "postgres"].includes(E2E_CONTROL_PLANE)) {
  throw new Error(
    `PROVISA_E2E_CONTROL_PLANE must be sqlite|postgres, got: ${E2E_CONTROL_PLANE}`,
  );
}
if (RUNS_TRINO && E2E_CONTROL_PLANE === "sqlite") {
  throw new Error(
    "PROVISA_E2E_CONTROL_PLANE=sqlite cannot serve the Trino lane: Trino loads its catalog " +
      "specs over JDBC from inside its container and needs a networked control plane. Use " +
      "PROVISA_E2E_LANE=core, or run the Trino lane on postgres.",
  );
}
// Control-plane isolation: state.tenant_db scopes every org's tables/domains/relationships to a
// Postgres schema named org_<ORG_ID> on the SHARED control-plane database (provisa/core/models.py
// resolves org_id from this env var). Without an override the e2e backend lands in org_default —
// the same schema the dev backend (start-ui.sh) already writes to — and their two configs'
// differing `pets` table sources collide as a duplicate domain+table registration. A distinct
// ORG_ID isolates the e2e run to its own schema with zero new infrastructure.
const E2E_ORG_ID = process.env.PROVISA_E2E_ORG_ID ?? "e2e";
// Trino backend uses a separate data dir and org to avoid any state collision with the DuckDB backend.
const E2E_TRINO_DATA_DIR = process.env.PROVISA_E2E_TRINO_DATA_DIR ?? path.resolve(__dirname, "../.playwright-trino-data");
const E2E_TRINO_ORG_ID = process.env.PROVISA_E2E_TRINO_ORG_ID ?? "e2e_trino";

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
// Trino backend uses a minimal config (domains only, no pre-registered sources) so the
// TrinoPgBackedConnector PG-host networking requirement is not triggered at startup.
const E2E_TRINO_CONFIG_PATH =
  process.env.PROVISA_E2E_TRINO_CONFIG ?? path.resolve(E2E_TRINO_DATA_DIR, "provisa-trino.yaml");
if (RUNS_TRINO) {
  fs.mkdirSync(path.dirname(E2E_TRINO_CONFIG_PATH), { recursive: true });
  fs.copyFileSync(
    path.resolve(__dirname, "../config/provisa-trino-e2e.yaml"),
    E2E_TRINO_CONFIG_PATH,
  );
}

// global-setup.ts/global-teardown.ts run in this same process — exporting these lets
// them derive the backend URL from the single source of truth above instead of
// hardcoding it a second time.
process.env.PROVISA_E2E_API_PORT = String(E2E_API_PORT);
// Exported so global-setup can pre-warm the Vite dev server before workers start.
process.env.PROVISA_E2E_UI_PORT = String(E2E_UI_PORT);
// Exported so global-setup can bootstrap the Trino backend.
process.env.PROVISA_E2E_TRINO_API_PORT = String(E2E_TRINO_API_PORT);
process.env.PROVISA_E2E_TRINO_CONFIG = E2E_TRINO_CONFIG_PATH;
// Exported so global-setup knows whether a Trino backend exists to bootstrap.
process.env.PROVISA_E2E_LANE = LANE;

// The control-plane postgres (see E2E_ORG_ID comment above) is the shared `provisa`
// compose project's `postgres` service. Its host port is docker-assigned (`${PG_PORT:-5432}`
// in docker-compose.core.yml) and can drift from the literal 5432 default baked into
// ControlPlaneConfig (and from whatever stale value .env's PLATFORM_DATABASE_URL carries) —
// asking the running container is the single source of truth, so the e2e backend's env
// always overrides both URLs with it rather than duplicating a port number that can go stale.
function resolveControlPlanePort(): string {
  // Cache file: written on success so retry workers can load config even if the postgres
  // container is evicted by Docker VM memory pressure mid-run. The port is immutable for
  // the lifetime of a run (container binding is set at compose startup and never changes).
  const portCacheFile = path.resolve(E2E_DATA_DIR, "pg-port");
  try {
    const output = execFileSync(
      "docker",
      ["compose", "-f", path.resolve(__dirname, "../docker-compose.core.yml"), "port", "postgres", "5432"],
      { cwd: path.resolve(__dirname, ".."), encoding: "utf8" },
    ).trim();
    const port = output.split(":").pop();
    if (!port) {
      throw new Error(`Could not resolve control-plane postgres port from docker output: ${output}`);
    }
    fs.writeFileSync(portCacheFile, port, "utf8");
    return port;
  } catch (err) {
    // Postgres container may have been evicted by Docker VM memory pressure mid-run.
    // Fall back to the cached port written at run start — the binding is immutable.
    if (fs.existsSync(portCacheFile)) {
      return fs.readFileSync(portCacheFile, "utf8").trim();
    }
    throw err;
  }
}

// SQLite control plane: two files under the lane's own data dir. ORG_ID isolation is by FILE here,
// not by schema — Capabilities.schemas is false on SQLite, so OrgRouter (core/database.py) puts each
// org in a sibling org_<id>.db.
// resolveControlPlanePort() is deliberately NOT called on this path: it shells out to
// `docker compose port` and throws when no daemon is running, which is the whole point of the lane.
//
// The files are recreated per run, exactly as E2E_CONFIG_PATH is re-copied above: this is V1, there
// are no migrations, so a control plane left over from a run that predates a schema_org column
// fails every write with "no such column". The lane bootstraps all of its state through
// global-setup's PUT /admin/config, so nothing is lost by starting empty.
function sqliteControlPlaneEnv(dataDir: string): Record<string, string> {
  fs.mkdirSync(dataDir, { recursive: true });
  for (const f of fs.readdirSync(dataDir)) {
    if (/\.db(-wal|-shm)?$/.test(f)) fs.rmSync(path.join(dataDir, f));
  }
  return {
    TENANT_DATABASE_URL: `sqlite+aiosqlite:///${path.join(dataDir, "tenant.db")}`,
    PLATFORM_DATABASE_URL: `sqlite+aiosqlite:///${path.join(dataDir, "platform.db")}`,
  };
}

function postgresControlPlaneEnv(port: string): Record<string, string> {
  const pgPassword = process.env.PG_PASSWORD ?? "provisa";
  const url = `postgresql+asyncpg://provisa:${pgPassword}@localhost:${port}/provisa`;
  return { TENANT_DATABASE_URL: url, PLATFORM_DATABASE_URL: url };
}

// Only resolved on the Postgres path — the Trino webServer below also needs the raw port for
// PROVISA_ENGINE_CONTROL_PLANE_PORT, and that webServer only exists when the control plane is PG.
const pgPort = E2E_CONTROL_PLANE === "postgres" ? resolveControlPlanePort() : null;
const controlPlaneEnv =
  pgPort === null ? sqliteControlPlaneEnv(E2E_DATA_DIR) : postgresControlPlaneEnv(pgPort);

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
        // Increase Vite's Node.js heap: 6 concurrent workers loading the full React app
        // bundle (Monaco, Cytoscape, Istanbul-instrumented sources) can OOM the default
        // 1.5 GB heap, crashing the dev server mid-run.
        NODE_OPTIONS: "--max-old-space-size=4096",
      },
      reuseExistingServer: !process.env.CI,
      timeout: 30000,
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
    // Trino-backed backend for sharepoint/splunk tests.  sharepoint/splunk require
    // TrinoBackend.register_source() to create a Trino catalog; NativeBackend (DuckDB) is a
    // no-op so the schema dropdown never populates.  A minimal config (domains only, no
    // pre-registered sources) avoids TrinoPgBackedConnector startup failures (that connector
    // requires PG_HOST resolvable from inside the Trino Docker container).
    //
    // Declared only when the Trino lane runs: booting a Trino-engine backend costs a cold JVM
    // and a compose stack, and the core lane's specs never address it.
    ...(RUNS_TRINO ? [{
      command: `bash -c 'cd .. && .venv/bin/uvicorn main:app --host 0.0.0.0 --port ${E2E_TRINO_API_PORT}'`,
      url: `http://localhost:${E2E_TRINO_API_PORT}/health`,
      env: {
        GRPC_PORT: String(E2E_TRINO_GRPC_PORT),
        FLIGHT_PORT: String(E2E_TRINO_FLIGHT_PORT),
        PROVISA_BOLT_PORT: String(E2E_TRINO_BOLT_PORT),
        PROVISA_MCP_PORT: String(E2E_TRINO_MCP_PORT),
        PROVISA_PGWIRE_PORT: String(E2E_TRINO_PGWIRE_PORT),
        PROVISA_DATA_DIR: E2E_TRINO_DATA_DIR,
        PROVISA_CONFIG: E2E_TRINO_CONFIG_PATH,
        ORG_ID: E2E_TRINO_ORG_ID,
        GRAPHQL_DEMO_URL: `http://localhost:${E2E_GRAPHQL_DEMO_PORT}/graphql`,
        PETSTORE_BASE_URL: `http://localhost:${E2E_PETSTORE_PORT}/api/v3`,
        PROVISA_ENGINE: "trino",
        // Trino runs inside Docker; "localhost" in the app's control-plane URL
        // resolves to the Trino container itself, not the host.  These vars
        // make engine_visible_address() (trino_system_catalogs.py) substitute
        // the Docker host address so Trino's JDBC catalog specs reach Postgres.
        PROVISA_ENGINE_CONTROL_PLANE_HOST: "host.docker.internal",
        PROVISA_ENGINE_CONTROL_PLANE_PORT: String(pgPort),
        ...controlPlaneEnv,
      },
      reuseExistingServer: !process.env.CI,
      // Trino backend startup includes register_system_catalogs() which executes
      // DROP + CREATE CATALOG for each system catalog via Trino JDBC — each round
      // trip can take 10-30 s on a cold Trino JVM. 300 s gives headroom for 3 catalogs
      // × 2 ops × 30 s plus seed_ops_trino() Iceberg DDL on a cold JIT.
      timeout: 300000,
    }] : []),
  ],
  // The lane split is expressed as projects so `--project=core` / `--project=trino` selects it
  // per-run, while PROVISA_E2E_LANE controls which servers get booted for it. TRINO_SPECS is the
  // exhaustive list of specs that address the Trino backend (they import TRINO_BACKEND_URL from
  // ./coverage); everything else runs on the DuckDB backend and belongs to core.
  projects: [
    ...(RUNS_CORE
      ? [{ name: "core", testIgnore: TRINO_SPECS }]
      : []),
    ...(RUNS_TRINO
      ? [{ name: "trino", testMatch: TRINO_SPECS }]
      : []),
  ],
});
