// Copyright (c) 2026 Kenneth Stott
// Canary: 3f3483dc-d197-4627-8a8f-f3d2b1485276
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import * as fs from "fs";
import * as path from "path";
import { fileURLToPath } from "url";

import { startNeo4jContainer } from "./neo4j-container";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const CONFIG_PATH = path.resolve(__dirname, "../../config/provisa-install.yaml");
const SNAPSHOT_PATH = CONFIG_PATH + ".snapshot";
// One backend per Playwright worker — see playwright.config.ts. Every one of them boots empty and
// has to be bootstrapped here; a worker whose backend never got its config would see no tables at
// all. The list is published by playwright.config.ts, which runs in this same process.
const BACKEND_URLS = (process.env.PROVISA_E2E_BACKEND_PORTS ?? "8901")
  .split(",")
  .map((port) => `http://localhost:${port}`);
// Use 127.0.0.1 explicitly — Node.js resolves `localhost` to ::1 on this platform
// but the Vite dev server binds to 127.0.0.1 (IPv4) only.
const UI_URL = `http://127.0.0.1:${process.env.PROVISA_E2E_UI_PORT ?? "3901"}`;
// Trino backend (sharepoint/splunk tests) — PROVISA_E2E_TRINO_CONFIG is set by playwright.config.ts.
const TRINO_BACKEND_URL = `http://localhost:${process.env.PROVISA_E2E_TRINO_API_PORT ?? "8990"}`;
const TRINO_CONFIG_PATH =
  process.env.PROVISA_E2E_TRINO_CONFIG ??
  path.resolve(__dirname, "../../.playwright-trino-data/provisa-trino.yaml");

/** PUT /admin/config with connection-level retry.
 *
 * Rationale: Playwright's webServer waits for GET /health to return 200, which
 * happens as soon as uvicorn starts accepting connections — before all async
 * startup tasks (OTel, Flight, pgwire, config load) complete. The first heavy
 * request arrives in that narrow window and the kernel drops the connection
 * ("other side closed" / ECONNRESET). Retrying a few times with a brief delay
 * bridges that window without masking real HTTP-level errors (4xx/5xx).
 */
async function putAdminConfig(url: string, body: string): Promise<Response> {
  const RETRIES = 5;
  const DELAY_MS = 3000;
  for (let attempt = 1; attempt <= RETRIES; attempt++) {
    try {
      const res = await fetch(url, {
        method: "PUT",
        headers: { "Content-Type": "application/yaml" },
        body,
      });
      return res; // HTTP response received (may be non-2xx — caller checks)
    } catch (err) {
      if (attempt === RETRIES) throw err;
      await new Promise((r) => setTimeout(r, DELAY_MS));
    }
  }
  throw new Error("unreachable");
}

/** Bring one worker's backend from a bare uvicorn to a queryable, warmed-up instance. */
async function bootstrapBackend(BACKEND_URL: string, yaml: string) {
  const res = await putAdminConfig(`${BACKEND_URL}/admin/config`, yaml);
  if (!res.ok) {
    throw new Error(`Config reload failed: ${res.status} ${await res.text()}`);
  }

  // Ensure the setup wizard will not block page tests: if needs_setup=true, run the
  // setup endpoint to create the initial admin user.  The config already contains
  // auth.provider = basic, so POST /setup with provider=basic completes the flow.
  const statusRes = await fetch(`${BACKEND_URL}/setup/status`);
  if (statusRes.ok) {
    const status = (await statusRes.json()) as { needs_setup: boolean };
    if (status.needs_setup) {
      const setupRes = await fetch(`${BACKEND_URL}/setup`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          provider: "basic",
          mode: "single",
          admin_username: "admin",
          admin_password: "admin",
        }),
      });
      if (!setupRes.ok && setupRes.status !== 409) {
        // 409 = user already exists; treat as success
        throw new Error(`Setup failed: ${setupRes.status} ${await setupRes.text()}`);
      }
    }
  }

  // Wait for schema to rebuild (graph-schema endpoint reflects PetStore tables)
  for (let i = 0; i < 20; i++) {
    await new Promise((r) => setTimeout(r, 500));
    const schema = await fetch(`${BACKEND_URL}/data/graph-schema`).then((r) => r.json());
    const labels: string[] = (schema.node_labels ?? []).map((n: { label: string }) => n.label);
    if (labels.some((l) => l.startsWith("PetStore:"))) break;
    if (i === 19)
      throw new Error("Schema did not rebuild with PetStore labels after config reload");
  }

  // Warm up DuckDB before tests run.  graph-show-children and graph-query-panel-height both
  // run Cypher queries against PetStore tables on page load.  DuckDB's first query against
  // a cold SQLite-backed source takes 30-90 s (file scan + materialisation).  When multiple
  // workers run these specs concurrently that cold query blocks the Python event loop, starving
  // auth GraphQL calls the other tests need, causing timeouts.  Running sequential warm-up
  // queries here — before any test clock starts — ensures the DuckDB page cache is hot for
  // all subsequent specs.
  //
  // Inquiries: used by graph-show-children and graph-query-panel-height queries on page load.
  const warmRes = await fetch(`${BACKEND_URL}/data/cypher`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query: "MATCH (n:PetStore:Inquiries) RETURN n LIMIT 5", params: {} }),
  });
  if (!warmRes.ok) {
    throw new Error(`DuckDB warm-up query failed: ${warmRes.status} ${await warmRes.text()}`);
  }

  // Pets: used by graph-show-children "Show children" operation which fetches HAS_PETS relations,
  // and by graph-query-panel-height which runs OPTIONAL MATCH (a:Inquiries)-[:HAS_PETS]->(b:Pets).
  // Pets and Inquiries are different materialisations even though they share the same SQLite source.
  const petsWarmRes = await fetch(`${BACKEND_URL}/data/cypher`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query: "MATCH (n:PetStore:Pets) RETURN n LIMIT 5", params: {} }),
  });
  if (!petsWarmRes.ok) {
    throw new Error(
      `Pets DuckDB warm-up query failed: ${petsWarmRes.status} ${await petsWarmRes.text()}`,
    );
  }

  // Warm up the Shelter domain too — cypher-variable-length-path and cypher-impute-edges
  // run cross-source queries early in the test suite. Without this warm-up, the first
  // query against a cold Shelter source can take 30-90 s, triggering the 30 s Playwright
  // test timeout.
  const shelterWarmRes = await fetch(`${BACKEND_URL}/data/cypher`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query: "MATCH (n:Shelter:Employees) RETURN n LIMIT 5", params: {} }),
  });
  if (!shelterWarmRes.ok) {
    throw new Error(
      `Shelter DuckDB warm-up query failed: ${shelterWarmRes.status} ${await shelterWarmRes.text()}`,
    );
  }
}

export default async function globalSetup() {
  // Start the Neo4j export target now, before any browser exists — see neo4j-container.ts for why
  // it cannot start mid-run. `docker run -d` returns immediately; the spec still waits for the
  // engine to accept queries, and by then the container has had the whole bootstrap to boot.
  // The trino lane does not carry neo4j-docker-export.spec.ts, so it starts nothing.
  if (process.env.PROVISA_E2E_LANE !== "trino") startNeo4jContainer();

  const yaml = fs.readFileSync(CONFIG_PATH, "utf8");
  fs.writeFileSync(SNAPSHOT_PATH, yaml);
  // In parallel: the bootstrap is dominated by each backend's cold DuckDB warm-up queries
  // (30-90 s apiece), and they are separate processes on separate data dirs with nothing to
  // serialise on. Serially this would add minutes to every run's fixed cost.
  await Promise.all(BACKEND_URLS.map((url) => bootstrapBackend(url, yaml)));

  // Bootstrap the Trino-backed backend (sharepoint/splunk tests).  It uses a minimal
  // config (domains only, no pre-registered sources) so TrinoPgBackedConnector is never
  // invoked at startup.  No DuckDB warm-up needed — the Trino backend has no sqlite tables.
  // The minimal config was already written to TRINO_CONFIG_PATH by playwright.config.ts.
  // Skipped in the core lane, where playwright.config.ts declares no Trino webServer at all —
  // there is nothing listening on TRINO_BACKEND_URL to configure.
  if (process.env.PROVISA_E2E_LANE !== "core") {
    const trinoYaml = fs.readFileSync(TRINO_CONFIG_PATH, "utf8");
    const trinoRes = await putAdminConfig(`${TRINO_BACKEND_URL}/admin/config`, trinoYaml);
    if (!trinoRes.ok) {
      throw new Error(
        `Trino backend config reload failed: ${trinoRes.status} ${await trinoRes.text()}`,
      );
    }
  }

  // Pre-warm the Vite dev server: fetch the root so Node compiles the full React bundle once
  // before 6 workers all navigate concurrently. Without this, all 6 parallel worker processes
  // trigger simultaneous heavy compilation of Monaco + Cytoscape + Istanbul-instrumented sources,
  // spiking memory and potentially OOM-crashing the dev server even with the 4 GB heap cap.
  // global-setup runs serially before any worker starts, so this single fetch amortises the
  // compile cost across the whole suite rather than concentrating it at suite start.
  try {
    await fetch(UI_URL, { signal: AbortSignal.timeout(60000) });
  } catch (err) {
    // A connection error here means the Vite server is not up yet — Playwright's webServer
    // waits for the port before calling globalSetup, so this should not happen.
    throw new Error(`Vite dev server pre-warm fetch to ${UI_URL} failed`, { cause: err });
  }
}
