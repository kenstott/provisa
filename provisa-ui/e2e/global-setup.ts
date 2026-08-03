// Copyright (c) 2026 Kenneth Stott
// Canary: 3f3483dc-d197-4627-8a8f-f3d2b1485276
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import * as fs from "fs";
import * as path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const CONFIG_PATH = path.resolve(__dirname, "../../config/provisa-install.yaml");
const SNAPSHOT_PATH = CONFIG_PATH + ".snapshot";
const BACKEND_URL = `http://localhost:${process.env.PROVISA_E2E_API_PORT ?? "8901"}`;

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

export default async function globalSetup() {
  const yaml = fs.readFileSync(CONFIG_PATH, "utf8");
  fs.writeFileSync(SNAPSHOT_PATH, yaml);
  const res = await putAdminConfig(`${BACKEND_URL}/admin/config`, yaml);
  if (!res.ok) {
    throw new Error(`Config reload failed: ${res.status} ${await res.text()}`);
  }

  // Ensure the setup wizard will not block page tests: if needs_setup=true, run the
  // setup endpoint to create the initial admin user.  The config already contains
  // auth.provider = basic, so POST /setup with provider=basic completes the flow.
  const statusRes = await fetch(`${BACKEND_URL}/setup/status`);
  if (statusRes.ok) {
    const status = await statusRes.json() as { needs_setup: boolean };
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
    if (i === 19) throw new Error("Schema did not rebuild with PetStore labels after config reload");
  }

  // Warm up DuckDB before tests run.  graph-show-children and graph-query-panel-height both
  // run a Cypher query against PetStore:Inquiries on page load.  DuckDB's first query against
  // a cold SQLite-backed source takes 30-90 s (file scan + materialisation).  When two workers
  // run these specs concurrently that cold query blocks the Python event loop, starving the
  // auth GraphQL calls the other test needs, causing it to time-out.  Running one warm-up
  // query here — before any test clock starts — ensures the DuckDB page cache is hot for
  // all subsequent specs.
  const warmRes = await fetch(`${BACKEND_URL}/data/cypher`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query: "MATCH (n:PetStore:Inquiries) RETURN n LIMIT 5", params: {} }),
  });
  if (!warmRes.ok) {
    throw new Error(`DuckDB warm-up query failed: ${warmRes.status} ${await warmRes.text()}`);
  }
}
