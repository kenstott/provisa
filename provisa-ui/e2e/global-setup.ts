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

export default async function globalSetup() {
  const yaml = fs.readFileSync(CONFIG_PATH, "utf8");
  fs.writeFileSync(SNAPSHOT_PATH, yaml);
  const res = await fetch("http://localhost:8000/admin/config", {
    method: "PUT",
    headers: { "Content-Type": "application/yaml" },
    body: yaml,
  });
  if (!res.ok) {
    throw new Error(`Config reload failed: ${res.status} ${await res.text()}`);
  }

  // Ensure the setup wizard will not block page tests: if needs_setup=true, run the
  // setup endpoint to create the initial admin user.  The config already contains
  // auth.provider = basic, so POST /setup with provider=basic completes the flow.
  const statusRes = await fetch("http://localhost:8000/setup/status");
  if (statusRes.ok) {
    const status = await statusRes.json() as { needs_setup: boolean };
    if (status.needs_setup) {
      const setupRes = await fetch("http://localhost:8000/setup", {
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
    const schema = await fetch("http://localhost:8000/data/graph-schema").then((r) => r.json());
    const labels: string[] = (schema.node_labels ?? []).map((n: { label: string }) => n.label);
    if (labels.some((l) => l.startsWith("PetStore:"))) return;
  }
  throw new Error("Schema did not rebuild with PetStore labels after config reload");
}
