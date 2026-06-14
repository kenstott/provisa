// Copyright (c) 2026 Kenneth Stott
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
  // Wait for schema to rebuild (graph-schema endpoint reflects PetStore tables)
  for (let i = 0; i < 20; i++) {
    await new Promise((r) => setTimeout(r, 500));
    const schema = await fetch("http://localhost:8000/data/graph-schema").then((r) => r.json());
    const labels: string[] = (schema.node_labels ?? []).map((n: { label: string }) => n.label);
    if (labels.some((l) => l.startsWith("PetStore:"))) return;
  }
  throw new Error("Schema did not rebuild with PetStore labels after config reload");
}
