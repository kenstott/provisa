// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import { defineConfig } from "@playwright/test";
import * as fs from "fs";
import * as path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
// Same root .env the local harness reads — it is where the break-glass credentials live.
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

/**
 * REQ-1472: the cloud target — the same e2e specs, run against a deployment this process does not
 * own (cloud.provisa.dev by default).
 *
 * A separate config rather than a branch inside playwright.config.ts: that file's entire body is
 * provisioning — port discovery, N backend processes, demo servers, a Vite dev server, on-disk
 * control-plane seeding — and none of it may run here. What is left after removing it is this
 * file, and keeping the two apart means the local harness cannot acquire a code path that skips
 * its own setup.
 *
 * Not every spec is eligible. The list below is the specs that drive the SPA against the
 * deployment's own demo sources; the rest seed or redirect a backend they would have to own.
 */
const CLOUD_URL = process.env.PROVISA_E2E_CLOUD_URL ?? "https://cloud.provisa.dev";
const STORAGE_STATE =
  process.env.PROVISA_E2E_STORAGE_STATE ??
  path.resolve(__dirname, "../.playwright-data/cloud-storage-state.json");

process.env.PROVISA_E2E_TARGET = "cloud";
process.env.PROVISA_E2E_CLOUD_URL = CLOUD_URL;
process.env.PROVISA_E2E_STORAGE_STATE = STORAGE_STATE;

const CLOUD_SPECS = [
  "**/tables-preview.spec.ts",
  "**/relationships-header.spec.ts",
  "**/reports-tab.spec.ts",
  "**/graph-favorites.spec.ts",
  "**/openapi-explorer.spec.ts",
  "**/jsonapi-explorer.spec.ts",
  "**/grpc-explorer.spec.ts",
  "**/glossary.spec.ts",
  "**/tour-page-preload.spec.ts",
];

export default defineConfig({
  globalSetup: "./e2e/cloud-setup.ts",
  testDir: "./e2e",
  // One deployment, one control plane, one shared engine: workers here contend for the same
  // state rather than getting a backend each, which is what the local harness's per-worker
  // processes exist to prevent. Serial is the only isolation available against a shared target.
  workers: 1,
  // A shard that has idled to zero takes 90-120 s to come back (REQ-1448), and the first
  // navigation of the run is what triggers it.
  timeout: 180000,
  retries: 1,
  use: {
    baseURL: CLOUD_URL,
    storageState: STORAGE_STATE,
    headless: true,
  },
  projects: [{ name: "cloud", testMatch: CLOUD_SPECS }],
});
