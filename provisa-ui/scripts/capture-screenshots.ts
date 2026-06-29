// Copyright (c) 2026 Kenneth Stott
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import { chromium } from "@playwright/test";
import { spawn, ChildProcess } from "child_process";
import * as fs from "fs";
import * as path from "path";
import { fileURLToPath } from "url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");
const UI_ROOT = path.resolve(__dirname, "..");
const IMAGES_DIR = path.resolve(ROOT, "docs/images");
const BACKEND_HEALTH = "http://localhost:8000/health";
const BACKEND_URL = "http://localhost:8000";
const FRONTEND_URL = "http://localhost:3000";
const VIEWPORT = { width: 1440, height: 900 };

const SCREENS: [string, string, string][] = [
  ["/query",          "query-explorer.png",    "Query Language Explorer"],
  ["/nl",             "natural-language.png",  "Natural Language Query"],
  ["/graph",          "graph-view.png",        "Graph Visualization"],
  ["/schema",         "schema-voyager.png",    "Schema Voyager"],
  ["/sources",        "data-sources.png",      "Data Sources"],
  ["/tables",         "table-registration.png","Table Registration"],
  ["/relationships",  "relationships.png",     "Relationships"],
  ["/security/roles", "security-roles.png",    "Security Roles"],
];

async function probe(url: string): Promise<boolean> {
  try {
    const res = await fetch(url, { signal: AbortSignal.timeout(1000) });
    return res.ok || res.status < 500;
  } catch {
    return false;
  }
}

async function waitFor(url: string, timeoutMs = 30_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await probe(url)) return;
    await new Promise((r) => setTimeout(r, 500));
  }
  throw new Error(`Timed out waiting for ${url}`);
}

async function main(): Promise<void> {
  fs.mkdirSync(IMAGES_DIR, { recursive: true });

  const spawned: ChildProcess[] = [];

  if (!await probe(BACKEND_HEALTH)) {
    console.log("Starting backend…");
    spawned.push(
      spawn("bash", ["-c", ".venv/bin/uvicorn main:app --host 0.0.0.0 --port 8000"], {
        cwd: ROOT,
        stdio: "pipe",
      })
    );
    await waitFor(BACKEND_HEALTH, 30_000);
    console.log("Backend ready.");
  }

  if (!await probe(FRONTEND_URL)) {
    console.log("Starting frontend…");
    spawned.push(spawn("npm", ["run", "dev"], { cwd: UI_ROOT, stdio: "pipe" }));
    await waitFor(FRONTEND_URL, 30_000);
    console.log("Frontend ready.");
  }

  // Reload config (mirrors global-setup)
  const configPath = path.resolve(ROOT, "config/provisa-install.yaml");
  const yaml = fs.readFileSync(configPath, "utf8");
  const configRes = await fetch(`${BACKEND_URL}/admin/config`, {
    method: "PUT",
    headers: { "Content-Type": "application/yaml" },
    body: yaml,
  });
  if (!configRes.ok) {
    throw new Error(`Config reload failed: ${configRes.status} ${await configRes.text()}`);
  }

  // Complete setup wizard if this is a fresh install
  const statusRes = await fetch(`${BACKEND_URL}/setup/status`);
  if (statusRes.ok) {
    const { needs_setup } = (await statusRes.json()) as { needs_setup: boolean };
    if (needs_setup) {
      const r = await fetch(`${BACKEND_URL}/setup`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          provider: "basic",
          mode: "single",
          admin_username: "admin",
          admin_password: "admin",
        }),
      });
      if (!r.ok && r.status !== 409) {
        throw new Error(`Setup failed: ${r.status} ${await r.text()}`);
      }
    }
  }

  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage({ viewport: VIEWPORT });

  // Authenticate if the app requires it
  await page.goto(`${FRONTEND_URL}/query`, { waitUntil: "networkidle" });
  if (page.url().includes("/login")) {
    await page.fill("#username", "admin");
    await page.fill("#password", "admin");
    await page.click('button[type="submit"]');
    await page.waitForURL(/\/(query|graph|sources|tables)/, { timeout: 10_000 });
  }

  for (const [route, filename, label] of SCREENS) {
    console.log(`Capturing ${label}…`);
    try {
      await page.goto(`${FRONTEND_URL}${route}`, { waitUntil: "networkidle", timeout: 15_000 });
      // Extra settle time for animations and lazy-loaded content
      await page.waitForTimeout(1500);
      const dest = path.join(IMAGES_DIR, filename);
      await page.screenshot({ path: dest });
      console.log(`  → docs/images/${filename}`);
    } catch (err) {
      console.warn(`  ✗ ${label}: ${(err as Error).message}`);
    }
  }

  await browser.close();

  for (const proc of spawned) proc.kill();

  console.log("\nDone. Screenshots saved to docs/images/");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
