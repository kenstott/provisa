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

import type { Page } from "@playwright/test";

const GQL_QUERY = `query InquiryCountByUser @noCache {
  ps__inquiriesGroupBy(by: [userId]) {
    groupKey
    aggregate {
      count
    }
    nodes {
      status
      message
      pet {
        breedName
        available
        price
      }
    }
  }
}`;

// Cross-source path query plus a second domain — renders a connected graph of
// pets, breeds, and inquiries alongside the shelter assignments cluster.
const CYPHER_QUERY = `MATCH p=()-[:BREED_INFO]-()-[r:HAS_INQUIRY]->()
OPTIONAL MATCH (mAssignments:Shelter:Assignments)
RETURN p, mAssignments`;

// Cross-source so it actually exercises the Trino coordinator (single-source
// queries bypass federation). Avoids the all-domain path that touches the
// provisa_admin catalog. Used to confirm the engine is warm before capturing.
const FEDERATION_PROBE = "MATCH (p:Pets)-[r]-(b:AnimalBreeds) RETURN p, b LIMIT 3";

const NL_PROMPT = "show inquiry count by user";

interface Screen {
  route: string;
  filename: string;
  label: string;
  // Optional interaction that populates and runs a query before the screenshot.
  // When present, it fully owns navigation; otherwise the loop does a plain goto.
  prep?: (page: Page) => Promise<void>;
}

const SCREENS: Screen[] = [
  {
    route: "/query",
    filename: "query-explorer.png",
    label: "Query Language Explorer",
    prep: async (page) => {
      // QueryPage auto-runs when it mounts with router state { query, autoRun }.
      // Land on another route first so the remount picks up the state fresh.
      await page.goto(`${FRONTEND_URL}/sources`, { waitUntil: "domcontentloaded", timeout: 15_000 });
      await page.waitForTimeout(800);
      // Open the Provisa tools panel (live Semantic SQL + Cypher translations)
      // and enable query stats so the capture matches the Explorer's full UI.
      await page.evaluate(() => {
        localStorage.setItem("query:visiblePlugin", "Provisa");
        localStorage.setItem("query:statsEnabled", "true");
      });
      await page.evaluate((query) => {
        const idx = ((window.history.state && window.history.state.idx) || 0) + 1;
        const st = { usr: { query, autoRun: true }, key: "shot", idx };
        window.history.pushState(st, "", "/query");
        window.dispatchEvent(new PopStateEvent("popstate", { state: st }));
      }, GQL_QUERY);
      await page
        .waitForFunction(
          () => {
            const el = document.querySelector(".graphiql-response");
            return !!el && /inquiriesGroupBy|"data"/.test(el.textContent || "");
          },
          { timeout: 25_000 },
        )
        .catch(() => {});
      await page.waitForTimeout(1500);
    },
  },
  {
    route: "/nl",
    filename: "natural-language.png",
    label: "Natural Language Query",
    prep: async (page) => {
      // Navigate once and let the schema load — NL generation needs it, and a
      // fresh reload before each attempt produces empty branches. Then retry
      // generation in place until the GraphQL and Cypher branches populate
      // (their result tables make the most compelling shot). Generation streams
      // from a non-deterministic LLM, so some runs leave branches empty.
      await page.goto(`${FRONTEND_URL}/nl`, { waitUntil: "networkidle", timeout: 20_000 }).catch(() => {});
      await page.waitForTimeout(3500);
      await page.fill(".nl-textarea", NL_PROMPT);
      for (let attempt = 1; attempt <= 6; attempt++) {
        await page.click(".nl-submit-btn");
        await page
          .waitForFunction(
            () => {
              const btn = document.querySelector(".nl-submit-btn");
              const idle = !!btn && !/Generating/.test(btn.textContent || "");
              return idle && !document.querySelector(".nl-branch-loading");
            },
            { timeout: 90_000 },
          )
          .catch(() => {});
        await page.waitForTimeout(2000);
        const ready = await page.evaluate(() => {
          const panels = Array.from(document.querySelectorAll(".nl-branch-panel"));
          // graphql is panel 1, cypher is panel 2 (order: sql, graphql, cypher, …)
          return [1, 2].every((i) => {
            const q = panels[i] && panels[i].querySelector(".nl-branch-query");
            return !!q && (q.textContent || "").trim().length > 10;
          });
        });
        if (ready) break;
        console.log(`  …NL branches incomplete, regenerating (attempt ${attempt})`);
        // Let the shared query connection settle before re-submitting.
        await page.waitForTimeout(3000);
      }
      await page.waitForTimeout(1000);
    },
  },
  {
    route: "/graph",
    filename: "graph-view.png",
    label: "Graph Visualization",
    prep: async (page) => {
      // Mount /graph fresh with router state so the editor seeds CYPHER_QUERY.
      await page.goto(`${FRONTEND_URL}/sources`, { waitUntil: "domcontentloaded", timeout: 15_000 });
      await page.waitForTimeout(800);
      // Turn on imputed relationships so the graph shows inferred edges between
      // visible nodes, not just those returned by the query.
      await page.evaluate(() => localStorage.setItem("provisa.graph.autoImpute", "true"));
      await page.evaluate((query) => {
        const idx = ((window.history.state && window.history.state.idx) || 0) + 1;
        const st = { usr: { query }, key: "shot", idx };
        window.history.pushState(st, "", "/graph");
        window.dispatchEvent(new PopStateEvent("popstate", { state: st }));
      }, CYPHER_QUERY);
      await page.waitForTimeout(1800);
      await page.click(".graph-run-btn");
      // Federated path query takes several seconds — wait for the results frame
      // to report rendered nodes, then let the force layout settle.
      await page
        .waitForFunction(
          () => /Displaying\s+\d+\s+node/.test(document.body.textContent || ""),
          { timeout: 30_000 },
        )
        .catch(() => {});
      await page.waitForTimeout(2500);
      // Group nodes by domain to show off the clustering (hulls per domain).
      await page.selectOption(".gf-attr-select", "domain").catch(() => {});
      await page.waitForTimeout(4500);
    },
  },
  { route: "/schema",         filename: "schema-voyager.png",    label: "Schema Voyager" },
  { route: "/sources",        filename: "data-sources.png",      label: "Data Sources" },
  { route: "/tables",         filename: "table-registration.png",label: "Table Registration" },
  { route: "/relationships",  filename: "relationships.png",     label: "Relationships" },
  { route: "/security/roles", filename: "security-roles.png",    label: "Security Roles" },
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

// The federation engine (Trino) initializes lazily after the backend is up.
// Poll a single-source Cypher query until it executes cleanly so the graph and
// NL captures show results instead of "server is still initializing" errors.
async function waitForFederation(timeoutMs = 240_000): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  const body = JSON.stringify({ query: FEDERATION_PROBE, params: {} });
  const probeOnce = async (): Promise<boolean> => {
    try {
      const res = await fetch(`${BACKEND_URL}/data/cypher`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body,
      });
      const json = (await res.json()) as { error?: string; rows?: unknown[] };
      return !json.error && Array.isArray(json.rows);
    } catch {
      return false;
    }
  };
  // Trino returns SERVER_STARTING_UP intermittently during warmup, so one clean
  // result is not enough — require several consecutive successes before trusting it.
  let streak = 0;
  while (Date.now() < deadline) {
    if (await probeOnce()) {
      if (++streak >= 3) return;
    } else {
      streak = 0;
    }
    await new Promise((r) => setTimeout(r, 4_000));
  }
  console.warn("  ⚠ Federation engine not fully ready; capturing anyway.");
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

  console.log("Waiting for federation engine…");
  await waitForFederation();
  console.log("Federation ready.");

  for (const { route, filename, label, prep } of SCREENS) {
    console.log(`Capturing ${label}…`);
    try {
      if (prep) {
        await prep(page);
      } else {
        try {
          await page.goto(`${FRONTEND_URL}${route}`, { waitUntil: "networkidle", timeout: 15_000 });
        } catch {
          // Some views (e.g. graph) hold a persistent connection so the network
          // never idles — fall back to DOM load and rely on the settle timeout.
          await page.goto(`${FRONTEND_URL}${route}`, { waitUntil: "domcontentloaded", timeout: 15_000 });
        }
        // Extra settle time for animations and lazy-loaded content
        await page.waitForTimeout(2500);
      }
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
