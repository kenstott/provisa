// Copyright (c) 2026 Kenneth Stott
// Canary: fe60a112-9cd3-4bca-bd68-72f2812f4a46
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

// REQ-1922 two-region selector, end to end against the two-region demo (eu + us) that
// scripts/launch-demo-regions.sh brings up in its spec-owned --test mode: unique unexposed ports, a
// temp data dir, torn down after. Kept OUT of the default e2e lanes: it skips unless RUN_REGIONS_DEMO
// is set, so `--project=core` collects it and moves on. infra-features adds a CI lane that sets the
// flag (and, when this is proven, gives it its own Playwright project).
//
// PROOF IS BLOCKED until replica-layout-2's region-qualified naming lands on regions-3c (two regions
// share one Postgres). Do not set RUN_REGIONS_DEMO before then.

import { spawn, type ChildProcess } from "node:child_process";
import path from "node:path";
import { test, expect } from "./coverage";

const RUN = process.env.RUN_REGIONS_DEMO === "1";

interface Ports {
  euUi: number;
  euApi: number;
  usUi: number;
  usApi: number;
}

let proc: ChildProcess | null = null;
let ports: Ports | null = null;

async function launch(): Promise<Ports> {
  const script = path.resolve(__dirname, "../../scripts/launch-demo-regions.sh");
  proc = spawn("bash", [script, "--test"], { stdio: ["ignore", "pipe", "inherit"] });
  return await new Promise<Ports>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("demo-regions did not print PORTS in 180s")), 180_000);
    let buf = "";
    proc!.stdout!.on("data", (d: Buffer) => {
      buf += d.toString();
      const m = buf.match(/PORTS eu_ui=(\d+) eu_api=(\d+) us_ui=(\d+) us_api=(\d+)/);
      if (m) {
        clearTimeout(timer);
        resolve({ euUi: +m[1], euApi: +m[2], usUi: +m[3], usApi: +m[4] });
      }
    });
    proc!.on("exit", (code) => reject(new Error(`launcher exited early (${code})`)));
  });
}

test.beforeAll(async () => {
  test.skip(!RUN, "two-region demo not provable yet (region-qualified names pending); set RUN_REGIONS_DEMO=1 to run");
  ports = await launch();
});

test.afterAll(async () => {
  // The launcher's --test teardown trap wipes its temp data dir when the process group is killed.
  if (proc) proc.kill("SIGINT");
});

test.describe("REQ-1922 region selector, two-region demo", () => {
  test("the selector defaults to the connected region, says how many it hides, and switches", async ({ page }) => {
    await page.goto(`http://127.0.0.1:${ports!.euUi}/admin/tables`);
    const selector = page.getByLabel("Region");
    await expect(selector).toBeVisible();
    // Default is the node's region (eu): us-homed rows are hidden and counted.
    await expect(page.getByTestId("region-hidden-count")).toBeVisible();
    // Switching to all regions shows everything (nothing hidden).
    await selector.click();
    await page.getByRole("option", { name: /all regions/i }).click();
    await expect(page.getByTestId("region-hidden-count")).toHaveCount(0);
  });

  test("a new table defaults to the node's region on registration", async () => {
    // TODO(prove): open Tables -> register, assert the region field preselects `eu` on the eu node.
    test.fixme(true, "fill in against the running demo once proof is unblocked");
  });

  test("a us node reads an eu-homed table, served from eu's replica", async () => {
    // TODO(prove): query intake_eu on the us API and assert rows come back (a cross-region READ,
    // governed by RLS — not by data_residency).
    test.fixme(true, "fill in against the running demo once proof is unblocked");
  });

  test("data_residency refuses a region EDIT it does not cover, naming the region", async () => {
    // TODO(prove): as eu_resident (grant = eu, no_region), change a us-homed table's region on the
    // eu node -> refused, message names `us`; then set the no-region table's region to eu -> succeeds.
    // data_residency gates setting the region, not reads.
    test.fixme(true, "fill in against the running demo once proof is unblocked");
  });

  test("stopping the eu node refuses the cross-region read by name", async () => {
    // TODO(prove): stop eu, read intake_eu on us, assert a by-name refusal (503).
    test.fixme(true, "fill in against the running demo once proof is unblocked");
  });
});
