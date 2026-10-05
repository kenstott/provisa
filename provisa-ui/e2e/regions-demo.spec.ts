// Copyright (c) 2026 Kenneth Stott
// Canary: b986aa28-aebf-4bff-a711-244ba6aaec71
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

// REQ-1922 two-region selector, end to end against the two-region demo (eu + us) that
// scripts/launch-demo-regions.sh brings up in its spec-owned --test mode: unique unexposed ports, a
// temp data dir, fixed logins, torn down after. It is its OWN Playwright project (`regions-demo`) and
// runs only when that project is selected (`--project regions-demo`); the default projects exclude it
// by testMatch/testIgnore. No skip -- selecting the project is the gate.

import { spawn, execSync, type ChildProcess } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { test, expect } from "./coverage";
import { request as pwRequest, type APIRequestContext } from "@playwright/test";

const HERE = path.dirname(fileURLToPath(import.meta.url));

interface Env {
  euUi: number;
  euApi: number;
  usUi: number;
  usApi: number;
  operatorPw: string;
  residentPw: string;
}

let proc: ChildProcess | null = null;
let env: Env;
let operatorTok = "";
let residentTok = "";

function launch(): Promise<Env> {
  const script = path.resolve(HERE, "../../scripts/launch-demo-regions.sh");
  proc = spawn("bash", [script, "--test"], { stdio: ["ignore", "pipe", "inherit"] });
  return new Promise<Env>((resolve, reject) => {
    // Generous: a fresh worktree builds the UI bundle once (provisa/_ui) before the nodes start.
    const timer = setTimeout(() => reject(new Error("demo-regions did not print PORTS/CREDS_FILE in 420s")), 420_000);
    let buf = "";
    proc!.stdout!.on("data", (d: Buffer) => {
      buf += d.toString();
      const p = buf.match(/PORTS eu_ui=(\d+) eu_api=(\d+) us_ui=(\d+) us_api=(\d+)/);
      // The launcher never prints a password: it names a 0600 file in its own temp dir.
      const c = buf.match(/CREDS_FILE (\S+)/);
      if (p && c) {
        clearTimeout(timer);
        const creds = JSON.parse(fs.readFileSync(c[1], "utf8")) as { operator: string; resident: string };
        resolve({ euUi: +p[1], euApi: +p[2], usUi: +p[3], usApi: +p[4], operatorPw: creds.operator, residentPw: creds.resident });
      }
    });
    proc!.on("exit", (code) => reject(new Error(`launcher exited early (${code})`)));
  });
}

/** A bearer token from /auth/login for a basic-auth user. */
async function login(api: number, username: string, password: string): Promise<string> {
  const ctx: APIRequestContext = await pwRequest.newContext();
  const r = await ctx.post(`http://127.0.0.1:${api}/auth/login`, { data: { username, password } });
  expect(r.ok(), `login ${username}: ${r.status()}`).toBeTruthy();
  const tok = (await r.json()).access_token as string;
  await ctx.dispose();
  return tok;
}

/** An API context for ``api`` authenticated as the given bearer token. */
async function asRole(api: number, token: string): Promise<APIRequestContext> {
  return pwRequest.newContext({
    baseURL: `http://127.0.0.1:${api}`,
    extraHTTPHeaders: { Authorization: `Bearer ${token}` },
  });
}

test.beforeAll(async () => {
  test.setTimeout(480_000); // covers the one-time UI build in launch() on a fresh worktree
  env = await launch();
  // operator is the data-plane admin (org_admin); resident is the restricted steward (eu_resident).
  // The break-glass superuser is control-plane only and holds no data capabilities, so all admin
  // data work runs as operator (REQ-1921 / platform-tenant separation).
  operatorTok = await login(env.euApi, "operator", env.operatorPw);
  residentTok = await login(env.euApi, "resident", env.residentPw);
});

test.afterAll(async () => {
  if (proc) proc.kill("SIGINT"); // --test teardown trap wipes the temp data dir
});

test.describe("REQ-1922 region selector, two-region demo", () => {
  test("the selector defaults to the connected region, shows the hidden count, and switches", async ({ page }) => {
    // Authenticate the UI as the operator (org_admin) by seeding the token the login flow stores.
    await page.addInitScript((t) => localStorage.setItem("provisa_token", t as string), operatorTok);
    await page.goto(`http://127.0.0.1:${env.euUi}/tables`);
    // A freshly-invited member meets a one-time "You've been added to <org>" dialog that covers the
    // page; acknowledge it like a first-time operator would, so the tables page is interactive.
    const gotIt = page.getByRole("button", { name: "Got it" });
    await gotIt.waitFor({ state: "visible", timeout: 15_000 }).then(
      () => gotIt.click(),
      () => undefined, // no dialog (already acknowledged) — nothing to dismiss
    );
    // The Mantine Select exposes both the input and its listbox with aria-label "Region"; target the
    // input specifically to avoid a strict-mode match of two elements.
    const selector = page.getByRole("textbox", { name: "Region" });
    await expect(selector).toBeVisible();
    // Default is the node's region (eu): us-homed rows are hidden and counted.
    await expect(page.getByTestId("region-hidden-count")).toBeVisible();
    await selector.click();
    await page.getByRole("option", { name: /all regions/i }).click();
    await expect(page.getByTestId("region-hidden-count")).toHaveCount(0);
  });

  test("a new table defaults to the node's region on registration", async () => {
    const eu = await asRole(env.euApi, operatorTok);
    // A distinct new table (not the config's no-region `breeds`, which test 5 needs).
    const reg = await eu.post("/admin/graphql", {
      data: {
        query:
          'mutation { registerTable(input: {sourceId: "shelter_files", domainId: "shelter", schemaName: "main", tableName: "breeds2", fileGlob: "breeds/*.csv", columns: [{name: "name", visibleTo: ["org_admin"], dataType: "varchar", isPrimaryKey: true}]}) { success } }',
      },
    });
    expect(reg.ok()).toBeTruthy();
    const q = await eu.post("/admin/graphql", { data: { query: "{ tables { tableName region } }" } });
    const tables = (await q.json()).data.tables as { tableName: string; region: string | null }[];
    // A new table registered on the eu node defaults to eu (its source names no region).
    expect(tables.find((t) => t.tableName === "breeds2")?.region).toBe("eu");
    await eu.dispose();
  });

  test("a us node reads an eu-homed table, served from eu's built replica", async () => {
    const eu = await asRole(env.euApi, operatorTok);
    const us = await asRole(env.usApi, operatorTok);
    // Poke the eu node to build intake_eu's replica, then the cross-region read on us succeeds.
    let rows: unknown[] | null = null;
    for (let i = 0; i < 12 && !rows; i++) {
      await eu.post("/data/sql", { data: { sql: 'SELECT "id" FROM "intake_eu"', role: "org_admin" } });
      await new Promise((r) => setTimeout(r, 6000));
      const r = await us.post("/data/sql", { data: { sql: 'SELECT "id" FROM "intake_eu" ORDER BY "id"', role: "org_admin" } });
      if (r.ok()) {
        const j = await r.json();
        if (j?.data?.sql?.length) rows = j.data.sql;
      }
    }
    expect(rows, "us never got a cross-region read of intake_eu").not.toBeNull();
    await eu.dispose();
    await us.dispose();
  });

  test("data_residency refuses a region edit it does not cover (resident), naming the region", async () => {
    const admin = await asRole(env.euApi, operatorTok);
    const q = await admin.post("/admin/graphql", { data: { query: "{ tables { id tableName } }" } });
    const tables = (await q.json()).data.tables as { id: number; tableName: string }[];
    const idOf = (n: string) => tables.find((t) => t.tableName === n)!.id;
    await admin.dispose();

    const resident = await asRole(env.euApi, residentTok);
    // resident's data_residency grant covers eu + no_region, not us: moving the us-homed table is
    // refused BY NAME (names us).
    const refuse = await resident.post("/admin/graphql", {
      data: { query: `mutation { setTableRegion(tableId: ${idOf("intake_us")}, region: "eu") { success message } }` },
    });
    const refuseBody = (await refuse.json()).data.setTableRegion as { success: boolean; message: string };
    expect(refuseBody.success).toBeFalsy();
    expect(refuseBody.message.toLowerCase()).toContain("us");
    // Setting the no-region table to a covered region (eu) succeeds.
    const ok = await resident.post("/admin/graphql", {
      data: { query: `mutation { setTableRegion(tableId: ${idOf("breeds")}, region: "eu") { success } }` },
    });
    expect((await ok.json()).data.setTableRegion.success).toBeTruthy();
    await resident.dispose();
  });

  test("with the eu node stopped, the us cross-region read is refused by name", async () => {
    // Stop just the eu node (its API port), then the us read of the eu-homed table is refused and the
    // message names the region — its replica is no longer reachable.
    try {
      execSync(`lsof -nP -iTCP:${env.euApi} -sTCP:LISTEN -t | xargs kill`, { stdio: "ignore" });
    } catch {
      /* already down */
    }
    await new Promise((r) => setTimeout(r, 6000));
    const us = await asRole(env.usApi, operatorTok);
    const r = await us.post("/data/sql", { data: { sql: 'SELECT "id" FROM "intake_eu"', role: "org_admin" } });
    const body = await r.text();
    expect(r.ok()).toBeFalsy();
    expect(body.toLowerCase()).toContain("eu");
    await us.dispose();
  });
});
