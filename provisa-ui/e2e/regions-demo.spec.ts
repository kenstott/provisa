// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

// REQ-1922 two-region selector, end to end against the two-region demo (eu + us) that
// scripts/launch-demo-regions.sh brings up in its spec-owned --test mode: unique unexposed ports, a
// temp data dir, fixed logins, torn down after. It is its OWN Playwright project (`regions-demo`) and
// runs only when that project is selected (`--project regions-demo`); the default projects exclude it
// by testMatch/testIgnore. No skip -- selecting the project is the gate.

import { spawn, execSync, type ChildProcess } from "node:child_process";
import path from "node:path";
import { test, expect } from "./coverage";
import { request as pwRequest, type APIRequestContext } from "@playwright/test";

interface Env {
  euUi: number;
  euApi: number;
  usUi: number;
  usApi: number;
  adminPw: string;
}

let proc: ChildProcess | null = null;
let env: Env;
let adminTok = "";

function launch(): Promise<Env> {
  const script = path.resolve(__dirname, "../../scripts/launch-demo-regions.sh");
  proc = spawn("bash", [script, "--test"], { stdio: ["ignore", "pipe", "inherit"] });
  return new Promise<Env>((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("demo-regions did not print PORTS/CREDS in 240s")), 240_000);
    let buf = "";
    proc!.stdout!.on("data", (d: Buffer) => {
      buf += d.toString();
      const p = buf.match(/PORTS eu_ui=(\d+) eu_api=(\d+) us_ui=(\d+) us_api=(\d+)/);
      const c = buf.match(/CREDS admin=(\S+) resident=(\S+)/);
      if (p && c) {
        clearTimeout(timer);
        resolve({ euUi: +p[1], euApi: +p[2], usUi: +p[3], usApi: +p[4], adminPw: c[1] });
      }
    });
    proc!.on("exit", (code) => reject(new Error(`launcher exited early (${code})`)));
  });
}

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
  test.setTimeout(300_000);
  env = await launch();
  adminTok = await login(env.euApi, "admin", env.adminPw);
});

test.afterAll(async () => {
  if (proc) proc.kill("SIGINT"); // --test teardown trap wipes the temp data dir
});

test.describe("REQ-1922 region selector, two-region demo", () => {
  test("the selector defaults to the connected region, shows the hidden count, and switches", async ({ page }) => {
    // Authenticate the UI by seeding the same token the login flow stores (REQ-1472 key).
    await page.addInitScript((t) => localStorage.setItem("provisa_token", t as string), adminTok);
    await page.goto(`http://127.0.0.1:${env.euUi}/admin/tables`);
    const selector = page.getByLabel("Region");
    await expect(selector).toBeVisible();
    // Default is the node's region (eu): us-homed rows are hidden and counted.
    await expect(page.getByTestId("region-hidden-count")).toBeVisible();
    await selector.click();
    await page.getByRole("option", { name: /all regions/i }).click();
    await expect(page.getByTestId("region-hidden-count")).toHaveCount(0);
  });

  test("a new table defaults to the node's region on registration", async () => {
    const eu = await asRole(env.euApi, adminTok);
    const reg = await eu.post("/admin/graphql", {
      data: {
        query:
          'mutation { registerTable(input: {sourceId: "shelter_files", domainId: "shelter", schemaName: "main", tableName: "breeds", fileGlob: "breeds/*.csv", columns: [{name: "name", visibleTo: ["org_admin"], dataType: "varchar", isPrimaryKey: true}]}) { success } }',
      },
    });
    expect(reg.ok()).toBeTruthy();
    const q = await eu.post("/admin/graphql", { data: { query: "{ tables { tableName region } }" } });
    const tables = (await q.json()).data.tables as { tableName: string; region: string | null }[];
    // A new table on the eu node defaults to eu (its source names no region).
    expect(tables.find((t) => t.tableName === "breeds")?.region).toBe("eu");
    await eu.dispose();
  });

  test("a us node reads an eu-homed table, served from eu's built replica", async () => {
    const eu = await asRole(env.euApi, adminTok);
    const us = await asRole(env.usApi, adminTok);
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

  test("with the eu node stopped, the us cross-region read is refused by name", async () => {
    // Stop just the eu node (its API port), then the us read of the eu-homed table is refused and the
    // message names the region — its replica is no longer reachable.
    try {
      execSync(`lsof -nP -iTCP:${env.euApi} -sTCP:LISTEN -t | xargs kill`, { stdio: "ignore" });
    } catch {
      /* already down */
    }
    await new Promise((r) => setTimeout(r, 6000));
    const us = await asRole(env.usApi, adminTok);
    const r = await us.post("/data/sql", { data: { sql: 'SELECT "id" FROM "intake_eu"', role: "org_admin" } });
    const body = await r.text();
    expect(r.ok()).toBeFalsy();
    expect(body.toLowerCase()).toContain("eu");
    await us.dispose();
  });
});
