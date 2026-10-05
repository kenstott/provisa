// Copyright (c) 2026 Kenneth Stott
// Canary: 22f66aec-9969-4e42-8e95-5452fbb4f344
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1671 fanout: OLAP/lakehouse SourceTypes through the UI, DIRECT-driver half — hiveserver2 and
// exasol. Three screens, no API shortcuts, same shape as source-to-query.spec.ts (which this file
// deliberately does not touch — see that file's own module doc for the pattern this one follows).
//
// The four Trino-connector-ONLY types this REQ also fans out (pinot, hive_s3, hive, druid — no
// DIRECT driver, register_source() is a no-op on the native/DuckDB engine —
// EngineBackend.register_source in provisa/federation/backend.py — so the schema dropdown can
// never populate against it) live in the sibling file source-to-query-olap-lake-trino.spec.ts, NOT
// here. See that file's own module doc for why: this file's own "core" project runs under
// PROVISA_E2E_LANE=core in real CI (ui-e2e-core.yml hardcodes it), which never starts the
// Trino-backed webServer (playwright.config.ts's RUNS_TRINO gate) — a Trino-connector-only test
// living in this file was reachable only by accident, whenever someone verified it locally with
// LANE left at its default "all". Splitting by file lets playwright.config.ts's TRINO_SPECS
// exclude the Trino-only cases from "core" and include them in "trino" at file granularity,
// leaving hiveserver2/exasol (real DIRECT drivers, correctly core-lane) undisturbed here.
//
//   - hiveserver2 has a real DIRECT driver (provisa/executor/drivers/hive.py's HiveDriver, over
//     impyla) and runs against the plain core-lane DuckDB backend, exactly like every other
//     source-to-query.spec.ts case.
//   - exasol (a real DIRECT driver too) lives in source-to-query-exasol.spec.ts: its image is
//     amd64-only, so only an amd64 host collects that file (playwright.config.ts AMD64_ONLY_SPECS).
//
//   Run: cd provisa-ui && npx playwright test source-to-query-olap-lake --project=core
//
// Ports: this file's own containers live in the 369xx range, distinct from the other e2e files'
// demo-source-containers.ts (33xxx/35xxx/37xxx/38xxx/39xxx) and source-to-query.spec.ts's extra
// RDBMS block (33xxx).

import { execFileSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { test, expect } from "./coverage";
import {
  openRegisterForm,
  openSourcesForm,
  pickSchemaAndTable,
  runSqlOnPage,
  releaseDraftTables,
  setSourceCacheTtl,
  submitSourceAndExpectListed,
} from "./source-to-query-helpers";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const PROVISION = path.join(ROOT, "demo", "sources", "provision.py");
const PYTHON = path.join(ROOT, ".venv", "bin", "python");
const PREFIX = "provisa-s2q-olap-lake";

const E2E_HIVESERVER2_PORT = Number(process.env.PROVISA_DEMO_HIVESERVER2_PORT ?? 36910);

function provision(cmd: "up" | "down", names: string[], env: Record<string, string> = {}): void {
  try {
    execFileSync(
      PYTHON,
      [
        PROVISION,
        cmd,
        "--prefix",
        PREFIX,
        ...Object.entries(env).flatMap(([k, v]) => ["--env", `${k}=${v}`]),
        ...names,
      ],
      { stdio: "pipe", env: { ...process.env, ...env } },
    );
  } catch (e) {
    if (cmd === "down") return; // a project that was never started removes nothing
    throw e;
  }
}

// ---------------------------------------------------------------------------------------------
// hiveserver2 — DIRECT driver (impyla), core lane / DuckDB backend, no Trino routing needed.
// ---------------------------------------------------------------------------------------------
test.describe("source to query through the UI: hiveserver2 (REQ-1731)", () => {
  test.beforeAll(() => {
    provision("up", ["hiveserver2"], {
      PROVISA_DEMO_HIVESERVER2_PORT: String(E2E_HIVESERVER2_PORT),
    });
  });

  test.afterAll(() => {
    provision("down", ["hiveserver2"]);
  });

  test("hiveserver2: add the source, register a table, query it on the SQL page", async ({
    page,
  }) => {
    // Root cause (confirmed live, isolated network, no contention): hiveserver2 IS SIMPLE_RDBMS
    // (constants.ts), and SourceFormFields.tsx renders the "Database" input `required` for every
    // SIMPLE_RDBMS type but druid. This test never filled it, so the browser's native HTML5
    // required-field validation silently blocked form submission on click — no submit event ever
    // fired, so createSource never reached the backend (confirmed: zero GraphQL/server activity
    // in the backend log for the whole span of the failing run). That is exactly why it read as a
    // "silent" failure with no error banner: React's onSubmit handler, and the try/catch around
    // it that would have shown one, never ran. Every other SIMPLE_RDBMS case in
    // source-to-query.spec.ts fills Database for the same reason (see e.g. its redis/cassandra
    // cases: "the form requires one"). Fixed here the same way, with the fixture's real database.
    test.setTimeout(180000);
    const stamp = Date.now();
    const sourceId = `e2e_hiveserver2_${stamp}`;

    await openSourcesForm(page);
    await page.getByTestId("sources-id-input").fill(sourceId);
    await page.getByTestId("sources-type-select").selectOption("hiveserver2");
    await page.getByLabel(/^Host/).fill("localhost");
    await page.getByLabel(/^Port/).fill(String(E2E_HIVESERVER2_PORT));
    await page.getByLabel(/^Database/).fill("wh"); // demo/sources/hiveserver2/prime.py's seeded db
    // auth_mechanism defaults to PLAIN (SourceFormFields.tsx), which is what demo/sources/
    // hiveserver2's stock HS2 speaks — left untouched.
    // REQ-1907: a source the engine lands needs a landing clock before a table is registered.
    await setSourceCacheTtl(page, 300);
    await submitSourceAndExpectListed(page, sourceId);

    await openRegisterForm(page, sourceId);
    await pickSchemaAndTable(page, "wh", "widgets");
    await expect(page.getByTestId("register-table-col-selected-name")).toBeVisible({
      timeout: 60000,
    });
    await page.getByTestId("register-table-submit").click();
    const row = page.locator(".data-table tbody tr").filter({ hasText: sourceId }).first();
    await expect(row).toBeVisible({ timeout: 120000 });
    // REQ-1921: registered through the admin, it starts as draft; released, it is read.
    await releaseDraftTables(page, { sourceId });
    const res = await page.request.post("/admin/graphql", {
      data: { query: "{ tables { sourceId dqDataset } }" },
    });
    const tables = (await res.json()).data.tables as {
      sourceId: string;
      dqDataset: string | null;
    }[];
    const mine = tables.find((t) => t.sourceId === sourceId);
    expect(mine?.dqDataset).toBeTruthy();
    const registered = mine!.dqDataset!.split("/").pop()!;

    const rows = await runSqlOnPage(
      page,
      `SELECT id, name FROM pet_store.${registered} ORDER BY id`,
    );
    expect(rows).toHaveLength(3);
    expect(rows[0]).toEqual(["1", "Widget A"]);
    expect(rows[2]).toEqual(["3", "Widget C"]);
  });
});
