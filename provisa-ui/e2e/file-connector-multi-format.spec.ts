// Copyright (c) 2026 Kenneth Stott
// Canary: 9e2d4b7a-3f1c-4a8e-b6d5-1a2c3e4f5b6d
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1690: a `files` source's Register Table picker must list one table per file the bundled
// Calcite pgwire server actually recognizes — csv/json/parquet natively, xlsx/md/docx/pptx/html
// via conversion — not just csv (the pre-fix hardcoded-CSV bug this whole REQ-1690 line of fixes
// closed: connector_duckdb.py's DuckDBFilesConnector, introspect.py's dead `native_tables` "files"
// shortcut, and pgwire_replica.py's missing `recursive` operand, which silently dropped a
// subdirectory's files). `.jsonl` and `.xml` are deliberately included too, NOT because the
// adapter supports them (confirmed absent from FileSchema.java's CONVERTIBLE_EXTENSIONS /
// TABLE_SOURCE_EXTENSIONS) but so a regression that started silently picking them up — or one
// that stopped picking up a format this test does expect — shows up here either way.

import { execFileSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { test, expect, BACKEND_URL } from "./coverage";
import { deleteSourceAndItsTables } from "./delete-source";
import {
  discoveredColumnCount,
  DOMAIN,
  openRegisterForm,
  openSourcesForm,
  pickSchemaAndTable,
  runSqlOnPage,
  submitRegisterAndExpectListed,
  submitSourceAndExpectListed,
} from "./source-to-query-helpers";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const PYTHON = path.join(ROOT, ".venv", "bin", "python");
const MAKE_FIXTURES = path.join(ROOT, "provisa-ui", "e2e", "make-multi-format-fixtures.py");

// Exact table names for the three top-level NATIVE formats (file stem, no conversion step).
const NATIVE_TABLES = ["customers", "orders", "sales"];
// CONVERTED formats: Calcite's own sub-naming (e.g. "products__sheet1") isn't part of this
// contract, so match by stem prefix instead of an exact table name.
const CONVERTED_STEMS = ["products", "notes", "memo", "deck", "page"];
// The subdirectory file (recursive discovery, REQ-1690's `recursive: true` fix): Calcite flattens
// the path into the table name as "<subdir>__<stem>", the same "__" convention it uses for a
// multi-sheet/multi-table conversion — verified live, not assumed.
const SUBDIR_TABLE = "subdir__extra";
// Confirmed NOT recognized by the current adapter (see the header comment) — must never appear.
const UNSUPPORTED_STEMS = ["events", "config"];

let fixtureDir: string;
// The source this run added: removed with its tables after the run, so the table it registers
// ("customers" in DOMAIN) is not left to clash with another spec registering that name.
let addedSource: string | null = null;

async function gql(query: string, variables: Record<string, unknown> = {}) {
  const res = await fetch(`${BACKEND_URL}/admin/graphql`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query, variables }),
  });
  return res.json();
}

test.describe("file connector: every recognized format is discoverable, columned, queryable (REQ-1690)", () => {
  test.beforeAll(() => {
    fixtureDir = fs.mkdtempSync(path.join(os.tmpdir(), "provisa-multi-format-"));
    execFileSync(PYTHON, [MAKE_FIXTURES, fixtureDir], { encoding: "utf8" });
  });

  test.afterAll(async () => {
    if (addedSource) await deleteSourceAndItsTables(gql, addedSource);
    if (fixtureDir) fs.rmSync(fixtureDir, { recursive: true, force: true });
  });

  test("files source over a mixed-format directory: table list, columns, query", async ({
    page,
  }) => {
    test.setTimeout(300000); // cold pgwire-file JVM start + POI conversion of 5 non-native formats

    const stamp = Date.now();
    const sourceId = `e2e_multi_format_${stamp}`;
    addedSource = sourceId;
    const schemaName = sourceId.replace(/-/g, "_"); // pgwire_replica.schema_name()'s convention

    // ── 1. Add the files source, pointed at the plain directory (not a glob) ─────────────────
    await openSourcesForm(page);
    await page.getByTestId("sources-id-input").fill(sourceId);
    await page.getByTestId("sources-type-select").selectOption("files");
    await page.waitForSelector('[data-testid="files-path-input"]', { timeout: 5000 });
    await page.getByTestId("files-path-input").fill(fixtureDir);
    await submitSourceAndExpectListed(page, sourceId);

    // ── 2. Register Table: enumerate every discovered table ──────────────────────────────────
    await openRegisterForm(page, sourceId);
    const schemaSelect = page.getByTestId("register-table-schema-select");
    await expect(schemaSelect.locator(`option[value='${schemaName}']`)).toHaveCount(1, {
      timeout: 180000, // first attach on a cold JVM: bundle already resolved, but still a JVM boot
    });
    if ((await schemaSelect.inputValue()) !== schemaName) {
      await schemaSelect.selectOption(schemaName);
    }
    const tableSelect = page.getByTestId("register-table-table-select");
    // Wait for at least the native tables (cheapest, no conversion) before reading the full list.
    await expect(tableSelect.locator(`option[value='customers']`)).toHaveCount(1, {
      timeout: 120000,
    });
    // Real entries render as <option value={tbl.name}>{tbl.name}</option> (value === text) — only
    // the picker's own placeholder ("Select table...", value="") differs, so drop empty values.
    const discovered = await tableSelect
      .locator("option")
      .evaluateAll((opts) => opts.map((o) => (o as HTMLOptionElement).value).filter(Boolean));

    for (const name of NATIVE_TABLES) {
      expect(
        discovered,
        `expected native table "${name}", discovered: ${discovered.join(", ")}`,
      ).toContain(name);
    }
    expect(
      discovered,
      `expected subdirectory table "${SUBDIR_TABLE}", discovered: ${discovered.join(", ")}`,
    ).toContain(SUBDIR_TABLE);
    for (const stem of CONVERTED_STEMS) {
      expect(
        discovered.some((n) => n.toLowerCase().startsWith(stem)),
        `expected a converted table starting with ${stem}, discovered: ${discovered.join(", ")}`,
      ).toBe(true);
    }
    for (const stem of UNSUPPORTED_STEMS) {
      expect(
        discovered.some((n) => n.toLowerCase().includes(stem)),
        `${stem} is not a recognized Calcite file-adapter extension and must not appear ` +
          `(discovered: ${discovered.join(", ")})`,
      ).toBe(false);
    }

    // ── 3. Columns populated: every discovered table resolves at least one typed column ───────
    for (const name of discovered) {
      await tableSelect.selectOption(name);
      const colCount = await discoveredColumnCount(page);
      expect(colCount, `table ${name} resolved zero columns`).toBeGreaterThan(0);
      // The short register step asks a type only for a column discovery could not type: none
      // asked means every column resolved one.
      await expect(
        page.locator('[data-testid^="register-table-type-"]'),
        `table ${name} has columns with no resolved data type`,
      ).toHaveCount(0);
    }

    // ── 4. Query one of them for real, through the SQL page ──────────────────────────────────
    await pickSchemaAndTable(page, schemaName, "customers");
    const sqlName = await submitRegisterAndExpectListed(page, sourceId);
    const rows = await runSqlOnPage(page, `SELECT * FROM ${DOMAIN.replace(/-/g, "_")}.${sqlName}`);
    expect(rows.length, `query against ${sqlName} returned no rows`).toBeGreaterThan(0);
  });
});
