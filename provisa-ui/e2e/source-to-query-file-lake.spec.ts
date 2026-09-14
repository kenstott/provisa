// Copyright (c) 2026 Kenneth Stott
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1741: delta_lake and iceberg through the real UI (Sources form -> Register Table form ->
// SQL page), against the DuckDB federation engine — the same three-screen shape as
// source-to-query.spec.ts's sqlite/csv/parquet cases, since delta_lake/iceberg are likewise
// local-file-based, no docker, no network service (DuckDBDeltaConnector/DuckDBIcebergConnector,
// connector_duckdb.py, REQ-899): a SCAN-mechanism connector reading the table in place via
// delta_scan/iceberg_scan, one view per source (no ATTACH, no nested schema/table hierarchy).
//
// Fixtures: a real local Delta table (written with the `deltalake` package) and a real local
// Iceberg table (written with `pyiceberg`), generated once in beforeAll by
// make-file-lake-fixtures.py — the exact same fixture-writing pattern already proven by
// tests/integration/test_duckdb_delta_source_e2e.py and
// tests/integration/test_embedded_pg_duckdb_iceberg_e2e.py's helpers. The app backend runs
// natively (not in a container) so an absolute host path is directly readable by DuckDB.
//
// hudi is NOT covered here: there is no DuckDB hudi connector anywhere in
// provisa/federation/connector_duckdb.py (grep confirms zero "hudi" hits) — the only hudi
// connector is ClickHouseHudiConnector (provisa/federation/clickhouse_connectors.py:258-260,
// REQ-1178), registered exclusively in the ClickHouse-as-engine connector table
// (provisa/federation/engine.py:698). hudi is reachable only under ClickHouse-as-engine, which is
// out of scope for this DuckDB-only pass — see the final report for the file:line citations.
//
// Along the way this uncovered a real registration-flow bug (fixed as part of REQ-1741,
// provisa/api/admin/introspect.py): delta_lake/iceberg had no native_schemas/native_tables branch,
// so the Register Table schema/table pickers would never populate for them (the REQ-1673 seam
// explicitly returns None/[] for a view_ddl-mechanism source, and an empty list short-circuits
// available_schemas/available_tables before the engine-catalog fallback ever runs). Same fix
// REQ-1732 already applied to csv/parquet, now extended to delta_lake/iceberg.

import { execFileSync } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { test, expect } from "./coverage";
import {
  openRegisterForm,
  openSourcesForm,
  pickSchemaAndTable,
  runSqlOnPage,
  submitRegisterAndExpectListed,
  submitSourceAndExpectListed,
} from "./source-to-query-helpers";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const PYTHON = path.join(ROOT, ".venv", "bin", "python");
const MAKE_FIXTURES = path.join(ROOT, "provisa-ui", "e2e", "make-file-lake-fixtures.py");

let deltaTablePath: string;
let icebergTablePath: string;
let fixtureDir: string;

test.describe("source to query through the UI, file-based lakehouse types (REQ-1741)", () => {
  test.beforeAll(() => {
    fixtureDir = fs.mkdtempSync(path.join(os.tmpdir(), "provisa-file-lake-"));
    const out = execFileSync(PYTHON, [MAKE_FIXTURES, fixtureDir], { encoding: "utf8" });
    const [delta, iceberg] = out.trim().split("\n");
    deltaTablePath = delta;
    icebergTablePath = iceberg;
  });

  test.afterAll(() => {
    if (fixtureDir) fs.rmSync(fixtureDir, { recursive: true, force: true });
  });

  test("delta_lake: add the source, register its one table, query it on the SQL page", async ({
    page,
  }) => {
    test.setTimeout(180000);
    const stamp = Date.now();
    const sourceId = `e2e_delta_${stamp}`;

    // 1. Sources form — "Warehouse Path" writes to form.path (SourceFormFields.tsx delta_lake/
    // iceberg branch), read by DuckDBDeltaConnector directly off source.path.
    await openSourcesForm(page);
    await page.getByTestId("sources-id-input").fill(sourceId);
    await page.getByTestId("sources-type-select").selectOption("delta_lake");
    await page.getByLabel(/Warehouse Path/).fill(deltaTablePath);
    await submitSourceAndExpectListed(page, sourceId);

    // 2. Register Table form — one view per source, named after the source id itself (like
    // csv/parquet), fixed "main" placeholder schema.
    await openRegisterForm(page, sourceId);
    await pickSchemaAndTable(page, "main", sourceId);
    await expect(page.getByTestId("register-table-col-selected-name")).toBeVisible({
      timeout: 60000,
    });
    const registered = await submitRegisterAndExpectListed(page, sourceId);

    // 3. SQL page — the 4 seed rows make-file-lake-fixtures.py wrote into the Delta table
    const rows = await runSqlOnPage(
      page,
      `SELECT id, name, species FROM pet_store.${registered} ORDER BY id`,
    );
    expect(rows).toHaveLength(4);
    expect(rows[0]).toEqual(["1", "Fido", "dog"]);
    expect(rows[3]).toEqual(["4", "Tweety", "bird"]);
  });

  test("iceberg: add the source, register its one table, query it on the SQL page", async ({
    page,
  }) => {
    test.setTimeout(180000);
    const stamp = Date.now();
    const sourceId = `e2e_iceberg_${stamp}`;

    await openSourcesForm(page);
    await page.getByTestId("sources-id-input").fill(sourceId);
    await page.getByTestId("sources-type-select").selectOption("iceberg");
    await page.getByLabel(/Warehouse Path/).fill(icebergTablePath);
    await submitSourceAndExpectListed(page, sourceId);

    await openRegisterForm(page, sourceId);
    await pickSchemaAndTable(page, "main", sourceId);
    await expect(page.getByTestId("register-table-col-selected-name")).toBeVisible({
      timeout: 60000,
    });
    const registered = await submitRegisterAndExpectListed(page, sourceId);

    const rows = await runSqlOnPage(
      page,
      `SELECT id, name, species FROM pet_store.${registered} ORDER BY id`,
    );
    expect(rows).toHaveLength(4);
    expect(rows[0]).toEqual(["1", "Fido", "dog"]);
    expect(rows[3]).toEqual(["4", "Tweety", "bird"]);
  });
});
