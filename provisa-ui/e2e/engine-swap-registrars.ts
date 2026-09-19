// Copyright (c) 2026 Kenneth Stott
// Canary: 09521fbc-2ce8-4a54-8ec4-9eb344236a83
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

// REQ-1730: the per-type register* functions engine-swap.spec.ts's test bodies call — each
// registers one source+table against the DuckDB backend and returns a Registration (the SQL to
// requery and the rows every engine must answer identically, per engine-swap-helpers.ts's
// runSwapCase). Split out of engine-swap.spec.ts purely to stay under the repo's max-lines lint
// rule as REQ-1730 grew to cover more source types — see engine-swap.spec.ts's own module doc for
// the harness's actual design rationale and invocation instructions.

import { execFileSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";

import { expect } from "./coverage";
import type { Page } from "./coverage";
import {
  E2E_CASSANDRA_PORT,
  E2E_ES_PORT,
  E2E_MONGO_PORT,
  E2E_NEO4J_HTTP_PORT,
  E2E_PROMETHEUS_PORT,
  E2E_REDIS_PORT,
  E2E_SPARQL_PORT,
  E2E_SPLUNK_PORT,
} from "./demo-source-containers";
import {
  existingSourcePath,
  openRegisterForm,
  openSourcesForm,
  pickSchemaAndTable,
  registeredTableNames,
  submitRegisterAndExpectListed,
  submitSourceAndExpectListed,
} from "./source-to-query-helpers";
import {
  E2E_AIRPORT_PORT,
  E2E_EXASOL_PORT,
  E2E_FIREBIRD_PORT,
  E2E_KAFKA_PORT,
  E2E_DRUID_BROKER_PORT,
  E2E_PINOT_BROKER_PORT,
  E2E_PINOT_CONTROLLER_PORT,
  E2E_KAFKA_SCHEMA_REGISTRY_PORT,
  E2E_RSS_PORT,
  E2E_WS_PORT,
  E2E_TRINO_SOURCE_PORT,
  FILE_LAKE_HOST_DIR,
  PYTHON,
  RDB_WIDGETS_PORTS,
  ROOT,
  SWAP_REGISTER_TIMEOUT_MS,
} from "./engine-swap-helpers";
import { REBOOT_BACKEND_URL } from "./engine-swap-helpers";
import type { RedshiftConnection } from "./engine-swap-helpers";
import type { Registration } from "./engine-swap-helpers";

// REQ-1730: the duckdb-as-a-source fixture, generated (not committed — *.duckdb is gitignored)
// by registerDuckdbSource's own caller before the test runs, same file
// source-to-query-community-ext.spec.ts's own `duckdb` test uses.
export const WIDGETS_DUCKDB_PATH = path.join(ROOT, "demo", "files", "widgets.duckdb");

export async function registerNeo4j(page: Page): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_neo4j_${stamp}`;
  const tableName = `adopter_${stamp}`;
  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("neo4j");
  await page.getByTestId("neo4j-host-input").fill("localhost");
  await page.getByTestId("neo4j-port-input").fill(String(E2E_NEO4J_HTTP_PORT));
  await page.getByTestId("neo4j-database-input").fill("neo4j");
  await submitSourceAndExpectListed(page, sourceId);

  await openRegisterForm(page, sourceId);
  await expect(page.getByTestId("register-table-schema-select")).toHaveCount(0);
  await page.getByTestId("register-table-neo4j-table-name").fill(tableName);
  await page
    .getByTestId("register-table-neo4j-cypher")
    .fill(
      "MATCH (a:Adopter) RETURN a.adopter_id AS adopter_id, a.name AS name, a.city AS city " +
        "ORDER BY a.adopter_id",
    );
  await page.getByTestId("register-table-neo4j-preview").click();
  await expect(page.getByTestId("register-table-neo4j-preview-rows")).toBeVisible({
    timeout: 60000,
  });
  const registered = await submitRegisterAndExpectListed(page, sourceId, SWAP_REGISTER_TIMEOUT_MS);

  return {
    label: "neo4j",
    sourceId,
    sql: `SELECT adopter_id, name, city FROM pet_store.${registered} ORDER BY adopter_id`,
    assertRows: (rows) => {
      expect(rows).toHaveLength(8);
      expect(rows[0]).toEqual(["1", "Sara Kim", "Portland"]);
      expect(rows[7]).toEqual(["8", "Mark Torres", "Tacoma"]);
    },
    reachableOn: ["trino"],
  };
}

export async function registerMongodb(
  page: Page,
  baseUrl = "",
  // REQ-1730 scenario 2 (test-only): "localhost" (the default, correct for the DuckDB-bound
  // registration backend every other caller uses) is unreachable from INSIDE Trino's own
  // container — a Trino-primary-from-boot registration needs "host.docker.internal" here instead,
  // same class of docker-topology translation reprovisionSourceOnEngine/
  // rewriteHostForContainerizedEngine already apply post-hoc; this parameter lets a caller apply
  // it up front, before the schema-introspection call that a post-hoc rewrite is too late for.
  host = "localhost",
): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_mongodb_${stamp}`;
  const tableName = "product_reviews";
  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("mongodb");
  await page.getByLabel(/^Host/).fill(host);
  await page.getByLabel(/^Port/).fill(String(E2E_MONGO_PORT));
  await page.getByLabel(/^Database/).fill("provisa");
  await submitSourceAndExpectListed(page, sourceId);

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, "provisa", tableName);
  await expect(page.getByTestId("register-table-col-selected-reviewer")).toBeVisible({
    timeout: 60000,
  });
  const registered = await submitRegisterAndExpectListed(
    page,
    sourceId,
    SWAP_REGISTER_TIMEOUT_MS,
    baseUrl,
  );

  return {
    label: "mongodb",
    sourceId,
    sql: `SELECT product_id, reviewer, rating FROM pet_store.${registered} ORDER BY reviewer`,
    assertRows: (rows) => {
      expect(rows).toHaveLength(10);
      expect(rows[0]).toEqual(["1", "alice", "5"]);
      expect(rows[9]).toEqual(["7", "jack", "1"]);
    },
    reachableOn: ["trino"],
  };
}

/** Connection + introspection shape for one demo/sources/<name> RDBMS fixture — every one of them
 * primes the identical widgets(id, name) + 3 rows (see each prime.py, verified live 2026-09-16
 * during this extension), so one registrar covers all of them; only what differs (type key,
 * fixed port, credentials, the schema its rows land in, and int vs string id rendering) is data. */
export interface RdbWidgetsConfig {
  type: string;
  port: number;
  username: string;
  password: string;
  database: string;
  schema: string;
  /** Defaults to "widgets" — override for a dialect whose unquoted CREATE TABLE folds case
   * differently (Oracle upper-cases unquoted identifiers, matching source-to-query-generic-
   * rdbms.spec.ts's own "SYSTEM"/"WIDGETS" precedent for this exact fixture). */
  table?: string;
  /** Defaults to "name" — same case-folding override as `table`, for the "name" COLUMN
   * (RegisterTableForm.tsx's column checkboxes use the raw introspected column name verbatim,
   * e.g. Oracle's own ALL_TAB_COLUMNS reports "NAME" for this unquoted column). */
  nameColumn?: string;
  /** Defaults to ["trino"] — override for a type with no Trino connector (hiveserver2, saphana:
   * DuckDB-only proof, like firebird/airport, see the module doc). */
  reachableOn?: string[];
  /** SAP HANA Express's indexserver verified live not to start under Docker Desktop's Apple
   * Silicon VM — a real amd64 Linux host is required (RUNNING_IN_CI). */
  needsCi?: boolean;
  /** Override for `test.setTimeout()` around this type's `beforeAll` provisioning hook. Defaults
   * to Playwright's own global test timeout (90s, playwright.config.ts) when unset — fine for
   * every RDBMS here except Oracle: gvenzl/oracle-free creates its database from scratch on first
   * boot and its own compose healthcheck already documents this can take several minutes
   * (demo/sources/oracle/compose.yml's start_period/retries). Verified live (2026-09-18):
   * `docker inspect .State.Health.Status` went healthy at ~335s. `provisionSwapSource`'s
   * `execFileSync(..., "up", "--wait")` call blocks for that whole duration with NO timeout of
   * its own — the enclosing Playwright `beforeAll` hook is what was actually timing out at the
   * default 90s, well before registration was ever attempted, not `submitRegisterAndExpectListed`'s
   * later 300s wait as originally suspected. splunk's `beforeAll` already sets an explicit
   * `test.setTimeout(900000)` for the identical reason (a slow fixture, not a slow app). */
  bootTimeoutMs?: number;
}

// RESOLVED (2026-09-18): oracle's earlier "registration stall" was misdiagnosed. Introspection
// and registration both work fine — the real cause was `provisionSwapSource`'s blocking
// `execFileSync(..., "up", "--wait")` call inside this loop's `beforeAll` never finishing before
// Playwright's own default 90s test/hook timeout (playwright.config.ts:376) fired.
// gvenzl/oracle-free creates its database from scratch on first boot; verified live via
// `docker inspect .State.Health.Status` that this genuinely takes ~335s (matching
// demo/sources/oracle/compose.yml's own documented start_period/retries), nowhere near 90s.
// Every OTHER type in this array boots in well under 90s, which is why this was invisible for
// them. Fixed with `bootTimeoutMs` below (splunk's own `beforeAll` already sets an explicit
// `test.setTimeout(900000)` for the identical reason — a slow fixture, not a slow app).
export const RDB_WIDGETS_SOURCES: RdbWidgetsConfig[] = [
  { type: "postgresql", port: RDB_WIDGETS_PORTS.postgresql, username: "provisa", password: "provisa", database: "provisa_demo", schema: "public" },
  { type: "mysql", port: RDB_WIDGETS_PORTS.mysql, username: "root", password: "provisa", database: "provisa_demo", schema: "provisa_demo" },
  { type: "mariadb", port: RDB_WIDGETS_PORTS.mariadb, username: "root", password: "provisa", database: "provisa_demo", schema: "provisa_demo" },
  { type: "sqlserver", port: RDB_WIDGETS_PORTS.sqlserver, username: "sa", password: "Provisa_2026!", database: "master", schema: "dbo" },
  { type: "cockroachdb", port: RDB_WIDGETS_PORTS.cockroachdb, username: "root", password: "", database: "defaultdb", schema: "public" },
  { type: "yugabytedb", port: RDB_WIDGETS_PORTS.yugabytedb, username: "yugabyte", password: "yugabyte", database: "yugabyte", schema: "public" },
  { type: "greenplum", port: RDB_WIDGETS_PORTS.greenplum, username: "gpadmin", password: "", database: "postgres", schema: "public" },
  { type: "tidb", port: RDB_WIDGETS_PORTS.tidb, username: "root", password: "", database: "test", schema: "test" },
  { type: "clickhouse", port: RDB_WIDGETS_PORTS.clickhouse, username: "default", password: "provisa", database: "default", schema: "default" },
  { type: "oracle", port: RDB_WIDGETS_PORTS.oracle, username: "system", password: "provisa", database: "FREEPDB1", schema: "SYSTEM", table: "WIDGETS", nameColumn: "NAME", bootTimeoutMs: 480000 },
  // hiveserver2: stock HS2 PLAIN auth takes no real credentials (demo/sources/hiveserver2/prime.py) —
  // "Database" is HTML5-required for every SIMPLE_RDBMS type but druid (verified live in
  // source-to-query-olap-lake.spec.ts's own hiveserver2 test), username/password are not.
  { type: "hiveserver2", port: RDB_WIDGETS_PORTS.hiveserver2, username: "", password: "", database: "wh", schema: "wh", reachableOn: [] },
  // saphana: CI-only (needsCi) — see source-to-query-generic-rdbms.spec.ts's own saphana test,
  // whose exact schema/table/column casing (SYSTEM/WIDGETS/NAME) and password default
  // (PROVISA_DEMO_SAPHANA_PASSWORD's own default, HXEHana1) this mirrors.
  { type: "saphana", port: RDB_WIDGETS_PORTS.saphana, username: "SYSTEM", password: "HXEHana1", database: "HXE", schema: "SYSTEM", table: "WIDGETS", nameColumn: "NAME", reachableOn: [], needsCi: true },
];

export function registerRdbWidgets(cfg: RdbWidgetsConfig): (page: Page) => Promise<Registration> {
  return async (page: Page): Promise<Registration> => {
    const stamp = Date.now();
    const sourceId = `e2e_swap_${cfg.type}_${stamp}`;
    await openSourcesForm(page);
    await page.getByTestId("sources-id-input").fill(sourceId);
    await page.getByTestId("sources-type-select").selectOption(cfg.type);
    await page.getByLabel(/^Host/).fill("localhost");
    await page.getByLabel(/^Port/).fill(String(cfg.port));
    await page.getByLabel(/^Username/).fill(cfg.username);
    await page.getByLabel(/^Password/).fill(cfg.password);
    await page.getByLabel(/^Database/).fill(cfg.database);
    // The demo compose fixture (demo/sources/sqlserver/compose.yml) uses SQL Server's own
    // self-signed cert — Trino's default (encrypt=true, trustServerCertificate=false) refuses it
    // outright (JDBC_ERROR: PKIX path building failed), so the swap's Trino leg needs the same
    // opt-in trust flag a real internal/on-prem deployment with a self-signed cert would use.
    if (cfg.type === "sqlserver") {
      // Mantine's Select renders a readonly <input> + listbox popup, never a native <select> —
      // .selectOption() doesn't apply (same click-then-pick pattern every other Mantine Select in
      // this suite uses, e.g. glossary.spec.ts's domain picker).
      await page.getByTestId("sqlserver-cert-trust-select").click();
      await page
        .getByRole("option", { name: "Trust Server Certificate (self-signed / internal CA)" })
        .click();
    }
    await submitSourceAndExpectListed(page, sourceId);

    await openRegisterForm(page, sourceId);
    await pickSchemaAndTable(page, cfg.schema, cfg.table ?? "widgets");
    await expect(
      page.getByTestId(`register-table-col-selected-${cfg.nameColumn ?? "name"}`),
    ).toBeVisible({
      timeout: 60000,
    });
    const registered = await submitRegisterAndExpectListed(page, sourceId, SWAP_REGISTER_TIMEOUT_MS);

    return {
      label: cfg.type,
      sourceId,
      sql: `SELECT id, name FROM pet_store.${registered} ORDER BY id`,
      assertRows: (rows) => {
        expect(rows).toHaveLength(3);
        expect(rows[0]).toEqual(["1", "Widget A"]);
        expect(rows[1]).toEqual(["2", "Widget B"]);
        expect(rows[2]).toEqual(["3", "Widget C"]);
      },
      reachableOn: cfg.reachableOn ?? ["trino"],
    };
  };
}

/** delta_lake and iceberg share this exact shape (SCAN-mechanism, one view per source named after
 * the source id itself, fixed "main" placeholder schema — same as csv/parquet) — see
 * source-to-query-file-lake.spec.ts's own registrars, which this mirrors. */
export function registerFileLake(sourceType: "delta_lake" | "iceberg", tablePath: () => string) {
  return async (page: Page): Promise<Registration> => {
    const stamp = Date.now();
    const sourceId = `e2e_swap_${sourceType}_${stamp}`;
    await openSourcesForm(page);
    await page.getByTestId("sources-id-input").fill(sourceId);
    await page.getByTestId("sources-type-select").selectOption(sourceType);
    await page.getByLabel(/Warehouse Path/).fill(tablePath());
    await submitSourceAndExpectListed(page, sourceId);

    await openRegisterForm(page, sourceId);
    await pickSchemaAndTable(page, "main", sourceId);
    await expect(page.getByTestId("register-table-col-selected-name")).toBeVisible({
      timeout: 60000,
    });
    const registered = await submitRegisterAndExpectListed(page, sourceId, SWAP_REGISTER_TIMEOUT_MS);

    return {
      label: sourceType,
      sourceId,
      sql: `SELECT id, name, species FROM pet_store.${registered} ORDER BY id`,
      assertRows: (rows) => {
        expect(rows).toHaveLength(4);
        expect(rows[0]).toEqual(["1", "Fido", "dog"]);
        expect(rows[3]).toEqual(["4", "Tweety", "bird"]);
      },
      reachableOn: ["trino"],
    };
  };
}

/** csv/parquet: single-file sources (REQ-1732). Trino's `file` connector (TrinoCsvConnector/
 * TrinoParquetConnector, trino_connectors.py) names a glob-matched file's table after the file's
 * own basename, with no override — but DuckDB records this source's one table as "main".<source
 * id> (matching delta_lake/iceberg's own fixed-schema convention), and the swap harness needs
 * BOTH engines to resolve the identical schema.table pair. So this copies the shared demo fixture
 * into a fresh, never-deleted subdirectory of FILE_LAKE_HOST_DIR under a filename that IS the
 * source id — the resulting Trino table is then named exactly `sourceId`, matching DuckDB. Reuses
 * the same identity-mounted directory delta_lake/iceberg already use (docker-compose.core.yml);
 * no new mount needed since the `file` connector has no embedded-location check to satisfy.
 *
 * parquet initially looked unreachable (zero tables under the connector's LINQ4J default) — root
 * cause turned out to be that connector's Parquet statistics extractor calling Hadoop's
 * UserGroupInformation.getCurrentUser(), which throws on the JDK 25 Trino runs (JEP 486 removed
 * the Security Manager API it depends on) and silently excludes the table rather than failing
 * loud. execution-engine=DUCKDB (documented in the plugin's own config as reading "CSV/Parquet
 * natively without Hadoop") sidesteps it entirely — verified live 2026-09-17 against both a
 * single parquet file and a multi-file recursive glob. */
export function registerSingleFile(
  sourceType: "csv" | "parquet",
  demoFixture: string,
  fieldLabel: RegExp,
  query: (table: string) => string,
  assertRows: (rows: string[][]) => void,
) {
  return async (page: Page): Promise<Registration> => {
    const stamp = Date.now();
    // NOT the usual "e2e_swap_" prefix every other registrar in this file uses: Trino's `file`
    // connector derives a table name from the matched file's OWN basename by running it through
    // some identifier-normalizing pass that splits a bare digit sandwiched between two letters
    // with no separator — "e2e" (e-2-e, no underscore) comes back as "e2_e" (verified live
    // 2026-09-17: a file literally named e2e_swap_csv_<stamp>.csv produced a table named
    // e2_e_swap_csv_<stamp>, never matching the schema.table pair DuckDB recorded under the
    // unmangled source id). "swap_" alone has no such digit-between-letters run, so it survives
    // unmangled — verified against the same connector.
    const sourceId = `swap_${sourceType}_${stamp}`;
    const runDir = path.join(FILE_LAKE_HOST_DIR, `run_${stamp}`);
    fs.mkdirSync(runDir, { recursive: true });
    const ext = sourceType === "csv" ? "csv" : "parquet";
    const filePath = path.join(runDir, `${sourceId}.${ext}`);
    fs.copyFileSync(path.resolve(ROOT, demoFixture), filePath);

    await openSourcesForm(page);
    await page.getByTestId("sources-id-input").fill(sourceId);
    await page.getByTestId("sources-type-select").selectOption(sourceType);
    await page.getByLabel(fieldLabel).fill(filePath);
    await submitSourceAndExpectListed(page, sourceId);

    await openRegisterForm(page, sourceId);
    await pickSchemaAndTable(page, "main", sourceId);
    const registered = await submitRegisterAndExpectListed(page, sourceId, SWAP_REGISTER_TIMEOUT_MS);

    return {
      label: sourceType,
      sourceId,
      sql: query(registered),
      assertRows,
      reachableOn: ["trino"],
    };
  };
}

/** files: a directory glob exposing MULTIPLE tables (unlike csv/parquet's one-file-one-table) —
 * DuckDBFilesConnector/TrinoFilesConnector both derive schema = source.id (hyphens -> underscores)
 * and one table per `<name>.csv` in the glob'd directory, matching file-connector.spec.ts's own
 * proven SCHEMA_NAME convention (its DuckDB-only test, never run against Trino). Copies the whole
 * northwind fixture set into a fresh run directory under FILE_LAKE_HOST_DIR (same identity-mounted
 * dir csv/parquet/delta_lake/iceberg already use) so both engines see the same absolute glob. */
export function registerFiles(): (page: Page) => Promise<Registration> {
  return async (page: Page): Promise<Registration> => {
    const stamp = Date.now();
    const sourceId = `swap_files_${stamp}`; // no "e2e_" prefix — see registerSingleFile's comment
    const runDir = path.join(FILE_LAKE_HOST_DIR, `run_${stamp}`);
    fs.mkdirSync(runDir, { recursive: true });
    const northwindDir = path.resolve(ROOT, "demo/files/northwind");
    for (const f of fs.readdirSync(northwindDir)) {
      fs.copyFileSync(path.join(northwindDir, f), path.join(runDir, f));
    }

    await openSourcesForm(page);
    await page.getByTestId("sources-id-input").fill(sourceId);
    await page.getByTestId("sources-type-select").selectOption("files");
    await page.getByLabel(/Directory Glob/).fill(`${runDir}/**`);
    await submitSourceAndExpectListed(page, sourceId);

    await openRegisterForm(page, sourceId);
    await pickSchemaAndTable(page, sourceId.replace(/-/g, "_"), "customers");
    const registered = await submitRegisterAndExpectListed(page, sourceId, SWAP_REGISTER_TIMEOUT_MS);

    return {
      label: "files",
      // customerID/companyName etc. are deliberately NOT selected here: DuckDBFilesConnector's own
      // apply_convention (a camelCase-aware Python splitter) registers this table's columns as
      // customer_id/company_name, but Trino's file connector (execution-engine=DUCKDB) merely
      // lowercases each header with no word-boundary splitting — customerid/companyname, a
      // DIFFERENT physical name for the identical logical column (verified live 2026-09-17: query
      // against the registered `customer_id` name 404s as COLUMN_NOT_FOUND under Trino). Every
      // ALREADY-single-word header (address/city/country/...) lowercases identically under both
      // conventions, so this sticks to those instead of chasing a per-engine column-name mapping
      // that doesn't exist in this harness.
      sourceId,
      sql: `SELECT address, city, country FROM pet_store.${registered} ORDER BY address LIMIT 3`,
      assertRows: (rows) => {
        expect(rows).toHaveLength(3);
        expect(rows[0]).toEqual(["1 rue Alsace-Lorraine", "Toulouse", "France"]);
      },
      reachableOn: ["trino"],
    };
  };
}

/** soda / great_expectations (REQ-1730): data-quality checkers, not conventional data sources —
 * per source-to-query-special-cases.spec.ts's own REQ-1742 investigation, a checker source has no
 * remote table of its own to register+SELECT from; it connects back through Provisa's OWN pgwire
 * endpoint (schema_mutation.py's create_source auto-fills that mapping for a checker type) and
 * scans an ALREADY-governed table via a dq_contract. This registrar therefore first registers a
 * small csv scan target (the exact registerSingleFile shape, inlined here for direct access to
 * the compiled table name), then registers the checker source against it, adds one dataset-scope
 * rule, dry-runs it for real, and submits — landing is POLL-cadence (provisa/dq/runner.py's
 * run_contract via make_dq_loader, the SAME wire_new_poll_jobs mechanism as rss, watermarked by
 * scan_time — REQ-1770), so registerRss's own "Cache TTL" edit-form step is reused verbatim to
 * give the poll job a real cadence; RegisterTableForm has no Cache TTL field of its own. */
function registerDqChecker(
  checkerType: "soda" | "great_expectations",
  checkType: string,
): (page: Page) => Promise<Registration> {
  return async (page: Page): Promise<Registration> => {
    const stamp = Date.now();

    // 1. Scan target: a plain csv source+table (same fixture/shape as registerSingleFile).
    const scanSourceId = `swap_${checkerType}_scan_${stamp}`;
    const runDir = path.join(FILE_LAKE_HOST_DIR, `run_${stamp}`);
    fs.mkdirSync(runDir, { recursive: true });
    const scanFilePath = path.join(runDir, `${scanSourceId}.csv`);
    fs.copyFileSync(path.resolve(ROOT, "demo/files/customers.csv"), scanFilePath);

    await openSourcesForm(page);
    await page.getByTestId("sources-id-input").fill(scanSourceId);
    await page.getByTestId("sources-type-select").selectOption("csv");
    await page.getByLabel(/CSV File Path/).fill(scanFilePath);
    await submitSourceAndExpectListed(page, scanSourceId);

    await openRegisterForm(page, scanSourceId);
    await pickSchemaAndTable(page, "main", scanSourceId);
    const scannedTable = await submitRegisterAndExpectListed(
      page,
      scanSourceId,
      SWAP_REGISTER_TIMEOUT_MS,
    );

    // 2. The checker source itself — no connection fields (NO_CONNECTION_TYPES, constants.ts).
    const sourceId = `e2e_swap_${checkerType}_${stamp}`;
    const resultsTable = `e2e_swap_${checkerType}_scan_${stamp}`;
    await openSourcesForm(page);
    await page.getByTestId("sources-id-input").fill(sourceId);
    await page.getByTestId("sources-type-select").selectOption(checkerType);
    await submitSourceAndExpectListed(page, sourceId);

    // 3. Register Table form: no schema/table picker for a checker, only the DQ contract panel.
    await openRegisterForm(page, sourceId);
    await expect(page.getByTestId("register-table-dq")).toBeVisible();
    await expect(page.getByTestId("register-table-schema-select")).toHaveCount(0);

    await page.getByTestId("dq-dataset-table-select").click();
    await page.getByRole("option", { name: `pet_store.${scannedTable}`, exact: true }).click();
    await page.getByTestId("register-table-dq-results-table").fill(resultsTable);
    await page.getByTestId("register-table-alias").fill(`${resultsTable}_quality`);

    // A dataset-scope row_count / row-count-between rule needs no column pick (matches
    // source-to-query-special-cases.spec.ts's own soda/great_expectations cases verbatim).
    await page.getByTestId("dq-check-type").click();
    await page.getByRole("option", { name: checkType, exact: true }).click();
    if (await page.getByTestId("dq-comparator").isVisible()) {
      await page.getByTestId("dq-comparator").click();
      await page.getByRole("option", { name: "must_be_greater_than", exact: true }).click();
      await page.getByTestId("dq-threshold").fill("0");
    }
    if (await page.getByTestId("dq-param-min_value").isVisible()) {
      await page.getByTestId("dq-param-min_value").fill("0");
      await page.getByTestId("dq-param-max_value").fill("1000000");
    }
    await page.getByTestId("dq-add-check").click();
    await expect(page.getByTestId("dq-check-rows")).toContainText(checkType);

    // Run the dry run for real before submitting — this checker type's genuine "query my data"
    // analog (dryRunContract scans through sourceId + contractText directly).
    await page.getByTestId("dq-dry-run").click();
    await expect(page.getByTestId("dq-dry-run-result")).toBeVisible({ timeout: 30000 });
    const outcomeBadge = page.getByTestId("dq-dry-run-result").locator(".mantine-Badge-root");
    await expect(outcomeBadge.first()).toBeVisible();
    await expect(outcomeBadge.first()).toHaveText(/pass|fail/);

    const registered = await submitRegisterAndExpectListed(
      page,
      sourceId,
      SWAP_REGISTER_TIMEOUT_MS,
    );

    // Give the poll job a real cadence — RegisterTableForm has no Cache TTL field, only
    // TableEditForm's (registerRss's own established step for the identical poll-landed shape).
    await page.goto("/tables");
    await page.waitForSelector(".page-header", { timeout: 15000 });
    const row = page.locator(".data-table tbody tr").filter({ hasText: sourceId }).first();
    await row.waitFor({ timeout: 15000 });
    await row.locator("td").first().click();
    const editBtn = page.getByTestId("table-read-view-edit").first();
    await editBtn.waitFor({ timeout: 10000 });
    await editBtn.click();
    await page.getByLabel(/^Cache TTL/).fill("5");
    await page.getByTestId("table-edit-save").click();
    await expect(page.getByTestId("table-edit-save")).toBeHidden({ timeout: 15000 });

    return {
      label: checkerType,
      sourceId,
      sql: `SELECT checker, outcome FROM pet_store.${registered} ORDER BY scan_time DESC LIMIT 1`,
      assertRows: (rows) => {
        expect(rows).toHaveLength(1);
        // REQ-1730: contract.py's CHECKERS frozenset ({"soda", "great_expectations"}) is the
        // canonical value the scan writes to this column — not a display label. The registrar
        // previously asserted a separate Title Case string ("Soda"/"Great Expectations") that
        // never matched, reproduced live (Received: "soda"/"great_expectations" against an
        // Expected Title Case string) before this fix.
        expect(rows[0][0]).toBe(checkerType);
        expect(rows[0][1]).toMatch(/pass|fail/);
      },
      reachableOn: ["trino"],
      pollTimeoutMs: 120000,
    };
  };
}

/** REQ-1730: google_sheets: SCAN-mechanism (one DuckDB view per source, fixed "main" schema, table named
 * after the source id — DuckDBGsheetsConnector, introspect.py's google_sheets branch), Trino
 * reaches it via a real ATTACH_R connector (TrinoGsheetsConnector, trino_connectors.py). No
 * throwaway-sheet path exists (the service account has zero Drive quota, per gsheets-e2e-fixture
 * project memory) — reads the SAME durable shared fixture sheet (GOOGLE_APPLICATION_CREDENTIALS +
 * GSHEETS_TEST_SHEET_ID) source-to-query-special-cases.spec.ts's own google_sheets test already
 * proved end to end; the caller test.skip()s when those env vars aren't set to it. */
export async function registerGsheets(page: Page): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_gsheets_${stamp}`;
  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("google_sheets");
  await page.getByTestId("google-sheets-credentials-input").fill(process.env.GOOGLE_APPLICATION_CREDENTIALS!);
  await page.getByTestId("google-sheets-sheet-id-input").fill(process.env.GSHEETS_TEST_SHEET_ID!);
  await submitSourceAndExpectListed(page, sourceId);

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, "main", sourceId);
  const registered = await submitRegisterAndExpectListed(page, sourceId, SWAP_REGISTER_TIMEOUT_MS);

  return {
    label: "google_sheets",
    sourceId,
    sql: `SELECT id, name FROM pet_store.${registered} ORDER BY id`,
    assertRows: (rows) => {
      expect(rows).toHaveLength(3);
      expect(rows[0][1]).toBe("Widget A");
    },
    reachableOn: ["trino"],
  };
}

export const registerSoda = registerDqChecker("soda", "row_count");
export const registerGreatExpectations = registerDqChecker(
  "great_expectations",
  "expect_table_row_count_to_be_between",
);

/** snowflake: the first cloud-warehouse-class type in this harness. No docker fixture (a real
 * warehouse, not a container) — cloud_warehouse_seed.py (already proven by
 * source-to-query-cloud-warehouse.spec.ts's own live-Snowflake test) seeds/tears down
 * PROVISA_UI_E2E.PUBLIC.WIDGETS directly, and this mirrors that spec's exact UI flow so the same
 * account/database quirks (unquoted Database field, upper-cased identifiers) apply identically. */
export async function registerSnowflake(page: Page): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_snowflake_${stamp}`;

  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("snowflake");
  await page.getByLabel(/Account URL/).fill(process.env.SNOWFLAKE_ACCOUNT!);
  await page.getByLabel(/^Database/).fill("PROVISA_UI_E2E");
  await page.getByLabel(/^Warehouse$/).fill(process.env.SNOWFLAKE_WAREHOUSE ?? "COMPUTE_WH");
  await page.getByRole("textbox", { name: "Authentication" }).click();
  await page.getByRole("option", { name: "Username / Password", exact: true }).click();
  await page.getByLabel(/^Username/).fill(process.env.SNOWFLAKE_USER!);
  await page.getByLabel(/^Password/).fill(process.env.SNOWFLAKE_PASSWORD!);
  await submitSourceAndExpectListed(page, sourceId);

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, "PUBLIC", "WIDGETS");
  await expect(page.getByTestId("register-table-col-selected-ID")).toBeVisible({ timeout: 120000 });
  await expect(page.getByTestId("register-table-col-selected-NAME")).toBeVisible();
  const registered = await submitRegisterAndExpectListed(page, sourceId, SWAP_REGISTER_TIMEOUT_MS);

  return {
    label: "snowflake",
    sourceId,
    sql: `SELECT id, name FROM pet_store.${registered} ORDER BY id`,
    assertRows: (rows) => {
      expect(rows).toEqual([
        ["1", "sprocket"],
        ["2", "cog"],
        ["3", "gear"],
      ]);
    },
    reachableOn: ["trino"],
  };
}

/** fabric: unlike databricks (a real Trino JDBC connector, `"databricks": "delta_lake"`), fabric
 * has NO entry in `_TRINO_JDBC_TYPES` — under Trino it falls through to the SAME generic
 * `WarehouseNativeConnector` DIRECT-driver land-and-query path every engine gets from
 * `FederationEngine.complete_reach()` (REQ-947, engine.py), the identical mechanism
 * `_DRIVER_FACTORIES`-backed types use everywhere (mirrors grpc_remote/graphql_remote's landing,
 * just via a direct DB driver — `MssqlWarehouseDriver`, Azure AD auth — instead of an API call).
 * So this is the first swap-harness type proving that GENERIC land path under Trino specifically,
 * not a native-connector ATTACH swap like databricks/snowflake/bigquery. UI flow copied verbatim
 * from source-to-query-cloud-warehouse.spec.ts's own proven `fabric` case (REQ-1747): Ambient
 * Credential auth (az login on this machine, no service-principal fields), seeded/torn down by
 * cloud_warehouse_seed.py's `_fabric()`, which resumes the Fabric capacity first (REQ-1775) since
 * a paused capacity rejects the SQL connection outright. */
export async function registerFabric(page: Page): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_fabric_${stamp}`;

  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("fabric");
  await page.getByRole("textbox", { name: /Server/ }).fill(process.env.FABRIC_SQL_SERVER!);
  await page.getByRole("textbox", { name: /^Database/ }).fill(process.env.FABRIC_DATABASE!);
  await page.getByRole("textbox", { name: "Authentication" }).click();
  await page
    .getByRole("option", { name: "Ambient Credential (az login / managed identity)", exact: true })
    .click();
  await submitSourceAndExpectListed(page, sourceId);

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, "provisa_ui_e2e", "widgets");
  await expect(page.getByTestId("register-table-col-selected-id")).toBeVisible({ timeout: 120000 });
  await expect(page.getByTestId("register-table-col-selected-name")).toBeVisible();
  const registered = await submitRegisterAndExpectListed(page, sourceId, SWAP_REGISTER_TIMEOUT_MS);

  return {
    label: "fabric",
    sourceId,
    sql: `SELECT id, name FROM pet_store.${registered} ORDER BY id`,
    assertRows: (rows) => {
      expect(rows).toEqual([
        ["1", "sprocket"],
        ["2", "cog"],
        ["3", "gear"],
      ]);
    },
    reachableOn: ["trino"],
  };
}

/** databricks: same cloud-warehouse-class pattern as registerSnowflake — a real workspace, not a
 * container, seeded/torn down by cloud_warehouse_seed.py's `_databricks()`. Trino reaches it via
 * the plain JDBC family (`"databricks": "delta_lake"` in trino_connectors.py's
 * `_TRINO_JDBC_TYPES`), already wired — no new connector needed, unlike exasol/bigquery earlier
 * this session. `host` is the bare workspace hostname (DATABRICKS_SERVER_HOSTNAME, no `https://`
 * prefix) per tests/integration/test_databricks_source_e2e.py's own proven SourcePool.add() shape;
 * the form's placeholder shows a URL but the field just wants the hostname. `http_path` rides via
 * federation_hints (not a standard connection field), same as that integration test's `extra`. */
export async function registerDatabricks(page: Page): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_databricks_${stamp}`;

  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("databricks");
  await page.getByLabel(/Workspace URL/).fill(process.env.DATABRICKS_SERVER_HOSTNAME!);
  await page.getByLabel(/^Catalog/).fill("workspace");
  await page.getByLabel(/SQL Warehouse HTTP Path/).fill(process.env.DATABRICKS_HTTP_PATH!);
  await page.getByRole("textbox", { name: "Authentication" }).click();
  await page.getByRole("option", { name: "Personal Access Token", exact: true }).click();
  await page.getByLabel(/Access Token/).fill(process.env.DATABRICKS_TOKEN!);
  await submitSourceAndExpectListed(page, sourceId);

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, "provisa_ui_e2e", "widgets");
  await expect(page.getByTestId("register-table-col-selected-id")).toBeVisible({ timeout: 120000 });
  await expect(page.getByTestId("register-table-col-selected-name")).toBeVisible();
  const registered = await submitRegisterAndExpectListed(page, sourceId, SWAP_REGISTER_TIMEOUT_MS);

  return {
    label: "databricks",
    sourceId,
    sql: `SELECT id, name FROM pet_store.${registered} ORDER BY id`,
    assertRows: (rows) => {
      expect(rows).toEqual([
        ["1", "sprocket"],
        ["2", "cog"],
        ["3", "gear"],
      ]);
    },
    reachableOn: ["trino"],
  };
}

/** bigquery: second cloud-warehouse-class type (see registerSnowflake's own comment for the
 * pattern). Auth is Application Default Credentials — the backend process's own
 * GOOGLE_APPLICATION_CREDENTIALS env var, which TrinoBigQueryConnector (trino_connectors.py)
 * also reads server-side to build bigquery.credentials-key, so no path field needs filling here. */
export async function registerBigquery(page: Page): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_bigquery_${stamp}`;

  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("bigquery");
  await page.getByLabel(/Project ID/).fill(process.env.GOOGLE_CLOUD_PROJECT!);
  await page.getByRole("textbox", { name: "Authentication" }).click();
  await page.getByRole("option", { name: "Application Default Credentials", exact: true }).click();
  await submitSourceAndExpectListed(page, sourceId);

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, "provisa_ui_e2e", "widgets");
  await expect(page.getByTestId("register-table-col-selected-id")).toBeVisible({ timeout: 120000 });
  await expect(page.getByTestId("register-table-col-selected-name")).toBeVisible();
  const registered = await submitRegisterAndExpectListed(page, sourceId, SWAP_REGISTER_TIMEOUT_MS);

  return {
    label: "bigquery",
    sourceId,
    sql: `SELECT id, name FROM pet_store.${registered} ORDER BY id`,
    assertRows: (rows) => {
      expect(rows).toEqual([
        ["1", "sprocket"],
        ["2", "cog"],
        ["3", "gear"],
      ]);
    },
    reachableOn: ["trino"],
  };
}

/** redshift: unlike snowflake/databricks/bigquery/fabric (standing accounts this harness never
 * provisions), Redshift Serverless is AWS-only and billable — the caller provisions it via
 * `provisionRedshift("up")` (see engine-swap-helpers.ts) BEFORE calling this, and passes the
 * resulting connection down. Trino reaches it through the plain JDBC family
 * (`_TRINO_JDBC_TYPES["redshift"] = "redshift"` in trino_connectors.py, already wired — no new
 * connector needed). Seeded table matches `tests/integration/test_redshift_source_e2e.py`'s own
 * shape (`public.provisa_widgets_e2e`, (id, name) rows) via `scripts/redshift_e2e.py`'s `up`. */
export function registerRedshift(conn: RedshiftConnection): (page: Page) => Promise<Registration> {
  return async (page: Page): Promise<Registration> => {
    const stamp = Date.now();
    const sourceId = `e2e_swap_redshift_${stamp}`;

    await openSourcesForm(page);
    await page.getByTestId("sources-id-input").fill(sourceId);
    await page.getByTestId("sources-type-select").selectOption("redshift");
    await page.getByLabel(/^Host/).fill(conn.host);
    await page.getByLabel(/^Port/).fill(String(conn.port));
    await page.getByLabel(/^Database/).fill(conn.database);
    await page.getByLabel(/^Username/).fill(conn.user);
    await page.getByLabel(/^Password/).fill(conn.password);
    await submitSourceAndExpectListed(page, sourceId);

    await openRegisterForm(page, sourceId);
    await pickSchemaAndTable(page, "public", "provisa_widgets_e2e");
    await expect(page.getByTestId("register-table-col-selected-id")).toBeVisible({ timeout: 120000 });
    await expect(page.getByTestId("register-table-col-selected-name")).toBeVisible();
    const registered = await submitRegisterAndExpectListed(page, sourceId, SWAP_REGISTER_TIMEOUT_MS);

    return {
      label: "redshift",
      sourceId,
      sql: `SELECT id, name FROM pet_store.${registered} ORDER BY id`,
      assertRows: (rows) => {
        expect(rows).toEqual([
          ["1", "Widget A"],
          ["2", "Widget B"],
          ["3", "Widget C"],
        ]);
      },
      reachableOn: ["trino"],
    };
  };
}

export async function registerElasticsearch(page: Page): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_es_${stamp}`;
  const tableName = "support_tickets";
  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("elasticsearch");
  await page.getByLabel(/^Host/).fill("localhost");
  await page.getByLabel(/^Port/).fill(String(E2E_ES_PORT));
  await submitSourceAndExpectListed(page, sourceId);

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, "default", tableName);
  await expect(page.getByTestId("register-table-col-selected-ticket_id")).toBeVisible({
    timeout: 60000,
  });
  const registered = await submitRegisterAndExpectListed(page, sourceId, SWAP_REGISTER_TIMEOUT_MS);

  return {
    label: "elasticsearch",
    sourceId,
    sql: `SELECT ticket_id, status, priority FROM pet_store.${registered} ORDER BY ticket_id`,
    assertRows: (rows) => {
      expect(rows).toHaveLength(6);
      expect(rows[0]).toEqual(["T-1001", "open", "2"]);
      expect(rows[5]).toEqual(["T-1006", "open", "2"]);
    },
    reachableOn: ["trino"],
  };
}

export async function registerRedis(page: Page): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_redis_${stamp}`;
  const tableName = "support_agent";
  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("redis");
  await page.getByLabel(/^Host/).fill("localhost");
  await page.getByLabel(/^Port/).fill(String(E2E_REDIS_PORT));
  await page.getByLabel(/^Database/).fill("0");
  await submitSourceAndExpectListed(page, sourceId);

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, "default", tableName);
  await expect(page.getByTestId("register-table-col-selected-name")).toBeVisible({
    timeout: 60000,
  });
  const registered = await submitRegisterAndExpectListed(page, sourceId, SWAP_REGISTER_TIMEOUT_MS);

  return {
    label: "redis",
    sourceId,
    sql: `SELECT agent_id, name, team FROM pet_store.${registered} ORDER BY agent_id`,
    assertRows: (rows) => {
      expect(rows).toHaveLength(5);
      expect(rows[0]).toEqual(["1", "Ann Lee", "tier1"]);
      expect(rows[4]).toEqual(["5", "Eli Stone", "escalations"]);
    },
    reachableOn: ["trino"],
  };
}

export async function registerCassandra(page: Page): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_cassandra_${stamp}`;
  const tableName = "intake_events";
  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("cassandra");
  await page.getByLabel(/^Host/).fill("localhost");
  await page.getByLabel(/^Port/).fill(String(E2E_CASSANDRA_PORT));
  await page.getByLabel(/^Database/).fill("shelter_ops");
  await submitSourceAndExpectListed(page, sourceId);

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, "shelter_ops", tableName);
  await expect(page.getByTestId("register-table-col-selected-event_id")).toBeVisible({
    timeout: 60000,
  });
  const registered = await submitRegisterAndExpectListed(page, sourceId, SWAP_REGISTER_TIMEOUT_MS);

  return {
    label: "cassandra",
    sourceId,
    sql: `SELECT event_id, event_type, animal_name FROM pet_store.${registered} ORDER BY event_id`,
    assertRows: (rows) => {
      expect(rows).toHaveLength(7);
      expect(rows[0]).toEqual(["1", "intake", "Buddy"]);
      expect(rows[6]).toEqual(["7", "adoption", "Mittens"]);
    },
    reachableOn: ["trino"],
  };
}

export async function registerSparql(page: Page): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_sparql_${stamp}`;
  const tableName = `volunteer_${stamp}`;
  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("sparql");
  await page
    .getByTestId("sparql-endpoint-input")
    .fill(`http://localhost:${E2E_SPARQL_PORT}/provisa/query`);
  await submitSourceAndExpectListed(page, sourceId);

  await openRegisterForm(page, sourceId);
  await expect(page.getByTestId("register-table-schema-select")).toHaveCount(0);
  await page.getByTestId("register-table-sparql-table-name").fill(tableName);
  await page
    .getByTestId("register-table-sparql-query")
    .fill(
      "PREFIX s: <http://provisa.dev/shelter#> SELECT ?volunteer_id ?name ?program " +
        "WHERE { ?v a s:Volunteer ; s:id ?volunteer_id ; s:name ?name ; s:program ?program } " +
        "ORDER BY ?volunteer_id",
    );
  await page.getByTestId("register-table-sparql-preview").click();
  await expect(page.getByTestId("register-table-sparql-preview-rows")).toBeVisible({
    timeout: 60000,
  });
  const registered = await submitRegisterAndExpectListed(page, sourceId, SWAP_REGISTER_TIMEOUT_MS);

  return {
    label: "sparql",
    sourceId,
    sql: `SELECT volunteer_id, name, program FROM pet_store.${registered} ORDER BY volunteer_id`,
    assertRows: (rows) => {
      expect(rows).toHaveLength(6);
      expect(rows[0]).toEqual(["V-01", "Grace Hall", "adoption"]);
      expect(rows[5]).toEqual(["V-06", "Noah Bryce", "fostering"]);
    },
    reachableOn: ["trino"],
  };
}

export async function registerPrometheus(page: Page): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_prometheus_${stamp}`;
  const tableName = "up";
  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("prometheus");
  await page.getByTestId("prometheus-url-input").fill(`http://localhost:${E2E_PROMETHEUS_PORT}`);
  await submitSourceAndExpectListed(page, sourceId);

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, "default", tableName);
  await expect(page.getByTestId("register-table-col-selected-job")).toBeVisible({
    timeout: 60000,
  });
  const registered = await submitRegisterAndExpectListed(page, sourceId, SWAP_REGISTER_TIMEOUT_MS);

  return {
    label: "prometheus",
    sourceId,
    // "up" is a boolean gauge (always 1 when scraped), not a growing counter — identical under
    // any engine at any time, unlike a counter metric would be.
    sql: `SELECT job, CAST(MAX(value) AS INTEGER) AS healthy FROM pet_store.${registered} GROUP BY job ORDER BY job`,
    assertRows: (rows) => expect(rows).toEqual([["prometheus", "1"]]),
    reachableOn: ["trino"],
  };
}

/** splunk: DuckDB reaches it via the connector's bundled Calcite pgwire bridge, a per-source JVM
 * whose schema is the sql-normalized SOURCE ID (`pgwire_replica.schema_name()`). Trino reaches it
 * via a genuinely different, standalone plugin (`trino/plugins/trino-splunk/`, also wrapping
 * Calcite's splunk adapter) that — until now — always registered its schema under the FIXED
 * string `"splunk"` regardless of configuration, verified live via a real
 * `FederationError(..., SCHEMA_NOT_FOUND, "Schema '<source_id>' does not exist")`: a table
 * registered under DuckDB records schema=<source_id>, and swapped to Trino the compiler emits
 * that SAME physical schema name, which the plugin's hardcoded schema never had. NOT the
 * registration-timing gap this harness's module doc originally guessed
 * (`reprovisionSourceOnEngine`'s createSource replay runs fine and does create the Trino catalog)
 * — a genuine physical-schema-naming mismatch, fixed upstream: `calcite/splunk`'s `SplunkDriver`
 * now honors a `schema` connection property (kenstott/calcite@28db96ca1), and
 * `TrinoSplunkConnector.details()` (trino_connectors.py) passes the SAME
 * `pgwire_replica.schema_name()` value Trino sees under both engines. sharepoint has the
 * identical shape (tests/integration/test_sharepoint_source_e2e.py:28, fixed `"sharepoint"`
 * schema) — same root cause, not yet fixed upstream (its plugin is a separate codebase). */
export async function registerSplunk(page: Page): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_splunk_${stamp}`;
  const tableName = "shelter_alerts";

  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("splunk");
  await page.getByTestId("splunk-host-input").fill("localhost");
  await page.getByTestId("splunk-port-input").fill(String(E2E_SPLUNK_PORT));
  await page.getByTestId("splunk-auth-mode-select").click();
  await page.getByRole("option", { name: "Username / Password" }).click();
  await page.getByTestId("splunk-username-input").fill("admin");
  await page.getByTestId("splunk-password-input").fill("Provisa_2026!");
  await page.getByTestId("splunk-disable-ssl-checkbox").check();
  await submitSourceAndExpectListed(page, sourceId);

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, sourceId, tableName);
  await expect(page.getByTestId("register-table-col-selected-alert_id")).toBeVisible({
    timeout: 120000,
  });
  await expect(page.getByTestId("register-table-col-selected-animal_name")).toBeVisible();
  const registered = await submitRegisterAndExpectListed(page, sourceId, SWAP_REGISTER_TIMEOUT_MS);

  return {
    label: "splunk",
    sourceId,
    sql: `SELECT alert_type, COUNT(*) AS alerts FROM pet_store.${registered} GROUP BY alert_type ORDER BY alert_type`,
    assertRows: (rows) => {
      expect(rows).toEqual([
        ["adoption_hold", "1"],
        ["intake", "2"],
        ["medical", "2"],
        ["transfer", "2"],
      ]);
    },
    reachableOn: ["trino"],
  };
}

/** sharepoint: a real live tenant (SP_* in .env), not a container — same cloud-warehouse-class
 * pattern as snowflake/databricks/fabric, just no seed/teardown step needed (reads an existing
 * document library, writes nothing). UI flow copied verbatim from source-to-query.spec.ts's own
 * proven sharepoint case (REQ-1747-adjacent). Unlike splunk (kenstott/calcite@28db96ca1/
 * @4a042e872, a genuine upstream bug), the underlying trino-sharepoint plugin ALREADY had a fully
 * working `schema` catalog property (verified by reading SharePointConfig.java/
 * SharePointClientModule.java/SharePointListDriver.java before assuming the same fix was needed —
 * see [[feedback-verify-old-exclusion-labels]]) — only TrinoSharepointConnector.details()
 * (trino_connectors.py) needed to start passing it. */
export async function registerSharepoint(page: Page): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_sharepoint_${stamp}`;
  const tableName = "documents";
  // The Calcite pgwire server runs with its bundle directory as cwd, so a relative SP_CERT_PATH
  // (.env authors it as ./sharepoint.pfx) has to be resolved here, same as source-to-query.spec.ts.
  const certPath = path.resolve(ROOT, process.env.SP_CERT_PATH!);

  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("sharepoint");
  await page.getByTestId("sharepoint-site-url-input").fill(process.env.SP_SITE_URL!);
  await page.getByTestId("sharepoint-tenant-id-input").fill(process.env.SP_TENANT_ID!);
  await page.getByTestId("sharepoint-auth-type-select").click();
  await page.getByRole("option", { name: "Certificate", exact: true }).click();
  await page.getByTestId("sharepoint-client-id-input").fill(process.env.SP_CLIENT_ID!);
  await page.getByTestId("sharepoint-cert-path-input").fill(certPath);
  await page.getByTestId("sharepoint-cert-password-input").fill(process.env.SP_CERT_PASSWORD ?? "");
  await submitSourceAndExpectListed(page, sourceId);

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, sourceId, tableName);
  await expect(page.getByTestId("register-table-col-selected-id")).toBeVisible({ timeout: 120000 });
  await expect(page.getByTestId("register-table-col-selected-title")).toBeVisible();
  const registered = await submitRegisterAndExpectListed(page, sourceId, SWAP_REGISTER_TIMEOUT_MS);

  return {
    label: "sharepoint",
    sourceId,
    // The tenant's document library contents are real and may legitimately be empty, so the
    // aggregate's SHAPE is the assertion (matches source-to-query.spec.ts's own case) — same
    // shape under both engines is exactly what this harness is proving, not a fixed row count.
    sql: `SELECT COUNT(*) AS document_count FROM pet_store.${registered}`,
    assertRows: (rows) => {
      expect(rows).toHaveLength(1);
      expect(rows[0]).toHaveLength(1);
      expect(Number(rows[0][0])).toBeGreaterThanOrEqual(0);
    },
    reachableOn: ["trino"],
  };
}

export async function registerSqlite(page: Page): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_sqlite_${stamp}`;
  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("sqlite");
  await page.getByLabel(/SQLite File Path/).fill("./demo/files/inquiries.sqlite");
  await submitSourceAndExpectListed(page, sourceId);

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, "main", "users");
  await expect(page.getByTestId("register-table-col-selected-name")).toBeVisible({
    timeout: 60000,
  });
  const registered = await submitRegisterAndExpectListed(page, sourceId, SWAP_REGISTER_TIMEOUT_MS);

  return {
    label: "sqlite",
    sourceId,
    sql: `SELECT id, name, email FROM pet_store.${registered} ORDER BY id`,
    assertRows: (rows) => {
      expect(rows).toHaveLength(10);
      expect(rows[0]).toEqual(["1", "Alice Nguyen", "alice@example.com"]);
    },
    // REQ-1730: sqlite has no Trino connector, but IS reachable on Trino via landing (now that
    // sqlite is in strategy.py's _MATERIALIZE_ONLY — events/source_loader.py's make_sqlite_loader
    // already had a working, engine-independent row-fetch). Reachable on pg too, via
    // SqliteFdwConnector — see the dedicated scenario-1 pg-reboot test in engine-swap.spec.ts.
    reachableOn: ["trino"],
  };
}

export async function registerGraphqlRemote(page: Page, endpointOverride?: string): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_gql_${stamp}`;
  const namespace = `e2e_swap_gql_${stamp}`;
  // REQ-1730 scenario 1 (test-only): the reboot harness's minimal config has no baked-in
  // "graphql-demo" source for existingSourcePath to read a path off of — pass the already-running
  // demo server's URL directly (spawnRebootBackend's own GRAPHQL_DEMO_URL env, same default port).
  const endpoint = endpointOverride ?? (await existingSourcePath(page, "graphql-demo"));

  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("graphql");
  await page.getByTestId("graphql-endpoint-input").fill(endpoint);
  await page.getByTestId("graphql-namespace-input").fill(namespace);
  await submitSourceAndExpectListed(page, sourceId);

  const tableNames = await registeredTableNames(page, sourceId);
  const breedTable = tableNames.find((n) => n.includes("animal_breed"));
  expect(breedTable, `no animal_breeds table registered for ${sourceId}`).toBeTruthy();
  const grant = await page.request.post("/admin/graphql", {
    data: {
      query: `mutation($t: TableInput!) { updateTable(input: $t) { success message } }`,
      variables: {
        t: {
          sourceId,
          domainId: "",
          schemaName: "graphql",
          tableName: breedTable,
          columns: [
            { name: "name", visibleTo: ["*"] },
            { name: "species", visibleTo: ["*"] },
          ],
        },
      },
    },
  });
  expect(grant.ok(), await grant.text()).toBeTruthy();
  const grantJson = await grant.json();
  expect(grantJson.errors, JSON.stringify(grantJson.errors)).toBeUndefined();
  expect(grantJson.data.updateTable.success, grantJson.data.updateTable.message).toBeTruthy();

  return {
    label: "graphql_remote",
    sourceId,
    sql: `SELECT name, species FROM graphql.${breedTable} ORDER BY name`,
    assertRows: (rows) => {
      expect(rows).toHaveLength(6);
      expect(rows.map((r) => r[0])).toEqual([
        "African Lion",
        "Barbary Lion",
        "Golden Retriever",
        "Holland Lop",
        "Maine Coon",
        "Siamese",
      ]);
    },
    reachableOn: ["trino"],
  };
}

// grpc_remote has no Trino connector — reachability runs through the same landing path as
// graphql_remote/openapi (rows written into the Postgres-backed materialize store Trino reads via
// its provisa_admin catalog), which REQ-1730's ensure_cache_schema fix made work. See
// demo/grpc_remote_server for the proto-based fixture this registers against (REQ-1742).
export async function registerGrpcRemote(page: Page, port: number): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_grpc_${stamp}`;
  const namespace = `e2e_swap_grpc_${stamp}`;
  const protoPath = path.join(ROOT, "demo", "grpc_remote_server", "animal_catalog.proto");

  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("grpc");
  await page.getByTestId("grpc-proto-path-input").fill(protoPath);
  await page.getByTestId("grpc-server-address-input").fill(`localhost:${port}`);
  await page.getByTestId("grpc-namespace-input").fill(namespace);
  await submitSourceAndExpectListed(page, sourceId);

  const tableNames = await registeredTableNames(page, sourceId);
  const breedTable = tableNames.find((n) => n.includes("ListBreeds"));
  expect(breedTable, `no ListBreeds table registered for ${sourceId}`).toBeTruthy();
  const grant = await page.request.post("/admin/graphql", {
    data: {
      query: `mutation($t: TableInput!) { updateTable(input: $t) { success message } }`,
      variables: {
        t: {
          sourceId,
          domainId: "",
          schemaName: "grpc_remote",
          tableName: breedTable,
          columns: [
            { name: "name", visibleTo: ["*"] },
            { name: "species", visibleTo: ["*"] },
            { name: "avg_lifespan_years", visibleTo: ["*"] },
          ],
        },
      },
    },
  });
  expect(grant.ok(), await grant.text()).toBeTruthy();
  const grantJson = await grant.json();
  expect(grantJson.errors, JSON.stringify(grantJson.errors)).toBeUndefined();
  expect(grantJson.data.updateTable.success, grantJson.data.updateTable.message).toBeTruthy();

  return {
    label: "grpc_remote",
    sourceId,
    sql: `SELECT name, species FROM grpc_remote.${breedTable} ORDER BY name`,
    assertRows: (rows) => {
      expect(rows).toHaveLength(3);
      expect(rows.map((r) => r[0])).toEqual(["Holland Lop", "Labrador Retriever", "Siamese"]);
    },
    reachableOn: ["trino"],
  };
}

// ingest (REQ-1730): a NO_CONNECTION_TYPES push receiver (POST /data/ingest/<source>/<table>,
// provisa/ingest/router.py) — no Sources-form connection fields, no adapter fetch. Rows land
// SYNCHRONOUSLY into the SAME control-plane DB state.tenant_db mirrors (provisa/ingest/engine.py's
// _init_ingest_engines), which under this harness's Postgres control plane is the identical
// Postgres schema Trino's own `provisa_admin` catalog reads live — unlike grpc_remote/graphql_remote
// (a materialize-store copy reconcile_landed_tables writes asynchronously), there is no landing
// delay to wait out: the POST itself is the write. source-to-query-streaming.spec.ts's own proven
// ingest case (REQ-1739/1771, 4 real bugs fixed there) is this registrar's exact template —
// "default"-schema pick (source_id as the one table, REQ-1745), placeholder ext_id/value columns
// with per-column JSON-path extraction (id/value), pet_store.<registered> as the query surface
// (register_table already overrides ingest's stored schema_name to the real control-plane schema
// at registration time — REAL BUG #3 in that file's own comment — so pet_store here is the same
// logical-domain surface every plain pickSchemaAndTable registrar uses, not a literal physical one).
//
// sourceId deliberately has NO "_<digit>" boundary (every OTHER registrar's `_${stamp}` would
// give it one): the SQL page's compiled query resolves a table through its own GraphQL-field-name
// round trip (compiler.naming/sql_rewrite.semantic_table_name), which has no way to mark a word
// boundary immediately before a digit and so silently drops an underscore that directly precedes
// one — reproduced live via Trino with the `_${stamp}` form: the POSTed row committed and was
// visible via a fresh Postgres connection immediately afterward, yet the SQL page's compiled query
// always answered zero rows, because ingest's own physical DDL (app_loaders.py's
// _init_ingest_engines) creates the table under the RAW registered_tables.table_name (with the
// underscore), not the compiler's post-round-trip name (without it) — every OTHER type's physical
// table is created via that SAME compiled name already, so this mismatch is specific to ingest.
// "_id<stamp>" (a letter between the underscore and the digits, unlike every other registrar's
// bare "_<stamp>") sidesteps the collision entirely for this registrar; the underlying naming gap
// is real but narrow (any ingest source id containing "_<digit>") and is documented, not fixed,
// in REQ-1730's own amendment. A literal hyphen was tried first and rejected: the "default"-schema
// picker's physical table name IS the source id verbatim (REQ-1745), and an unquoted hyphen in
// `CREATE TABLE IF NOT EXISTS <name>` parses as subtraction — a SQL syntax error, not a naming
// mismatch — so the source id must stay a single valid unquoted SQL identifier throughout.
export async function registerIngest(page: Page): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_ingest_id${stamp}`;

  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("ingest");
  await submitSourceAndExpectListed(page, sourceId);

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, "default", sourceId);
  await expect(page.getByTestId("register-table-col-selected-ext_id")).toBeVisible({
    timeout: 30000,
  });
  await page.getByTestId("register-table-col-path-ext_id").fill("id");
  await page.getByTestId("register-table-col-path-value").fill("value");
  const registered = await submitRegisterAndExpectListed(page, sourceId, SWAP_REGISTER_TIMEOUT_MS);

  const res = await page.request.post(`/data/ingest/${sourceId}/${sourceId}`, {
    data: { id: "abc-1", value: "hello-ingest" },
  });
  expect(res.ok(), await res.text()).toBeTruthy();

  return {
    label: "ingest",
    sourceId,
    sql: `SELECT ext_id, value FROM pet_store.${registered} ORDER BY ext_id`,
    assertRows: (rows) => {
      expect(rows).toEqual([["abc-1", "hello-ingest"]]);
    },
    reachableOn: ["trino"],
  };
}

export async function registerOpenapi(page: Page, specUrlOverride?: string): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_openapi_${stamp}`;
  // REQ-1730 scenario 1 (test-only): see registerGraphqlRemote's identical comment.
  const specUrl = specUrlOverride ?? (await existingSourcePath(page, "petstore-api"));
  const baseUrl = specUrl.replace(/\/openapi\.json$/, "");

  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("openapi");
  await page.getByTestId("openapi-spec-path-input").fill(specUrl);
  await page.getByTestId("openapi-base-url-input").fill(baseUrl);
  await submitSourceAndExpectListed(page, sourceId);

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, "openapi", "getInventory");
  const registered = await submitRegisterAndExpectListed(page, sourceId, SWAP_REGISTER_TIMEOUT_MS);

  return {
    label: "openapi",
    sourceId,
    sql: `SELECT * FROM pet_store.${registered}`,
    assertRows: (rows) => expect(rows.length).toBeGreaterThan(0),
    reachableOn: ["trino"],
  };
}

// govdata (REQ-1730): no Trino connector (strategy.py's _MATERIALIZE_ONLY) — same landing path as
// grpc_remote/graphql_remote/openapi above (rows written into the Postgres-backed materialize
// store Trino reads via its provisa_admin catalog). A REAL external API (AskAmerica/US government
// open data), no offline mock — same credential gate source-to-query-special-cases.spec.ts's own
// govdata case uses (FREE_ASKAMERICA_KEY in .env). Unlike ingest, govdata's registered columns
// come from live introspection (fetch_columns against the real API) so they already match its
// landed shape — none of REQ-1730's ingest-specific landing_worklist/reconcile_table conflict
// applies here.
export async function registerGovdata(page: Page): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_govdata_${stamp}`;
  const apiKey = process.env.FREE_ASKAMERICA_KEY ?? "";

  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("govdata");
  await page.getByTestId("govdata-subject-WEATHER").check();
  await page.getByTestId("govdata-api-key-input").fill(apiKey);
  await submitSourceAndExpectListed(page, sourceId);

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, "weather", "nws_stations");
  const registered = await submitRegisterAndExpectListed(page, sourceId, SWAP_REGISTER_TIMEOUT_MS);

  return {
    label: "govdata",
    sourceId,
    sql: `SELECT * FROM pet_store.${registered} LIMIT 5`,
    assertRows: (rows) => expect(rows.length).toBeGreaterThan(0),
    reachableOn: ["trino"],
  };
}

// rss (REQ-1730): no Trino connector (strategy.py's _MATERIALIZE_ONLY) — but UNLIKE every other
// type in this set, rss lands through a background POLL job (wire_new_poll_jobs, REQ-1770), not
// synchronously on the query itself. Both DuckDB's own registration AND the Trino-side replay
// (reprovisionSourceOnEngine's createSource call) trigger _rebuild_schemas, which starts a FRESH
// poll job for whichever backend runs it — and since the feed fixture (engine-swap.spec.ts's own
// beforeAll/afterAll, port E2E_RSS_PORT) re-serves the SAME static XML on every request, a poll
// job started on Trino's backend AFTER the swap lands the identical items just fine. This is what
// makes rss tractable where websocket (a one-shot push, never replayed) is not. `pollTimeoutMs` on
// the returned Registration (requeryOnEngine, engine-swap-helpers.ts) retries the whole query
// every 5s until the poll job's first tick lands something, instead of the single attempt every
// other registration here gets.
// sourceId deliberately has NO "_<digit>" boundary (registerIngest's own comment, REQ-1730's
// documented naming gap): the SQL page's compiled query resolves a table through a GraphQL-
// field-name round trip that silently drops an underscore immediately preceding a digit, so
// ingest/rss's physical table name (created verbatim from the source id) and the compiled
// query-time name can diverge for the usual `_${stamp}` ending every OTHER registrar here uses.
// Reproduced live: the poll job landed the real 2 rows into "e2e_swap_rss_<stamp>" every 5s
// (confirmed with direct store_writer.land() tracing) while the SQL page's compiled query for the
// same registration always answered zero rows.
export async function registerRss(page: Page): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_rss_id${stamp}`;

  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("rss");
  await page.getByLabel(/^Host/).fill("localhost");
  await page.getByLabel(/^Port/).fill(String(E2E_RSS_PORT));
  await page.getByTestId("rss-use-ssl-checkbox").uncheck();
  await submitSourceAndExpectListed(page, sourceId);

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, "default", sourceId);
  await expect(page.getByTestId("register-table-col-selected-id")).toBeVisible({
    timeout: 30000,
  });
  const registered = await submitRegisterAndExpectListed(page, sourceId, SWAP_REGISTER_TIMEOUT_MS);

  // Tables page: give the poll job an actual cadence (RegisterTableForm has no Cache TTL field,
  // only TableEditForm's own) — the same step source-to-query-streaming.spec.ts's own rss case
  // uses, and the reason wire_new_poll_jobs (REQ-1770) exists: saving here re-runs
  // _rebuild_schemas on an already-running runtime, which is exactly the path that needs to start
  // (or, on Trino's side after the swap, RE-start) this table's poll job.
  await page.goto("/tables");
  await page.waitForSelector(".page-header", { timeout: 15000 });
  const row = page.locator(".data-table tbody tr").filter({ hasText: sourceId }).first();
  await row.waitFor({ timeout: 15000 });
  // Click the FIRST cell, not the row's own (default-center) point: TablesPage.tsx renders an
  // actions column with its own `<Table.Td onClick={(e) => e.stopPropagation()}>` guard (so an
  // action button click doesn't also toggle the row), and a row wide enough for that column to
  // sit at the row's horizontal midpoint swallows a plain `row.click()` there — reproduced live
  // (the row locator resolves and clicks with no error, but the row never expands).
  await row.locator("td").first().click();
  const editBtn = page.getByTestId("table-read-view-edit").first();
  await editBtn.waitFor({ timeout: 10000 });
  await editBtn.click();
  await page.getByLabel(/^Cache TTL/).fill("5");
  await page.getByTestId("table-edit-save").click();
  await expect(page.getByTestId("table-edit-save")).toBeHidden({ timeout: 15000 });

  return {
    label: "rss",
    sourceId,
    sql: `SELECT id, title FROM pet_store.${registered} ORDER BY id`,
    assertRows: (rows) => {
      expect(rows).toEqual([
        ["item-1", "First item"],
        ["item-2", "Second item"],
      ]);
    },
    reachableOn: ["trino"],
    pollTimeoutMs: 120000,
  };
}

// websocket (REQ-1730): no Trino connector (strategy.py's _MATERIALIZE_ONLY) — lands through the
// SAME REQ-1733 CDC-listener mechanism as kafka (push_wiring.py's wire_push_listeners), unlike
// rss's poll cadence. Tractable for this harness for the same reason rss is: the fixture (the
// `ws` package, source-to-query-streaming.spec.ts's own pattern) sends its fixed 2-event payload
// on EVERY new connection, not just the first ever, so a fresh listener wire_push_listeners opens
// on the Trino-bound backend after the swap (triggered the same way as rss's poll job — every
// _rebuild_schemas call, including the createSource replay itself) receives the same 2 events
// again. Unlike rss, wire_push_listeners' own per-node idempotency (state.push_listener_
// disconnects) only marks a node visited on an ACTUAL successful listener start, not eagerly on
// every attempt (no wire_new_poll_jobs-style permanent-exclusion risk) — but pollTimeoutMs is
// still used since opening a live connection and landing its first events is asynchronous either
// way. CDC landing hard-requires a declared primary key (register-table-col-pk-id), unlike rss.
export async function registerWebsocket(page: Page): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_ws_id${stamp}`;

  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("websocket");
  await page.getByLabel(/^Host/).fill("localhost");
  await page.getByLabel(/^Port/).fill(String(E2E_WS_PORT));
  await submitSourceAndExpectListed(page, sourceId);

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, "default", sourceId);
  await expect(page.getByTestId("register-table-col-selected-id")).toBeVisible({
    timeout: 30000,
  });
  await page.getByTestId("register-table-col-pk-id").check();
  const registered = await submitRegisterAndExpectListed(page, sourceId, SWAP_REGISTER_TIMEOUT_MS);

  return {
    label: "websocket",
    sourceId,
    sql: `SELECT id, value FROM pet_store.${registered} ORDER BY id`,
    assertRows: (rows) => {
      expect(rows).toEqual([
        ["ws-1", "hello"],
        ["ws-2", "world"],
      ]);
    },
    reachableOn: ["trino"],
    pollTimeoutMs: 120000,
  };
}

export async function registerFirebird(page: Page): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_firebird_${stamp}`;
  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("firebird");
  await page.getByLabel(/^Host/).fill("localhost");
  await page.getByLabel(/^Port/).fill(String(E2E_FIREBIRD_PORT));
  await page.getByLabel(/^Username/).fill("provisa");
  await page.getByLabel(/^Password/).fill("provisa");
  // The firebird extension's DSN path is the file's IN-CONTAINER path (FIREBIRD_DATABASE=test.fdb
  // under /firebird/data — see demo/sources/firebird/compose.yml), not a host path.
  await page.getByLabel(/^Database/).fill("/firebird/data/test.fdb");
  await submitSourceAndExpectListed(page, sourceId);

  await openRegisterForm(page, sourceId);
  // demo/sources/firebird/prime.py quotes lower-case identifiers ("widgets"/"id"/"name") so
  // DuckDB's ATTACH surfaces them exactly as written — Firebird folds UNQUOTED identifiers to
  // upper case, and the generic registerTable mutation has no apply_sql_name normalization step
  // (unlike graphql_remote_router's registration), so an unquoted schema would persist verbatim.
  await pickSchemaAndTable(page, "main", "widgets");
  await expect(page.getByTestId("register-table-col-selected-name")).toBeVisible({
    timeout: 60000,
  });
  const registered = await submitRegisterAndExpectListed(page, sourceId, SWAP_REGISTER_TIMEOUT_MS);

  return {
    label: "firebird",
    sourceId,
    sql: `SELECT id, name FROM pet_store.${registered} ORDER BY id`,
    assertRows: (rows) => {
      expect(rows).toHaveLength(3);
      expect(rows[0]).toEqual(["1", "Widget A"]);
      expect(rows[2]).toEqual(["3", "Widget C"]);
    },
    // REQ-1730: firebird has no Trino connector, but now lands into the materialize store (added
    // to strategy.py's _MATERIALIZE_ONLY, read via events/source_loader.py's new
    // make_firebird_loader — a scratch DuckDB connection ATTACHed through the same `firebird`
    // community extension the live engine itself uses).
    reachableOn: ["trino"],
  };
}

export async function registerAirport(page: Page): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_airport_${stamp}`;
  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("airport");
  await page.getByLabel(/^Host/).fill("localhost");
  await page.getByLabel(/^Port/).fill(String(E2E_AIRPORT_PORT));
  await submitSourceAndExpectListed(page, sourceId);

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, "test", "widgets");
  await expect(page.getByTestId("register-table-col-selected-name")).toBeVisible({
    timeout: 60000,
  });
  const registered = await submitRegisterAndExpectListed(page, sourceId, SWAP_REGISTER_TIMEOUT_MS);

  return {
    label: "airport",
    sourceId,
    sql: `SELECT id, name FROM pet_store.${registered} ORDER BY id`,
    assertRows: (rows) => {
      expect(rows).toHaveLength(3);
      expect(rows[0]).toEqual(["1", "Widget A"]);
      expect(rows[2]).toEqual(["3", "Widget C"]);
    },
    // REQ-1730: same fix as firebird just above — airport now lands via the new
    // make_airport_loader (a scratch DuckDB connection through the `airport` extension).
    reachableOn: ["trino"],
  };
}

// REQ-1730: duckdb-as-a-SOURCE — ATTACHing a SECOND local .duckdb file, distinct from DuckDB as
// Provisa's own engine (source-to-query-community-ext.spec.ts's own `duckdb` test proves the
// plain register+query path; this ports that same fixture into the reboot harness). Its
// executor/drivers/registry.py `_make_duckdb` DirectDriver entry means FederationEngine.
// complete_reach() (REQ-947) gives it a generic DIRECT connector on EVERY engine — federate()
// resolves it to Strategy.MATERIALIZED universally, no strategy.py change needed (unlike
// firebird/airport, whose gap was the missing _MATERIALIZE_ONLY membership itself).
export async function registerDuckdbSource(page: Page): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_duckdbsrc_${stamp}`;
  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("duckdb");
  await page.getByLabel(/^File Path/).fill(WIDGETS_DUCKDB_PATH);
  await submitSourceAndExpectListed(page, sourceId);

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, "main", "widgets");
  await expect(page.getByTestId("register-table-col-selected-name")).toBeVisible({
    timeout: 60000,
  });
  const registered = await submitRegisterAndExpectListed(page, sourceId, SWAP_REGISTER_TIMEOUT_MS);

  return {
    label: "duckdb-source",
    sourceId,
    sql: `SELECT id, name FROM pet_store.${registered} ORDER BY id`,
    assertRows: (rows) => {
      expect(rows).toHaveLength(3);
      expect(rows[0]).toEqual(["1", "Widget A"]);
      expect(rows[2]).toEqual(["3", "Widget C"]);
    },
    reachableOn: ["trino"],
  };
}

// REQ-1730: trino-as-a-SOURCE (REQ-994) — Provisa reads a remote Trino/Presto coordinator
// directly via the SQLAlchemy trino dialect (executor/drivers/registry.py's `_make_trino`),
// distinct from Trino as the federation ENGINE. Same complete_reach()/DIRECT-driver reasoning as
// duckdb-as-a-source above — already MATERIALIZED-reachable on every engine, no strategy.py
// change needed. `tpch` is the fixture Trino coordinator's own built-in synthetic-data connector
// (demo/sources/trino/catalog/tpch.properties) — zero seed step needed.
export async function registerTrinoSource(page: Page): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_trinosrc_${stamp}`;
  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("trino");
  await page.getByLabel(/^Host/).fill("localhost");
  await page.getByLabel(/^Port/).fill(String(E2E_TRINO_SOURCE_PORT));
  await page.getByLabel(/^Username/).fill("provisa");
  await page.getByLabel(/^Database/).fill("tpch");
  await submitSourceAndExpectListed(page, sourceId);

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, "tiny", "nation");
  await expect(page.getByTestId("register-table-col-selected-name")).toBeVisible({
    timeout: 60000,
  });
  const registered = await submitRegisterAndExpectListed(page, sourceId, SWAP_REGISTER_TIMEOUT_MS);

  return {
    label: "trino-source",
    sourceId,
    sql: `SELECT nationkey, name FROM pet_store.${registered} ORDER BY nationkey LIMIT 5`,
    assertRows: (rows) => {
      expect(rows).toHaveLength(5);
      expect(rows[0]).toEqual(["0", "ALGERIA"]);
      expect(rows[4]).toEqual(["4", "EGYPT"]);
    },
    reachableOn: ["trino"],
  };
}

/** Live SingleStore Cloud shared-tier workspace — SINGLESTORE_HOST/PORT/USERNAME/PASSWORD/DATABASE
 * in .env, widgets(id, name) seeded once directly into it (no per-run provisioning: unlike the
 * ephemeral redshift/synapse lanes, this workspace is standing infrastructure). */
export async function registerSinglestore(page: Page): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_singlestore_${stamp}`;
  const database = process.env.SINGLESTORE_DATABASE!;
  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("singlestore");
  await page.getByLabel(/^Host/).fill(process.env.SINGLESTORE_HOST!);
  await page.getByLabel(/^Port/).fill(process.env.SINGLESTORE_PORT!);
  await page.getByLabel(/^Username/).fill(process.env.SINGLESTORE_USERNAME!);
  await page.getByLabel(/^Password/).fill(process.env.SINGLESTORE_PASSWORD!);
  await page.getByLabel(/^Database/).fill(database);
  await submitSourceAndExpectListed(page, sourceId);

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, database, "widgets");
  await expect(page.getByTestId("register-table-col-selected-name")).toBeVisible({
    timeout: 60000,
  });
  const registered = await submitRegisterAndExpectListed(page, sourceId, SWAP_REGISTER_TIMEOUT_MS);

  return {
    label: "singlestore",
    sourceId,
    sql: `SELECT id, name FROM pet_store.${registered} ORDER BY id`,
    assertRows: (rows) => {
      expect(rows).toHaveLength(3);
      expect(rows[0]).toEqual(["1", "Widget A"]);
      expect(rows[2]).toEqual(["3", "Widget C"]);
    },
    // Materializable on DuckDB (executor/drivers/registry.py's `_make_mysql`, MySQL
    // wire-compatible) AND live-attached on Trino (a real JDBC connector) — the full swap, like
    // the original 9, not the DuckDB-only dead end firebird/airport are stuck in.
    reachableOn: ["trino"],
  };
}

/** exasol: same amd64-only/CI-gated shape as singlestore above, plus one more wrinkle —
 * Exasol 8 always serves TLS with a self-signed certificate regenerated every container boot
 * (demo/sources/exasol/prime.py's own module doc), so there is no fixed fingerprint to hardcode.
 * The caller reads it back from PROVISA_DEMO_EXASOL_FINGERPRINT_FILE right after provisioning
 * (mirrors source-to-query-olap-lake.spec.ts's own exasol case) and passes it in via this
 * accessor, the same closure-over-a-file-scope-variable shape registerFileLake uses for
 * deltaTablePath/icebergTablePath. Trino reaches exasol through the generic JDBC connector
 * (models.py's jdbc_url() already has a real exasol branch, REQ-1097) — no connector fix needed
 * here, unlike delta_lake/iceberg/snowflake/bigquery earlier this session. */
export async function registerExasol(page: Page, fingerprint: () => string): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_exasol_${stamp}`;
  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("exasol");
  await page.getByLabel(/^Host/).fill("localhost");
  await page.getByLabel(/^Port/).fill(String(E2E_EXASOL_PORT));
  await page.getByLabel(/^Username/).fill("sys");
  await page.getByLabel(/^Password/).fill("exasol");
  await page.getByLabel(/^Database/).fill("PROVISA");
  await page.getByLabel(/^Authentication/).selectOption("tls_fingerprint");
  await page.getByLabel(/TLS Fingerprint/).fill(fingerprint());
  await submitSourceAndExpectListed(page, sourceId);

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, "PROVISA", "WIDGETS");
  await expect(page.getByTestId("register-table-col-selected-NAME")).toBeVisible({
    timeout: 60000,
  });
  const registered = await submitRegisterAndExpectListed(page, sourceId, SWAP_REGISTER_TIMEOUT_MS);

  return {
    label: "exasol",
    sourceId,
    sql: `SELECT id, name FROM pet_store.${registered} ORDER BY id`,
    assertRows: (rows) => {
      expect(rows).toHaveLength(3);
      expect(rows[0]).toEqual(["1", "Widget A"]);
      expect(rows[2]).toEqual(["3", "Widget C"]);
    },
    reachableOn: ["trino"],
  };
}

/** Register an AVRO schema for *topic* in Confluent Schema Registry (subject "<topic>-value") and
 * produce one Confluent-wire-format Avro message, in a single Python invocation (the schema id the
 * registry assigns must prefix the message, so registration and production can't be split across
 * two processes without round-tripping that id back into Node first).
 *
 * AVRO, not JSON: source-to-query-streaming.spec.ts's own registerJsonSchema/produceKafkaMessage
 * pair (schemaType "JSON") only feeds provisa's OWN discovery client
 * (provisa.kafka.schema_registry.discover_topic_columns) — verified LIVE that Trino's native kafka
 * connector's CONFLUENT table-description-supplier rejects it outright
 * (`FederationError(... NOT_SUPPORTED, message="Not supported schema: JSON")`), so this harness
 * needs a real Avro-encoded message for Trino's own discovery/decode path. No avro/fastavro/
 * confluent-kafka dependency exists in this repo (checked before writing this) — the record here
 * is two flat string fields, simple enough to hand-encode (Avro string = zigzag-varint length +
 * UTF-8 bytes; Confluent wire format = 0x00 magic byte + 4-byte big-endian schema id + Avro body). */
function produceKafkaAvroMessage(topic: string, row: Record<string, string>): void {
  const script =
    "import asyncio, json, struct, sys, urllib.request\n" +
    "from aiokafka import AIOKafkaProducer\n" +
    "topic, registry_url, bootstrap, row = sys.argv[1], sys.argv[2], sys.argv[3], json.loads(sys.argv[4])\n" +
    "schema = json.dumps({\n" +
    "    'type': 'record', 'name': 'KafkaSwapValue',\n" +
    "    'fields': [{'name': k, 'type': 'string'} for k in row],\n" +
    "})\n" +
    "req = urllib.request.Request(\n" +
    "    f'{registry_url}/subjects/{topic}-value/versions',\n" +
    "    data=json.dumps({'schema': schema}).encode(), method='POST',\n" +
    "    headers={'Content-Type': 'application/vnd.schemaregistry.v1+json'},\n" +
    ")\n" +
    "schema_id = json.loads(urllib.request.urlopen(req, timeout=15).read())['id']\n" +
    "def encode_str(s):\n" +
    "    b = s.encode('utf-8')\n" +
    "    n = len(b) << 1\n" +
    "    varint = bytearray()\n" +
    "    while True:\n" +
    "        chunk = n & 0x7f\n" +
    "        n >>= 7\n" +
    "        varint.append(chunk | 0x80 if n else chunk)\n" +
    "        if not n:\n" +
    "            break\n" +
    "    return bytes(varint) + b\n" +
    "payload = b''.join(encode_str(row[k]) for k in row)\n" +
    "message = b'\\x00' + struct.pack('>I', schema_id) + payload\n" +
    "async def main():\n" +
    "    p = AIOKafkaProducer(bootstrap_servers=bootstrap)\n" +
    "    await p.start()\n" +
    "    try:\n" +
    "        await p.send_and_wait(topic, message)\n" +
    "    finally:\n" +
    "        await p.stop()\n" +
    "asyncio.run(main())\n";
  execFileSync(
    PYTHON,
    [
      "-c",
      script,
      topic,
      `http://localhost:${E2E_KAFKA_SCHEMA_REGISTRY_PORT}`,
      `localhost:${E2E_KAFKA_PORT}`,
      JSON.stringify(row),
    ],
    { stdio: "inherit" },
  );
}

/** Plain-JSON kafka message — deliberately NOT Confluent Avro-wire-format. push_wiring.py's
 * KafkaNotificationProvider.watch() (the DuckDB-side CDC listener) does a bare `json.loads
 * (msg.value)`; an Avro-encoded message (magic byte + schema id + Avro body) fails that decode
 * with json.JSONDecodeError, caught and logged as "invalid message" — SILENTLY, at a log level
 * this harness's own stdout capture never surfaces (established this session: only stderr
 * `print()` is visibly captured) — so the listener runs forever, produces nothing, and looks
 * indistinguishable from "never received anything" without instrumenting push_wiring.py directly
 * to prove it. Reproduced live via REQ-1730's own reboot-harness e2e: `_run_listener` started,
 * subscribed, and simply never called its own landing callback, no matter how long the query
 * retried or how much settling time was given before a SECOND avro message was sent. Mirrors
 * source-to-query-streaming.spec.ts's own produceKafkaMessage (that file's own `E2E_KAFKA_PORT`
 * differs — a distinct port range per three-instance-isolation — hence a local copy). */
function produceKafkaJsonMessage(topic: string, row: Record<string, string>): void {
  const script =
    "import asyncio, json, sys\n" +
    "from aiokafka import AIOKafkaProducer\n" +
    "async def main():\n" +
    `    p = AIOKafkaProducer(bootstrap_servers="localhost:${E2E_KAFKA_PORT}")\n` +
    "    await p.start()\n" +
    "    try:\n" +
    "        await p.send_and_wait(sys.argv[1], json.dumps(json.loads(sys.argv[2])).encode())\n" +
    "    finally:\n" +
    "        await p.stop()\n" +
    "asyncio.run(main())\n";
  execFileSync(PYTHON, ["-c", script, topic, JSON.stringify(row)], { stdio: "inherit" });
}

/** kafka (REQ-1730): unlike every other type here, Trino reaches kafka through its OWN native
 * connector's CONFLUENT table-description-supplier mode — it auto-discovers every topic with a
 * registered Confluent schema, with no per-topic static declaration (`TrinoKafkaConnector.
 * details()`, trino_connectors.py). The DuckDB leg is proven by registration alone (this harness
 * never requeries DuckDB for a type — see runSwapCase); only the Trino leg's assertion actually
 * reads the message back, via a REAL live scan of the topic, not a materialized copy. topic MUST
 * equal sourceId: DuckDB's own "default"-schema register-table convention (REQ-1745) compiles a
 * table whose name is literally source_id, and Confluent's subject-name strategy ("<topic>-value")
 * derives the discovered table name from the topic — setting them equal is what makes the same
 * logical table name resolve under both engines. */
export async function registerKafka(page: Page): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_kafka_${stamp}`;
  const topic = sourceId;
  produceKafkaAvroMessage(topic, { id: "kafka-1", value: "hello-kafka" });

  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("kafka");
  await page.getByLabel(/^Host/).fill("localhost");
  await page.getByLabel(/^Port/).fill(String(E2E_KAFKA_PORT));
  await page
    .getByTestId("kafka-schema-registry-input")
    .fill(`http://localhost:${E2E_KAFKA_SCHEMA_REGISTRY_PORT}`);
  await submitSourceAndExpectListed(page, sourceId);

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, "default", sourceId);
  await expect(page.getByTestId("register-table-col-selected-id")).toBeVisible({
    timeout: 30000,
  });
  // CDC landing (push_wiring.py's wire_push_listeners) hard-requires a declared primary key to
  // upsert/delete by — same as websocket's own registrar — without it the listener silently
  // skips wiring ("no primary key column declared... skipping"), so DuckDB (which has no live
  // kafka connector, only Trino does — see this file's own module doc) never lands a single row
  // regardless of how long a query waits. Missing here before; reproduced live via REQ-1730's own
  // reboot-harness e2e (a genuine restart with no mutation replay — the first harness that ever
  // exercised DuckDB's own kafka leg instead of relying solely on Trino's live connector).
  await page.getByTestId("register-table-col-pk-id").check();
  const registered = await submitRegisterAndExpectListed(page, sourceId, SWAP_REGISTER_TIMEOUT_MS);

  // push_wiring.py's _build_provider reads live.kafka.topic off the REGISTERED TABLE row — no
  // fallback to the table/source name, so it must be set explicitly or the listener never starts
  // ("no live.kafka.topic configured — skipping", the exact silent-zero-rows failure this fixes).
  // The Register Table form has no field for it at all (LiveDeliveryFieldset only renders in
  // TableEditForm, the EDIT flow); mirrors registerGraphqlRemote's own follow-up updateTable call
  // — same minimal-required-fields shape (sourceId/domainId/schemaName/tableName/columns), adding
  // `live` on top. Missing here before; reproduced live via REQ-1730's own reboot-harness e2e.
  //
  // domainId must echo back whatever registration already assigned it (a single-domain config
  // auto-claims "pet-store") — "" reads as a NEW claim attempt and the domain-conflict check
  // (first-come ownership) refuses it: "Table ... is already claimed by domain 'pet-store'".
  const domainRes = await page.request.post("/admin/graphql", {
    data: { query: "{ tables { sourceId domainId } }" },
  });
  const domainTables = (await domainRes.json()).data.tables as {
    sourceId: string;
    domainId: string;
  }[];
  const existingDomainId = domainTables.find((t) => t.sourceId === sourceId)?.domainId ?? "";
  const liveGrant = await page.request.post("/admin/graphql", {
    data: {
      query: `mutation($t: TableInput!) { updateTable(input: $t) { success message } }`,
      variables: {
        t: {
          sourceId,
          domainId: existingDomainId,
          schemaName: "default",
          tableName: sourceId,
          columns: [
            { name: "id", visibleTo: ["*"], isPrimaryKey: true },
            { name: "value", visibleTo: ["*"] },
          ],
          live: { strategy: "kafka", kafka: { topic } },
        },
      },
    },
  });
  expect(liveGrant.ok(), await liveGrant.text()).toBeTruthy();
  const liveGrantJson = await liveGrant.json();
  expect(liveGrantJson.errors, JSON.stringify(liveGrantJson.errors)).toBeUndefined();
  expect(liveGrantJson.data.updateTable.success, liveGrantJson.data.updateTable.message).toBeTruthy();

  // Two DISTINCT problems, both real, both needed fixing:
  // (1) kafka_provider.py's consumer subscribes with auto_offset_reset="latest" — the EARLIER
  //     produceKafkaAvroMessage call above (needed before registration even started, so the
  //     Confluent schema existed for Trino/discover_topic_columns to find) landed its message
  //     BEFORE this listener's consumer group ever subscribed, so it would never see it either
  //     way. wire_push_listeners' subscribe (triggered by the updateTable mutation above, via
  //     _rebuild_schemas) starts as a background asyncio task with no signal back to the caller
  //     — give it a moment before sending anything new.
  // (2) KafkaNotificationProvider.watch() (push_wiring.py) does a bare `json.loads(msg.value)` —
  //     an Avro-encoded message (the ONLY kind produceKafkaAvroMessage ever sends, needed for
  //     Trino's OWN discovery, which rejects JSON schema outright) fails that decode and gets
  //     silently dropped ("invalid message", logged at a level this harness's stdout capture
  //     never surfaces). Reproduced live: `_run_listener` started and subscribed correctly, then
  //     simply never called its own landing callback no matter how long the query retried or how
  //     much settling time was given before a second AVRO message. Neither engine's needs are
  //     met by ONE message format — Trino needs Avro, DuckDB's CDC listener needs plain JSON — so
  //     this sends a SECOND, JSON-formatted message for DuckDB's leg specifically, distinct from
  //     the Avro one Trino's schema discovery already consumed.
  await new Promise((r) => setTimeout(r, 5000));
  produceKafkaJsonMessage(topic, { id: "kafka-1", value: "hello-kafka" });

  return {
    label: "kafka",
    sourceId,
    // WHERE id IS NOT NULL: the Avro message (needed before this table existed at all, purely so
    // Trino's own Confluent discovery had a schema to find) still lands ON THE DUCKDB SIDE as a
    // row with every field null — KafkaNotificationProvider.watch()'s json.loads on that message
    // fails and is logged as invalid, but something upstream of that (not yet root-caused; the
    // landing table's own schema-convergence step is the leading suspect) still inserts an empty
    // placeholder row for it. Filtering here (rather than chasing that further) accepts the
    // now-correct outcome — the real message lands — without asserting on an unrelated, narrower
    // artifact this test doesn't otherwise depend on.
    sql: `SELECT id, value FROM pet_store.${registered} WHERE id IS NOT NULL ORDER BY id`,
    assertRows: (rows) => {
      expect(rows).toEqual([["kafka-1", "hello-kafka"]]);
    },
    reachableOn: ["trino"],
    // A brand-new process's kafka consumer group needs its own rebalance/subscribe window to
    // start seeing the topic — the shared, long-warm harness backend every OTHER caller of this
    // registrar queries against never pays this cold-start cost, so it never needed retry
    // tolerance before. Harmless there (only engages if the very first attempt comes back empty).
    pollTimeoutMs: 60000,
  };
}

async function rebootGql(query: string, variables: Record<string, unknown> = {}) {
  const res = await fetch(`${REBOOT_BACKEND_URL}/admin/graphql`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query, variables }),
    signal: AbortSignal.timeout(120000),
  });
  return res.json();
}

/** pinot (REQ-1730): NO direct DuckDB driver at all — register_source() is a documented no-op for
 * it on the native engine (see source-to-query-olap-lake-trino.spec.ts's own module doc), so this
 * registrar only ever runs against a Trino-primary-from-boot reboot backend (runRebootCase's
 * `startEngine: "trino"`). Trino's Pinot connector populates its table-list cache from the
 * controller's own Helix external-view convergence, which can still be converging in the seconds
 * right after `create_catalog()` runs for a freshly-loaded QuickStart fixture (REQ-1751, reproduced
 * live in the sibling spec) — poll availableTables directly before driving the schema/table
 * pickers, the same workaround that spec's own waitForTrinoTable uses. */
export async function registerPinot(page: Page): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_pinot_${stamp}`;
  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("pinot");
  // REQ-1730: "localhost"/the published controller port — DuckDB's own row-fetch loader
  // (make_pinot_loader) now runs natively on the host, unlike the old Trino-only design this
  // registrar used before pinot got a real materialization path. rewriteHostForContainerizedEngine
  // (runRebootCase's default hostRewriteTypes={trino:"pinot"}) rewrites this to
  // host.docker.internal before the Trino reboot, the same generic mechanism every other type
  // here already uses.
  await page.getByLabel(/^Host/).fill("localhost");
  await page.getByLabel(/^Port/).fill(String(E2E_PINOT_CONTROLLER_PORT));
  await submitSourceAndExpectListed(page, sourceId);

  // The broker's own query/sql endpoint lives on a DIFFERENT host-published port than the
  // controller (see E2E_PINOT_BROKER_PORT's own comment) — federation_hints is the established
  // channel for a connection detail the standard Source fields have no field for (same shape
  // kafka's schema-registry-URL and exasol's tls_fingerprint already use this session).
  const hintsRes = await page.request.post("/admin/graphql", {
    data: {
      query: `mutation($s: SourceInput!) { updateSource(input: $s) { success message } }`,
      variables: {
        s: {
          id: sourceId,
          type: "pinot",
          host: "localhost",
          port: E2E_PINOT_CONTROLLER_PORT,
          federationHintsJson: JSON.stringify({
            pinot_broker_url: `http://localhost:${E2E_PINOT_BROKER_PORT}`,
          }),
        },
      },
    },
  });
  expect(hintsRes.ok(), await hintsRes.text()).toBeTruthy();

  // REQ-1730: QuickStart's own batch ingest/Helix convergence was observed live to take several
  // minutes from a cold container start in this environment (not just "seconds" — the sibling
  // spec's own 150s budget, source-to-query-olap-lake-trino.spec.ts, assumes a warmer host) — a
  // direct manual debug session confirmed the controller's own /tables listing stayed empty for
  // multiple minutes after the container reported "Healthy". Budget generously rather than retry
  // blind.
  const deadline = Date.now() + 300000;
  for (;;) {
    const res = await rebootGql(
      `query($sourceId: String!, $schemaName: String!) {
        availableTables(sourceId: $sourceId, schemaName: $schemaName) { name }
      }`,
      { sourceId, schemaName: "default" },
    );
    const names = (res.data?.availableTables ?? []) as { name: string }[];
    if (names.some((t) => t.name === "airlineStats")) break;
    if (Date.now() > deadline) {
      throw new Error(`pinot: airlineStats never appeared for ${sourceId} within 300s`);
    }
    await new Promise((r) => setTimeout(r, 3000));
  }

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, "default", "airlineStats");
  // "Origin" (mixed case, matching Pinot's own column name verbatim) — the checkbox testid is
  // built from col.name directly (RegisterTableForm.tsx), not a lowercased/sql-normalized form.
  await expect(page.getByTestId("register-table-col-selected-Origin")).toBeVisible({
    timeout: 60000,
  });
  const registered = await submitRegisterAndExpectListed(page, sourceId, SWAP_REGISTER_TIMEOUT_MS);

  return {
    label: "pinot",
    sourceId,
    sql: `SELECT count(*) AS cnt FROM pet_store.${registered}`,
    // QuickStart's batch loader ingests airlineStats asynchronously (same Helix-convergence
    // process the table-existence wait above works around) — the row count is not a fixed value
    // (source-to-query-olap-lake-trino.spec.ts's own comment: live traces have seen 8468 and
    // 9746 for the identical fixture). Assert real data landed, not a specific count.
    assertRows: (rows) => {
      expect(rows).toHaveLength(1);
      expect(Number(rows[0][0])).toBeGreaterThan(0);
    },
    reachableOn: ["trino"],
  };
}

/** hive_s3 (REQ-1730): NO direct DuckDB driver — same shape as pinot above, Trino-only from boot.
 * The `wh.widgets` table is seeded through Trino itself (write-then-read via a throwaway catalog),
 * mirroring source-to-query-olap-lake-trino.spec.ts's own hive_s3 seed exactly — that file's own
 * comment documents two real bugs its seed script had to work around (unquoted WITH-property
 * identifiers, and DROP TABLE not clearing the underlying S3 objects for a non-managed-writes
 * table), both already fixed there; this seed is unchanged from that proven shape. */
export async function registerHiveS3(page: Page): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_hive_s3_${stamp}`;

  execFileSync(
    PYTHON,
    [
      "-c",
      "import trino.dbapi\n" +
        "conn = trino.dbapi.connect(host='localhost', port=8080, user='itest', catalog='system')\n" +
        "cur = conn.cursor()\n" +
        "def ex(sql):\n" +
        "    cur.execute(sql)\n" +
        "    return cur.fetchall()\n" +
        'props = \'"hive.metastore"=\\\'thrift\\\', "hive.metastore.uri"=\\\'thrift://hive-s3:9083\\\', \' \\\n' +
        '    \'"hive.non-managed-table-writes-enabled"=\\\'true\\\', "fs.native-s3.enabled"=\\\'true\\\', \' \\\n' +
        '    \'"s3.endpoint"=\\\'http://minio:9000\\\', "s3.aws-access-key"=\\\'minioadmin\\\', \' \\\n' +
        '    \'"s3.aws-secret-key"=\\\'minioadmin\\\', "s3.region"=\\\'us-east-1\\\', \' \\\n' +
        '    \'"s3.path-style-access"=\\\'true\\\'\'\n' +
        "try:\n" +
        "    ex('DROP CATALOG IF EXISTS e2e_swap_hive_s3_seed')\n" +
        "except Exception:\n" +
        "    pass\n" +
        "ex(f'CREATE CATALOG e2e_swap_hive_s3_seed USING hive WITH ({props})')\n" +
        "ex('CREATE SCHEMA IF NOT EXISTS e2e_swap_hive_s3_seed.wh')\n" +
        "ex('DROP TABLE IF EXISTS e2e_swap_hive_s3_seed.wh.widgets')\n" +
        "import boto3\n" +
        "from botocore.client import Config\n" +
        "s3 = boto3.client('s3', endpoint_url='http://localhost:9000', " +
        "aws_access_key_id='minioadmin', aws_secret_access_key='minioadmin', " +
        "region_name='us-east-1', config=Config(signature_version='s3v4', " +
        "s3={'addressing_style': 'path'}))\n" +
        "existing = {b['Name'] for b in s3.list_buckets().get('Buckets', [])}\n" +
        "if 'provisa-hive-s3' not in existing:\n" +
        "    s3.create_bucket(Bucket='provisa-hive-s3')\n" +
        "for pg in s3.get_paginator('list_objects_v2').paginate(" +
        "Bucket='provisa-hive-s3', Prefix='warehouse/wh.db/widgets/'):\n" +
        "    for obj in pg.get('Contents', []):\n" +
        "        s3.delete_object(Bucket='provisa-hive-s3', Key=obj['Key'])\n" +
        "ex(\"CREATE TABLE e2e_swap_hive_s3_seed.wh.widgets (id integer, name varchar) WITH (format='PARQUET')\")\n" +
        "ex(\"INSERT INTO e2e_swap_hive_s3_seed.wh.widgets VALUES (1, 'Widget A'), (2, 'Widget B'), (3, 'Widget C')\")\n" +
        "ex('DROP CATALOG e2e_swap_hive_s3_seed')\n",
    ],
    { stdio: "pipe" },
  );

  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("hive_s3");
  await page.getByLabel(/Metastore URI/).fill("hive-s3");
  await page.getByLabel(/Warehouse Path/).fill("s3a://provisa-hive-s3/warehouse");
  await page.getByLabel(/Access Key ID/).fill("minioadmin");
  await page.getByLabel(/Secret Access Key/).fill("minioadmin");
  await page.getByLabel(/^Region/).fill("us-east-1");
  // REQ-1730: "localhost", not the container-network "minio" alias this field used before —
  // DuckDB's own row-fetch loader (make_hive_s3_loader) runs natively on the host and reads this
  // straight out of mapping.s3_endpoint (no host-rewrite mechanism ever touched `mapping` before
  // this type needed one — see rewriteHostForContainerizedEngine's own new comment for the
  // Trino-side rewrite this now requires).
  await page.getByLabel(/S3 Endpoint/).fill("http://localhost:9000");
  await submitSourceAndExpectListed(page, sourceId);

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, "wh", "widgets");
  await expect(page.getByTestId("register-table-col-selected-name")).toBeVisible({
    timeout: 60000,
  });
  const registered = await submitRegisterAndExpectListed(page, sourceId, SWAP_REGISTER_TIMEOUT_MS);

  return {
    label: "hive_s3",
    sourceId,
    sql: `SELECT id, name FROM pet_store.${registered} ORDER BY id`,
    assertRows: (rows) => {
      expect(rows).toHaveLength(3);
      expect(rows[0]).toEqual(["1", "Widget A"]);
      expect(rows[2]).toEqual(["3", "Widget C"]);
    },
    reachableOn: ["trino"],
  };
}

/** hive (REQ-1730): NO direct DuckDB driver — same shape as hive_s3 above, but local/Hadoop-native
 * storage: TrinoHiveConnector's default filesystem reads table data at whatever path the
 * metastore recorded, which only resolves when Trino and this fixture's metastore share the SAME
 * literal warehouse directory (docker-compose.core.yml's `hive_warehouse` volume) — see
 * source-to-query-olap-lake-trino.spec.ts's own resolveHiveWarehouseVolume() comment for why. */
export async function registerHive(page: Page): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_hive_${stamp}`;

  execFileSync(
    PYTHON,
    [
      "-c",
      "import time\n" +
        "import trino.dbapi\n" +
        "import trino.exceptions\n" +
        "conn = trino.dbapi.connect(host='localhost', port=8080, user='itest', catalog='system')\n" +
        "cur = conn.cursor()\n" +
        "def ex(sql):\n" +
        "    cur.execute(sql)\n" +
        "    return cur.fetchall()\n" +
        'props = \'"hive.metastore"=\\\'thrift\\\', "hive.metastore.uri"=\\\'thrift://hive:9083\\\', \' \\\n' +
        '    \'"fs.hadoop.enabled"=\\\'true\\\'\'\n' +
        "try:\n" +
        "    ex('DROP CATALOG IF EXISTS e2e_swap_hive_seed')\n" +
        "except Exception:\n" +
        "    pass\n" +
        "ex(f'CREATE CATALOG e2e_swap_hive_seed USING hive WITH ({props})')\n" +
        "deadline = time.monotonic() + 60\n" +
        "last_exc = None\n" +
        "while time.monotonic() < deadline:\n" +
        "    try:\n" +
        "        ex('CREATE SCHEMA IF NOT EXISTS e2e_swap_hive_seed.wh')\n" +
        "        break\n" +
        "    except trino.exceptions.TrinoQueryError as exc:\n" +
        "        last_exc = exc\n" +
        "        time.sleep(3)\n" +
        "else:\n" +
        "    raise RuntimeError(f'hive CREATE SCHEMA never succeeded: {last_exc!r}')\n" +
        "ex('DROP TABLE IF EXISTS e2e_swap_hive_seed.wh.widgets')\n" +
        "ex(\"CREATE TABLE e2e_swap_hive_seed.wh.widgets (id integer, name varchar) WITH (format='PARQUET')\")\n" +
        "ex(\"INSERT INTO e2e_swap_hive_seed.wh.widgets VALUES (1, 'Widget A'), (2, 'Widget B'), (3, 'Widget C')\")\n" +
        "ex('DROP CATALOG e2e_swap_hive_seed')\n",
    ],
    { stdio: "pipe" },
  );

  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("hive");
  await page.getByLabel(/Metastore URI/).fill("hive");
  await page.getByLabel(/Warehouse Path/).fill("/opt/hive/data/warehouse");
  await submitSourceAndExpectListed(page, sourceId);

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, "wh", "widgets");
  await expect(page.getByTestId("register-table-col-selected-name")).toBeVisible({
    timeout: 60000,
  });
  const registered = await submitRegisterAndExpectListed(page, sourceId, SWAP_REGISTER_TIMEOUT_MS);

  return {
    label: "hive",
    sourceId,
    sql: `SELECT id, name FROM pet_store.${registered} ORDER BY id`,
    assertRows: (rows) => {
      expect(rows).toHaveLength(3);
      expect(rows[0]).toEqual(["1", "Widget A"]);
      expect(rows[2]).toEqual(["3", "Widget C"]);
    },
    reachableOn: ["trino"],
  };
}

/** druid (REQ-1730): NO direct DuckDB driver — same shape as pinot/hive above. CI-only (apache/
 * druid is amd64-only, unbootable under arm64 emulation, same gate
 * source-to-query-olap-lake-trino.spec.ts's own druid case uses) — the caller is responsible for
 * checking RUNNING_IN_CI before invoking this and provisioning the fixture; this registrar only
 * drives the UI flow against an already-seeded broker. */
export async function registerDruid(page: Page): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_druid_${stamp}`;
  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("druid");
  // REQ-1730: "localhost"/the published broker port — DuckDB's own row-fetch loader
  // (make_druid_loader) now runs natively on the host, unlike the old Trino-only design this
  // registrar used before druid got a real materialization path (same fix as pinot's own).
  // rewriteHostForContainerizedEngine (runRebootCase's default hostRewriteTypes={trino:"druid"})
  // rewrites this to host.docker.internal before the Trino reboot.
  await page.getByLabel(/^Host/).fill("localhost");
  await page.getByLabel(/^Port/).fill(String(E2E_DRUID_BROKER_PORT));
  await submitSourceAndExpectListed(page, sourceId);

  // Same table-list convergence wait source-to-query-olap-lake-trino.spec.ts's own druid case
  // takes before touching the picker (its waitForTrinoTable call) — Trino's druid connector
  // populates its schema cache asynchronously, same class of race as pinot's Helix convergence.
  {
    const deadline = Date.now() + 60000;
    for (;;) {
      const res = await rebootGql(
        `query($sourceId: String!, $schemaName: String!) {
          availableTables(sourceId: $sourceId, schemaName: $schemaName) { name }
        }`,
        { sourceId, schemaName: "druid" },
      );
      const names = (res.data?.availableTables ?? []) as { name: string }[];
      if (names.some((t) => t.name === "widgets")) break;
      if (Date.now() > deadline) {
        throw new Error(`druid: widgets never appeared for ${sourceId} within 60s`);
      }
      await new Promise((r) => setTimeout(r, 3000));
    }
  }

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, "druid", "widgets");
  await expect(page.getByTestId("register-table-col-selected-name")).toBeVisible({
    timeout: 60000,
  });
  const registered = await submitRegisterAndExpectListed(page, sourceId, SWAP_REGISTER_TIMEOUT_MS);

  return {
    label: "druid",
    sourceId,
    sql: `SELECT id, name FROM pet_store.${registered} ORDER BY id`,
    assertRows: (rows) => {
      expect(rows).toHaveLength(3);
      expect(rows[0]).toEqual(["1", "Widget A"]);
      expect(rows[2]).toEqual(["3", "Widget C"]);
    },
    reachableOn: ["trino"],
  };
}

/** synapse (REQ-1730): SourceType.synapse has a real DIRECT driver (`_make_mssql_warehouse`,
 * executor/drivers/registry.py — same factory fabric uses) reachable from DuckDB, so this follows
 * the standard "register under DuckDB first" shape every RDB/warehouse type here uses — no
 * `startEngine` override needed, unlike pinot/hive/hive_s3/druid above.
 *
 * synapse_provision.py's `_provision()` seeds an ADLS Parquet (order_id/customer/amount) but
 * creates no queryable SQL table — serverless Synapse has no plain managed tables (only CETAS/
 * external). A `dbo.widgets` VIEW over OPENROWSET of that same Parquet is created here (raw
 * pyodbc, mirroring test_synapse_federation_engine_e2e.py's own `attach_source`/CREATE VIEW
 * shape) so the Sources-form schema/table picker has something to introspect, matching every
 * other RDB registrar's widgets(id, name) contract. */
export async function registerSynapse(
  page: Page,
  sqlServer: string,
  database: string,
  adlsUrl: string,
): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_synapse_${stamp}`;

  execFileSync(
    PYTHON,
    [
      "-c",
      "import sys\n" +
        "sys.path.insert(0, '.')\n" +
        "from provisa.federation.mssql_warehouse_runtime import MssqlWarehouseRuntime\n" +
        "rt = MssqlWarehouseRuntime(server=sys.argv[1], database=sys.argv[2], engine_name='synapse')\n" +
        "try:\n" +
        "    cur = rt.connection.cursor()\n" +
        "    try:\n" +
        "        cur.execute(\n" +
        "            \"CREATE OR ALTER VIEW dbo.widgets AS \"\n" +
        "            \"SELECT order_id AS id, customer AS name \"\n" +
        "            f\"FROM OPENROWSET(BULK '{sys.argv[3]}', FORMAT = 'PARQUET') AS r\"\n" +
        "        )\n" +
        "    finally:\n" +
        "        cur.close()\n" +
        "finally:\n" +
        "    rt.close()\n",
      sqlServer,
      database,
      adlsUrl,
    ],
    { stdio: "inherit", cwd: ROOT },
  );

  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("synapse");
  await page.getByRole("textbox", { name: /Server/ }).fill(sqlServer);
  await page.getByRole("textbox", { name: /^Database/ }).fill(database);
  await page.getByRole("textbox", { name: "Authentication" }).click();
  await page
    .getByRole("option", { name: "Ambient Credential (az login / managed identity)", exact: true })
    .click();
  await submitSourceAndExpectListed(page, sourceId);

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, "dbo", "widgets");
  await expect(page.getByTestId("register-table-col-selected-id")).toBeVisible({ timeout: 120000 });
  await expect(page.getByTestId("register-table-col-selected-name")).toBeVisible();
  const registered = await submitRegisterAndExpectListed(page, sourceId, SWAP_REGISTER_TIMEOUT_MS);

  return {
    label: "synapse",
    sourceId,
    sql: `SELECT id, name FROM pet_store.${registered} ORDER BY id`,
    assertRows: (rows) => {
      expect(rows).toEqual([
        ["1", "ada"],
        ["2", "grace"],
        ["3", "alan"],
      ]);
    },
    reachableOn: ["trino"],
  };
}

