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

import fs from "node:fs";
import path from "node:path";

import { expect } from "./coverage";
import type { Page } from "./coverage";
import {
  E2E_MONGO_PORT,
  E2E_NEO4J_HTTP_PORT,
} from "./demo-source-containers";
import {
  expectDiscoveredColumns,
  openRegisterForm,
  openSourcesForm,
  pickSchemaAndTable,
  submitRegisterAndExpectListed,
  submitSourceAndExpectListed,
  FORM_FIELD,
  setSourceCacheTtl,
} from "./source-to-query-helpers";
import {
  FILE_LAKE_HOST_DIR,
  RDB_WIDGETS_PORTS,
  ROOT,
  SWAP_REGISTER_TIMEOUT_MS,
} from "./engine-swap-helpers";
import type { RedshiftConnection } from "./engine-swap-helpers";
import type { Registration } from "./engine-swap-helpers";

/** Every registrar's submit. A source registered here is read by engines that do not attach it,
 * so its tables LAND, and a landed table whose change signal is the TTL needs a landing clock
 * (REQ-1907): without one the table registration is refused -- "change_signal 'ttl' is TTL-based
 * and the table lands, so add a cache_ttl to the table or its source" -- and the row the
 * registrar waits for never appears (swap lane, exasol and saphana, run 37918608303). Set here,
 * once, for every source type, not registrar by registrar. */
export async function submitSwapSource(page: Page, sourceId: string): Promise<void> {
  await setSourceCacheTtl(page, 300);
  await submitSourceAndExpectListed(page, sourceId);
}

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
  await submitSwapSource(page, sourceId);

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
  await page.getByLabel(/^Host/).and(page.locator(FORM_FIELD)).fill(host);
  await page.getByLabel(/^Port/).and(page.locator(FORM_FIELD)).fill(String(E2E_MONGO_PORT));
  await page.getByLabel(/^Database/).and(page.locator(FORM_FIELD)).fill("provisa");
  await submitSwapSource(page, sourceId);

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, "provisa", tableName);
  await expectDiscoveredColumns(page, ["reviewer"], 60000);
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
    await page.getByLabel(/^Host/).and(page.locator(FORM_FIELD)).fill("localhost");
    await page.getByLabel(/^Port/).and(page.locator(FORM_FIELD)).fill(String(cfg.port));
    await page.getByLabel(/^Username/).fill(cfg.username);
    await page.getByLabel(/^Password/).fill(cfg.password);
    await page.getByLabel(/^Database/).and(page.locator(FORM_FIELD)).fill(cfg.database);
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
    await submitSwapSource(page, sourceId);

    await openRegisterForm(page, sourceId);
    await pickSchemaAndTable(page, cfg.schema, cfg.table ?? "widgets");
    await expectDiscoveredColumns(page, [cfg.nameColumn ?? "name"], 60000);
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
    await submitSwapSource(page, sourceId);

    await openRegisterForm(page, sourceId);
    await pickSchemaAndTable(page, "main", sourceId);
    await expectDiscoveredColumns(page, ["name"], 60000);
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
    await submitSwapSource(page, sourceId);

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
    await page.getByTestId("files-path-input").fill(`${runDir}/**`);
    await submitSwapSource(page, sourceId);

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
    await submitSwapSource(page, sourceId);

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
    // Cache TTL is on the table's "Load and timeliness" section.
    await page.getByTestId("table-section-load").click();
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
  await submitSwapSource(page, sourceId);

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
  await page.getByLabel(/^Database/).and(page.locator(FORM_FIELD)).fill("PROVISA_UI_E2E");
  await page.getByLabel(/^Warehouse$/).fill(process.env.SNOWFLAKE_WAREHOUSE ?? "COMPUTE_WH");
  await page.getByRole("textbox", { name: "Authentication" }).click();
  await page.getByRole("option", { name: "Username / Password", exact: true }).click();
  await page.getByLabel(/^Username/).fill(process.env.SNOWFLAKE_USER!);
  await page.getByLabel(/^Password/).fill(process.env.SNOWFLAKE_PASSWORD!);
  await submitSwapSource(page, sourceId);

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, "PUBLIC", "WIDGETS");
  await expectDiscoveredColumns(page, ["ID"], 120000);
  await expectDiscoveredColumns(page, ["NAME"]);
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
  await submitSwapSource(page, sourceId);

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, "provisa_ui_e2e", "widgets");
  await expectDiscoveredColumns(page, ["id"], 120000);
  await expectDiscoveredColumns(page, ["name"]);
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
  await submitSwapSource(page, sourceId);

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, "provisa_ui_e2e", "widgets");
  await expectDiscoveredColumns(page, ["id"], 120000);
  await expectDiscoveredColumns(page, ["name"]);
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
  await submitSwapSource(page, sourceId);

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, "provisa_ui_e2e", "widgets");
  await expectDiscoveredColumns(page, ["id"], 120000);
  await expectDiscoveredColumns(page, ["name"]);
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
    await page.getByLabel(/^Host/).and(page.locator(FORM_FIELD)).fill(conn.host);
    await page.getByLabel(/^Port/).and(page.locator(FORM_FIELD)).fill(String(conn.port));
    await page.getByLabel(/^Database/).and(page.locator(FORM_FIELD)).fill(conn.database);
    await page.getByLabel(/^Username/).fill(conn.user);
    await page.getByLabel(/^Password/).fill(conn.password);
    await submitSwapSource(page, sourceId);

    await openRegisterForm(page, sourceId);
    await pickSchemaAndTable(page, "public", "provisa_widgets_e2e");
    await expectDiscoveredColumns(page, ["id"], 120000);
    await expectDiscoveredColumns(page, ["name"]);
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
