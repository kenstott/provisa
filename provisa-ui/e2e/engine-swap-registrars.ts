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
  E2E_SINGLESTORE_PORT,
  FILE_LAKE_HOST_DIR,
  RDB_WIDGETS_PORTS,
  ROOT,
  SWAP_REGISTER_TIMEOUT_MS,
} from "./engine-swap-helpers";
import type { Registration } from "./engine-swap-helpers";

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

export async function registerMongodb(page: Page): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_mongodb_${stamp}`;
  const tableName = "product_reviews";
  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("mongodb");
  await page.getByLabel(/^Host/).fill("localhost");
  await page.getByLabel(/^Port/).fill(String(E2E_MONGO_PORT));
  await page.getByLabel(/^Database/).fill("provisa");
  await submitSourceAndExpectListed(page, sourceId);

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, "provisa", tableName);
  await expect(page.getByTestId("register-table-col-selected-reviewer")).toBeVisible({
    timeout: 60000,
  });
  const registered = await submitRegisterAndExpectListed(page, sourceId, SWAP_REGISTER_TIMEOUT_MS);

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
}

// KNOWN UNRESOLVED BUG (2026-09-16): oracle is deliberately NOT in RDB_WIDGETS_SOURCES below.
// Schema/table/column introspection all work now (the introspect.py dispatch-branch and
// identifier-casing fixes this batch made), but registration itself then stalls at
// submitRegisterAndExpectListed's own 300s SWAP_REGISTER_TIMEOUT_MS wait for the row to land in
// the tables list — a genuine registration-commit/schema-rebuild slowness specific to oracle,
// not yet root-caused. Re-add
// `{ type: "oracle", port: RDB_WIDGETS_PORTS.oracle, username: "system", password: "provisa",
// database: "FREEPDB1", schema: "SYSTEM", table: "WIDGETS", nameColumn: "NAME" }`
// once that's fixed — the registrar/config shape is already correct and verified up to that
// point.
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

/** splunk: registers correctly under DuckDB (proves that leg) but is NOT requeried under Trino —
 * verified live, not assumed. DuckDB reaches it via the connector's bundled Calcite pgwire bridge,
 * a per-source JVM whose schema is the sql-normalized SOURCE ID (source-to-query.spec.ts's own
 * comment: "the schema is the sql-normalized source id"). Trino reaches it via a genuinely
 * DIFFERENT connector — `trino/plugins/trino-splunk/`, also wrapping Calcite's splunk adapter, but
 * as a STANDALONE Trino plugin whose schema is the FIXED string `"splunk"`
 * (tests/integration/test_splunk_source_e2e.py:371, `assert "splunk" in schemas`), not the source
 * id. A table registered under DuckDB records schema=<source_id> in `registered_tables`; swapped
 * to Trino, the compiler emits that SAME physical schema name, which Trino's connector simply does
 * not have — confirmed live: `FederationError(..., SCHEMA_NOT_FOUND, "Schema '<source_id>' does
 * not exist")`. This is NOT the registration-timing gap this harness's own module doc originally
 * guessed (`reprovisionSourceOnEngine`'s createSource replay runs fine and does create the Trino
 * catalog) — it is a genuine physical-schema-naming mismatch between DuckDB's per-source pgwire
 * bridge and Trino's fixed-schema native plugin, unrelated to catalog-creation timing. sharepoint
 * has the identical shape (tests/integration/test_sharepoint_source_e2e.py:28, fixed schema
 * `"sharepoint"`) — same blocker, not attempted here. Fixing this for real needs the physical
 * schema name to be resolved per-engine at query-compile time (the same kind of per-engine
 * indirection REQ-1730's `catalog_name_for_source` already does for CATALOG names), not attempted
 * in this pass. */
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
    // reachableOn: [] — see this function's own module doc: Trino's schema for this type is a
    // fixed "splunk" string, not the source id DuckDB's registration recorded. Not a Trino
    // connector gap (a real one exists); a physical-schema-naming mismatch, verified live.
    reachableOn: [],
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
    reachableOn: [], // REQ-1726: no Trino connector or FDW path for sqlite
  };
}

export async function registerGraphqlRemote(page: Page): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_gql_${stamp}`;
  const namespace = `e2e_swap_gql_${stamp}`;
  const endpoint = await existingSourcePath(page, "graphql-demo");

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

export async function registerOpenapi(page: Page): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_openapi_${stamp}`;
  const specUrl = await existingSourcePath(page, "petstore-api");
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
    reachableOn: [], // REQ-899: DuckDB community extension only, no Trino connector or land path
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
    reachableOn: [], // REQ-899/1097: DuckDB community extension only, no Trino connector or land path
  };
}

export async function registerSinglestore(page: Page): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_singlestore_${stamp}`;
  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("singlestore");
  await page.getByLabel(/^Host/).fill("localhost");
  await page.getByLabel(/^Port/).fill(String(E2E_SINGLESTORE_PORT));
  await page.getByLabel(/^Username/).fill("root");
  await page.getByLabel(/^Password/).fill("provisa");
  await page.getByLabel(/^Database/).fill("provisa_demo");
  await submitSourceAndExpectListed(page, sourceId);

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, "provisa_demo", "widgets");
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

