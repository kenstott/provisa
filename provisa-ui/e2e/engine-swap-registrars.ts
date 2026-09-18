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
  E2E_KAFKA_SCHEMA_REGISTRY_PORT,
  E2E_RSS_PORT,
  E2E_WS_PORT,
  E2E_SINGLESTORE_PORT,
  FILE_LAKE_HOST_DIR,
  PYTHON,
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
    reachableOn: [], // REQ-1726: no Trino connector or FDW path for sqlite
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

