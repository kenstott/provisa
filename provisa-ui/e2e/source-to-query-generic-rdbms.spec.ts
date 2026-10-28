// Copyright (c) 2026 Kenneth Stott
// Canary: 1a2b3c4d-5e6f-4a7b-8c9d-0e1f2a3b4c5d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1671: source to query through the UI for eight SIMPLE_RDBMS-shaped SourceTypes that had no
// e2e coverage before this file: mariadb, sqlserver, oracle, cockroachdb, yugabytedb, greenplum,
// tidb, clickhouse. Same three-screen shape as source-to-query.spec.ts's mysql case (Sources form
// -> Register Table form -> SQL page), each against its own demo/sources/<name> container started
// only for this describe block (provisionExtraSources's pattern in source-to-query.spec.ts).
//
// REQ-1768: postgresql (the most-used SourceType) added to this file's SOURCES/tests — its own
// registration flow had zero e2e coverage; every other test only exercised it indirectly via the
// pre-seeded pet_store baseline fixture.
//
// Port range: 358xx-359xx — distinct from source-to-query.spec.ts (33062/33081),
// engine-swap.spec.ts (33051/33061/33071) and demo-source-containers.ts's shared range
// (33xxx/35433/36xxx/37xxx/38xxx/39xxx for the chinook/splunk/etc containers this file does not
// touch).

import { execFileSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { test, expect } from "./coverage";
import {
  expectDiscoveredColumns,
  openRegisterForm,
  openSourcesForm,
  pickSchemaAndTable,
  runSqlOnPage,
  submitRegisterAndExpectListed,
  submitSourceAndExpectListed,
  FORM_FIELD,
  removeTestSources,
} from "./source-to-query-helpers";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const PROVISION = path.join(ROOT, "demo", "sources", "provision.py");
const PYTHON = path.join(ROOT, ".venv", "bin", "python");
const PREFIX = "provisa-s2q-rdbms";

const E2E_MARIADB_PORT = 35810;
const E2E_TIDB_PORT = 35820;
const E2E_COCKROACHDB_PORT = 35830;
const E2E_YUGABYTEDB_PORT = 35840;
const E2E_CLICKHOUSE_PORT = 35850;
const E2E_SQLSERVER_PORT = 35860;
const E2E_ORACLE_PORT = 35870;
const E2E_GREENPLUM_PORT = 35880;
const E2E_POSTGRESQL_PORT = 35890;
// saphana/greenplum only actually run in CI (ubuntu-latest is a real amd64 Linux host) — both are
// verified live against a real amd64 Docker host (saphana: a temporary Vultr instance, this
// session; greenplum: amd64-only image, never runnable under this repo's arm64 local dev). Locally
// (Docker Desktop's Apple Silicon VM) both still skip: saphana's hdbindexserver never starts there
// (see compose.yml's module comment) and greenplum's from-scratch cluster boot under QEMU
// emulation realistically exceeds a reasonable single e2e budget. process.env.CI is set to "true"
// by ui-e2e-core.yml specifically (see that workflow's own env block).
const RUNNING_IN_CI = process.env.CI === "true";
const E2E_SAPHANA_HOST = process.env.PROVISA_DEMO_SAPHANA_HOST ?? "localhost";
const E2E_SAPHANA_PORT = Number(process.env.PROVISA_DEMO_SAPHANA_PORT ?? 39041);
const E2E_SAPHANA_PASSWORD = process.env.PROVISA_DEMO_SAPHANA_PASSWORD ?? "HXEHana1";

// pyodbc/unixODBC + "ODBC Driver 18 for SQL Server" must be present on the HOST running this
// Playwright process's backend (provisa/executor/drivers/sqlserver.py links libodbc at import) —
// a host dependency this file cannot install. Detected once at module load so a host missing it
// skips cleanly instead of failing every assertion after a broken registration.
function hasSqlServerOdbcDriver(): boolean {
  try {
    execFileSync(PYTHON, ["-c", "import pyodbc; pyodbc.drivers()"], { stdio: "pipe" });
    return true;
  } catch {
    return false;
  }
}
const SQLSERVER_ODBC_AVAILABLE = hasSqlServerOdbcDriver();

// One source's containers at a time. This file used to start all ten in a single hook before its
// first test and keep them up for the whole file; on the 16 GB runner that left under 200 MB free
// from the moment SAP HANA came up (run 38050265191: hdbnameserver, hdbindexserver, sqlservr,
// Oracle, a 4 GB Fuseki and the rest together, load average past 100), and every page load after
// it timed out. A test now starts the one source it reads and the hook below removes it when the
// test ends, passed or failed.
//
// How long each may take to pull, start and prime before its test's own budget begins.
// saphana: HXE's tenant database commonly takes 5-15 minutes past container-healthy before
// prime.py's connect-retry loop (900 s) succeeds. oracle: gvenzl/oracle-free creates its database
// on first boot. greenplum: a from-scratch cluster.
const PROVISION_BUDGET_MS: Record<string, number> = {
  mariadb: 300000,
  tidb: 300000,
  cockroachdb: 300000,
  yugabytedb: 420000,
  clickhouse: 300000,
  sqlserver: 420000,
  oracle: 900000,
  postgresql: 300000,
  greenplum: 900000,
  saphana: 1500000,
};

function provision(cmd: "up" | "down", source: string): void {
  const env = {
    ...process.env,
    PROVISA_DEMO_MARIADB_PORT: String(E2E_MARIADB_PORT),
    PROVISA_DEMO_TIDB_PORT: String(E2E_TIDB_PORT),
    PROVISA_DEMO_COCKROACHDB_PORT: String(E2E_COCKROACHDB_PORT),
    // CockroachDB's defaults take a quarter of the host's memory for its cache and another for SQL;
    // the runner was out of memory during this source's test (run 40115205350).
    PROVISA_DEMO_COCKROACHDB_CACHE: "256MiB",
    PROVISA_DEMO_COCKROACHDB_SQL_MEMORY: "256MiB",
    PROVISA_DEMO_YUGABYTEDB_PORT: String(E2E_YUGABYTEDB_PORT),
    PROVISA_DEMO_CLICKHOUSE_PORT: String(E2E_CLICKHOUSE_PORT),
    PROVISA_DEMO_SQLSERVER_PORT: String(E2E_SQLSERVER_PORT),
    PROVISA_DEMO_ORACLE_PORT: String(E2E_ORACLE_PORT),
    PROVISA_DEMO_GREENPLUM_PORT: String(E2E_GREENPLUM_PORT),
    PROVISA_DEMO_POSTGRESQL_PORT: String(E2E_POSTGRESQL_PORT),
    PROVISA_DEMO_SAPHANA_PORT: String(E2E_SAPHANA_PORT),
    PROVISA_DEMO_SAPHANA_PASSWORD: E2E_SAPHANA_PASSWORD,
  };
  execFileSync(PYTHON, [PROVISION, cmd, "--prefix", PREFIX, source], { stdio: "inherit", env });
}

// The sources this test started, removed by afterEach. The provisioning project runs one worker
// (playwright.config.ts), so one list serves the file.
const started: string[] = [];

/** Start `source` for the running test and give the test `testBudgetMs` on top of the time the
 * source may take to come up. Call it after any `test.skip`, so a skipped test starts nothing. */
function useSource(source: string, testBudgetMs: number): void {
  const budget = PROVISION_BUDGET_MS[source];
  if (budget === undefined) throw new Error(`no provisioning budget for source '${source}'`);
  test.setTimeout(testBudgetMs + budget);
  started.push(source);
  provision("up", source);
}

test.describe("source to query through the UI: generic RDBMS types (REQ-1671)", () => {
  test.afterEach(async () => {
    await removeTestSources();
    // Every source this test started comes down, also after a failure; a `down` that fails is
    // a container left holding the runner's memory, so it fails the test loudly.
    for (const source of started.splice(0)) provision("down", source);
  });

  test("mariadb: add the source, register a table, query it on the SQL page", async ({ page }) => {
    // REQ-1757: ROOT CAUSE FOUND AND FIXED. The SQL page's query against pet_store.<table> was
    // failing with a genuine MariaDB syntax error ("...near '"provisa_demo"."widgets" AS
    // "widgets" ORDER BY id LIMIT 100'") because schema_mutation.py's create_source/update_source
    // mutations hardcoded `state.source_dialects[input.id] = ""` for every dynamically-registered
    // source, regardless of type. pgwire/_pipeline.py's DIRECT-route compile step then did
    // `dialect = decision.dialect or "postgres"` — `""` is falsy, so it silently fell back to
    // postgres dialect (ANSI double-quoted identifiers) for ANY UI-registered source, not just
    // mariadb/tidb. Fixed by reading the real dialect (core/models.py's Source.dialect, backed by
    // core/source_registry.py's SOURCE_TO_DIALECT — the same map app_loaders.py's config-load path
    // already used) instead of hardcoding "".
    useSource("mariadb", 180000);
    const stamp = Date.now();
    const sourceId = `e2e_mariadb_${stamp}`;

    await openSourcesForm(page);
    await page.getByTestId("sources-id-input").fill(sourceId);
    await page.getByTestId("sources-type-select").selectOption("mariadb");
    await page.getByLabel(/^Host/).and(page.locator(FORM_FIELD)).fill("localhost");
    await page.getByLabel(/^Port/).and(page.locator(FORM_FIELD)).fill(String(E2E_MARIADB_PORT));
    await page.getByLabel(/^Username/).fill("root");
    await page.getByLabel(/^Password/).fill("provisa");
    await page.getByLabel(/^Database/).and(page.locator(FORM_FIELD)).fill("provisa_demo");
    await submitSourceAndExpectListed(page, sourceId);

    await openRegisterForm(page, sourceId);
    await pickSchemaAndTable(page, "provisa_demo", "widgets");
    await expectDiscoveredColumns(page, ["name"], 60000);
    const registered = await submitRegisterAndExpectListed(page, sourceId);

    const rows = await runSqlOnPage(
      page,
      `SELECT id, name FROM pet_store.${registered} ORDER BY id`,
    );
    expect(rows).toHaveLength(3);
    expect(rows[0]).toEqual(["1", "Widget A"]);
    expect(rows[2]).toEqual(["3", "Widget C"]);
  });

  test("postgresql: add the source, register a table, query it on the SQL page", async ({
    page,
  }) => {
    // REQ-1768: postgresql is the most-used SourceType in the product but had zero e2e coverage
    // of its own registration flow — every other e2e test only exercised it indirectly via the
    // pre-seeded pet_store baseline fixture.
    useSource("postgresql", 180000);
    const stamp = Date.now();
    const sourceId = `e2e_postgresql_${stamp}`;

    await openSourcesForm(page);
    await page.getByTestId("sources-id-input").fill(sourceId);
    await page.getByTestId("sources-type-select").selectOption("postgresql");
    await page.getByLabel(/^Host/).and(page.locator(FORM_FIELD)).fill("localhost");
    await page.getByLabel(/^Port/).and(page.locator(FORM_FIELD)).fill(String(E2E_POSTGRESQL_PORT));
    await page.getByLabel(/^Username/).fill("provisa");
    await page.getByLabel(/^Password/).fill("provisa");
    await page.getByLabel(/^Database/).and(page.locator(FORM_FIELD)).fill("provisa_demo");
    await submitSourceAndExpectListed(page, sourceId);

    await openRegisterForm(page, sourceId);
    await pickSchemaAndTable(page, "public", "widgets");
    await expectDiscoveredColumns(page, ["name"], 60000);
    const registered = await submitRegisterAndExpectListed(page, sourceId);

    const rows = await runSqlOnPage(
      page,
      `SELECT id, name FROM pet_store.${registered} ORDER BY id`,
    );
    expect(rows).toHaveLength(3);
    expect(rows[0]).toEqual(["1", "Widget A"]);
    expect(rows[2]).toEqual(["3", "Widget C"]);
  });

  test("tidb: add the source, register a table, query it on the SQL page", async ({ page }) => {
    // REQ-1749: the schema/table/column picker bug is fixed (introspect.py's native_schemas/
    // _native_tables_rdbms/native_columns were missing "tidb" from their mysql/mariadb dispatch
    // tuples). REQ-1757: the SAME SQL-page dialect bug documented on the mariadb test above is
    // also fixed (schema_mutation.py's hardcoded empty-string dialect, affecting any UI-registered
    // source — see that test's comment for the full mechanism).
    useSource("tidb", 180000);
    const stamp = Date.now();
    const sourceId = `e2e_tidb_${stamp}`;

    await openSourcesForm(page);
    await page.getByTestId("sources-id-input").fill(sourceId);
    await page.getByTestId("sources-type-select").selectOption("tidb");
    await page.getByLabel(/^Host/).and(page.locator(FORM_FIELD)).fill("localhost");
    await page.getByLabel(/^Port/).and(page.locator(FORM_FIELD)).fill(String(E2E_TIDB_PORT));
    await page.getByLabel(/^Username/).fill("root");
    await page.getByLabel(/^Database/).and(page.locator(FORM_FIELD)).fill("test");
    await submitSourceAndExpectListed(page, sourceId);

    await openRegisterForm(page, sourceId);
    await pickSchemaAndTable(page, "test", "widgets");
    await expectDiscoveredColumns(page, ["name"], 60000);
    const registered = await submitRegisterAndExpectListed(page, sourceId);

    const rows = await runSqlOnPage(
      page,
      `SELECT id, name FROM pet_store.${registered} ORDER BY id`,
    );
    expect(rows).toHaveLength(3);
    expect(rows[0]).toEqual(["1", "Widget A"]);
    expect(rows[2]).toEqual(["3", "Widget C"]);
  });

  test("cockroachdb: add the source, register a table, query it on the SQL page", async ({
    page,
  }) => {
    useSource("cockroachdb", 180000);
    const stamp = Date.now();
    const sourceId = `e2e_cockroachdb_${stamp}`;

    await openSourcesForm(page);
    await page.getByTestId("sources-id-input").fill(sourceId);
    await page.getByTestId("sources-type-select").selectOption("cockroachdb");
    await page.getByLabel(/^Host/).and(page.locator(FORM_FIELD)).fill("localhost");
    await page.getByLabel(/^Port/).and(page.locator(FORM_FIELD)).fill(String(E2E_COCKROACHDB_PORT));
    await page.getByLabel(/^Username/).fill("root");
    await page.getByLabel(/^Database/).and(page.locator(FORM_FIELD)).fill("defaultdb");
    await submitSourceAndExpectListed(page, sourceId);

    await openRegisterForm(page, sourceId);
    await pickSchemaAndTable(page, "public", "widgets");
    await expectDiscoveredColumns(page, ["name"], 60000);
    const registered = await submitRegisterAndExpectListed(page, sourceId);

    const rows = await runSqlOnPage(
      page,
      `SELECT id, name FROM pet_store.${registered} ORDER BY id`,
    );
    expect(rows).toHaveLength(3);
    expect(rows[0]).toEqual(["1", "Widget A"]);
    expect(rows[2]).toEqual(["3", "Widget C"]);
  });

  test("yugabytedb: add the source, register a table, query it on the SQL page", async ({
    page,
  }) => {
    useSource("yugabytedb", 240000);
    const stamp = Date.now();
    const sourceId = `e2e_yugabytedb_${stamp}`;

    await openSourcesForm(page);
    await page.getByTestId("sources-id-input").fill(sourceId);
    await page.getByTestId("sources-type-select").selectOption("yugabytedb");
    await page.getByLabel(/^Host/).and(page.locator(FORM_FIELD)).fill("localhost");
    await page.getByLabel(/^Port/).and(page.locator(FORM_FIELD)).fill(String(E2E_YUGABYTEDB_PORT));
    await page.getByLabel(/^Username/).fill("yugabyte");
    await page.getByLabel(/^Password/).fill("yugabyte");
    await page.getByLabel(/^Database/).and(page.locator(FORM_FIELD)).fill("yugabyte");
    await submitSourceAndExpectListed(page, sourceId);

    await openRegisterForm(page, sourceId);
    await pickSchemaAndTable(page, "public", "widgets");
    await expectDiscoveredColumns(page, ["name"], 60000);
    const registered = await submitRegisterAndExpectListed(page, sourceId);

    const rows = await runSqlOnPage(
      page,
      `SELECT id, name FROM pet_store.${registered} ORDER BY id`,
    );
    expect(rows).toHaveLength(3);
    expect(rows[0]).toEqual(["1", "Widget A"]);
    expect(rows[2]).toEqual(["3", "Widget C"]);
  });

  test("clickhouse: add the source, register a table, query it on the SQL page", async ({
    page,
  }) => {
    useSource("clickhouse", 180000);
    const stamp = Date.now();
    const sourceId = `e2e_clickhouse_${stamp}`;

    await openSourcesForm(page);
    await page.getByTestId("sources-id-input").fill(sourceId);
    await page.getByTestId("sources-type-select").selectOption("clickhouse");
    await page.getByLabel(/^Host/).and(page.locator(FORM_FIELD)).fill("localhost");
    await page.getByLabel(/^Port/).and(page.locator(FORM_FIELD)).fill(String(E2E_CLICKHOUSE_PORT));
    await page.getByLabel(/^Username/).fill("default");
    await page.getByLabel(/^Password/).fill("provisa");
    await page.getByLabel(/^Database/).and(page.locator(FORM_FIELD)).fill("default");
    await submitSourceAndExpectListed(page, sourceId);

    await openRegisterForm(page, sourceId);
    await pickSchemaAndTable(page, "default", "widgets");
    await expectDiscoveredColumns(page, ["name"], 60000);
    const registered = await submitRegisterAndExpectListed(page, sourceId);

    const rows = await runSqlOnPage(
      page,
      `SELECT id, name FROM pet_store.${registered} ORDER BY id`,
    );
    expect(rows).toHaveLength(3);
    expect(rows[0]).toEqual(["1", "Widget A"]);
    expect(rows[2]).toEqual(["3", "Widget C"]);
  });

  test("sqlserver: add the source, register a table, query it on the SQL page", async ({
    page,
  }) => {
    test.skip(
      !SQLSERVER_ODBC_AVAILABLE,
      "host running the backend has no unixODBC / ODBC Driver 18 for SQL Server registered " +
        "(provisa/executor/drivers/sqlserver.py links libodbc at import) — same host dependency " +
        "tests/integration/test_sqlserver_source_e2e.py's importorskip guards against",
    );
    useSource("sqlserver", 180000);
    const stamp = Date.now();
    const sourceId = `e2e_sqlserver_${stamp}`;

    await openSourcesForm(page);
    await page.getByTestId("sources-id-input").fill(sourceId);
    await page.getByTestId("sources-type-select").selectOption("sqlserver");
    await page.getByLabel(/^Host/).and(page.locator(FORM_FIELD)).fill("localhost");
    await page.getByLabel(/^Port/).and(page.locator(FORM_FIELD)).fill(String(E2E_SQLSERVER_PORT));
    await page.getByLabel(/^Username/).fill("sa");
    await page.getByLabel(/^Password/).fill("Provisa_2026!");
    await page.getByLabel(/^Database/).and(page.locator(FORM_FIELD)).fill("master");
    await submitSourceAndExpectListed(page, sourceId);

    await openRegisterForm(page, sourceId);
    await pickSchemaAndTable(page, "dbo", "widgets");
    await expectDiscoveredColumns(page, ["name"], 60000);
    const registered = await submitRegisterAndExpectListed(page, sourceId);

    const rows = await runSqlOnPage(
      page,
      `SELECT id, name FROM pet_store.${registered} ORDER BY id`,
    );
    expect(rows).toHaveLength(3);
    expect(rows[0]).toEqual(["1", "Widget A"]);
    expect(rows[2]).toEqual(["3", "Widget C"]);
  });

  test("oracle: add the source, register a table, query it on the SQL page", async ({ page }) => {
    // gvenzl/oracle-free's first boot creates the database from scratch; the healthcheck alone
    // is given a 180s start_period + 60 retries in demo/sources/oracle/compose.yml.
    useSource("oracle", 360000);
    const stamp = Date.now();
    const sourceId = `e2e_oracle_${stamp}`;

    await openSourcesForm(page);
    await page.getByTestId("sources-id-input").fill(sourceId);
    await page.getByTestId("sources-type-select").selectOption("oracle");
    await page.getByLabel(/^Host/).and(page.locator(FORM_FIELD)).fill("localhost");
    await page.getByLabel(/^Port/).and(page.locator(FORM_FIELD)).fill(String(E2E_ORACLE_PORT));
    await page.getByLabel(/^Username/).fill("system");
    await page.getByLabel(/^Password/).fill("provisa");
    await page.getByLabel(/^Database/).and(page.locator(FORM_FIELD)).fill("FREEPDB1");
    await submitSourceAndExpectListed(page, sourceId);

    await openRegisterForm(page, sourceId);
    await pickSchemaAndTable(page, "SYSTEM", "WIDGETS");
    await expectDiscoveredColumns(page, ["NAME"], 60000);
    const registered = await submitRegisterAndExpectListed(page, sourceId);

    const rows = await runSqlOnPage(
      page,
      `SELECT id, name FROM pet_store.${registered} ORDER BY id`,
    );
    expect(rows).toHaveLength(3);
    expect(rows[0]).toEqual(["1", "Widget A"]);
    expect(rows[2]).toEqual(["3", "Widget C"]);
  });

  // Greenplum: datagrip/greenplum:6.8 (docker-compose.test.yml's own fixture) publishes an
  // amd64-only manifest — under QEMU emulation on an arm64 host, the entrypoint's from-scratch
  // GPDB cluster build realistically takes well beyond what is reasonable for a single e2e case
  // (docker-compose.test.yml itself budgets a 180s start_period plus 40 retries at 10s = up to
  // ~580s just for the healthcheck). Skipped locally for that reason; ui-e2e-core.yml's
  // ubuntu-latest runner is a genuine amd64 host, so this runs for real there (RUNNING_IN_CI gate).
  // Superuser gpadmin, trust auth (no password), default db postgres — same shape
  // tests/integration/test_greenplum_source_e2e.py uses against the identical image.
  test("greenplum: add the source, register a table, query it on the SQL page", async ({
    page,
  }) => {
    test.skip(
      !RUNNING_IN_CI,
      "datagrip/greenplum:6.8 is amd64-only; under QEMU emulation the from-scratch GPDB " +
        "cluster boot realistically exceeds a reasonable single e2e budget locally — runs for " +
        "real in CI (ubuntu-latest is a genuine amd64 host)",
    );
    useSource("greenplum", 360000);
    const stamp = Date.now();
    const sourceId = `e2e_greenplum_${stamp}`;

    await openSourcesForm(page);
    await page.getByTestId("sources-id-input").fill(sourceId);
    await page.getByTestId("sources-type-select").selectOption("greenplum");
    await page.getByLabel(/^Host/).and(page.locator(FORM_FIELD)).fill("localhost");
    await page.getByLabel(/^Port/).and(page.locator(FORM_FIELD)).fill(String(E2E_GREENPLUM_PORT));
    await page.getByLabel(/^Username/).fill("gpadmin");
    await page.getByLabel(/^Database/).and(page.locator(FORM_FIELD)).fill("postgres");
    await submitSourceAndExpectListed(page, sourceId);

    await openRegisterForm(page, sourceId);
    await pickSchemaAndTable(page, "public", "widgets");
    await expectDiscoveredColumns(page, ["name"], 60000);
    const registered = await submitRegisterAndExpectListed(page, sourceId);

    const rows = await runSqlOnPage(
      page,
      `SELECT id, name FROM pet_store.${registered} ORDER BY id`,
    );
    expect(rows).toHaveLength(3);
    expect(rows[0]).toEqual(["1", "Widget A"]);
    expect(rows[2]).toEqual(["3", "Widget C"]);
  });

  // saphana (REQ-1753): SAP HANA Express (saplabs/hanaexpress) is a full HANA instance (~6.4GB,
  // needs 8GB+ RAM live, 5-15 min cold start) whose indexserver genuinely fails to start under
  // Docker Desktop's Apple Silicon VM (verified live: hdbnameserver/hdbcompileserver/
  // hdbpreprocessor/hdbwebdispatcher all start, the wrapper reports "Startup finished!", but
  // hdbindexserver — the actual database engine — never launches; community reports, including
  // SAP's own "Running Hana Express on M1 Macs" blog, confirm this is an unresolved
  // Docker-Desktop-on-Apple-Silicon limitation, not a config issue). This test, its fixture
  // (demo/sources/saphana/compose.yml + prime.py), and the product fix that makes a saphana SOURCE
  // reachable at all (registry.py's _SQLALCHEMY_FALLBACK entry, REQ-1753 — saphana previously
  // existed only as a whole ACTIVE ENGINE choice, never as a source registrable under another
  // engine like every RDBMS type above) were all verified END TO END against a real x86_64 Linux
  // Docker host (a temporary Vultr instance). ui-e2e-core.yml's ubuntu-latest runner is a genuine
  // amd64 host too, so provision() above starts this fixture there and this test runs for real
  // (RUNNING_IN_CI gate) — locally it still skips. PROVISA_DEMO_SAPHANA_HOST/PORT/PASSWORD remain
  // env-overridable to point at any other real amd64 Docker host for a manual run.
  test("saphana: add the source, register a table, query it on the SQL page", async ({ page }) => {
    test.skip(
      !RUNNING_IN_CI,
      "saplabs/hanaexpress's indexserver does not start under Docker Desktop's Apple Silicon VM " +
        "(verified live, not a config issue — see comment above); runs for real in CI " +
        "(ubuntu-latest is a genuine amd64 host)",
    );
    useSource("saphana", 900000);
    const stamp = Date.now();
    const sourceId = `e2e_saphana_${stamp}`;

    await openSourcesForm(page);
    await page.getByTestId("sources-id-input").fill(sourceId);
    await page.getByTestId("sources-type-select").selectOption("saphana");
    await page.getByLabel(/^Host/).and(page.locator(FORM_FIELD)).fill(E2E_SAPHANA_HOST);
    await page.getByLabel(/^Port/).and(page.locator(FORM_FIELD)).fill(String(E2E_SAPHANA_PORT));
    await page.getByLabel(/^Username/).fill("SYSTEM");
    await page.getByLabel(/^Password/).fill(E2E_SAPHANA_PASSWORD);
    await page.getByLabel(/^Database/).and(page.locator(FORM_FIELD)).fill("HXE");
    await submitSourceAndExpectListed(page, sourceId);

    await openRegisterForm(page, sourceId);
    await pickSchemaAndTable(page, "SYSTEM", "WIDGETS");
    await expectDiscoveredColumns(page, ["NAME"], 60000);
    const registered = await submitRegisterAndExpectListed(page, sourceId);

    const rows = await runSqlOnPage(
      page,
      `SELECT id, name FROM pet_store.${registered} ORDER BY id`,
    );
    expect(rows).toHaveLength(3);
    expect(rows[0]).toEqual(["1", "Widget A"]);
    expect(rows[2]).toEqual(["3", "Widget C"]);
  });
});
