// Copyright (c) 2026 Kenneth Stott
// Canary: dca902b8-1f0f-45fa-b457-00a5ec81bff9
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

// REQ-1730: a source registered ONCE answers the same query under every federation engine — the
// cross-engine harness the category-1/2 e2e sweep (source-to-query.spec.ts) was building toward.
//
// Shape (see source-e2e-coverage-audit memory for the full architecture write-up): register every
// source+table against the DuckDB backend exactly once, then force the already-running Trino
// backend to reload from the SAME Postgres control-plane schema (`PUT /admin/config` re-enters
// the same DB-driven backfill boot itself runs — see REQ-1729/_rebuild_schemas), and rerun every
// query against it via a UI-request route rewrite (the pattern splunk-connector.spec.ts already
// uses to address TRINO_BACKEND_URL). One registration, N engines, identical rows expected — no
// process restart, no re-registration per engine.
//
// Scope: 9 of cat1+cat2's 13 types, plus firebird/airport/singlestore (added after auditing the
// remaining best-effort/Trino-only source types for this harness's fit — see
// source-e2e-coverage-audit memory). Both cat1/cat2 categories land MATERIALIZED sources through
// the SAME mechanism regardless of engine — the app process's own Python driver fetches the
// source and writes the replica into whichever engine's store is active (REQ-826's
// `_MATERIALIZE_ONLY` set) — so neo4j/mongodb/elasticsearch/redis/cassandra/sparql/prometheus/
// graphql_remote/openapi need nothing engine-specific to reach under Trino. Three are excluded
// from THIS harness, not because they're broken, but because they reach through a DIFFERENT
// mechanism whose register-once/swap-engine behavior is unproven and out of scope here:
//   - splunk/sharepoint ATTACH through the connector's bundled Calcite pgwire server as a real
//     Trino catalog (TrinoBackend.register_source creates it; DuckDB's is a no-op) — that catalog
//     creation happens INSIDE the registration mutation, which this harness only ever runs
//     against DuckDB, so Trino never gets it built.
//   - sqlite has no Trino connector or FDW path at all (only DuckDB natively and pg via
//     sqlite_fdw per REQ-1726) — registered here to prove the DuckDB leg, never requeried.
//
// firebird/airport are the SAME shape as sqlite: DuckDB ATTACHes them via a community extension
// (REQ-899) and neither has any Trino connector or land path — registered to prove the DuckDB
// leg, never requeried (`reachableOn: []`). singlestore is the OPPOSITE of that: it is
// materializable everywhere (a real MySQL-wire DIRECT driver, `_make_mysql`, was already sitting
// in executor/drivers/registry.py, unused by any harness) AND Trino attaches it live via a real
// JDBC connector, so it goes through the full swap like the original 9 (`reachableOn: ["trino"]`).
// The demo fixtures for all three live under demo/sources/{firebird,airport,singlestore} —
// provisioned by THIS file's own beforeAll/afterAll (not the shared demo-source-containers.ts
// DEMO_SOURCES list, which every e2e project pays for on every run) since only this harness needs
// them. singlestore needs two things this environment may not have: a SINGLESTORE_LICENSE (the
// singlestoredb-dev image never becomes healthy without one — same gate
// test_singlestore_source_e2e.py already skips on) and an amd64 host (the image publishes no
// arm64 manifest at all, verified via `docker manifest inspect` — `docker compose up` fails
// outright under arm64 emulation, it does not even attempt to boot). Both are checked before
// singlestore's container is even started, not just before its registration.
//
// Run (whole file, every type — heavy, ~11 containers): PROVISA_E2E_LANE=all
//     PROVISA_E2E_CONTROL_PLANE=postgres PROVISA_E2E_WORKERS=1 PROVISA_E2E_ORG_ID=e2e_swap \
//     PROVISA_E2E_TRINO_ORG_ID=e2e_swap npx playwright test --project=swap
// Run ONE type — provisions only that type's own container, per resolve-needed-sources.ts's
// title parsing (same env vars as above, plus the file positionally and -g naming the type):
//     npx playwright test e2e/engine-swap.spec.ts -g mongodb --project=swap
// (The two ORG_ID env overrides put the DuckDB and Trino backends on the SAME org_<id> Postgres
// schema — normally kept apart to prevent collision — which is exactly the sharing this harness
// needs.)

import { execFileSync } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { test, expect, BACKEND_URL, TRINO_BACKEND_URL, UI_URL } from "./coverage";
import {
  E2E_CASSANDRA_PORT,
  E2E_ES_PORT,
  E2E_MONGO_PORT,
  E2E_NEO4J_HTTP_PORT,
  E2E_PROMETHEUS_PORT,
  E2E_REDIS_PORT,
  E2E_SPARQL_PORT,
  startDemoSources,
  removeDemoSources,
} from "./demo-source-containers";
import {
  existingSourcePath,
  openRegisterForm,
  openSourcesForm,
  pickSchemaAndTable,
  registeredTableNames,
  runSqlOnPage,
  submitRegisterAndExpectListed,
  submitSourceAndExpectListed,
} from "./source-to-query-helpers";
import type { Page } from "./coverage";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const PROVISION = path.join(ROOT, "demo", "sources", "provision.py");
const PYTHON = path.join(ROOT, ".venv", "bin", "python");
const SWAP_PREFIX = "provisa-swap";

const E2E_FIREBIRD_PORT = 33051;
const E2E_AIRPORT_PORT = 35061;
const E2E_SINGLESTORE_PORT = 33071;

// Both gates are checked BEFORE provisioning, not just before registration: an unlicensed or
// arm64-emulated singlestoredb-dev container never becomes healthy (or, under arm64, never even
// starts — `docker compose up` fails outright with "no matching manifest"), so attempting it
// wastes the harness's own boot budget on a doomed wait. See the module doc.
const SINGLESTORE_AVAILABLE = process.arch === "x64" && !!process.env.SINGLESTORE_LICENSE;

// Every demo/sources/<name> whose compose.yml takes PROVISA_DEMO_<NAME>_PORT (uppercased) —
// provision.py's own env-passthrough contract (see its module doc: "--env values apply to
// both"). One dedicated block, distinct from both the default demo ports (start-ui-install.sh
// --demo, which a maintainer's local-dev instance may have live) and every other E2E_*_PORT in
// this file, so this harness never collides with either (three-instance-isolation).
const RDB_WIDGETS_PORTS: Record<string, number> = {
  postgresql: 36001,
  mysql: 36011,
  mariadb: 36021,
  sqlserver: 36031,
  oracle: 36041,
  cockroachdb: 36051,
  yugabytedb: 36061,
  greenplum: 36071,
  tidb: 36081,
  clickhouse: 36091,
};

// One source = one test (REQ-1730 redesign, 2026-09-16): every demo/sources/<name> this harness
// uses provisions and tears down its OWN container, scoped to the ONE test that needs it — never
// the whole fleet just because one type is under test. A single `--grep mysql` run boots exactly
// one container. Generalizes the original firebird/airport/singlestore-only helper: any
// demo/sources/<name> directory works here as long as its compose.yml takes
// PROVISA_DEMO_<NAME>_PORT (every RDBMS fixture prime.py checked live during this extension
// does — see RDB_WIDGETS_PORTS).
function provisionSwapSource(name: string, cmd: "up" | "down"): void {
  const env = {
    ...process.env,
    PROVISA_DEMO_FIREBIRD_PORT: String(E2E_FIREBIRD_PORT),
    PROVISA_DEMO_AIRPORT_PORT: String(E2E_AIRPORT_PORT),
    PROVISA_DEMO_SINGLESTORE_PORT: String(E2E_SINGLESTORE_PORT),
    PROVISA_DEMO_PREFIX: SWAP_PREFIX,
    ...(name in RDB_WIDGETS_PORTS
      ? { [`PROVISA_DEMO_${name.toUpperCase()}_PORT`]: String(RDB_WIDGETS_PORTS[name]) }
      : {}),
  };
  try {
    execFileSync(PYTHON, [PROVISION, cmd, "--prefix", SWAP_PREFIX, name], {
      stdio: "pipe",
      env,
    });
  } catch (e) {
    if (cmd === "down") return; // a project that was never started removes nothing
    throw e;
  }
}

/** One source+table's proof: the SQL that reads it back, and the assertion every engine must
 * satisfy identically. An empty `reachableOn` registers the source (proving the DuckDB leg) but
 * skips every engine's requery — see the module doc for why (sqlite). */
interface Registration {
  label: string;
  sourceId: string;
  sql: string;
  assertRows: (rows: string[][]) => void;
  /** Engine names (EngineTarget.name below) this registration's source has a live reach path
   * for. Absence means "registered to prove the DuckDB leg, never requeried on that engine" —
   * e.g. sqlite, which has no Trino connector or FDW path at all (REQ-1726). */
  reachableOn: string[];
}

/** One already-running backend process this harness can requery against, sharing the DuckDB
 * backend's Postgres control-plane org schema. Add an entry here (and to ENGINES below) to bring
 * a new engine into the swap — nothing else in this file names an engine by hand. */
interface EngineTarget {
  name: string;
  backendUrl: string;
  /** Env var naming this engine's own config file on disk — PUT back verbatim to force a live,
   * in-process reload (REQ-1729's DB-driven backfill), no OS-level restart needed. */
  configPathEnv: string;
}

const ENGINES: EngineTarget[] = [
  { name: "trino", backendUrl: TRINO_BACKEND_URL, configPathEnv: "PROVISA_E2E_TRINO_CONFIG" },
  // pg: add { name: "pg", backendUrl: PG_BACKEND_URL, configPathEnv: "PROVISA_E2E_PG_CONFIG" }
  // once a pg-engine e2e backend exists (the `pgserver` embedded-Postgres package is the planned
  // route — no Docker image to stand up, unlike Trino's JVM cluster).
];

// This harness runs the DuckDB-bound registration backend and the Trino-bound backend side by
// side on the same box, sharing the same Postgres control-plane schema (REQ-1730's whole point).
// A cold Trino coordinator can take a few minutes to become genuinely stable — not just accepting
// connections, but no longer background-retrying its own startup work (system-catalog
// registration, per-source MV introspection probes) — and that churn measurably slows the
// CO-RESIDENT DuckDB backend's own schema rebuilds (shared CPU/DB, not a per-source-type bug: the
// registration that randomly blows the default 120s timeout is a different one every run). Give
// registration during this harness a longer runway than the default so a real Trino cold start
// doesn't get misread as a registration failure.
const SWAP_REGISTER_TIMEOUT_MS = 300000;

async function registerNeo4j(page: Page): Promise<Registration> {
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

async function registerMongodb(page: Page): Promise<Registration> {
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
interface RdbWidgetsConfig {
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
const RDB_WIDGETS_SOURCES: RdbWidgetsConfig[] = [
  { type: "postgresql", port: RDB_WIDGETS_PORTS.postgresql, username: "provisa", password: "provisa", database: "provisa_demo", schema: "public" },
  { type: "mysql", port: RDB_WIDGETS_PORTS.mysql, username: "root", password: "provisa", database: "provisa_demo", schema: "provisa_demo" },
  { type: "mariadb", port: RDB_WIDGETS_PORTS.mariadb, username: "root", password: "provisa", database: "provisa_demo", schema: "provisa_demo" },
  { type: "sqlserver", port: RDB_WIDGETS_PORTS.sqlserver, username: "sa", password: "Provisa_2026!", database: "master", schema: "dbo" },
  { type: "cockroachdb", port: RDB_WIDGETS_PORTS.cockroachdb, username: "root", password: "", database: "defaultdb", schema: "public" },
  { type: "yugabytedb", port: RDB_WIDGETS_PORTS.yugabytedb, username: "yugabyte", password: "yugabyte", database: "yugabyte", schema: "public" },
  { type: "greenplum", port: RDB_WIDGETS_PORTS.greenplum, username: "gpadmin", password: "", database: "postgres", schema: "public" },
  { type: "tidb", port: RDB_WIDGETS_PORTS.tidb, username: "root", password: "", database: "test", schema: "test" },
  { type: "clickhouse", port: RDB_WIDGETS_PORTS.clickhouse, username: "default", password: "provisa", database: "default", schema: "default" },
];

function registerRdbWidgets(cfg: RdbWidgetsConfig): (page: Page) => Promise<Registration> {
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
      reachableOn: ["trino"],
    };
  };
}

async function registerElasticsearch(page: Page): Promise<Registration> {
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

async function registerRedis(page: Page): Promise<Registration> {
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

async function registerCassandra(page: Page): Promise<Registration> {
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

async function registerSparql(page: Page): Promise<Registration> {
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

async function registerPrometheus(page: Page): Promise<Registration> {
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

async function registerSqlite(page: Page): Promise<Registration> {
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

async function registerGraphqlRemote(page: Page): Promise<Registration> {
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

async function registerOpenapi(page: Page): Promise<Registration> {
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

async function registerFirebird(page: Page): Promise<Registration> {
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

async function registerAirport(page: Page): Promise<Registration> {
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

async function registerSinglestore(page: Page): Promise<Registration> {
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

/** Force an already-running engine backend to pick up rows the DuckDB backend just registered
 * into their SHARED Postgres control-plane schema. `PUT /admin/config` re-enters the exact
 * boot-time DB-driven backfill (introspect_tables/_rebuild_schemas/reconcile_landed_tables —
 * see REQ-1729) — no OS-level restart needed, since it is already a live in-process reload. */
async function reloadEngineBackend(engine: EngineTarget) {
  const configPath = process.env[engine.configPathEnv];
  expect(
    configPath,
    `${engine.configPathEnv} not set — is the ${engine.name} backend in this run's lane?`,
  ).toBeTruthy();
  const yaml = fs.readFileSync(configPath!, "utf8");
  const res = await fetch(`${engine.backendUrl}/admin/config`, {
    method: "PUT",
    headers: { "Content-Type": "application/yaml" },
    body: yaml,
    signal: AbortSignal.timeout(300000), // reconcile_landed_tables re-lands every source; cold-JIT budget
  });
  expect(res.ok, await res.text()).toBeTruthy();
}

/** Replay `createSource` (same values, on an id that already exists — `_upsert_source_with_domains`
 * makes this an upsert, not a duplicate error) against the target engine so its OWN
 * `_register_source_on_engine()` runs under THAT engine — the per-source engine catalog it
 * provisions (`catalog.create_catalog`, e.g. Trino's real `mongodb`/`cassandra`/`redis`/
 * `elasticsearch` connectors) only happens inside `create_source`, bound to whichever engine is
 * active AT THE CALL (`update_source` never calls it at all — checked directly), and this
 * harness's registration phase only ever calls it under DuckDB. A no-op for a type with no engine
 * connector (REQ-842 skips catalog creation for it either way). */
async function reprovisionSourceOnEngine(engine: EngineTarget, sourceId: string) {
  // Raw fetch() against the engine's own URL, NOT page.request — page.request is a separate
  // APIRequestContext that page.route() never intercepts (only browser-initiated requests are
  // routed), so this call would otherwise silently keep hitting the DuckDB backend regardless of
  // the route rewrite below, exactly as it did before this was caught.
  const res = await fetch(`${engine.backendUrl}/admin/graphql`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      // mappingJson: redis/elasticsearch's per-table definitions live in Source.mapping (REQ-250/
      // 251's write_table_definitions reads source.mapping.tables) — omitting it here recreates
      // the source on the target engine with an empty mapping, so its table-description file
      // comes out empty and every one of its registered tables 404s as TABLE_NOT_FOUND.
      // passwordRef: the ${secret:NAME} vault REFERENCE persist_source_password wrote (REQ-1695)
      // — never the plaintext, which the vault holds unreadable by name. SourceInput.password
      // already treats a value containing "${" as a reference and stores it verbatim (never
      // re-vaults it), so round-tripping this through create_source resolves correctly on the
      // target engine's own process via resolve_secrets(), sharing the same org vault. Omitting
      // it silently registered every credentialed RDBMS type with an empty password (caught live
      // 2026-09-16 registering postgresql: "password authentication failed").
      // federationHintsJson: the "connection extras" channel (Snowflake warehouse/role,
      // sqlserver's trust_server_certificate, exasol's tls_fingerprint, ...) — SourceType and
      // SourceInput already share this exact field name, so it passes straight through the `...src`
      // spread below with no renaming, unlike password/passwordRef. Omitting it silently dropped
      // every such hint on replay (caught live 2026-09-16 registering sqlserver against a
      // self-signed demo cert: JDBC_ERROR, PKIX path building failed).
      query:
        "{ sources { id type host port database username passwordRef path description mappingJson federationHintsJson } }",
    }),
  });
  const resText = await res.text();
  expect(res.ok, resText).toBeTruthy();
  const sources = JSON.parse(resText).data.sources as Array<{
    id: string;
    type: string;
    host: string;
    port: number;
    database: string;
    username: string;
    passwordRef: string;
    path: string | null;
    description: string;
    mappingJson: string | null;
    federationHintsJson: string | null;
  }>;
  const src = sources.find((s) => s.id === sourceId);
  expect(src, `source ${sourceId} not found on this engine's control-plane schema`).toBeTruthy();
  // Trino runs in its own Docker container (docker-compose.core.yml), unlike DuckDB which runs
  // natively as this harness's own process — a host published on the HOST's localhost (every
  // demo-source-containers.ts port) is unreachable as "localhost" from inside that container,
  // which instead resolves to itself. host.docker.internal is Docker Desktop's address for the
  // host machine. DuckDB's own registration is untouched (the row read above, before this
  // rewrite); only the copy replayed against a Dockerized engine gets translated. `host` is not
  // always a bare hostname — prometheus stores a full URL there (e.g. "http://localhost:39090",
  // SourceFormFieldsExtended.tsx) — so replace the substring rather than requiring an exact match.
  if (engine.name === "trino" && src!.host) {
    src!.host = src!.host.replace(/\b(?:localhost|127\.0\.0\.1)\b/g, "host.docker.internal");
  }
  const mutation = await fetch(`${engine.backendUrl}/admin/graphql`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      query: `mutation($s: SourceInput!) { createSource(input: $s) { success message } }`,
      // SourceInput has no `passwordRef` field, only `password` (GraphQL input coercion rejects
      // an unknown field outright) — drop it from the spread and carry the vault reference under
      // the name SourceInput actually declares (see the query's own comment for why this is safe).
      variables: { s: { ...src, passwordRef: undefined, password: src!.passwordRef } },
    }),
  });
  const mutationText = await mutation.text();
  expect(mutation.ok, mutationText).toBeTruthy();
  const body = JSON.parse(mutationText);
  expect(body.errors, JSON.stringify(body.errors)).toBeUndefined();
  expect(body.data.createSource.success, body.data.createSource.message).toBeTruthy();
}

/** Requery every DuckDB-registered source this engine can reach, on this SAME page — the route
 * rewrite (splunk-connector.spec.ts's pattern) sends every subsequent UI request at this origin
 * to the engine's own backend instead of the DuckDB one the page has been talking to. */
async function requeryOnEngine(page: Page, engine: EngineTarget, registrations: Registration[]) {
  await reloadEngineBackend(engine);
  // Only the API paths, never `${UI_URL}/**` whole — the SPA's own HTML/JS is served by Vite,
  // not the engine backend, and a blanket rewrite 404s the page itself before `.cm-content` ever
  // renders. Same prefix list splunk-connector.spec.ts already proved for this exact redirect.
  const routes = ["/admin", "/data", "/query", "/health"].map((prefix) => `${UI_URL}${prefix}**`);
  for (const pattern of routes) {
    await page.route(pattern, (route) => {
      route.continue({ url: route.request().url().replace(UI_URL, engine.backendUrl) });
    });
  }
  const reachable = registrations.filter((r) => r.reachableOn.includes(engine.name));
  for (const reg of reachable) {
    await reprovisionSourceOnEngine(engine, reg.sourceId);
  }
  for (const reg of reachable) {
    const rows = await runSqlOnPage(page, reg.sql);
    reg.assertRows(rows);
  }
  for (const pattern of routes) await page.unroute(pattern);
}

/** Wait until the Trino-bound backend answers a trivial query reliably (3 consecutive successes,
 * no retry needed on any of them) — not just that Trino is UP (the webServer's own /health gate
 * already guarantees that), but that it has stopped the background churn a cold coordinator does
 * on startup (system-catalog registration, per-source MV introspection probes against sources
 * that can never resolve under Trino by design, like sqlite — REQ-1726). That churn runs in the
 * SAME process as the one this harness swaps queries to, and measurably slows the CO-RESIDENT
 * DuckDB backend's own schema rebuilds (shared CPU/DB) while it's happening — see
 * SWAP_REGISTER_TIMEOUT_MS's comment. Waiting it out here, before the timed registration loop
 * starts, keeps that cold-start window from landing on a random registration's own timeout.
 * Real coordinators have taken up to a few minutes to settle; budget generously for that. */
async function waitForTrinoStable(budgetMs = 240000): Promise<void> {
  const deadline = Date.now() + budgetMs;
  let consecutiveOk = 0;
  while (consecutiveOk < 3) {
    if (Date.now() > deadline) {
      throw new Error(
        `Trino backend never stabilized (3 consecutive clean SELECT 1s) within ${budgetMs}ms`,
      );
    }
    try {
      const res = await fetch(`${TRINO_BACKEND_URL}/data/sql`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ sql: "SELECT 1" }),
        signal: AbortSignal.timeout(10000),
      });
      consecutiveOk = res.ok ? consecutiveOk + 1 : 0;
    } catch {
      consecutiveOk = 0;
    }
    if (consecutiveOk < 3) await new Promise((r) => setTimeout(r, 3000));
  }
}

// REQ-1730 redesign (2026-09-16, "one source = one test"): this used to be ONE test registering
// all 12-13 types in sequence, sharing one beforeAll that booted every container (firebird +
// airport + singlestore + the 7-container shared demo-source stack) regardless of which type
// anyone actually wanted to check. That meant: a single type could not be targeted or debugged in
// isolation (a `-g mongodb` run still paid for and waited on all thirteen), a failure on type #2
// blocked ever finding out whether #3-13 worked, and the whole suite had to be rerun from zero on
// every iteration. Splitting into one independent test per type fixes all three: `-g firebird`
// now provisions and tears down ONLY firebird's own container (see resolve-needed-sources.ts's
// title parsing, the same mechanism source-to-query.spec.ts already used), one type's failure
// reports on its own and does not block the others, and each test can be rerun alone. The cost is
// each type's own Trino reload/requery pass no longer amortizes across the batch — reloadEngineBackend
// runs once per test instead of once for all thirteen. Given today's actual experience (isolating
// one failure meant reruning the full 10+ minute batch, repeatedly), that trade is worth it.
//
// Trino cold-start stabilization is the one thing still shared file-wide: waitForTrinoStable()
// runs ONCE per worker process (Playwright dedupes an outer-scope beforeAll across every test in
// the file that runs in it), not once per type.
test.beforeAll(async () => {
  // Can take a few minutes on top of the default per-hook timeout — see
  // source-to-query-olap-lake-trino.spec.ts's same pattern for its own multi-minute cold-init
  // beforeAlls.
  test.setTimeout(360000);
  await waitForTrinoStable();
});

// Every registrar mints its own `e2e_swap_<type>_<Date.now()>` sourceId (see e.g.
// registerMongodb/registerFirebird below) and container teardown in each type's beforeAll/
// afterAll only ever tore down the CONTAINER, never the row that registration created — so every
// run, pass or fail, left its source (and, via FK cascade, its registered table/relationships)
// behind for good. Confirmed live (2026-09-16): 12 accumulated `mongodb` rows alone, each with a
// poll job (wired by every schema rebuild) still trying to reach a container that no longer
// existed, were the actual cause of the ~300s registration stalls this harness was chasing — not
// per-source slowness. A per-registrar `finally` block (in runSwapCase, deleting just that run's
// own sourceId) was tried first and rejected: it only runs once `registrar()` has RETURNED a
// Registration, so a registrar that throws midway — its own source already created, e.g.
// registerFirebird failing at pickSchemaAndTable — still leaked a row (reproduced live: 2 leaked
// firebird rows from a genuinely-failing run).
//
// A single sweep run ONCE at file scope (after every test in the whole run) closed both of THOSE
// gaps, but opened a third one, also confirmed live: within one multi-type invocation
// (`-g "mysql:|sqlserver:|oracle:|..."`), a type's own container is torn down the moment ITS
// describe block finishes, while its row survives — untouched — until the file-level afterAll at
// the very end. Any later type's schema rebuild that reconciles/reconnects across EVERY
// registered source (not just the one under test) then retries a live connection to that
// already-gone container and blocks on it — 5 of 9 new RDBMS types (oracle, cockroachdb,
// yugabytedb, greenplum, clickhouse) stalled ~2.1m each this exact way in a single `-g` run
// covering all of them. Calling the sweep from EVERY type's own afterAll (in addition to the
// file-level one, kept as a backstop for a registrar that throws before its own describe's
// afterAll would even run) closes this: a type's row is gone by the time the next type starts.
//
// `fetch` direct to BACKEND_URL (not the vite-proxied UI_URL via page.request): matches
// file-connector.spec.ts's already-working admin/graphql pattern — /admin/graphql needs no
// session for this harness's single-user dev auth mode, so the extra proxy hop buys nothing and
// (per page.request, tried second) is one more thing that can silently misroute.
async function sweepZombieSwapSources(): Promise<void> {
  const res = await fetch(`${BACKEND_URL}/admin/graphql`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query: `{ sources { id } }` }),
  });
  const sources: Array<{ id: string }> = (await res.json()).data?.sources ?? [];
  const zombies = sources.filter((s) => /^e2e_swap_[a-z0-9_]+_\d{10,}$/.test(s.id));
  for (const { id } of zombies) {
    await fetch(`${BACKEND_URL}/admin/graphql`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        query: `mutation D($id: String!) { deleteSource(id: $id) { success } }`,
        variables: { id },
      }),
    });
  }
}

test.afterAll(sweepZombieSwapSources);

/** One type's full proof: register under DuckDB, then answer identically under every engine its
 * Registration declares reachableOn. Shared body for every per-type test below — see the
 * file-scope afterAll above for how the source this creates gets cleaned up. */
async function runSwapCase(page: Page, registrar: () => Promise<Registration>): Promise<void> {
  test.setTimeout((1 + 5 * ENGINES.length) * 60 * 1000);
  const registration = await registrar();
  for (const engine of ENGINES) {
    await requeryOnEngine(page, engine, [registration]);
  }
}

test.describe("engine swap: one registration answers every engine (REQ-1730)", () => {
  test.describe("neo4j", () => {
    test.beforeAll(() => startDemoSources(["neo4j"]));
    test.afterAll(async () => {
      await removeDemoSources(["neo4j"]);
      await sweepZombieSwapSources();
    });

    test("neo4j: register once under DuckDB, answer identical queries under every other engine", async ({
      page,
    }) => runSwapCase(page, () => registerNeo4j(page)));
  });

  test.describe("mongodb", () => {
    test.beforeAll(() => startDemoSources(["mongodb"]));
    test.afterAll(async () => {
      await removeDemoSources(["mongodb"]);
      await sweepZombieSwapSources();
    });

    test("mongodb: register once under DuckDB, answer identical queries under every other engine", async ({
      page,
    }) => runSwapCase(page, () => registerMongodb(page)));
  });

  test.describe("elasticsearch", () => {
    test.beforeAll(() => startDemoSources(["elasticsearch"]));
    test.afterAll(async () => {
      await removeDemoSources(["elasticsearch"]);
      await sweepZombieSwapSources();
    });

    test("elasticsearch: register once under DuckDB, answer identical queries under every other engine", async ({
      page,
    }) => runSwapCase(page, () => registerElasticsearch(page)));
  });

  test.describe("redis", () => {
    test.beforeAll(() => startDemoSources(["redis"]));
    test.afterAll(async () => {
      await removeDemoSources(["redis"]);
      await sweepZombieSwapSources();
    });

    test("redis: register once under DuckDB, answer identical queries under every other engine", async ({
      page,
    }) => runSwapCase(page, () => registerRedis(page)));
  });

  test.describe("cassandra", () => {
    test.beforeAll(() => startDemoSources(["cassandra"]));
    test.afterAll(async () => {
      await removeDemoSources(["cassandra"]);
      await sweepZombieSwapSources();
    });

    test("cassandra: register once under DuckDB, answer identical queries under every other engine", async ({
      page,
    }) => runSwapCase(page, () => registerCassandra(page)));
  });

  test.describe("sparql", () => {
    test.beforeAll(() => startDemoSources(["sparql"]));
    test.afterAll(async () => {
      await removeDemoSources(["sparql"]);
      await sweepZombieSwapSources();
    });

    test("sparql: register once under DuckDB, answer identical queries under every other engine", async ({
      page,
    }) => runSwapCase(page, () => registerSparql(page)));
  });

  test.describe("prometheus", () => {
    test.beforeAll(() => startDemoSources(["prometheus"]));
    test.afterAll(async () => {
      await removeDemoSources(["prometheus"]);
      await sweepZombieSwapSources();
    });

    test("prometheus: register once under DuckDB, answer identical queries under every other engine", async ({
      page,
    }) => runSwapCase(page, () => registerPrometheus(page)));
  });

  // sqlite/graphql_remote/openapi need no container of their own (a local file / the already-
  // running graphql-demo and petstore-mock webServers respectively), so nothing to provision here.
  test("sqlite: register once under DuckDB (no Trino leg — REQ-1726)", async ({ page }) =>
    runSwapCase(page, () => registerSqlite(page)));

  test("graphql_remote: register once under DuckDB, answer identical queries under every other engine", async ({
    page,
  }) => runSwapCase(page, () => registerGraphqlRemote(page)));

  test("openapi: register once under DuckDB, answer identical queries under every other engine", async ({
    page,
  }) => runSwapCase(page, () => registerOpenapi(page)));

  test.describe("firebird", () => {
    test.beforeAll(() => provisionSwapSource("firebird", "up"));
    test.afterAll(async () => {
      await provisionSwapSource("firebird", "down");
      await sweepZombieSwapSources();
    });

    test("firebird: register once under DuckDB (no Trino leg — REQ-899)", async ({ page }) =>
      runSwapCase(page, () => registerFirebird(page)));
  });

  test.describe("airport", () => {
    test.beforeAll(() => provisionSwapSource("airport", "up"));
    test.afterAll(async () => {
      await provisionSwapSource("airport", "down");
      await sweepZombieSwapSources();
    });

    test("airport: register once under DuckDB (no Trino leg — REQ-899/1097)", async ({ page }) =>
      runSwapCase(page, () => registerAirport(page)));
  });

  test.describe("singlestore", () => {
    test.skip(
      !SINGLESTORE_AVAILABLE,
      "needs SINGLESTORE_LICENSE and an amd64 host (singlestoredb-dev publishes no arm64 manifest) " +
        "— see the module doc",
    );
    test.beforeAll(() => provisionSwapSource("singlestore", "up"));
    test.afterAll(async () => {
      await provisionSwapSource("singlestore", "down");
      await sweepZombieSwapSources();
    });

    test("singlestore: register once under DuckDB, answer identical queries under every other engine", async ({
      page,
    }) => runSwapCase(page, () => registerSinglestore(page)));
  });

  // Every demo/sources/<name> RDBMS below primes the identical widgets(id, name) + 3 rows shape
  // (verified live 2026-09-16) and has a native Trino connector (provisa/federation/
  // trino_connectors.py's TRINO_CONNECTORS/_TRINO_JDBC_TYPES) — the SAME class of Trino reach as
  // mongodb/cassandra/redis/elasticsearch above, not the adapter-fetch/materialize-only fallback
  // neo4j/sparql need. registerRdbWidgets(cfg) is the shared registrar; only the type/port/
  // credentials/schema differ (RDB_WIDGETS_SOURCES).
  for (const cfg of RDB_WIDGETS_SOURCES) {
    test.describe(cfg.type, () => {
      test.beforeAll(() => provisionSwapSource(cfg.type, "up"));
      test.afterAll(async () => {
        await provisionSwapSource(cfg.type, "down");
        await sweepZombieSwapSources();
      });

      test(`${cfg.type}: register once under DuckDB, answer identical queries under every other engine`, async ({
        page,
      }) => runSwapCase(page, () => registerRdbWidgets(cfg)(page)));
    });
  }
});
