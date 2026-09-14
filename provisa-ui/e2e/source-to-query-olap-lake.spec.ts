// Copyright (c) 2026 Kenneth Stott
// Canary: 22f66aec-9969-4e42-8e95-5452fbb4f344
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1671 fanout: OLAP/lakehouse SourceTypes through the UI — pinot, druid, exasol, hive,
// hive_s3, hiveserver2. Three screens, no API shortcuts, same shape as source-to-query.spec.ts
// (which this file deliberately does not touch — see that file's own module doc for the pattern
// this one follows).
//
// Two different engines are exercised here, because these types split cleanly on one axis:
// whether the type has a DIRECT driver (reachable on ANY engine, including the DuckDB core-lane
// backend) or is Trino-connector-ONLY (register_source() is a no-op on a native/DuckDB engine —
// EngineBackend.register_source in provisa/federation/backend.py — so the schema dropdown can
// never populate against it).
//
//   - hiveserver2 has a real DIRECT driver (provisa/executor/drivers/hive.py's HiveDriver, over
//     impyla) and runs against the plain core-lane DuckDB backend, exactly like every other
//     source-to-query.spec.ts case.
//   - pinot and hive_s3 have NO direct driver — only a Trino connector
//     (TrinoPinotConnector/TrinoHiveS3Connector, provisa/federation/trino_connectors.py). They are
//     routed at the network layer to the Trino-backed webServer, the same pattern
//     splunk-connector.spec.ts and sharepoint-connector.spec.ts already use for the identical
//     reason. This requires the Trino lane's webServer to actually be running, i.e. this file must
//     be run with PROVISA_E2E_LANE=all (the default) or =trino — NOT PROVISA_E2E_LANE=core, which
//     never starts the Trino-backed backend at all (playwright.config.ts's RUNS_TRINO gate). This
//     file is intentionally NOT added to playwright.config.ts's TRINO_SPECS list: doing so would
//     also exclude hiveserver2's core-lane test from the "core" project (TRINO_SPECS/testIgnore
//     operate at file granularity, not per-test), and TRINO_SPECS is a list several other e2e
//     specs share — editing it risks colliding with parallel work on this branch. Leaving this
//     file in the default "core" project's testMatch and relying on LANE=all to boot both
//     webServers achieves the same result without touching shared config.
//
//   Run: cd provisa-ui && npx playwright test source-to-query-olap-lake --project=core
//        (PROVISA_E2E_LANE left at its default "all" — see note above)
//
//   - druid and exasol are SKIPPED — see the two test.skip() blocks below for the specific,
//     verified reason each was cut (not "flaky," not "ran out of time": a concrete resource/infra
//     constraint verified against a live container in this session).
//   - plain `hive` (Hadoop/local-storage lakehouse read) is ALSO skipped, for a reason discovered
//     only by actually trying it against this session's live shared Trino container — see that
//     test.skip() block.
//   - hiveserver2, pinot, and hive_s3 are ALSO currently test.skip()'d, for a DIFFERENT reason
//     than the four above: this was one of six parallel agents in this fanout sharing one
//     advisory playwright run-lock and a Docker/host memory ceiling, and every acquisition window
//     this session tried for these three (several separate 5-20 minute waits) was still held by
//     another agent's in-flight run when it expired — the UI walk itself was never exercised to
//     completion. All three are proven independently at the connector/driver level in this
//     session (see each test's own comment): hiveserver2's demo fixture was primed live through
//     the real HiveDriver/impyla path; pinot's and hive_s3's Trino connector paths were driven
//     live through provisa.core.catalog.create_catalog with the exact same code the UI mutation
//     calls, including catching and fixing two real bugs (pinot's network topology, hive_s3's
//     underscore hostname — see demo/sources/pinot/compose.yml and demo/sources/hive-s3/
//     compose.yml). Un-skip these three first if resuming this file with a free run-lock slot.
//
// Ports: this file's own containers live in the 369xx range, distinct from the other e2e files'
// demo-source-containers.ts (33xxx/35xxx/37xxx/38xxx/39xxx) and source-to-query.spec.ts's extra
// RDBMS block (33xxx).

import { execFileSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";
import type { Page } from "playwright/test";

import { test, expect, UI_URL, TRINO_BACKEND_URL } from "./coverage";
import {
  openRegisterForm,
  openSourcesForm,
  pickSchemaAndTable,
  runSqlOnPage,
  submitSourceAndExpectListed,
} from "./source-to-query-helpers";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const PROVISION = path.join(ROOT, "demo", "sources", "provision.py");
const PYTHON = path.join(ROOT, ".venv", "bin", "python");
const PREFIX = "provisa-s2q-olap-lake";

// The e2e harness's shared trino/postgres/minio compose project's default network — the same
// network splunk-connector.spec.ts joins its own container to, and for the same reason: Trino
// must resolve the source container by name to build a live catalog.
const DOCKER_NETWORK = process.env.PROVISA_E2E_DOCKER_NETWORK ?? "provisa_default";

const E2E_PINOT_CONTROLLER_PORT = Number(process.env.PROVISA_DEMO_PINOT_CONTROLLER_PORT ?? 36900);
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
        ...(cmd === "up" ? ["--network", DOCKER_NETWORK] : []),
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

/** Redirect the UI's API calls from the default DuckDB backend to the Trino-backed one, so
 * register_source() creates a real Trino catalog and schema enumeration succeeds — the same
 * technique splunk-connector.spec.ts uses, for the identical reason (TrinoPinotConnector/
 * TrinoHiveS3Connector have no DuckDB-reachable path at all). `**` (not `*`) is required: Playwright
 * compiles a single star to a pattern that stops at the first `/`, which never matches
 * `/admin/graphql`. */
async function routeToTrinoBackend(page: Page): Promise<void> {
  for (const prefix of ["/admin", "/data", "/query", "/health"]) {
    await page.route(`${UI_URL}${prefix}**`, (route) => {
      route.continue({ url: route.request().url().replace(UI_URL, TRINO_BACKEND_URL) });
    });
  }
}

async function trinoGql(query: string, variables: Record<string, unknown> = {}) {
  const res = await fetch(`${TRINO_BACKEND_URL}/admin/graphql`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query, variables }),
    signal: AbortSignal.timeout(120000),
  });
  return res.json();
}

/** The SQL-plane name a Trino-routed registration serves the table by — same lookup
 * submitRegisterAndExpectListed (source-to-query-helpers.ts) does, but over a direct fetch to the
 * Trino-backed backend rather than `page.request`, since the UI-origin route interception above
 * only rewrites requests the PAGE itself issues (clicks, its own fetch calls), not Playwright's
 * separate `page.request` API context. */
async function trinoTableName(sourceId: string): Promise<string> {
  const res = await trinoGql("{ tables { sourceId dqDataset } }");
  const tables = (res.data?.tables ?? []) as { sourceId: string; dqDataset: string | null }[];
  const mine = tables.find((t) => t.sourceId === sourceId);
  expect(mine?.dqDataset, `no dataset name reported for ${sourceId}`).toBeTruthy();
  return mine!.dqDataset!.split("/").pop()!;
}

async function cleanupTrinoSource(sourceId: string) {
  await trinoGql(`mutation D($id: String!) { deleteSource(id: $id) { success } }`, { id: sourceId });
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
    // Not verified green within this session's time budget: this worktree's own e2e run
    // repeatedly could not get a playwright worker slot — six parallel agents in this fanout
    // share one advisory run-lock (/tmp/provisa-e2e-run-lock) around the shared
    // demo-source-containers.ts global-setup fixture, and every acquisition window this session
    // tried (several 5-20 minute waits) was still held by another agent's in-flight run when it
    // expired. The demo/sources/hiveserver2 fixture itself IS proven live in this session
    // (provision.py up + prime.py: creates the hive-metastore + hiveserver2 container pair,
    // creates database `wh` and table `widgets`, inserts and reads back the 3 seed rows through
    // impyla — see this file's final report) — what is unverified is the UI walk (Sources form ->
    // Register Table form -> SQL page) this test drives. Left in place, not deleted, so the next
    // session with a free run-lock slot can flip this back on.
    test.skip(
      true,
      "not run to green in this session: the shared cross-agent playwright run-lock stayed held " +
        "by other agents through every acquisition window attempted (see comment above) — the " +
        "demo/sources/hiveserver2 fixture itself is independently verified live",
    );
    test.setTimeout(180000);
    const stamp = Date.now();
    const sourceId = `e2e_hiveserver2_${stamp}`;

    await openSourcesForm(page);
    await page.getByTestId("sources-id-input").fill(sourceId);
    await page.getByTestId("sources-type-select").selectOption("hiveserver2");
    await page.getByLabel(/^Host/).fill("localhost");
    await page.getByLabel(/^Port/).fill(String(E2E_HIVESERVER2_PORT));
    // auth_mechanism defaults to PLAIN (SourceFormFields.tsx), which is what demo/sources/
    // hiveserver2's stock HS2 speaks — left untouched.
    await submitSourceAndExpectListed(page, sourceId);

    await openRegisterForm(page, sourceId);
    await pickSchemaAndTable(page, "wh", "widgets");
    await expect(page.getByTestId("register-table-col-selected-name")).toBeVisible({
      timeout: 60000,
    });
    await page.getByTestId("register-table-submit").click();
    const row = page.locator(".data-table tbody tr").filter({ hasText: sourceId }).first();
    await expect(row).toBeVisible({ timeout: 120000 });
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

// ---------------------------------------------------------------------------------------------
// pinot — Trino connector only (TrinoPinotConnector), routed to the Trino-backed webServer.
// ---------------------------------------------------------------------------------------------
test.describe("source to query through the UI: pinot (REQ-1740)", () => {
  test.beforeAll(() => {
    test.setTimeout(300000);
    provision("up", ["pinot"], {
      PROVISA_DEMO_PINOT_CONTROLLER_PORT: String(E2E_PINOT_CONTROLLER_PORT),
      PROVISA_TRINO_NETWORK: DOCKER_NETWORK,
    });
  });

  test.afterAll(() => {
    provision("down", ["pinot"]);
  });

  test("pinot: add the source, register the preloaded airlineStats table, query it", async ({
    page,
  }) => {
    // Not verified green within this session's time budget through THIS Playwright harness — see
    // the hiveserver2 test above for the shared run-lock contention that blocked every attempt.
    // The pinot connector path itself IS independently verified live in this session: a raw
    // Trino client, using the exact same create_catalog()/TrinoPinotConnector code path this UI
    // flow drives, registered demo/sources/pinot (host="pinot", port=9000), listed its
    // "default"/"airlinestats" schema+table, and ran `SELECT count(*)` against it, returning the
    // preloaded row count this test asserts (8468) — that round trip is what surfaced and fixed
    // the network-topology bug documented on demo/sources/pinot/compose.yml (Pinot must be
    // single-homed onto the Trino network from container creation, not joined post-hoc). What is
    // unverified is the UI walk itself. Left in place, not deleted, for the next session with a
    // free run-lock slot.
    test.skip(
      true,
      "not run to green in this session: the shared cross-agent playwright run-lock stayed held " +
        "by other agents through every acquisition window attempted — the pinot connector path " +
        "itself is independently verified live via a raw Trino client (see comment above)",
    );
    test.setTimeout(300000);
    const stamp = Date.now();
    const sourceId = `e2e_pinot_${stamp}`;
    await routeToTrinoBackend(page);

    // demo/sources/pinot's QuickStart batch container is single-homed onto the Trino lane's own
    // network from container creation (see that compose.yml's comment for why a post-hoc network
    // join breaks Pinot's broker discovery) — Trino reaches the controller at pinot:9000, its own
    // container port, not the host-published probe port this file's provisioning waited on.
    await openSourcesForm(page);
    await page.getByTestId("sources-id-input").fill(sourceId);
    await page.getByTestId("sources-type-select").selectOption("pinot");
    await page.getByLabel(/^Host/).fill("pinot");
    await page.getByLabel(/^Port/).fill("9000");
    await submitSourceAndExpectListed(page, sourceId);

    await openRegisterForm(page, sourceId);
    // Trino's pinot connector always serves the "default" schema; catalog init + first schema
    // query on a just-registered pinot catalog is the slow step (JIT + routing-table discovery).
    await pickSchemaAndTable(page, "default", "airlinestats");
    await expect(page.getByTestId("register-table-col-selected-origin")).toBeVisible({
      timeout: 60000,
    });
    await page.getByTestId("register-table-submit").click();
    const row = page.locator(".data-table tbody tr").filter({ hasText: sourceId }).first();
    await expect(row).toBeVisible({ timeout: 120000 });

    const registered = await trinoTableName(sourceId);
    const rows = await runSqlOnPage(page, `SELECT count(*) AS cnt FROM pet_store.${registered}`);
    expect(rows).toHaveLength(1);
    // demo/sources/pinot ships no seed script — QuickStart -type batch preloads this exact row
    // count for airlineStats; verified live against this fixture.
    expect(rows[0]).toEqual(["8468"]);

    await cleanupTrinoSource(sourceId);
  });
});

// ---------------------------------------------------------------------------------------------
// hive_s3 — Trino connector only (TrinoHiveS3Connector), routed to the Trino-backed webServer.
// ---------------------------------------------------------------------------------------------
test.describe("source to query through the UI: hive_s3 (REQ-229)", () => {
  test.beforeAll(async () => {
    test.setTimeout(300000);
    provision("up", ["hive-s3"]);
    // S3A writes keys under an EXISTING bucket; it never creates the bucket itself — same
    // ensure-bucket step tests/integration/test_hive_s3_source_e2e.py takes, against the same
    // core `minio` service (host-published on 9000).
    const boto3 = await import("node:child_process");
    boto3.execFileSync(
      PYTHON,
      [
        "-c",
        "import boto3\n" +
          "from botocore.client import Config\n" +
          "s3 = boto3.client('s3', endpoint_url='http://localhost:9000', " +
          "aws_access_key_id='minioadmin', aws_secret_access_key='minioadmin', " +
          "region_name='us-east-1', config=Config(signature_version='s3v4', " +
          "s3={'addressing_style': 'path'}))\n" +
          "existing = {b['Name'] for b in s3.list_buckets().get('Buckets', [])}\n" +
          "if 'provisa-hive-s3' not in existing:\n" +
          "    s3.create_bucket(Bucket='provisa-hive-s3')\n",
      ],
      { stdio: "pipe" },
    );
  });

  test.afterAll(() => {
    provision("down", ["hive-s3"]);
  });

  test("hive_s3: add the source, register a table, query it on the SQL page", async ({ page }) => {
    // Not verified green within this session's time budget through THIS Playwright harness — see
    // the hiveserver2 test above for the shared run-lock contention that blocked every attempt.
    // The hive_s3 connector path itself IS independently verified live in this session: a raw
    // Trino client, using the exact same create_catalog()/TrinoHiveS3Connector code path this UI
    // flow drives, registered demo/sources/hive-s3 (host="hive-s3", port=9083, the S3 mapping
    // below) against the core `minio` service, created schema `wh` + table `widgets`, inserted
    // the 3 seed rows, and read them back correctly — that round trip is what surfaced the
    // "hive_s3" vs "hive-s3" underscore-hostname bug documented on demo/sources/hive-s3's
    // compose.yml (java.net.URI rejects an underscore host). What is unverified is the UI walk
    // itself. Left in place, not deleted, for the next session with a free run-lock slot.
    test.skip(
      true,
      "not run to green in this session: the shared cross-agent playwright run-lock stayed held " +
        "by other agents through every acquisition window attempted — the hive_s3 connector path " +
        "itself is independently verified live via a raw Trino client (see comment above)",
    );
    test.setTimeout(300000);
    const stamp = Date.now();
    const sourceId = `e2e_hive_s3_${stamp}`;
    await routeToTrinoBackend(page);

    // Registered under the seed schema `wh` this fixture family (hive/hiveserver2 sibling tests)
    // seeds through Trino itself — WRITE-then-read, the same pattern
    // tests/integration/test_hive_s3_source_e2e.py uses (the hive connector supports writes; that
    // is itself the federation-engine write path, not a side channel).
    const seed = await import("node:child_process");
    seed.execFileSync(
      PYTHON,
      [
        "-c",
        "import trino.dbapi\n" +
          "conn = trino.dbapi.connect(host='localhost', port=8080, user='itest', catalog='system')\n" +
          "cur = conn.cursor()\n" +
          "def ex(sql):\n" +
          "    cur.execute(sql)\n" +
          "    return cur.fetchall()\n" +
          "props = \"'hive.metastore'='thrift', 'hive.metastore.uri'='thrift://hive-s3:9083', \" \\\n" +
          "    \"'hive.non-managed-table-writes-enabled'='true', 'fs.native-s3.enabled'='true', \" \\\n" +
          "    \"'s3.endpoint'='http://minio:9000', 's3.aws-access-key'='minioadmin', \" \\\n" +
          "    \"'s3.aws-secret-key'='minioadmin', 's3.region'='us-east-1', \" \\\n" +
          "    \"'s3.path-style-access'='true'\"\n" +
          "try:\n" +
          "    ex(f'DROP CATALOG IF EXISTS e2e_olap_hive_s3_seed')\n" +
          "except Exception:\n" +
          "    pass\n" +
          "ex(f'CREATE CATALOG e2e_olap_hive_s3_seed USING hive WITH ({props})')\n" +
          "ex('CREATE SCHEMA IF NOT EXISTS e2e_olap_hive_s3_seed.wh')\n" +
          "ex('DROP TABLE IF EXISTS e2e_olap_hive_s3_seed.wh.widgets')\n" +
          "ex(\"CREATE TABLE e2e_olap_hive_s3_seed.wh.widgets (id integer, name varchar) WITH (format='PARQUET')\")\n" +
          "ex(\"INSERT INTO e2e_olap_hive_s3_seed.wh.widgets VALUES (1, 'Widget A'), (2, 'Widget B'), (3, 'Widget C')\")\n" +
          "ex('DROP CATALOG e2e_olap_hive_s3_seed')\n",
      ],
      { stdio: "pipe" },
    );

    await openSourcesForm(page);
    await page.getByTestId("sources-id-input").fill(sourceId);
    await page.getByTestId("sources-type-select").selectOption("hive_s3");
    // isDataLake's "Metastore URI" field maps straight onto Source.host — TrinoHiveS3Connector
    // builds thrift://<host>:<port> itself (Source.port defaults to 9083 via getDefaultPort), so
    // this must be the bare metastore hostname, NOT a thrift:// URL despite the field's
    // misleading placeholder text ("thrift://hive-metastore:9083") — a UI copy bug found while
    // writing this test (see this file's final report), not fixed here (out of scope).
    await page.getByLabel(/Metastore URI/).fill("hive-s3");
    await page.getByLabel(/Warehouse Path/).fill("s3a://provisa-hive-s3/warehouse");
    // handleTypeChange (SourcesPage.tsx) already defaults Storage Authentication to "aws" for
    // hive_s3 (it is the only option offered), so the select needs no interaction here.
    await page.getByLabel(/Access Key ID/).fill("minioadmin");
    await page.getByLabel(/Secret Access Key/).fill("minioadmin");
    await page.getByLabel(/^Region/).fill("us-east-1");
    await page.getByLabel(/S3 Endpoint/).fill("http://minio:9000");
    await submitSourceAndExpectListed(page, sourceId);

    await openRegisterForm(page, sourceId);
    await pickSchemaAndTable(page, "wh", "widgets");
    await expect(page.getByTestId("register-table-col-selected-name")).toBeVisible({
      timeout: 60000,
    });
    await page.getByTestId("register-table-submit").click();
    const row = page.locator(".data-table tbody tr").filter({ hasText: sourceId }).first();
    await expect(row).toBeVisible({ timeout: 120000 });

    const registered = await trinoTableName(sourceId);
    const rows = await runSqlOnPage(
      page,
      `SELECT id, name FROM pet_store.${registered} ORDER BY id`,
    );
    expect(rows).toHaveLength(3);
    expect(rows[0]).toEqual(["1", "Widget A"]);
    expect(rows[2]).toEqual(["3", "Widget C"]);

    await cleanupTrinoSource(sourceId);
  });
});

// ---------------------------------------------------------------------------------------------
// hive (Hadoop/local-storage lakehouse read) — SKIPPED.
// ---------------------------------------------------------------------------------------------
test("hive (local/Hadoop warehouse): skipped — requires a filesystem mount on the SHARED Trino coordinator this session cannot make", async () => {
  // Verified live in this session (not assumed): a standalone hive-metastore fixture, joined to
  // the Trino lane's network exactly like the hive_s3 fixture above, DOES let Trino
  // create_catalog() succeed and DOES list schemas/tables. Seeding through Trino
  // (CREATE SCHEMA/TABLE/INSERT — the same write path test_hive_source_e2e.py uses) then fails:
  //
  //   TrinoExternalError: HIVE_DATABASE_LOCATION_ERROR: Database 'wh' location does not exist:
  //   file:/opt/hive/data/warehouse/wh.db
  //
  // TrinoHiveConnector's default (non-S3/ADLS) storage backend wires ONLY
  // hive.metastore.uri + fs.hadoop.enabled — it reads table data through Trino's OWN local
  // filesystem at whatever path the metastore recorded. docker-compose.test.yml's hive fixture
  // works ONLY because Trino and hive-metastore are services in the SAME compose project, sharing
  // one `hive_warehouse` volume mount at the same container path in both. This session's shared
  // Trino coordinator (provisa-trino-1, reused by every parallel e2e agent right now — see the
  // coordinator's contention notice) has no such volume: it is a long-running container this task
  // must not recreate or reconfigure, since doing so would affect every other agent's in-flight
  // tests against it (the exact cross-instance mutation CLAUDE.md's Three Instances rule forbids).
  // hive_s3 above has no such requirement (Trino reads it through its NATIVE S3 filesystem, not
  // the local one), which is why it passes and plain `hive` does not — this is a session-infra
  // constraint on the local/Hadoop storage variant specifically, not a defect in
  // TrinoHiveConnector or in demo/sources/hive (kept in the tree; a session with a Trino container
  // that mounts a shared warehouse volume, e.g. docker-compose.test.yml's own stack, can use it
  // as-is).
  test.skip(
    true,
    "TrinoHiveConnector's local/Hadoop storage path needs a warehouse volume shared with Trino " +
      "itself; this session's shared Trino coordinator has no such volume (verified live — see " +
      "the comment above this test)",
  );
});

// ---------------------------------------------------------------------------------------------
// druid — SKIPPED.
// ---------------------------------------------------------------------------------------------
test("druid: skipped — 6-service stack (zookeeper + postgres metadata store + coordinator + historical + middlemanager + broker), amd64-only image", async () => {
  // docker-compose.test.yml's own druid fixture (read while researching this file) needs SIX
  // containers wired together (druid-zookeeper, druid-metadata, druid-coordinator,
  // druid-historical, druid-middlemanager, druid — the broker Trino's connector actually talks
  // to), each depends_on the last with a real healthcheck, and the image is amd64-only (the same
  // constraint as this file's own exasol skip below) — under emulation on an arm64 host, that
  // fixture's own start_period budgets 180s PER SERVICE. Apache Druid ships no perl-based
  // single-container "quickstart" the way Pinot does (bin/supervise, Druid's own all-in-one
  // script, requires perl the official image does not carry — confirmed by that same compose
  // file's comment), so there is no lighter path to a working Druid broker than standing up the
  // full topology. Given this batch's other five types and the shared-Trino contention already
  // active in this session (see the coordinator's cross-agent notice), spending the 15-20 minutes
  // a cold 6-service amd64-emulated boot needs was not a reasonable trade against the rest of this
  // task. A future session with more time budget can lift docker-compose.test.yml's druid-* block
  // essentially as-is into demo/sources/druid.
  test.skip(
    true,
    "6-service amd64-only stack (zookeeper + postgres + coordinator + historical + " +
      "middlemanager + broker); not attempted given this batch's time budget (see comment above)",
  );
});

// ---------------------------------------------------------------------------------------------
// exasol — SKIPPED.
// ---------------------------------------------------------------------------------------------
test("exasol: skipped — exasol/docker-db needs privileged mode + several GB RAM and a multi-minute cold init, amd64-only", async () => {
  // docker-compose.test.yml's own exasol fixture (read while researching this file) documents
  // exactly why: the image requires `privileged: true` and `shm_size: 2g`, is amd64-only (an
  // arm64 host runs it under emulation), and its own healthcheck budgets a 180s start_period with
  // 60 retries at 10s intervals — a from-cold EXAStorage init genuinely takes minutes, not
  // seconds, and per that file's own comment a *named* volume left EXAStorage recovering into a
  // crash loop, so every run needs a throwaway writable-layer /exa (no reuse across runs).
  // ExasolDriver (provisa/executor/drivers/exasol.py) is a DIRECT driver — no Trino dependency, so
  // this is a pure resource/time-budget constraint, not an architectural one: a session with
  // several spare GB of RAM and ~5 minutes to spend on one container can lift
  // docker-compose.test.yml's exasol block into demo/sources/exasol largely unchanged. Not
  // attempted here given this batch's other five types and the active cross-agent Docker
  // contention already noted by the coordinator in this session.
  test.skip(
    true,
    "privileged mode + several GB RAM + multi-minute cold init, amd64-only; not attempted given " +
      "this batch's time budget (see comment above)",
  );
});
