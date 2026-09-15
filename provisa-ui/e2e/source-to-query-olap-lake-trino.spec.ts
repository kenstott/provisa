// Copyright (c) 2026 Kenneth Stott
// Canary: 9a1e4f2c-7b3d-4a6e-9c5f-1d8b6e2a4f0c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1671/REQ-1763 fanout, Trino-only half: pinot, hive_s3, hive, druid — the four
// source-to-query-olap-lake.spec.ts cases that have NO direct driver, only a Trino connector
// (TrinoPinotConnector/TrinoHiveS3Connector/TrinoHiveConnector/TrinoDruidConnector,
// provisa/federation/trino_connectors.py). register_source() is a no-op on the native/DuckDB
// engine for these types (EngineBackend.register_source, provisa/federation/backend.py), so the
// schema dropdown never populates against it — every test here routes at the network layer to the
// Trino-backed webServer via routeToTrinoBackend(), the same technique splunk-connector.spec.ts
// and sharepoint-connector.spec.ts use for the identical reason.
//
// REQ-1763 amendment (2026-09-15): this file WAS source-to-query-olap-lake.spec.ts's second half.
// It is split out here, and listed in playwright.config.ts's TRINO_SPECS, because that file's own
// "core" project selected it (TRINO_SPECS/testIgnore excludes by FILE, and the whole file lived in
// the default "core" testMatch) while ui-e2e-core.yml hardcodes PROVISA_E2E_LANE=core — which
// means RUNS_TRINO is false there and the Trino-backed webServer this file's tests need never
// starts. The four tests below were reachable only by accident: they passed whenever someone
// verified them locally with LANE left at its default "all" (which does start the Trino backend),
// never against the literal env the real CI core-lane workflow sets. Splitting the Trino-only
// cases into their own file — sharing every helper source-to-query-olap-lake.spec.ts's own header
// comment describes — lets TRINO_SPECS exclude exactly these tests from "core" and include them in
// "trino" at file granularity, without touching hiveserver2/exasol (which stay in the sibling file:
// they have real DIRECT drivers and correctly belong in "core").
//
//   Run: cd provisa-ui && PROVISA_E2E_LANE=trino npx playwright test --project=trino \
//        source-to-query-olap-lake-trino.spec.ts
//        (matches ui-e2e-trino.yml's own invocation — the "trino" project's testMatch is
//        TRINO_SPECS, so no extra flag is needed to select this file once it is a TRINO_SPECS entry)
//
//   - druid is CI-ONLY (REQ-1763's RUNNING_IN_CI pattern, same as saphana/greenplum in
//     source-to-query-generic-rdbms.spec.ts): apache/druid is amd64-only, unbootable under arm64
//     emulation, so it still skips on Apple Silicon local dev but runs for real on
//     ui-e2e-trino.yml's ubuntu-latest runner (a genuine amd64 host).
//   - plain `hive` (Hadoop/local-storage lakehouse read) needs docker-compose.core.yml's own live
//     `trino` container to resolve the hive_warehouse volume it shares with demo/sources/hive's
//     hive-metastore (resolveHiveWarehouseVolume() below) — guaranteed live here because the
//     "trino" project's own webServer brings docker-compose.core.yml's trino up before any test
//     runs, unlike the former "core"-lane placement this test degraded to a skip under.
//
// Ports: reuses source-to-query-olap-lake.spec.ts's own 369xx range (pinot/druid ports) — the two
// files never run in the same project/lane, so there is no port collision.

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
const PREFIX = "provisa-s2q-olap-lake-trino";

// The e2e harness's shared trino/postgres/minio compose project's default network — the same
// network splunk-connector.spec.ts joins its own container to, and for the same reason: Trino
// must resolve the source container by name to build a live catalog.
const DOCKER_NETWORK = process.env.PROVISA_E2E_DOCKER_NETWORK ?? "provisa_default";

const E2E_PINOT_CONTROLLER_PORT = Number(process.env.PROVISA_DEMO_PINOT_CONTROLLER_PORT ?? 36900);

// druid only actually runs in CI (ubuntu-latest is a real amd64 Linux host) — the image is
// amd64-only and unbootable under arm64 emulation (see that test's own comment below), the same
// constraint source-to-query-generic-rdbms.spec.ts's RUNNING_IN_CI gate documents for
// saphana/greenplum. process.env.CI is set to "true" by ui-e2e-trino.yml specifically.
const RUNNING_IN_CI = process.env.CI === "true";
const E2E_DRUID_COORD_PORT = Number(process.env.PROVISA_DEMO_DRUID_COORD_PORT ?? 36920);
const E2E_DRUID_BROKER_PORT = Number(process.env.PROVISA_DEMO_DRUID_BROKER_PORT ?? 36921);

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
 * technique splunk-connector.spec.ts uses, for the identical reason (these connectors have no
 * DuckDB-reachable path at all). `**` (not `*`) is required: Playwright compiles a single star to
 * a pattern that stops at the first `/`, which never matches `/admin/graphql`. */
async function routeToTrinoBackend(page: Page): Promise<void> {
  for (const prefix of ["/admin", "/data", "/query", "/health"]) {
    await page.route(`${UI_URL}${prefix}**`, (route) => {
      route.continue({ url: route.request().url().replace(UI_URL, TRINO_BACKEND_URL) });
    });
  }
}

// REQ-1763 amendment: demo/sources/hive is its OWN compose project (provision.py isolates it from
// docker-compose.core.yml on purpose), so its hive-metastore container cannot share a volume with
// the core `trino` service just by declaring the same volume KEY — Docker Compose prefixes volume
// names by project, so two different projects with a same-named volume key still get two different
// physical volumes. Never guessed/hardcoded here (no fallback — CLAUDE.md): resolved from the
// LIVE core Trino container's own mount, which is authoritative regardless of what project name
// docker-compose.core.yml ends up running under (no consumer of that file passes `-p`, but this
// makes the fix correct even if that ever changes).
function resolveHiveWarehouseVolume(): string {
  const cid = execFileSync(
    "docker",
    ["compose", "-f", path.join(ROOT, "docker-compose.core.yml"), "ps", "-q", "trino"],
    { cwd: ROOT, encoding: "utf8" },
  ).trim();
  if (!cid) {
    throw new Error(
      "docker-compose.core.yml's trino container is not running — cannot resolve the live " +
        "hive_warehouse volume to share with demo/sources/hive's hive-metastore",
    );
  }
  const mounts = JSON.parse(
    execFileSync("docker", ["inspect", cid, "--format", "{{json .Mounts}}"], {
      encoding: "utf8",
    }),
  ) as { Destination: string; Name?: string }[];
  const mount = mounts.find((m) => m.Destination === "/opt/hive/data/warehouse");
  if (!mount?.Name) {
    throw new Error(
      "docker-compose.core.yml's trino container has no volume mounted at " +
        "/opt/hive/data/warehouse — cannot share it with demo/sources/hive",
    );
  }
  return mount.Name;
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

/** Poll availableTables through the Trino-backed backend directly (bypassing the UI picker)
 * until the named table shows up, or fail after `timeoutMs`. Some Trino connectors — Pinot's
 * in particular — populate their table-list cache from the underlying system's own async
 * discovery (Helix external view convergence for a freshly-loaded QuickStart fixture), which can
 * still be converging in the seconds right after `create_catalog()` runs. That is a cold-start
 * race in the connector, not in Provisa's introspection code (verified live: identical
 * information_schema query against the identical catalog name returns 0 rows immediately after
 * catalog creation, then the expected rows once Pinot's side has caught up) — polling here masks
 * that race the same way a production deployment would never hit it (a real Pinot source is
 * already stable by the time someone registers it). */
async function waitForTrinoTable(
  sourceId: string,
  schemaName: string,
  tableName: string,
  timeoutMs = 150000,
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const res = await trinoGql(
      `query($sourceId: String!, $schemaName: String!) {
        availableTables(sourceId: $sourceId, schemaName: $schemaName) { name }
      }`,
      { sourceId, schemaName },
    );
    const names = (res.data?.availableTables ?? []) as { name: string }[];
    if (names.some((t) => t.name === tableName)) return;
    if (Date.now() > deadline) {
      throw new Error(
        `waitForTrinoTable: ${schemaName}.${tableName} never appeared for ${sourceId} within ${timeoutMs}ms`,
      );
    }
    await new Promise((r) => setTimeout(r, 3000));
  }
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
    // REQ-1751: NOT a Provisa bug — a cold-start race in Trino's OWN Pinot connector, live-traced
    // by exec'ing into the Trino container and running available_tables()'s exact
    // information_schema query against the identical catalog name a failing run had just created:
    // 0 rows immediately after CREATE CATALOG, then the expected "airlinestats" correctly listed
    // minutes later against that same catalog with nothing else touching it in between. The
    // connector's table-list cache is populated from the Pinot controller's Helix external view,
    // which had not yet converged for this freshly-loaded QuickStart fixture at the moment
    // create_catalog() ran. waitForTrinoTable() below polls past that convergence window directly
    // (bypassing the UI) before driving the picker, so the UI assertions below see a source that
    // is already stable — exactly what a production Pinot registration would see.
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
    // See REQ-1751 above: wait out the Trino Pinot connector's own cold-start discovery race
    // before touching the UI picker, so a slow-converging fixture doesn't masquerade as a
    // Provisa introspection bug.
    await waitForTrinoTable(sourceId, "default", "airlinestats");

    await openRegisterForm(page, sourceId);
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
    // demo/sources/pinot ships no seed script — QuickStart batch loads airlineStats' segments
    // asynchronously (same Helix-convergence process REQ-1751 above works around for the table's
    // very existence), so the row count observed here depends on how much of the batch has
    // ingested by query time and is NOT a fixed value — live traces of this fixture have seen
    // 8468 and 9746 for the identical table with no seed-script involvement at all. Assert real
    // data landed, not a specific count.
    expect(Number(rows[0][0])).toBeGreaterThan(0);

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
    // REQ-1752: ROOT CAUSE FOUND AND FIXED — a real UI bug, not a systemic introspection issue
    // (unlike pinot above, which was a third-party connector cold-start race). SourceFormFields.tsx's
    // Region field displayed a fallback default (`authFields.region ?? "us-east-1"`) that was never
    // actually written into authFields. Typing that exact same string is a same-value write: React's
    // controlled-input reconciliation drops it, onChange never fires, authFields.region stays unset.
    // mappingJson then omits "region" entirely, _hive_s3_props (trino_connectors.py) raises
    // ValueError, and _register_source_on_engine's best-effort catch (schema_common.py) swallows
    // it — the source record is created but its Trino catalog never is, so the schema picker sits
    // empty forever with zero visible error. Live-traced this session by instrumenting
    // create_catalog/_register_source_on_engine and the form's own submit handler (all reverted).
    // Fixed by making the field's displayed default a placeholder instead of a phantom value (see
    // SourceFormFields.tsx). (Separately, this test's own seed script had two more real bugs, both
    // now fixed: CREATE CATALOG ... WITH (...) quoted property KEYS as string literals instead of
    // double-quoted identifiers, which trinodb/trino:481 rejects with SYNTAX_ERROR; and DROP TABLE
    // not removing the underlying S3 objects for a non-managed-writes table, so a prior interrupted
    // seed poisoned the next run with HIVE_PATH_ALREADY_EXISTS.)
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
          // Property KEYS are Trino <identifier>s, not string literals — a dotted key like
          // hive.metastore needs a double-quoted identifier ("hive.metastore" = 'thrift'), not a
          // single-quoted string ('hive.metastore'='thrift'), which trinodb/trino:481 rejects with
          // SYNTAX_ERROR "mismatched input ''hive.metastore''. Expecting: <identifier>".
          'props = \'"hive.metastore"=\\\'thrift\\\', "hive.metastore.uri"=\\\'thrift://hive-s3:9083\\\', \' \\\n' +
          '    \'"hive.non-managed-table-writes-enabled"=\\\'true\\\', "fs.native-s3.enabled"=\\\'true\\\', \' \\\n' +
          '    \'"s3.endpoint"=\\\'http://minio:9000\\\', "s3.aws-access-key"=\\\'minioadmin\\\', \' \\\n' +
          '    \'"s3.aws-secret-key"=\\\'minioadmin\\\', "s3.region"=\\\'us-east-1\\\', \' \\\n' +
          '    \'"s3.path-style-access"=\\\'true\\\'\'\n' +
          "try:\n" +
          "    ex(f'DROP CATALOG IF EXISTS e2e_olap_hive_s3_seed')\n" +
          "except Exception:\n" +
          "    pass\n" +
          "ex(f'CREATE CATALOG e2e_olap_hive_s3_seed USING hive WITH ({props})')\n" +
          "ex('CREATE SCHEMA IF NOT EXISTS e2e_olap_hive_s3_seed.wh')\n" +
          "ex('DROP TABLE IF EXISTS e2e_olap_hive_s3_seed.wh.widgets')\n" +
          // DROP TABLE removes the metastore entry but NOT the underlying S3 objects for a
          // non-managed-writes table (hive.non-managed-table-writes-enabled=true means Trino
          // treats the table as externally owned even when non-ACID/"managed" per the metastore —
          // see _hive_metastore_props' own comment above). A prior run's interrupted seed (or this
          // test's own beforeAll re-running after a failure) left the S3 path populated, and the
          // next CREATE TABLE then died HIVE_PATH_ALREADY_EXISTS — reproduced live in this
          // session. Clearing the path explicitly makes the seed idempotent regardless of what a
          // previous attempt left behind.
          "import boto3\n" +
          "from botocore.client import Config\n" +
          "s3 = boto3.client('s3', endpoint_url='http://localhost:9000', " +
          "aws_access_key_id='minioadmin', aws_secret_access_key='minioadmin', " +
          "region_name='us-east-1', config=Config(signature_version='s3v4', " +
          "s3={'addressing_style': 'path'}))\n" +
          "for page in s3.get_paginator('list_objects_v2').paginate(" +
          "Bucket='provisa-hive-s3', Prefix='warehouse/wh.db/widgets/'):\n" +
          "    for obj in page.get('Contents', []):\n" +
          "        s3.delete_object(Bucket='provisa-hive-s3', Key=obj['Key'])\n" +
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
// hive (Hadoop/local-storage lakehouse read) — Trino connector only (TrinoHiveConnector), routed
// to the Trino-backed webServer, exactly like hive_s3/druid/pinot above. docker-compose.core.yml's
// own `trino` container has to be LIVE so resolveHiveWarehouseVolume() can read its actual
// hive_warehouse mount and hand it to demo/sources/hive's own compose project (see that project's
// compose.yml module comment for the full reasoning — REQ-1763 amendment). Under the "trino"
// project this is guaranteed: the Trino-backed webServer above brings docker-compose.core.yml's
// trino up before any test in this file runs, so resolveHiveWarehouseVolume() always finds it.
// ---------------------------------------------------------------------------------------------
test.describe("source to query through the UI: hive (REQ-1763)", () => {
  let hiveWarehouseVolume: string | null = null;

  test.beforeAll(() => {
    test.setTimeout(180000);
    hiveWarehouseVolume = resolveHiveWarehouseVolume();
    provision("up", ["hive"], { PROVISA_HIVE_WAREHOUSE_VOLUME: hiveWarehouseVolume });
  });

  test.afterAll(() => {
    if (!hiveWarehouseVolume) return;
    provision("down", ["hive"]);
  });

  test("hive: add the source, register a table, query it on the SQL page", async ({ page }) => {
    test.setTimeout(300000);
    const stamp = Date.now();
    const sourceId = `e2e_hive_${stamp}`;
    await routeToTrinoBackend(page);

    // Registered under the seed schema `wh`, same as the hive_s3/hiveserver2 siblings — seeds
    // THROUGH Trino itself (write-then-read, the same pattern test_hive_source_e2e.py and the
    // hive_s3 test above use), via a throwaway catalog dropped once seeding is done. The real
    // source registered through the UI below points its OWN catalog at the same metastore, which
    // sees the same physical warehouse data because hive-metastore and trino now share one volume.
    const seed = await import("node:child_process");
    seed.execFileSync(
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
          // Property KEYS are Trino <identifier>s, not string literals (see hive_s3's identical
          // note above) — "hive.metastore"='thrift', not 'hive.metastore'='thrift'.
          'props = \'"hive.metastore"=\\\'thrift\\\', "hive.metastore.uri"=\\\'thrift://hive:9083\\\', \' \\\n' +
          '    \'"fs.hadoop.enabled"=\\\'true\\\'\'\n' +
          "try:\n" +
          "    ex('DROP CATALOG IF EXISTS e2e_olap_hive_seed')\n" +
          "except Exception:\n" +
          "    pass\n" +
          "ex(f'CREATE CATALOG e2e_olap_hive_seed USING hive WITH ({props})')\n" +
          // Retry CREATE SCHEMA while the freshly-created catalog's metastore connection warms up
          // (test_hive_source_e2e.py's own pattern).
          "deadline = time.monotonic() + 60\n" +
          "last_exc = None\n" +
          "while time.monotonic() < deadline:\n" +
          "    try:\n" +
          "        ex('CREATE SCHEMA IF NOT EXISTS e2e_olap_hive_seed.wh')\n" +
          "        break\n" +
          "    except trino.exceptions.TrinoQueryError as exc:\n" +
          "        last_exc = exc\n" +
          "        time.sleep(3)\n" +
          "else:\n" +
          "    raise RuntimeError(f'hive CREATE SCHEMA never succeeded: {last_exc!r}')\n" +
          "ex('DROP TABLE IF EXISTS e2e_olap_hive_seed.wh.widgets')\n" +
          "ex(\"CREATE TABLE e2e_olap_hive_seed.wh.widgets (id integer, name varchar) WITH (format='PARQUET')\")\n" +
          "ex(\"INSERT INTO e2e_olap_hive_seed.wh.widgets VALUES (1, 'Widget A'), (2, 'Widget B'), (3, 'Widget C')\")\n" +
          "ex('DROP CATALOG e2e_olap_hive_seed')\n",
      ],
      { stdio: "pipe" },
    );

    await openSourcesForm(page);
    await page.getByTestId("sources-id-input").fill(sourceId);
    await page.getByTestId("sources-type-select").selectOption("hive");
    // isDataLake's "Metastore URI" field maps straight onto Source.host (same UI shape/quirk as
    // hive_s3 above) — the bare network alias, not a thrift:// URL. "Warehouse Path" is required
    // by the form for both hive and hive_s3 but TrinoHiveConnector.details() (trino_connectors.py)
    // never reads Source.database for the local/Hadoop storage path — filled only to satisfy the
    // required field.
    await page.getByLabel(/Metastore URI/).fill("hive");
    await page.getByLabel(/Warehouse Path/).fill("/opt/hive/data/warehouse");
    // Storage Authentication defaults to "none" (instance-role/local) for plain hive, which is
    // correct here — left untouched.
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
// druid — Trino connector only (TrinoDruidConnector), routed to the Trino-backed webServer.
// CI-only: apache/druid is amd64-only (linux/amd64 platform pin, same as exasol in the sibling
// file); under QEMU emulation on an arm64 host the 6-service topology's cold boot realistically
// exceeds a reasonable single e2e budget. ui-e2e-trino.yml's ubuntu-latest runner is a genuine
// amd64 host, so demo/sources/druid runs for real there (RUNNING_IN_CI gate) — locally it skips.
// ---------------------------------------------------------------------------------------------
test.describe("source to query through the UI: druid (REQ-1763)", () => {
  test.beforeAll(() => {
    // Mirrors source-to-query-generic-rdbms.spec.ts's RUNNING_IN_CI pattern: no test.skip()
    // signal inside beforeAll (a plain early return instead) — the per-test test.skip() below is
    // what reports the actual skip; this only avoids provisioning a fixture nothing will use.
    if (!RUNNING_IN_CI) return;
    // Apache Druid ships no perl-based single-container "quickstart" the way Pinot does, so this
    // is the full upstream multi-container layout (zookeeper + postgres metadata store +
    // coordinator + historical + middlemanager + broker) — demo/sources/druid/compose.yml lifts
    // docker-compose.test.yml's own druid-* block verbatim. Its own healthchecks budget up to
    // 180s start_period per service; this beforeAll's timeout has to cover the whole serial chain.
    test.setTimeout(900000);
    provision("up", ["druid"], {
      PROVISA_DEMO_DRUID_COORD_PORT: String(E2E_DRUID_COORD_PORT),
      PROVISA_DEMO_DRUID_BROKER_PORT: String(E2E_DRUID_BROKER_PORT),
      PROVISA_TRINO_NETWORK: DOCKER_NETWORK,
    });
    // prime.py ingests through Druid's own native batch API and polls the broker until the
    // segment is loaded and queryable — see that script's module doc.
    execFileSync(
      PYTHON,
      [path.join(ROOT, "demo", "sources", "druid", "prime.py")],
      {
        stdio: "inherit",
        env: {
          ...process.env,
          PROVISA_DEMO_DRUID_COORD_PORT: String(E2E_DRUID_COORD_PORT),
          PROVISA_DEMO_DRUID_BROKER_PORT: String(E2E_DRUID_BROKER_PORT),
        },
      },
    );
  });

  test.afterAll(() => {
    if (!RUNNING_IN_CI) return;
    provision("down", ["druid"]);
  });

  test("druid: add the source, register the widgets datasource, query it on the SQL page", async ({
    page,
  }) => {
    test.skip(
      !RUNNING_IN_CI,
      "apache/druid is amd64-only; the 6-service topology's cold boot under QEMU emulation " +
        "realistically exceeds a reasonable single e2e budget locally — runs for real in CI " +
        "(ubuntu-latest is a genuine amd64 host)",
    );
    test.setTimeout(300000);
    const stamp = Date.now();
    const sourceId = `e2e_druid_${stamp}`;
    await routeToTrinoBackend(page);

    // Named "druid" in compose.yml so provision.py's --network join aliases the broker "druid" on
    // Trino's own network too — Trino reaches it at its real container port (8082), not the
    // host-published probe port this file's own provisioning waited on.
    await openSourcesForm(page);
    await page.getByTestId("sources-id-input").fill(sourceId);
    await page.getByTestId("sources-type-select").selectOption("druid");
    await page.getByLabel(/^Host/).fill("druid");
    await page.getByLabel(/^Port/).fill("8082");
    await submitSourceAndExpectListed(page, sourceId);
    await waitForTrinoTable(sourceId, "druid", "widgets");

    await openRegisterForm(page, sourceId);
    await pickSchemaAndTable(page, "druid", "widgets");
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
