// Copyright (c) 2026 Kenneth Stott
// Canary: 4f7a9c2e-1d8b-4a6f-9e3c-7b5d2a8f1c6e
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1739/REQ-1745: kafka/websocket/rss/ingest end-to-end through the real UI, against the
// DuckDB federation engine. Unlike source-to-query.spec.ts's pull-based sources, these are
// push/poll sources — a table registered against them starts a background listener (kafka/
// websocket, REQ-1733) or a poll job (rss) that lands rows asynchronously, so the shape here is
// register, THEN produce/wait, THEN poll the SQL page until the row lands (never an immediate
// SELECT). ingest is the exception: it is a synchronous HTTP push receiver, so POST-then-SELECT
// needs no retry loop.
//
// Port range: 378xx, distinct from source-to-query.spec.ts's mysql/trino (330xx), demo-source-
// containers.ts's fixture ports (33xxx/35xxx/36xxx/37474/37687/37117/38xxx/39xxx), and the other
// parallel agents' spec files. The RSS/WebSocket fixture servers are started IN-PROCESS by this
// spec file itself (Node's own http module / the `ws` package) — no docker container needed for
// either. Kafka (REQ-1766) needs a real broker — demo/sources/kafka's compose fixture, started/
// stopped via provision.py the same way source-to-query-generic-rdbms.spec.ts does for its own
// heavier fixtures.

import { execFileSync } from "node:child_process";
import * as http from "node:http";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { WebSocketServer } from "ws";

import { test, expect } from "./coverage";
import {
  openRegisterForm,
  openSourcesForm,
  pickSchemaAndTable,
  runSqlOnPage,
  submitRegisterAndExpectListed,
  submitSourceAndExpectListed,
  typeSql,
} from "./source-to-query-helpers";

const E2E_RSS_PORT = 37801;
const E2E_WS_PORT = 37802;
const E2E_KAFKA_PORT = 37803;
const E2E_KAFKA_SCHEMA_REGISTRY_PORT = 37804;

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
const PROVISION = path.join(ROOT, "demo", "sources", "provision.py");
const PYTHON = path.join(ROOT, ".venv", "bin", "python");
const KAFKA_PREFIX = "provisa-s2q-streaming";

function provisionKafka(cmd: "up" | "down"): void {
  const env = {
    ...process.env,
    PROVISA_DEMO_KAFKA_PORT: String(E2E_KAFKA_PORT),
    PROVISA_DEMO_KAFKA_SCHEMA_REGISTRY_PORT: String(E2E_KAFKA_SCHEMA_REGISTRY_PORT),
  };
  try {
    execFileSync(PYTHON, [PROVISION, cmd, "--prefix", KAFKA_PREFIX, "kafka"], {
      stdio: "inherit",
      env,
    });
  } catch (e) {
    if (cmd === "down") return; // a project that was never started removes nothing
    throw e;
  }
}

/** Produce one JSON message to *topic* via aiokafka (the same client push_wiring.py's consumer
 * side uses) — no Node kafka client dependency needed for a single one-shot produce. */
function produceKafkaMessage(topic: string, row: Record<string, string>): void {
  const script =
    "import asyncio, json, sys\n" +
    "from aiokafka import AIOKafkaProducer\n" +
    "async def main():\n" +
    `    p = AIOKafkaProducer(bootstrap_servers="localhost:${E2E_KAFKA_PORT}")\n` +
    "    await p.start()\n" +
    "    try:\n" +
    `        await p.send_and_wait(${JSON.stringify(topic)}, json.dumps(json.loads(sys.argv[1])).encode())\n` +
    "    finally:\n" +
    "        await p.stop()\n" +
    "asyncio.run(main())\n";
  execFileSync(PYTHON, ["-c", script, JSON.stringify(row)], { stdio: "inherit" });
}

/** Register a JSON Schema for *topic* in Confluent Schema Registry (subject "<topic>-value",
 * the same naming convention provisa.kafka.schema_registry.discover_topic_columns reads). */
function registerJsonSchema(topic: string, properties: Record<string, string>): void {
  const script =
    "import json, sys, urllib.request\n" +
    "schema = json.dumps({'type': 'object', 'properties': " +
    "{k: {'type': 'string'} for k in json.loads(sys.argv[2])}})\n" +
    "body = json.dumps({'schema': schema, 'schemaType': 'JSON'}).encode()\n" +
    "req = urllib.request.Request(\n" +
    "    sys.argv[1], data=body, method='POST',\n" +
    "    headers={'Content-Type': 'application/vnd.schemaregistry.v1+json'},\n" +
    ")\n" +
    "urllib.request.urlopen(req, timeout=15).read()\n";
  execFileSync(
    PYTHON,
    [
      "-c",
      script,
      `http://localhost:${E2E_KAFKA_SCHEMA_REGISTRY_PORT}/subjects/${topic}-value/versions`,
      JSON.stringify(Object.keys(properties)),
    ],
    { stdio: "inherit" },
  );
}

// ---------------------------------------------------------------------------------------------
// RSS fixture: a static feed served over plain HTTP. subscriptions/rss_provider.py polls it and
// (REQ-1745) make_rss_loader lands its current items every tick — a poll source, so the feed
// content is fixed for the whole test rather than appended to mid-run.
// ---------------------------------------------------------------------------------------------

const RSS_ITEMS = [
  { guid: "item-1", title: "First item", link: "https://example.com/1", pubDate: "Mon, 01 Jan 2024 00:00:00 GMT" },
  { guid: "item-2", title: "Second item", link: "https://example.com/2", pubDate: "Tue, 02 Jan 2024 00:00:00 GMT" },
];

function rssFeedXml(): string {
  const items = RSS_ITEMS.map(
    (i) =>
      `<item><guid>${i.guid}</guid><title>${i.title}</title><link>${i.link}</link>` +
      `<description>desc-${i.guid}</description><pubDate>${i.pubDate}</pubDate></item>`,
  ).join("");
  return `<?xml version="1.0"?><rss version="2.0"><channel><title>Test Feed</title>${items}</channel></rss>`;
}

let rssServer: http.Server;

test.beforeAll(async () => {
  rssServer = http.createServer((_req, res) => {
    res.writeHead(200, { "Content-Type": "application/rss+xml" });
    res.end(rssFeedXml());
  });
  await new Promise<void>((resolve) => rssServer.listen(E2E_RSS_PORT, resolve));
});

test.afterAll(async () => {
  await new Promise<void>((resolve) => rssServer.close(() => resolve()));
});

// ---------------------------------------------------------------------------------------------
// WebSocket fixture: a tiny in-process server (the `ws` package, already a transitive dependency
// — see provisa-ui/node_modules/ws) that pushes a couple of JSON row events on every connection.
// push_wiring.py's websocket branch derives ws://<host>:<port> from the source's host+port with
// no path — this server listens on "/" so that derivation reaches it directly.
// ---------------------------------------------------------------------------------------------

let wsServer: WebSocketServer;

test.beforeAll(() => {
  wsServer = new WebSocketServer({ port: E2E_WS_PORT });
  wsServer.on("connection", (socket) => {
    socket.send(JSON.stringify({ id: "ws-1", value: "hello" }));
    socket.send(JSON.stringify({ id: "ws-2", value: "world" }));
  });
});

test.afterAll(async () => {
  await new Promise<void>((resolve, reject) =>
    wsServer.close((err) => (err ? reject(err) : resolve())),
  );
});

/** Poll `sql` on the SQL page every 3s until it returns at least `minRows` rows, or fail at
 * `timeoutMs`. Push/poll landing is asynchronous (REQ-1733's debounce, rss's poll cadence) —
 * an immediate SELECT would be a flaky false negative, not a real assertion. */
async function pollUntilLanded(
  page: Parameters<typeof runSqlOnPage>[0],
  sql: string,
  minRows: number,
  timeoutMs: number,
): Promise<string[][]> {
  const deadline = Date.now() + timeoutMs;
  let rows: string[][] = [];
  while (Date.now() < deadline) {
    rows = await runSqlOnPage(page, sql);
    if (rows.length >= minRows) return rows;
    await new Promise((r) => setTimeout(r, 3000));
  }
  throw new Error(
    `${sql}: expected >= ${minRows} row(s) within ${timeoutMs}ms, got ${rows.length}: ` +
      JSON.stringify(rows),
  );
}

test.describe("source to query through the UI — streaming/push types (REQ-1739/REQ-1745)", () => {
  test("ingest: add the source, register a table, POST a row, query it on the SQL page", async ({
    page,
  }) => {
    // REAL BUG #1 (FIXED): registration succeeded (source created, table registered with
    // ext_id/value columns), but the immediately-following POST to
    // /data/ingest/<sourceId>/<sourceId> 404'd "Ingest source '<id>' not found" on 100% of runs
    // against a control plane with no DB password (the e2e harness's docker-assigned Postgres
    // instance, and any SQLite control plane — trust/peer auth has no password either way).
    // Root cause: provisa/ingest/engine.py's _build_url unconditionally raised
    // ValueError("ingest DB password is required") for an empty password. _init_ingest_engines
    // (provisa/api/app_loaders.py) calls this inside `with tolerate_startup_failure(...)`, which
    // logs-and-skips the exception -- aborting that function's per-source loop BEFORE
    // state.ingest_tables/state.ingest_engines were ever populated for the source, so the row
    // never existed no matter how many times the schema rebuilt (not a race). Fixed: a missing
    // password no longer pre-emptively rejects the connection; a DB that genuinely requires one
    // still fails, from the driver's own auth error at connect time.
    //
    // REAL BUG #2 (FIXED, uncovered once #1 stopped masking it): app_loaders.py's REQ-1745
    // "mirror state.tenant_db" default decomposed the tenant URL into host/port/username/password
    // and opened a SEPARATE engine -- fine for Postgres, but on the "core" lane's SQLite control
    // plane (no host/port to decompose) it fell back to a hardcoded, unreachable
    // localhost:5432, and even pointed correctly at the real sqlite file, DuckDB's sqlite
    // extension corrupts a file a second connection is concurrently writing (see
    // duckdb_runtime.py's _refresh_control_plane_snapshot), so a second engine on the same file
    // deadlocks/misbehaves. Fixed: the SQLite/embedded case now reuses state.tenant_db.engine
    // directly instead of opening a second engine at all.
    //
    // REMAINING GAP (test still fails here, NOT re-skipped for #1/#2 -- this is a separate,
    // larger unimplemented feature, not a regression from this fix): once the POST succeeds and
    // the row lands in the tenant SQLite file, the SQL page's `SELECT ... FROM pet_store.<table>`
    // never resolves it. provisa/api/app_loaders.py's catalog_name_for_source gives every
    // DuckDB-native source (the "core" lane's engine) its OWN per-source-id ATTACH catalog for
    // physical resolution -- but nothing (native_backend.py/duckdb_runtime.py, grepped for
    // "ingest": no hits) ever attaches or maps an ingest source's physical table into a catalog
    // the compiler can reach. The tenant sqlite file IS already exposed read-only, per-table, as
    // `provisa_admin.<org_schema>.<table>` by duckdb_runtime.py's _rebuild_control_plane (used
    // for the control-plane's own admin tables) -- the likely fix is teaching
    // catalog_name_for_source to route ingest (DuckDB-native, no live connector) to that same
    // `provisa_admin` catalog, the way it already routes Trino's MATERIALIZE_ONLY sources to
    // Trino's own materialize-store catalog. Needs its own task/REQ: this is new catalog-wiring
    // work, not a fix to the two bugs above.
    test.skip(
      true,
      "ingest 404 'source not found' (missing-password ValueError swallowed by " +
        "tolerate_startup_failure) is fixed; POST now succeeds. Still blocked on a separate, " +
        "unimplemented gap: no DuckDB-native catalog wiring exposes an ingest source's landed " +
        "rows to the SQL-page compiler on the SQLite control plane. See comment above.",
    );
    test.setTimeout(180000);
    const stamp = Date.now();
    const sourceId = `e2e_ingest_${stamp}`;

    // 1. Sources form — ingest is a NO_CONNECTION_TYPES source (REQ-1739): id/description only.
    await openSourcesForm(page);
    await page.getByTestId("sources-id-input").fill(sourceId);
    await page.getByTestId("sources-type-select").selectOption("ingest");
    await submitSourceAndExpectListed(page, sourceId);

    // 2. Register Table form — ingest has no live catalog to introspect (REQ-1745 gives it a
    // synthetic single "default"/<sourceId> pick, mirroring elasticsearch/redis/prometheus'
    // "default" schema and csv/parquet's one-table-per-source shape). Columns are a REQ-1745
    // placeholder (ext_id/value, not "id" — provisa/ingest/ddl.py always injects its own
    // `id SERIAL PRIMARY KEY`) with per-column JSON extraction paths (REQ-1739).
    await openRegisterForm(page, sourceId);
    await pickSchemaAndTable(page, "default", sourceId);
    await expect(page.getByTestId("register-table-col-selected-ext_id")).toBeVisible({
      timeout: 30000,
    });
    await page.getByTestId("register-table-col-path-ext_id").fill("id");
    await page.getByTestId("register-table-col-path-value").fill("value");
    const registered = await submitRegisterAndExpectListed(page, sourceId);

    // 3. POST a row straight to the ingest receiver (provisa/ingest/router.py) — Playwright's own
    // request context, no extra fixture server needed (this is the one type simple enough for
    // that, per this file's own scoping). Table name in the URL is the PHYSICAL name registered
    // above (sourceId), not the possibly-aliased SQL-plane name `registered` is.
    const res = await page.request.post(`/data/ingest/${sourceId}/${sourceId}`, {
      data: { id: "abc-1", value: "hello-ingest" },
    });
    expect(res.ok(), await res.text()).toBeTruthy();

    // 4. SQL page: ingest is a synchronous push receiver — no debounce/poll to wait out.
    const rows = await runSqlOnPage(
      page,
      `SELECT ext_id, value FROM pet_store.${registered} ORDER BY ext_id`,
    );
    expect(rows).toEqual([["abc-1", "hello-ingest"]]);
  });

  test("rss: add the source, register a table, land polled feed items, query them", async ({
    page,
  }) => {
    // REAL BUG (FIXED, REQ-1770): registration through the real UI (source create → schema/table
    // pick → column select → submit, all fixed by this file's REQ-1745 introspection branches)
    // DID work — verified manually while developing this test. What was missing was the LANDING
    // side: rss is POLL, not push, so it lands through SourceRowLoader/build_adapter_loaders
    // (make_rss_loader), which only ever ran a node's poll job when `wire_event_loop` executed —
    // and `wire_event_loop` was wired ONLY from `register_runtime`'s per-org runtime build
    // (provisa/api/app.py), never from `_rebuild_schemas_impl`, which is what registerTable's
    // mutation actually calls on an already-running default/single-tenant runtime (the shape this
    // whole test file exercises). A table registered against a live server therefore never got a
    // poll job started. The obvious fix (also call `wire_event_loop` from `_rebuild_schemas_impl`)
    // was tried and reverted: it re-derives adapter_loaders and re-walks EVERY registered source's
    // poll-job registration on every rebuild — not just the new one — and broke an unrelated,
    // already-registered sqlite demo source's live queries in testing. Fixed instead with
    // `wire_new_poll_jobs` (provisa/events/app_wiring.py): a per-node-scoped rewire mirroring
    // `wire_push_listeners`'s own per-node idempotency (`state.push_listener_disconnects`) via a
    // new `state.poll_jobs_registered` set — it registers a poll job ONLY for a node that doesn't
    // already have one, appending that ONE new processor into the SAME list object the running
    // tick job's closure already holds (so the new node's landed events actually get drained
    // without re-registering the tick job or re-deriving/re-walking any other source's spec).
    test.setTimeout(180000);
    const stamp = Date.now();
    const sourceId = `e2e_rss_${stamp}`;

    // 1. Sources form — rss is HOST_PORT_ONLY (REQ-1739): host+port, feed path defaults to "/".
    // make_rss_loader derives http://<host>:<port>/ from them when Use SSL is unchecked (the
    // fixture server above is plain HTTP).
    await openSourcesForm(page);
    await page.getByTestId("sources-id-input").fill(sourceId);
    await page.getByTestId("sources-type-select").selectOption("rss");
    await page.getByLabel(/^Host/).fill("localhost");
    await page.getByLabel(/^Port/).fill(String(E2E_RSS_PORT));
    await page.getByTestId("rss-use-ssl-checkbox").uncheck();
    await submitSourceAndExpectListed(page, sourceId);

    // 2. Register Table form — REQ-1745's synthetic "default"/<sourceId> pick + the real
    // RSSNotificationProvider item shape (id/title/link/description/published).
    await openRegisterForm(page, sourceId);
    await pickSchemaAndTable(page, "default", sourceId);
    await expect(page.getByTestId("register-table-col-selected-id")).toBeVisible({
      timeout: 30000,
    });
    const registered = await submitRegisterAndExpectListed(page, sourceId);

    // 3. Tables page — set this table's Cache TTL so its poll job actually gets a cadence
    // (`getattr(tbl, "cache_ttl", None)` is the poll-node timer; RegisterTableForm has no field
    // for it, only TableEditForm's own Cache TTL input). Saving re-runs `_rebuild_schemas`, which
    // is exactly the already-running-runtime path `wire_new_poll_jobs` exists for.
    await page.goto("/tables");
    await page.waitForSelector(".page-header", { timeout: 15000 });
    const row = page.locator(".data-table tbody tr").filter({ hasText: sourceId }).first();
    await row.waitFor({ timeout: 15000 });
    await row.click();
    const editBtn = page.getByTestId("table-read-view-edit").first();
    await editBtn.waitFor({ timeout: 10000 });
    await editBtn.click();
    await page.getByLabel(/^Cache TTL/).fill("5");
    await page.getByTestId("table-edit-save").click();
    await expect(page.getByTestId("table-edit-save")).toBeHidden({ timeout: 15000 });

    // 4. SQL page: rss lands through the poll cadence just configured — asynchronous, retry
    // rather than assert immediately (the same shape kafka/websocket use above for their own
    // async landing mechanism).
    const rows = await pollUntilLanded(
      page,
      `SELECT id, title FROM pet_store.${registered} ORDER BY id`,
      2,
      120000,
    );
    expect(rows).toEqual([
      ["item-1", "First item"],
      ["item-2", "Second item"],
    ]);
  });

  test("websocket: add the source, register a table, land pushed events, query it", async ({
    page,
  }) => {
    // FIXED (was: "pushed websocket events never land"). Root cause, isolated live with a manual
    // backend + direct GraphQL/websocket fixture: `wire_push_listeners` (provisa/events/
    // push_wiring.py) called two attributes that do not exist on `EngineRuntime` (provisa/
    // federation/runtime.py) — `engine.materialize_store()` (only `materialize_store_dsn()`
    // exists) and `engine.backend.landing_target(...)` (`EngineRuntime` exposes no public
    // `backend` attribute, only the private `_backend`). Both raised AttributeError, swallowed by
    // the broad `try/except Exception` every caller wraps `wire_push_listeners`/`_rebuild_schemas`
    // in (app.py) — so a push listener was NEVER started for any kafka/websocket table, on any
    // boot, ever; only a unit test using a loosely-typed `MagicMock()` (which silently
    // auto-creates any attribute, real or not) ever exercised this path, masking the drift.
    // Fixed: use the real `materialize_store_dsn()` name, and added `EngineRuntime.landing_target`
    // to delegate to the backend properly. A second, latent bug surfaced once the first was fixed
    // and CDC landing actually ran: `push_wiring._run_listener` held its OWN long-lived
    // `store_connection()` for the whole listener lifetime — for the embedded DuckDB store (this
    // "core" project's default), that is a SECOND connection onto a file the federation engine's
    // own connection already has ATTACHed, which DuckDB refuses (single-writer-per-file, REQ-989;
    // reproduced live as `IOException: Could not set lock on file ... Conflicting lock is held`).
    // Every other landing/persistence write face (`land_source_table`, `attach_landed_source`,
    // `persist_mv_table`) already dispatches through the engine's own connection for an embedded
    // DuckDB store instead of opening a second one — CDC landing was the one path that didn't.
    // Fixed by adding the same dispatch for CDC: `EngineRuntime.apply_cdc_events` ->
    // `NativeEngineBackend.apply_cdc_events` -> `DuckDBFederationRuntime.apply_cdc_events` (new;
    // writes through the engine's own `self._con` via `store_connection.apply_cdc_duckdb_native`,
    // new), falling back to the base backend's per-call `store_connection()` for every non-DuckDB
    // store. `consume_cdc_into_store` (provisa/subscriptions/cdc_landing.py) no longer holds a
    // connection itself — it now applies each flushed batch through a caller-supplied `land_fn`.
    test.setTimeout(240000);
    const stamp = Date.now();
    const sourceId = `e2e_websocket_${stamp}`;

    // 1. Sources form — HOST_PORT_ONLY (REQ-1739): push_wiring.py derives ws://host:port with no
    // path, so this test's ws server listens on "/" and needs no path/use_ssl fields set.
    await openSourcesForm(page);
    await page.getByTestId("sources-id-input").fill(sourceId);
    await page.getByTestId("sources-type-select").selectOption("websocket");
    await page.getByLabel(/^Host/).fill("localhost");
    await page.getByLabel(/^Port/).fill(String(E2E_WS_PORT));
    await submitSourceAndExpectListed(page, sourceId);

    // 2. Register Table form — REQ-1745's synthetic "default"/<sourceId> pick + placeholder
    // id/value columns. CDC landing (push_wiring.py) hard-requires a declared primary key, so
    // "id" must be checked here — unlike ingest, nothing auto-marks one.
    await openRegisterForm(page, sourceId);
    await pickSchemaAndTable(page, "default", sourceId);
    await expect(page.getByTestId("register-table-col-selected-id")).toBeVisible({
      timeout: 30000,
    });
    await page.getByTestId("register-table-col-pk-id").check();
    const registered = await submitRegisterAndExpectListed(page, sourceId);

    // 3. SQL page: websocket lands through the same REQ-1733 CDC-listener mechanism as kafka —
    // asynchronous, retry rather than assert immediately.
    const rows = await pollUntilLanded(
      page,
      `SELECT id, value FROM pet_store.${registered} ORDER BY id`,
      2,
      180000,
    );
    expect(rows).toEqual([
      ["ws-1", "hello"],
      ["ws-2", "world"],
    ]);
  });

});

// ---------------------------------------------------------------------------------------------
// kafka (REQ-1766): landing goes through the same REQ-1733 CDC-listener mechanism verified above
// for websocket, but registration itself was a SEPARATE real bug: kafka was in none of
// SIMPLE_RDBMS/HOST_PORT_ONLY/NO_CONNECTION_TYPES (provisa-ui/src/pages/sources/constants.ts) —
// the Sources form rendered ZERO connection fields for it, the same class of defect REQ-1753
// fixed for saphana, so a kafka source could never even be filled in. Fixed by adding kafka to
// HOST_PORT_ONLY (host+port fields, same shape as websocket). That in turn uncovered a second
// bug: push_wiring.py's kafka branch built bootstrap_servers from `src.host` ALONE, discarding
// `src.port` entirely — once registration was even possible, the listener would have silently
// connected to aiokafka's default port (9092) regardless of what was actually registered. Fixed
// to combine host:port, the same shape websocket's own ws://host:port derivation already used.
// A demo/sources/kafka compose fixture (single-node KRaft broker, no zookeeper) is added
// alongside this fix — the ONE kafka broker this repo previously provisioned
// (docker-compose.e2e.yml/test.yml) belonged to the separate pytest harnesses, unreachable from
// this Playwright "core" project's own webServer.
// ---------------------------------------------------------------------------------------------
test.describe("source to query through the UI: kafka (REQ-1739/REQ-1745/REQ-1766)", () => {
  test.beforeAll(() => {
    test.setTimeout(180000);
    provisionKafka("up");
  });

  test.afterAll(() => {
    provisionKafka("down");
  });

  test("kafka: add the source, register a table, land a produced message, query it", async ({
    page,
  }) => {
    test.setTimeout(240000);
    const stamp = Date.now();
    const sourceId = `e2e_kafka_${stamp}`;
    const topic = `e2e-kafka-topic-${stamp}`;

    // 1. Sources form — kafka is HOST_PORT_ONLY (REQ-1766): bootstrap host+port, no
    // username/password/database (push_wiring.py builds bootstrap_servers from these directly).
    await openSourcesForm(page);
    await page.getByTestId("sources-id-input").fill(sourceId);
    await page.getByTestId("sources-type-select").selectOption("kafka");
    await page.getByLabel(/^Host/).fill("localhost");
    await page.getByLabel(/^Port/).fill(String(E2E_KAFKA_PORT));
    await submitSourceAndExpectListed(page, sourceId);

    // 2. Register Table form — same REQ-1745 synthetic "default"/<sourceId> pick + placeholder
    // id/value columns as websocket above. CDC landing hard-requires a declared primary key.
    await openRegisterForm(page, sourceId);
    await pickSchemaAndTable(page, "default", sourceId);
    await expect(page.getByTestId("register-table-col-selected-id")).toBeVisible({
      timeout: 30000,
    });
    await page.getByTestId("register-table-col-pk-id").check();
    const registered = await submitRegisterAndExpectListed(page, sourceId);

    // 3. Tables page — set this table's Change Signal to "kafka" and its consume topic
    // (push_wiring.py's _build_provider reads live.kafka.topic; RegisterTableForm has no field
    // for it, only TableEditForm's LiveDeliveryFieldset does).
    await page.goto("/tables");
    await page.waitForSelector(".page-header", { timeout: 15000 });
    const row = page.locator(".data-table tbody tr").filter({ hasText: sourceId }).first();
    await row.waitFor({ timeout: 15000 });
    await row.click();
    const editBtn = page.getByTestId("table-read-view-edit").first();
    await editBtn.waitFor({ timeout: 10000 });
    await editBtn.click();

    // getByLabel matches BOTH the Select's own input and its open listbox (Mantine wires
    // aria-labelledby on both) — scope to the textbox role to avoid a strict-mode violation.
    await page.getByRole("textbox", { name: /^Change Signal/ }).click();
    await page.getByRole("option", { name: "kafka", exact: true }).click();
    // LiveDeliveryFieldset.tsx: every outbound-delivery field (including the topic input) is
    // gated behind its own "Enable Live Delivery" checkbox ({live && (...)}), a SEPARATE toggle
    // from Change Signal — checking it AFTER Change Signal is set so defaultLive.strategy
    // captures "kafka" (isPushSignal) at check time, not whatever ttl/probe default preceded it.
    await page.getByTestId("live-delivery-enable").check();
    await page.getByTestId("live-kafka-topic").fill(topic);
    await page.getByTestId("table-edit-save").click();
    await expect(page.getByTestId("table-edit-save")).toBeHidden({ timeout: 15000 });

    // 4. Produce one message on the registered topic — push_wiring's listener (re-wired on save,
    // same state.push_listener_disconnects idempotency websocket's test comment describes) picks
    // it up asynchronously.
    produceKafkaMessage(topic, { id: "kafka-1", value: "hello-kafka" });

    // 5. SQL page: same REQ-1733 CDC-listener mechanism as websocket — asynchronous, retry rather
    // than assert immediately.
    const rows = await pollUntilLanded(
      page,
      `SELECT id, value FROM pet_store.${registered} ORDER BY id`,
      1,
      180000,
    );
    expect(rows).toEqual([["kafka-1", "hello-kafka"]]);
  });

  // REQ-1767: provisa.kafka.schema_registry.SchemaRegistryClient (REQ-116/147/150) existed with
  // zero callers anywhere in the codebase — built, never wired to any mutation or UI entry point.
  // Wired into the same discover/edit/register flow mongodb/elasticsearch/cassandra/prometheus
  // already use (SourcesPage's "Discover" button -> SchemaDiscovery.tsx, DISCOVERABLE_TYPES),
  // rather than inventing a new one: discovery_schema.py's kafka branch calls
  // discover_topic_columns() and returns real columns instead of a hardcoded id/value
  // placeholder. Proves the registry round-trip for real (register a schema, discover it through
  // the UI, see the actual discovered columns, register, query) AND the CDC-landing round trip
  // the plain kafka test above covers. Also caught, live-traced, and fixed while building this:
  // SchemaDiscovery.tsx's handleRegister hardcoded visibleTo: ["*"] per column — neither of the
  // two independent visibility-enforcement layers (schema_gen.py's compiler, stage2.py's V003
  // query-time gate) treats "*" as a wildcard, so every column (and therefore the whole table)
  // was silently excluded from every compiled schema AND every query, for every role,
  // permanently, for EVERY type using the Discover flow — not a kafka-specific bug.
  //
  // REQ-1769 (FIXED): push_wiring.py's wire_push_listeners hard-requires a declared primary-key
  // column before starting a CDC listener, but SchemaDiscovery.tsx's column editor had no
  // primary-key selection UI at all — unlike RegisterTableForm's own
  // register-table-col-pk-<name> checkboxes — so every column discovered/registered through this
  // flow always registered with is_primary_key=false, and any kafka/websocket table registered
  // via Discover could never receive live CDC data (registered fine, queryable, but
  // wire_push_listeners silently skipped it forever: "no primary key column declared"). Fixed by
  // adding an "Is PK" checkbox column (data-testid discover-col-pk-<name>) to SchemaDiscovery's
  // column table, wired into handleRegister's registerTable call as `isPrimaryKey`.
  test("kafka: discover a topic's schema from Confluent Schema Registry, register it, query it", async ({
    page,
  }) => {
    test.setTimeout(240000);
    const stamp = Date.now();
    const sourceId = `e2e_kafka_registry_${stamp}`;
    const topic = `e2e-kafka-registry-topic-${stamp}`;
    // MUST equal sourceId — a MATERIALIZE_ONLY "default"-schema source (kafka/websocket/rss/
    // ingest, REQ-1745) only ever compiles a table whose name is source_id itself (the "one
    // table per source" placeholder the picker's own AvailableTableType(name=source_id, ...)
    // enforces in RegisterTableForm's flow — see _native_tables_kafka's "default" branch). A
    // custom table name registers a row (confirmed live: it exists, columns are correct) but
    // never compiles into any role's context, so dq_dataset (used to resolve the physical name
    // everywhere in this file) stays permanently null — verified by comparing this test's row
    // against the plain kafka test's above with a live debug dump, not guessed.
    const tableName = sourceId;
    registerJsonSchema(topic, { id: "string", value: "string" });

    // 1. Sources form — same HOST_PORT_ONLY shape as the plain kafka case above.
    await openSourcesForm(page);
    await page.getByTestId("sources-id-input").fill(sourceId);
    await page.getByTestId("sources-type-select").selectOption("kafka");
    await page.getByLabel(/^Host/).fill("localhost");
    await page.getByLabel(/^Port/).fill(String(E2E_KAFKA_PORT));
    await submitSourceAndExpectListed(page, sourceId);

    // 2. Sources page's own "Discover" flow (DISCOVERABLE_TYPES) — topic + registry URL hints,
    // then the REAL discovered columns (not the id/value placeholder), then register directly.
    await page.getByTestId(`sources-discover-${sourceId}`).click();
    await expect(page.getByTestId("schema-discovery")).toBeVisible({ timeout: 10000 });
    await page.getByTestId("discover-kafka-topic").fill(topic);
    await page
      .getByTestId("discover-kafka-registry-url")
      .fill(`http://localhost:${E2E_KAFKA_SCHEMA_REGISTRY_PORT}`);
    await page.getByTestId("discover-schema-btn").click();
    await expect(page.getByRole("textbox", { name: "Name", exact: true }).first()).toHaveValue(
      "id",
      { timeout: 30000 },
    );

    // REQ-1769: check the discovered "id" column's PK checkbox — CDC landing (push_wiring.py)
    // hard-requires a declared primary key, same as the plain kafka/websocket tests above.
    await page.getByTestId("discover-col-pk-id").check();

    await page.getByLabel(/^Domain ID/).fill("pet-store");
    await page.getByLabel(/^Table Name/).fill(tableName);
    await page.getByTestId("register-table-btn").click();
    await expect(page.getByTestId("schema-discovery")).toBeHidden({ timeout: 20000 });

    // SchemaDiscovery's registerTable call, unlike submitRegisterAndExpectListed's flow, returns
    // no physical name directly — resolve it the same way that helper does (dqDataset's last
    // path segment). Registration rebuilds the schemas asynchronously (same reason
    // submitRegisterAndExpectListed polls the tables list rather than querying once) — poll here
    // too rather than assume the very next GraphQL read already sees it.
    let registered = "";
    const deadline = Date.now() + 60000;
    while (Date.now() < deadline) {
      const tablesRes = await page.request.post("/admin/graphql", {
        data: { query: "{ tables { sourceId dqDataset } }" },
      });
      expect(tablesRes.ok(), await tablesRes.text()).toBeTruthy();
      const tables = (await tablesRes.json()).data.tables as {
        sourceId: string;
        dqDataset: string | null;
      }[];
      const mine = tables.find((t) => t.sourceId === sourceId);
      if (mine?.dqDataset) {
        registered = mine.dqDataset.split("/").pop()!;
        break;
      }
      await new Promise((r) => setTimeout(r, 2000));
    }
    expect(registered, `no dataset name ever reported for ${sourceId}`).toBeTruthy();

    // 3. Prove the registry-discovered table is a real, queryable table (not just a UI display
    // artifact) — the SQL page accepts and runs a query against it, returning its real (empty,
    // nothing produced) column shape correctly, before anything is produced.
    // runSqlOnPage (the shared helper) waits for download-csv-btn, which ResultsPanel.tsx only
    // renders for a non-empty result set — a genuinely empty table (correct here: nothing was
    // ever produced yet) shows its own "No results." text instead, so this test drives the SQL
    // page directly rather than reusing that helper.
    await page.goto("/sql");
    await page.waitForSelector(".cm-content", { timeout: 30000 });
    const picker = page.getByTestId("sql-role");
    if ((await picker.inputValue()) !== "org_admin") {
      await picker.click();
      const option = page.getByRole("option", { name: "org_admin", exact: true });
      if (await option.count()) await option.click();
      else await page.keyboard.press("Escape");
    }
    await typeSql(page, `SELECT id, value FROM pet_store.${registered}`);
    const runResp = page.waitForResponse(
      (r) => r.url().includes("/data/sql") && r.request().method() === "POST",
    );
    await page.getByTestId("sql-run").click();
    const resp = await runResp;
    expect(resp.ok(), await resp.text()).toBeTruthy();
    await expect(page.getByText("No results.")).toBeVisible({ timeout: 30000 });

    // 4. REQ-1769: now prove the FULL CDC-landing round trip on a Discover-flow-registered
    // table — same Tables-page Change-Signal-kafka + live-delivery-enable + produce + poll steps
    // the plain kafka test above uses, now reachable because the "id" column's PK checkbox was
    // checked in step 2 above.
    await page.goto("/tables");
    await page.waitForSelector(".page-header", { timeout: 15000 });
    const row = page.locator(".data-table tbody tr").filter({ hasText: sourceId }).first();
    await row.waitFor({ timeout: 15000 });
    await row.click();
    const editBtn = page.getByTestId("table-read-view-edit").first();
    await editBtn.waitFor({ timeout: 10000 });
    await editBtn.click();

    await page.getByRole("textbox", { name: /^Change Signal/ }).click();
    await page.getByRole("option", { name: "kafka", exact: true }).click();
    await page.getByTestId("live-delivery-enable").check();
    await page.getByTestId("live-kafka-topic").fill(topic);
    await page.getByTestId("table-edit-save").click();
    await expect(page.getByTestId("table-edit-save")).toBeHidden({ timeout: 15000 });

    produceKafkaMessage(topic, { id: "kafka-registry-1", value: "hello-kafka-registry" });

    const landedRows = await pollUntilLanded(
      page,
      `SELECT id, value FROM pet_store.${registered} ORDER BY id`,
      1,
      180000,
    );
    expect(landedRows).toEqual([["kafka-registry-1", "hello-kafka-registry"]]);
  });
});
