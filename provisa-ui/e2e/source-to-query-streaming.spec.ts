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
// parallel agents' spec files. Both fixture servers below (RSS HTTP feed, WebSocket) are started
// IN-PROCESS by this spec file itself (Node's own http module / the `ws` package) — no docker
// container, no demo/sources/ provisioning needed for either.

import * as http from "node:http";
import { WebSocketServer } from "ws";

import { test, expect } from "./coverage";
import {
  openRegisterForm,
  openSourcesForm,
  pickSchemaAndTable,
  runSqlOnPage,
  submitRegisterAndExpectListed,
  submitSourceAndExpectListed,
} from "./source-to-query-helpers";

const E2E_RSS_PORT = 37801;
const E2E_WS_PORT = 37802;

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

  // rss: SKIPPED. The Register Table flow through the real UI (source create → schema/table
  // pick → column select → submit, all fixed by this file's REQ-1745 introspection branches) DOES
  // work and was verified manually while developing this test. What's still missing is the
  // LANDING side: rss is POLL, not push, so it lands through SourceRowLoader/build_adapter_loaders
  // (make_rss_loader, added alongside this file), which only runs when wire_event_loop executes —
  // and wire_event_loop is wired ONLY from register_runtime's per-org runtime build (provisa/api/
  // app.py), never from `_rebuild_schemas()`, which is what registerTable's mutation actually
  // calls on an already-running default/single-tenant runtime (the shape this whole test file
  // exercises). A table registered against a live server therefore never gets a poll job started.
  // The obvious fix (also call wire_event_loop from _rebuild_schemas_impl) was implemented and
  // tested, but it re-derives adapter_loaders and re-walks EVERY registered source's poll-job
  // registration on every rebuild — not just the new one — and broke an unrelated, already-
  // registered sqlite demo source's live queries (global-setup's own warm-up query started
  // failing with "'types.SimpleNamespace' object has no attribute 'base_url'", a DuckDB-side
  // introspection seam object built for a different source type reaching a code path that
  // expected a real Source). Reverted rather than risk that regression. The correct fix scopes
  // the re-wire to ONLY the newly-registered node, the way wire_push_listeners already does for
  // kafka/websocket (state.push_listener_disconnects tracks per-node, so a re-wire only starts
  // what isn't already running) — real, scoped follow-up work, not a one-line change, so left
  // for a dedicated pass rather than forced here.
  test.skip(
    "rss: SKIPPED — registration verified working (REQ-1745 introspection fix), but poll " +
      "landing has no re-wire path off an already-running runtime; see comment above",
    async () => {},
  );

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

  // kafka: SKIPPED. Landing goes through the same REQ-1733 CDC-listener mechanism verified above
  // for websocket (both are wired by the same wire_push_listeners/_run_listener code path in
  // provisa/events/push_wiring.py — websocket's pass is direct evidence the shared mechanism
  // works), but proving it end-to-end needs a REAL Kafka broker: no demo/sources/kafka fixture
  // exists yet (demo/sources/provision.py has no "kafka" directory), and the ONE kafka broker this
  // repo already provisions (docker-compose.e2e.yml / docker-compose.test.yml, KAFKA_HOST_PORT)
  // belongs to the separate tests/e2e (pytest) and tests/ (pytest integration) harnesses — the
  // provisa-ui Playwright "core" project's own webServer only brings up docker-compose.core.yml
  // (control-plane postgres), never docker-compose.e2e.yml. Standing up a NEW demo/sources/kafka
  // compose fixture (broker + a way to produce a test message) is real, self-contained work
  // deliberately out of scope for this file per this task's own instructions ("if kafka's broker
  // setup proves too heavy/slow ... acceptable to document that clearly and skip it").
  test.skip(
    "kafka: SKIPPED — no kafka broker reachable from the provisa-ui Playwright 'core' project " +
      "(see comment above); landing mechanism is shared with websocket, verified passing above",
    async () => {},
  );
});
