// Copyright (c) 2026 Kenneth Stott
// Canary: dca902b8-1f0f-45fa-b457-00a5ec81bff9
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

// REQ-1730: shared infrastructure for engine-swap.spec.ts — constants, the Registration/
// EngineTarget types, container provisioning, the DuckDB->Trino replay mechanism
// (reloadEngineBackend/reprovisionSourceOnEngine/requeryOnEngine), Trino cold-start
// stabilization, zombie-source cleanup, and runSwapCase (the shared per-type test body).
// Split out of engine-swap.spec.ts purely to stay under the repo's max-lines lint rule as
// REQ-1730 grew to cover more source types — see engine-swap.spec.ts's own module doc for the
// harness's actual design rationale and invocation instructions; see engine-swap-registrars.ts
// for the per-type register* functions this file's runSwapCase/ENGINES drive.

import { execFileSync, spawn, type ChildProcess } from "node:child_process";
import fs from "node:fs";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { test, expect, BACKEND_URL, TRINO_BACKEND_URL, UI_URL } from "./coverage";
import { runSqlOnPage, typeSql } from "./source-to-query-helpers";
import type { Page } from "./coverage";


export const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");
export const PROVISION = path.join(ROOT, "demo", "sources", "provision.py");
export const PYTHON = path.join(ROOT, ".venv", "bin", "python");
export const SWAP_PREFIX = "provisa-swap";
export const MAKE_FILE_LAKE_FIXTURES = path.join(ROOT, "provisa-ui", "e2e", "make-file-lake-fixtures.py");
export const CLOUD_WAREHOUSE_SEED = path.join(ROOT, "provisa-ui", "e2e", "cloud_warehouse_seed.py");
// delta_lake/iceberg (SCAN-mechanism, no ATTACH, no network service — connector_duckdb.py's
// DuckDBDeltaConnector/DuckDBIcebergConnector read a host path directly) are the first engine-swap
// types whose Trino leg needs a FILE the DuckDB-bound backend (native) and Trino (containerized)
// both reach — every prior type needed only a host rewrite (localhost -> host.docker.internal).
// docker-compose.core.yml mounts this exact directory into Trino's (and hive-metastore's)
// container at the SAME absolute path it has on the host — an identity mount, no path rewrite
// needed here the way `host` gets one: Iceberg's register_table requires its table_location
// argument to match the location pyiceberg's own metadata.json recorded verbatim, and that
// metadata is written by a NATIVE (host) process, so only an identical-path mount lets Trino
// resolve the same location string too (verified live 2026-09-16, ICEBERG_INVALID_METADATA
// "differs from location provided" when the two paths diverged).
export const FILE_LAKE_HOST_DIR = path.resolve(ROOT, ".e2e-file-lake");

export const E2E_FIREBIRD_PORT = 33051;
export const E2E_AIRPORT_PORT = 35061;
export const E2E_SINGLESTORE_PORT = 33071;
export const E2E_EXASOL_PORT = 33081;
// Exasol's TLS certificate is regenerated every container boot (demo/sources/exasol/prime.py's own
// module doc) — no fixed fingerprint to hardcode, so prime.py writes the one THIS run's container
// actually presents to a file the test reads back after provisioning, same as
// source-to-query-olap-lake.spec.ts's own exasol case.
export const E2E_EXASOL_FINGERPRINT_FILE = path.join(
  os.tmpdir(),
  "provisa-e2e-swap-exasol-fingerprint.txt",
);
// grpc_remote has no container fixture — demo/grpc_remote_server/server.py is a plain Python
// process the test spawns itself (same as source-to-query-special-cases.spec.ts's REQ-1742 case),
// not a demo-source-containers.ts entry or a provision.py-managed compose stack. Distinct port
// range from every other E2E_*_PORT in this file, per three-instance-isolation.
export const E2E_GRPC_REMOTE_PORT = 33091;
export const GRPC_REMOTE_SERVER_MODULE = "demo.grpc_remote_server.server";

// REQ-1730: kafka needs its own broker + Confluent Schema Registry (demo/sources/kafka's compose
// fixture — the same one source-to-query-streaming.spec.ts's 378xx-range provisionKafka() uses,
// started here instead via provisionSwapSource so it tears down through the shared
// sweepZombieSwapSources path). Distinct port range from every other E2E_*_PORT here AND from
// source-to-query-streaming.spec.ts's own 378xx range, per three-instance-isolation.
export const E2E_KAFKA_PORT = 33101;
export const E2E_KAFKA_SCHEMA_REGISTRY_PORT = 33102;

// rss's feed fixture is a plain in-process Node http.Server (this file's own beforeAll/afterAll,
// same shape as source-to-query-streaming.spec.ts's own — but a DISTINCT port from that file's
// 378xx range, per three-instance-isolation) — it re-serves the SAME static feed content on every
// request, which is what makes it tractable for this harness's "register once, swap engine,
// requery" pattern: a poll job started fresh on the Trino-bound backend after the swap re-polls
// and lands the identical items.
export const E2E_RSS_PORT = 33111;
// websocket's fixture (the `ws` package, same as source-to-query-streaming.spec.ts's own) sends
// its fixed 2-event payload on EVERY new connection, not just the first ever — the same "re-serve
// identical data to a fresh connection" property that makes rss tractable applies here too: Trino
// backend's own wire_push_listeners (push_wiring.py) opens its OWN fresh connection after the
// swap and receives the same 2 events again.
export const E2E_WS_PORT = 33112;

export async function waitForPort(port: number, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const ok = await new Promise<boolean>((resolve) => {
      const socket = net.connect({ host: "127.0.0.1", port }, () => {
        socket.end();
        resolve(true);
      });
      socket.on("error", () => resolve(false));
    });
    if (ok) return;
    if (Date.now() > deadline) throw new Error(`nothing listening on 127.0.0.1:${port}`);
    await new Promise((r) => setTimeout(r, 250));
  }
}

// Both gates are checked BEFORE provisioning, not just before registration: an unlicensed or
// arm64-emulated singlestoredb-dev container never becomes healthy (or, under arm64, never even
// starts — `docker compose up` fails outright with "no matching manifest"), so attempting it
// wastes the harness's own boot budget on a doomed wait. See the module doc.
export const SINGLESTORE_AVAILABLE = process.arch === "x64" && !!process.env.SINGLESTORE_LICENSE;

// Every demo/sources/<name> whose compose.yml takes PROVISA_DEMO_<NAME>_PORT (uppercased) —
// provision.py's own env-passthrough contract (see its module doc: "--env values apply to
// both"). One dedicated block, distinct from both the default demo ports (start-ui-install.sh
// --demo, which a maintainer's local-dev instance may have live) and every other E2E_*_PORT in
// this file, so this harness never collides with either (three-instance-isolation).
export const RDB_WIDGETS_PORTS: Record<string, number> = {
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
  hiveserver2: 36101,
  saphana: 36111,
};

// Mirrors source-to-query-generic-rdbms.spec.ts's own gate: saplabs/hanaexpress's indexserver
// verified live not to start under Docker Desktop's Apple Silicon VM — a real amd64 Linux host is
// required, which CI (ubuntu-latest) is and local arm64 dev is not.
export const RUNNING_IN_CI = process.env.CI === "true";

// Trino's own compose project's default network (docker-compose.core.yml, no explicit `networks:`
// block — verified live: `docker inspect provisa-trino-1` shows it on `provisa_default`, already
// aliased "trino"). Same default source-to-query-olap-lake-trino.spec.ts/splunk-connector.spec.ts
// use for their own `--network`/`--network-alias` container joins.
export const DOCKER_NETWORK = process.env.PROVISA_E2E_DOCKER_NETWORK ?? "provisa_default";

// One source = one test (REQ-1730 redesign, 2026-09-16): every demo/sources/<name> this harness
// uses provisions and tears down its OWN container, scoped to the ONE test that needs it — never
// the whole fleet just because one type is under test. A single `--grep mysql` run boots exactly
// one container. Generalizes the original firebird/airport/singlestore-only helper: any
// demo/sources/<name> directory works here as long as its compose.yml takes
// PROVISA_DEMO_<NAME>_PORT (every RDBMS fixture prime.py checked live during this extension
// does — see RDB_WIDGETS_PORTS).
//
// `network` (REQ-1730 kafka): provision.py's own `--network` join (joins the fixture's container
// onto an existing network under an alias, e.g. "kafka") — every other type here reaches Trino
// fine through reprovisionSourceOnEngine's plain host.docker.internal rewrite (a single-hop JDBC/
// HTTP connection), but Kafka's wire protocol is two-hop: a client's bootstrap connection gets
// handed back the broker's OWN advertised address for the listener it connected through, then
// reconnects using THAT address for every subsequent request (metadata/split-listing). kafka's
// compose fixture advertises its HOST listener as literally "localhost:<port>" (correct for the
// host-side aiokafka/schema-registry scripts this harness's own registrar uses), which is exactly
// as unreachable from inside Trino's container as a bare host.docker.internal rewrite would be —
// confirmed live: `FederationError(... KAFKA_SPLIT_ERROR, "Cannot list splits for table ...")`.
// Joining kafka's container onto Trino's OWN network instead gives Trino a route to the fixture's
// existing internal PLAINTEXT listener (already advertised as "kafka:29092", matching its
// `hostname: kafka` — no compose.yml change needed), which needs no redirect at all.
export function provisionSwapSource(
  name: string,
  cmd: "up" | "down",
  extraEnv: Record<string, string> = {},
  network?: string,
): void {
  const env = {
    ...process.env,
    PROVISA_DEMO_FIREBIRD_PORT: String(E2E_FIREBIRD_PORT),
    PROVISA_DEMO_AIRPORT_PORT: String(E2E_AIRPORT_PORT),
    PROVISA_DEMO_SINGLESTORE_PORT: String(E2E_SINGLESTORE_PORT),
    PROVISA_DEMO_PREFIX: SWAP_PREFIX,
    ...(name in RDB_WIDGETS_PORTS
      ? { [`PROVISA_DEMO_${name.toUpperCase()}_PORT`]: String(RDB_WIDGETS_PORTS[name]) }
      : {}),
    ...extraEnv,
  };
  try {
    execFileSync(
      PYTHON,
      [
        PROVISION,
        cmd,
        "--prefix",
        SWAP_PREFIX,
        ...(cmd === "up" && network ? ["--network", network] : []),
        name,
      ],
      { stdio: "pipe", env },
    );
  } catch (e) {
    if (cmd === "down") return; // a project that was never started removes nothing
    throw e;
  }
}

/** One source+table's proof: the SQL that reads it back, and the assertion every engine must
 * satisfy identically. An empty `reachableOn` registers the source (proving the DuckDB leg) but
 * skips every engine's requery — see the module doc for why (sqlite). */
export interface Registration {
  label: string;
  sourceId: string;
  sql: string;
  assertRows: (rows: string[][]) => void;
  /** Engine names (EngineTarget.name below) this registration's source has a live reach path
   * for. Absence means "registered to prove the DuckDB leg, never requeried on that engine" —
   * e.g. sqlite, which has no Trino connector or FDW path at all (REQ-1726). */
  reachableOn: string[];
  /** REQ-1730 (rss): set for a POLL-cadence-landed source, where `sql`'s FIRST answer on a
   * freshly-(re)registered engine can genuinely be zero rows — the poll job needs at least one
   * tick, unlike every other MATERIALIZE_ONLY type here (grpc_remote/graphql_remote/openapi/
   * govdata), which land synchronously on the query itself ("the first query on a
   * materialize-only source lands it first", source-to-query-helpers.ts's own runSqlOnPage
   * comment). When set, requeryOnEngine retries the WHOLE query (a fresh page load each time —
   * the SQL page has no live-refresh) every 5s up to this many milliseconds, instead of the
   * single attempt every other registration gets. */
  pollTimeoutMs?: number;
}

/** One already-running backend process this harness can requery against, sharing the DuckDB
 * backend's Postgres control-plane org schema. Add an entry here (and to ENGINES below) to bring
 * a new engine into the swap — nothing else in this file names an engine by hand. */
export interface EngineTarget {
  name: string;
  backendUrl: string;
  /** Env var naming this engine's own config file on disk — PUT back verbatim to force a live,
   * in-process reload (REQ-1729's DB-driven backfill), no OS-level restart needed. */
  configPathEnv: string;
}

export const ENGINES: EngineTarget[] = [
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
export const SWAP_REGISTER_TIMEOUT_MS = 300000;

/** Force an already-running engine backend to pick up rows the DuckDB backend just registered
 * into their SHARED Postgres control-plane schema. `PUT /admin/config` re-enters the exact
 * boot-time DB-driven backfill (introspect_tables/_rebuild_schemas/reconcile_landed_tables —
 * see REQ-1729) — no OS-level restart needed, since it is already a live in-process reload. */
export async function reloadEngineBackend(engine: EngineTarget) {
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
export async function reprovisionSourceOnEngine(engine: EngineTarget, sourceId: string) {
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
  if (engine.name === "trino" && src!.type === "kafka") {
    // kafka's own wire protocol is two-hop (see provisionSwapSource's `network` param doc): a
    // plain host.docker.internal rewrite of the HOST listener's port gets a bootstrap connection
    // through, but every subsequent split-listing request reuses whatever address the broker
    // advertises for that listener — "localhost:<port>", unreachable from inside Trino's
    // container. registerKafka's caller joins the fixture onto Trino's OWN network under alias
    // "kafka" (provisionSwapSource's `network`), so route Trino straight at the fixture's existing
    // internal PLAINTEXT listener (already advertised as "kafka:29092") instead of the published
    // HOST one.
    src!.host = "kafka";
    src!.port = 29092;
  } else if (engine.name === "trino" && src!.host && src!.type !== "rss" && src!.type !== "websocket") {
    // rss/websocket have no live Trino connector at all (strategy.py's _MATERIALIZE_ONLY) — their
    // "Trino leg" is a background poll/push job the app's OWN process runs (provisa/events/
    // push_wiring.py's make_rss_loader / websocket listener), and that process is NATIVE on both
    // backends here (only Trino's JVM coordinator itself runs in a container) — unlike every
    // OTHER MATERIALIZE_ONLY type reached through a real Trino connector (mongodb/redis/
    // cassandra/...), where the JDBC/driver connection really does originate INSIDE that
    // container and needs the host.docker.internal address. Reproduced live: rewriting rss's host
    // here left its poll job trying (from a plain native process) to resolve
    // "host.docker.internal", a Docker-Desktop-only DNS entry that means nothing outside a
    // container — the poll job never landed a single row in 120s, not a slow-tick false negative.
    src!.host = src!.host.replace(/\b(?:localhost|127\.0\.0\.1)\b/g, "host.docker.internal");
  }
  // kafka stores its Schema Registry URL in `database` (SourceFormFieldsExtended.tsx's isKafka
  // block, TrinoKafkaConnector.details() reads it) — same host-unreachable-from-container problem
  // as `host` above, and the regex is a no-op for every other type's non-URL `database` value.
  if (engine.name === "trino" && src!.database) {
    src!.database = src!.database.replace(/\b(?:localhost|127\.0\.0\.1)\b/g, "host.docker.internal");
  }
  // Unlike `host` above, a file-based source's `path` (delta_lake/iceberg/csv/parquet/files)
  // needs NO rewrite here: docker-compose.core.yml mounts FILE_LAKE_HOST_DIR into Trino's
  // container at that exact same absolute path (see FILE_LAKE_HOST_DIR's own comment for why an
  // identity mount, not a translated one, is required for Iceberg specifically).
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
/** One `sql` attempt on the SQL page — like `runSqlOnPage`, but returns `[]` instead of throwing
 * when the query answers with no rows, so a caller can retry rather than fail on the very first
 * attempt. Only used for `Registration.pollTimeoutMs` (rss's poll-cadence landing); every other
 * registration keeps using `runSqlOnPage`'s single-shot, fail-fast behavior. */
async function trySqlOnPage(page: Page, sql: string, role = "org_admin"): Promise<string[][]> {
  await page.goto("/sql");
  await page.waitForSelector(".cm-content", { timeout: 30000 });
  const picker = page.getByTestId("sql-role");
  if ((await picker.inputValue()) !== role) {
    await picker.click();
    const option = page.getByRole("option", { name: role, exact: true });
    if (await option.count()) await option.click();
    else await page.keyboard.press("Escape");
  }
  await typeSql(page, sql);
  const runResp = page.waitForResponse(
    (r) => r.url().includes("/data/sql") && r.request().method() === "POST",
  );
  await page.getByTestId("sql-run").click();
  const resp = await runResp;
  if (!resp.ok()) return [];
  const landed = await page
    .getByTestId("download-csv-btn")
    .isVisible({ timeout: 5000 })
    .catch(() => false);
  if (!landed) return [];
  const rows = page.locator(".sql-results-table tbody tr");
  return rows.evaluateAll((trs) =>
    trs.map((tr) => Array.from(tr.querySelectorAll("td")).map((td) => td.textContent?.trim() ?? "")),
  );
}

export async function requeryOnEngine(page: Page, engine: EngineTarget, registrations: Registration[]) {
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
    if (!reg.pollTimeoutMs) {
      const rows = await runSqlOnPage(page, reg.sql);
      reg.assertRows(rows);
      continue;
    }
    const deadline = Date.now() + reg.pollTimeoutMs;
    let rows: string[][] = [];
    for (;;) {
      rows = await trySqlOnPage(page, reg.sql);
      if (rows.length > 0 || Date.now() > deadline) break;
      // rss/websocket's poll job is wired by wire_new_poll_jobs (app_wiring.py) inside
      // _rebuild_schemas — a no-op if wire_event_loop's OWN async task hasn't finished populating
      // state.event_loop_processors yet. reloadEngineBackend's reload right before this loop can
      // race ahead of that completion, and a wire_new_poll_jobs call that loses this race is not
      // automatically retried by anything: nothing re-triggers the wiring until another mutation
      // calls _rebuild_schemas again. Re-issuing the (idempotent, upsert) createSource replay
      // each retry gives that race another chance to resolve.
      await reprovisionSourceOnEngine(engine, reg.sourceId);
      await new Promise((r) => setTimeout(r, 5000));
    }
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
export async function waitForTrinoStable(budgetMs = 240000): Promise<void> {
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

// REQ-1730 scenario 1: "if you changed engines, it required you to reboot the backend" (the
// user's own design intent) — every OTHER engine in this file is proven by replaying the
// createSource mutation against an already-running, already-differently-engined process
// (reprovisionSourceOnEngine/reloadEngineBackend). That is deliberately NOT a restart, and it
// hid a real gap (REQ-1730's `_replace_mode_cleanup`/`_build_source_pools_and_enums` never seeing
// a control-plane-only source) until a genuine reboot-based test was written. This section owns
// ONE dedicated backend process, entirely separate from every ENGINES/CORE_BACKENDS process
// above: it is killed and respawned with `PROVISA_ENGINE` flipped, on the SAME port/data dir/org,
// so "reboot with a different engine" is a real OS-level restart, not an in-process reload.
export type RebootEngineKind = "duckdb" | "trino";

// Same defaults playwright.config.ts's own graphql-demo/petstore-mock webServer entries use.
// Exported so registerGraphqlRemote/registerOpenapi callers (registrars have no baked-in
// "graphql-demo"/"petstore-api" source to read a path off of in the reboot harness's minimal
// config) can pass the exact URL those already-running demo servers answer on.
export const REBOOT_GRAPHQL_DEMO_URL = `http://localhost:${process.env.PROVISA_E2E_GRAPHQL_DEMO_PORT ?? 8907}/graphql`;
export const REBOOT_PETSTORE_OPENAPI_URL = `http://localhost:${process.env.PROVISA_E2E_PETSTORE_PORT ?? 8908}/api/v3/openapi.json`;

// 8996-9001 (this block's original default) collided live with docker-compose.core.yml's MinIO
// container (published on host 9000) and another already-bound service on the pgwire offset —
// this range is nowhere near any other port this harness or its compose stack uses.
const REBOOT_HTTP_PORT = Number(process.env.PROVISA_E2E_REBOOT_PORT ?? 18001);
const REBOOT_DATA_DIR = path.resolve(ROOT, "provisa-ui", ".playwright-reboot-data");
const REBOOT_CONFIG_PATH = path.join(REBOOT_DATA_DIR, "provisa.yaml");
// Unique per TEST, not per test-file invocation: NativeEngineBackend._attached (the DuckDB
// runtime's ATTACH dedup cache, native_backend.py) is keyed by "schema.table" ALONE, never source
// id, and never invalidated for the life of the process. A leftover same-schema/table source from
// an EARLIER run/type — even a DIFFERENT type: mysql and mariadb's RDB_WIDGETS_SOURCES entries
// both use schema "provisa_demo" and the same "widgets" table name — would win that cache key at
// boot-time warmup, before this file's own zombie sweep even runs, permanently starving every
// later registration under the SAME key of its own catalog attach. Reproduced live twice: once
// across retries of the SAME type (mongodb, fixed by scoping the org per invocation), and again
// across DIFFERENT types sharing one invocation (mysql vs. mariadb, same schema.table — a
// per-invocation org was not enough). `resetRebootOrg()` (called from `prepareRebootDataDir`, at
// the start of every `runRebootCase`/`runFreshEngineCase`) mints a fresh one per TEST instead.
let REBOOT_ORG_ID = process.env.PROVISA_E2E_REBOOT_ORG_ID ?? `e2e_reboot_${Date.now()}`;
function resetRebootOrg(): void {
  if (process.env.PROVISA_E2E_REBOOT_ORG_ID) return; // explicit override always wins
  REBOOT_ORG_ID = `e2e_reboot_${Date.now()}_${Math.floor(Math.random() * 1e6)}`;
}
export const REBOOT_BACKEND_URL = `http://localhost:${REBOOT_HTTP_PORT}`;
// Same fixed 32-byte key every other e2e backend in this file uses (see E2E_ENCRYPTION_KEY in
// playwright.config.ts) — not a real credential, just a value the vault can always decrypt with.
const REBOOT_ENCRYPTION_KEY = Buffer.from(Array.from({ length: 32 }, (_, i) => i + 1)).toString(
  "base64",
);
// Static docker-network addresses a Trino-engine boot needs so catalog specs it builds (and the
// system catalogs register_system_catalogs creates at startup) resolve from INSIDE Trino's own
// container — mirrors the "trino" webServer entry in playwright.config.ts verbatim; this process
// is not itself in that container, it just needs to embed the same addresses Trino will dial.
const REBOOT_TRINO_EXTRA_ENV: Record<string, string> = {
  PROVISA_ENGINE_CONTROL_PLANE_HOST: "postgres",
  PROVISA_ENGINE_CONTROL_PLANE_PORT: "5432",
  PROVISA_ENGINE_OTEL_S3_ENDPOINT: "http://minio:9000",
  PROVISA_ENGINE_LAKEHOUSE_METASTORE_HOST: "hive-metastore",
  PROVISA_ENGINE_LAKEHOUSE_METASTORE_PORT: "9083",
  PROVISA_ENGINE_QUERY_TIMEOUT: "300",
};

/** The compose-published host port for the shared control-plane postgres — same `docker compose
 * port` lookup playwright.config.ts's own resolveControlPlanePort() does, called directly here
 * rather than depending on its on-disk cache file (this section owns its own process lifecycle
 * independent of the CORE_BACKENDS/ENGINES webServers). */
function resolveSharedPgPort(): string {
  const output = execFileSync(
    "docker",
    ["compose", "-f", path.resolve(ROOT, "docker-compose.core.yml"), "port", "postgres", "5432"],
    { cwd: ROOT, encoding: "utf8" },
  ).trim();
  const port = output.split(":").pop();
  if (!port) {
    throw new Error(`Could not resolve control-plane postgres port from docker output: ${output}`);
  }
  return port;
}

function rebootControlPlaneEnv(): Record<string, string> {
  const pgPassword = process.env.PG_PASSWORD ?? "provisa";
  const url = `postgresql+asyncpg://provisa:${pgPassword}@localhost:${resolveSharedPgPort()}/provisa`;
  return { TENANT_DATABASE_URL: url, PLATFORM_DATABASE_URL: url };
}

/** Fresh, empty data dir + a mutable copy of the domains-only Trino e2e config (no `sources:` —
 * every source this test's process ever knows about is control-plane-only, which is the entire
 * point). Call once per test, before the first spawnRebootBackend — never between two reboots of
 * the SAME test, or the materialize store and control-plane org row this is meant to carry across
 * the restart would be wiped along with it. */
export function prepareRebootDataDir(): void {
  resetRebootOrg();
  fs.rmSync(REBOOT_DATA_DIR, { recursive: true, force: true });
  fs.mkdirSync(REBOOT_DATA_DIR, { recursive: true });
  fs.copyFileSync(path.resolve(ROOT, "config/provisa-trino-e2e.yaml"), REBOOT_CONFIG_PATH);
}

/** Spawn the dedicated reboot-harness backend under `engineKind`, on REBOOT_HTTP_PORT, and wait
 * for it to answer /health. `PROVISA_ENGINE` (an explicit env var) outranks the config file's own
 * `federation_engine: trino` — see engine.py's own precedence doc — so the same config file works
 * for every engineKind unchanged, exactly as an operator flips engines in production by editing
 * the env, not the config file (see provisa.env's PROVISA_ENGINE in a real deploy). */
export async function spawnRebootBackend(engineKind: RebootEngineKind): Promise<ChildProcess> {
  const env: NodeJS.ProcessEnv = {
    ...process.env,
    GRPC_PORT: String(REBOOT_HTTP_PORT + 1),
    FLIGHT_PORT: String(REBOOT_HTTP_PORT + 2),
    PROVISA_BOLT_PORT: String(REBOOT_HTTP_PORT + 3),
    PROVISA_MCP_PORT: String(REBOOT_HTTP_PORT + 4),
    PROVISA_PGWIRE_PORT: String(REBOOT_HTTP_PORT + 5),
    PROVISA_DATA_DIR: REBOOT_DATA_DIR,
    PROVISA_CONFIG: REBOOT_CONFIG_PATH,
    ORG_ID: REBOOT_ORG_ID,
    PROVISA_ENCRYPTION_KEY: REBOOT_ENCRYPTION_KEY,
    PROVISA_ENGINE: engineKind,
    // graphql_remote/openapi read these from the APP PROCESS's own env (not a per-source URL
    // field) — same defaults playwright.config.ts's own webServer entries use. No host rewrite is
    // ever needed for them: the app process that reads this env is native on EITHER engine (only
    // Trino's own JVM coordinator runs in a container), unlike a source whose driver connection
    // really does originate inside that container.
    GRAPHQL_DEMO_URL: REBOOT_GRAPHQL_DEMO_URL,
    PETSTORE_BASE_URL: REBOOT_PETSTORE_OPENAPI_URL.replace(/\/openapi\.json$/, ""),
    ...rebootControlPlaneEnv(),
    ...(engineKind === "trino" ? REBOOT_TRINO_EXTRA_ENV : {}),
  };
  const proc = spawn(
    path.join(ROOT, ".venv", "bin", "uvicorn"),
    ["main:app", "--host", "0.0.0.0", "--port", String(REBOOT_HTTP_PORT)],
    { cwd: ROOT, env, stdio: ["ignore", "pipe", "pipe"] },
  );
  let output = "";
  proc.stdout?.on("data", (d) => {
    output += d.toString();
    process.stderr.write(`[reboot:${engineKind}] ${d}`);
  });
  proc.stderr?.on("data", (d) => {
    output += d.toString();
    process.stderr.write(`[reboot:${engineKind}] ${d}`);
  });

  const deadline = Date.now() + 180000;
  for (;;) {
    if (proc.exitCode !== null) {
      throw new Error(
        `reboot backend (${engineKind}) exited early (code ${proc.exitCode}):\n${output.slice(-4000)}`,
      );
    }
    try {
      const res = await fetch(`${REBOOT_BACKEND_URL}/health`, { signal: AbortSignal.timeout(2000) });
      if (res.ok) return proc;
    } catch {
      // not accepting connections yet
    }
    if (Date.now() > deadline) {
      proc.kill("SIGKILL");
      throw new Error(
        `reboot backend (${engineKind}) never became healthy within ${deadline}ms:\n${output.slice(-4000)}`,
      );
    }
    await new Promise((r) => setTimeout(r, 500));
  }
}

/** Terminate the reboot-harness process (SIGTERM, SIGKILL after a 10s grace period) and wait for
 * it to actually exit — the next spawnRebootBackend binds the SAME port, so a lingering process
 * would make that bind fail rather than boot the new engine. */
export async function killRebootBackend(proc: ChildProcess): Promise<void> {
  if (proc.exitCode !== null) return;
  await new Promise<void>((resolve) => {
    proc.once("exit", () => resolve());
    proc.kill("SIGTERM");
    setTimeout(() => {
      if (proc.exitCode === null) proc.kill("SIGKILL");
    }, 10000);
  });
}

/** Query the reboot-harness backend directly (not through `page` — the browser's fetch stays
 * routed at REBOOT_BACKEND_URL across a reboot via the SAME page.route rewrite the caller already
 * installed for registration, but a raw fetch avoids re-typing SQL into the SQL Explorer editor
 * for every post-reboot check). Returns rows in `sql`'s own column order, matching what every
 * registrar's `assertRows` already expects from the UI table's own row-array rendering.
 *
 * Retries on failure for up to `timeoutMs`: a materialize-only source's first query on a FRESH
 * process can race a background wiring step that every other test in this file never hits (its
 * shared backend is already warm with 50+ sources by the time any single new one is queried) —
 * reproduced live on this exact harness's very first successful registration (DuckDB Binder
 * Error: catalog not yet attached), gone on the next attempt a few hundred ms later. */
export async function queryRebootBackend(sql: string, timeoutMs = 30000): Promise<string[][]> {
  const deadline = Date.now() + timeoutMs;
  let lastError = "";
  for (;;) {
    const res = await fetch(`${REBOOT_BACKEND_URL}/data/sql`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      // "org_admin", not "admin" — the reboot harness's config (provisa-trino-e2e.yaml) declares
      // only org_admin (auth.default_assignments), matching runSqlOnPage's own default role.
      body: JSON.stringify({ sql, role: "org_admin" }),
      signal: AbortSignal.timeout(60000),
    });
    if (res.ok) {
      const body = await res.json();
      const rowObjects = body.data.sql as Record<string, unknown>[];
      // Each SELECT item is either a bare column name (the row-object key itself) or an aliased
      // expression ("... AS alias") — the row object is keyed by the ALIAS, not the raw
      // expression (reproduced live: prometheus's own `CAST(MAX(value) AS INTEGER) AS healthy`
      // read back as `row["CAST(MAX(value) AS INTEGER) AS healthy"]`, always undefined).
      const columns = sql
        .replace(/^SELECT\s+/i, "")
        .split(/\s+FROM\s+/i)[0]
        .split(",")
        .map((c) => {
          const trimmed = c.trim();
          const asMatch = trimmed.match(/\s+AS\s+(\S+)\s*$/i);
          return (asMatch ? asMatch[1] : trimmed).replace(/^"(.*)"$/, "$1");
        });
      // Snowflake (and some other warehouse drivers) return column names UPPERCASED regardless
      // of how the SELECT list spelled them — an exact-key lookup on the lowercase SQL text
      // silently reads undefined. Fall back to a case-insensitive match.
      const lookup = (row: Record<string, unknown>, c: string): unknown => {
        if (c in row) return row[c];
        const lower = c.toLowerCase();
        const found = Object.keys(row).find((k) => k.toLowerCase() === lower);
        return found !== undefined ? row[found] : undefined;
      };
      return rowObjects.map((row) => columns.map((c) => String(lookup(row, c))));
    }
    lastError = await res.text();
    if (Date.now() > deadline) {
      expect(res.ok, lastError).toBeTruthy();
    }
    await new Promise((r) => setTimeout(r, 1000));
  }
}

/** Delete every leftover `e2e_swap_*` source under the reboot-harness org before a fresh case
 * registers a new one. `prepareRebootDataDir` only wipes LOCAL state — the source/table rows
 * themselves live in the shared Postgres control plane and survive across reboots (that
 * persistence is the entire point). Without this, `_attach_tbl`'s dedup key is schema+table only
 * (`native_backend.py`), not source id — a same-named leftover source from an earlier retry
 * (this registrar always uses schema "provisa"/table "product_reviews") silently wins the attach
 * for that key, and the CURRENT run's own source is left with a physical catalog that was never
 * created, "Catalog ... does not exist" on the very first query. Reproduced live on this exact
 * harness. Mirrors sweepZombieSwapSources' own regex, against REBOOT_BACKEND_URL instead of the
 * shared CORE_BACKENDS one. */
export async function sweepRebootZombieSources(): Promise<void> {
  const res = await fetch(`${REBOOT_BACKEND_URL}/admin/graphql`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query: `{ sources { id } }` }),
  });
  if (!res.ok) return; // nothing registered yet on a brand-new org — nothing to sweep
  const sources: Array<{ id: string }> = (await res.json()).data?.sources ?? [];
  const zombies = sources.filter((s) => /^e2e_swap_[a-z0-9_]+_?\d{10,}$/.test(s.id));
  for (const { id } of zombies) {
    await fetch(`${REBOOT_BACKEND_URL}/admin/graphql`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        query: `mutation D($id: String!) { deleteSource(id: $id) { success } }`,
        variables: { id },
      }),
    });
  }
}

/** Rewrite a source's host/database (Docker-unreachable "localhost"/"127.0.0.1") to
 * "host.docker.internal" via `updateSource` — same host-reachability translation
 * reprovisionSourceOnEngine applies on replay (see its own comment for the kafka two-hop special
 * case and the rss/websocket exclusion), but as a one-time persisted UPDATE instead of a mutation
 * replay: reboot's whole point is that NO mutation ever replays. `updateSource` does not itself
 * (re)issue an engine catalog (confirmed: `update_source`'s mutation handler never calls
 * `_register_source_on_engine`, unlike `create_source`'s) — it only fixes the row a LATER reboot's
 * boot-time catalog provisioning (REQ-1730's `extra_sources` plumbing) will read. Call this on the
 * DuckDB-registered source BEFORE rebooting into a containerized engine (currently just Trino);
 * a no-op for a type with no live Trino connector (REQ-842) since its catalog is never issued
 * either way. This is 100% test-harness Docker-topology plumbing, not anything a real deployment
 * needs — Trino and the app process share one network there. */
export async function rewriteHostForContainerizedEngine(sourceId: string, sourceType: string): Promise<void> {
  const res = await fetch(`${REBOOT_BACKEND_URL}/admin/graphql`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
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
  expect(src, `source ${sourceId} not found on the reboot harness's control-plane schema`).toBeTruthy();
  if (sourceType === "kafka") {
    src!.host = "kafka";
    src!.port = 29092;
  } else if (src!.host && sourceType !== "rss" && sourceType !== "websocket") {
    src!.host = src!.host.replace(/\b(?:localhost|127\.0\.0\.1)\b/g, "host.docker.internal");
  }
  if (src!.database) {
    src!.database = src!.database.replace(/\b(?:localhost|127\.0\.0\.1)\b/g, "host.docker.internal");
  }
  const mutation = await fetch(`${REBOOT_BACKEND_URL}/admin/graphql`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      query: `mutation($s: SourceInput!) { updateSource(input: $s) { success message } }`,
      variables: { s: { ...src, passwordRef: undefined, password: src!.passwordRef } },
    }),
  });
  const mutationText = await mutation.text();
  expect(mutation.ok, mutationText).toBeTruthy();
  const body = JSON.parse(mutationText);
  expect(body.errors, JSON.stringify(body.errors)).toBeUndefined();
  expect(body.data.updateSource.success, body.data.updateSource.message).toBeTruthy();
}

/** Every registrar's own `page.request.post("/admin/graphql", ...)` calls (submitRegisterAndExpect
 * Listed's dqDataset lookup, plus a handful of registrar-specific ones — graphql_remote/
 * grpc_remote/ingest) are NOT intercepted by `page.route()` (a separate APIRequestContext — see
 * reprovisionSourceOnEngine's own comment on this exact gotcha), so pointing `page`'s BROWSER
 * traffic at the reboot-harness backend still leaves those calls hitting the default/Vite-proxied
 * backend. Rather than thread a `baseUrl` param through every one of the 54 registrars, swap
 * `page.request` for a plain-`fetch` shim pointed at `REBOOT_BACKEND_URL` for the duration of `fn`
 * — every relative-path `page.request.post("/admin/graphql")` call anywhere in the registration
 * flow then resolves correctly, registrar-agnostic. Only `.post(url, {data, headers})` is ever
 * called through `page.request` anywhere in this codebase (confirmed by grep) — this shim
 * implements only that, not the full `APIRequestContext` surface. (`request.newContext()`, the
 * "real" way to mint a differently-scoped APIRequestContext, is a method on the top-level
 * `playwright.request` MODULE export — not available as a standalone import in this Playwright
 * version — and is NOT a method the per-test `request` FIXTURE instance itself exposes; confirmed
 * live: "request.newContext is not a function". This shim sidesteps that entirely.) */
function withRebootApiRequest<T>(page: Page, fn: () => Promise<T>): Promise<T> {
  const shim = {
    post: async (url: string, opts?: { data?: unknown; headers?: Record<string, string> }) => {
      const fullUrl = /^https?:\/\//.test(url) ? url : `${REBOOT_BACKEND_URL}${url}`;
      const res = await fetch(fullUrl, {
        method: "POST",
        headers: { "Content-Type": "application/json", ...(opts?.headers ?? {}) },
        body: opts?.data !== undefined ? JSON.stringify(opts.data) : undefined,
      });
      const bodyText = await res.text();
      return {
        ok: () => res.ok,
        status: () => res.status,
        text: async () => bodyText,
        json: async () => JSON.parse(bodyText),
      };
    },
  };
  const original = page.request;
  Object.defineProperty(page, "request", { value: shim, configurable: true });
  return fn().finally(() => {
    Object.defineProperty(page, "request", { value: original, configurable: true });
  });
}

/** REQ-1730 scenario 2: with engine X primary from a COLD START (not DuckDB, the harness's usual
 * registration backend), does the real Sources-form UI still work end to end — create source,
 * register table, query — through the ordinary path, no replay? Boots ONE dedicated reboot-
 * harness process under `engineKind`, points `page` at it for the WHOLE flow (registration
 * included, unlike scenario 1's reboot-after-register), runs `registrar` against it, then queries
 * through the real SQL Explorer (`runSqlOnPage`, not a raw fetch) — the UI itself is what's under
 * test here, e.g. TrinoBackend-only affordances like the schema dropdown (playwright.config.ts's
 * own sharepoint/splunk comment: "NativeBackend.register_source is a no-op so the schema dropdown
 * never populates") that a DuckDB-registered-then-replayed flow would never exercise. */
export async function runFreshEngineCase(
  page: Page,
  engineKind: RebootEngineKind,
  registrar: (page: Page) => Promise<Registration>,
): Promise<void> {
  prepareRebootDataDir();
  const proc = await spawnRebootBackend(engineKind);
  try {
    await sweepRebootZombieSources();
    const routes = ["/admin", "/data", "/query", "/health"].map((prefix) => `${UI_URL}${prefix}**`);
    for (const pattern of routes) {
      await page.route(pattern, (route) => {
        route.continue({ url: route.request().url().replace(UI_URL, REBOOT_BACKEND_URL) });
      });
    }
    const registration = await withRebootApiRequest(page, () => registrar(page));
    const rows = await runSqlOnPage(page, registration.sql);
    registration.assertRows(rows);
    for (const pattern of routes) await page.unroute(pattern);
  } finally {
    await killRebootBackend(proc);
  }
}

/** REQ-1730 scenario 1, generalized: register `registrar` once under DuckDB (the reboot-harness's
 * own dedicated process, not CORE_BACKENDS/ENGINES — same primitives scenario 2 uses), then for
 * each engine in `engineSequence`, genuinely KILL and RESPAWN that same process with the engine
 * flipped (same port/data dir/org) and query again — no replay of the registration mutation ever.
 * `hostRewrites` (default {}): per-engine host override to apply (via
 * `rewriteHostForContainerizedEngine`) before rebooting into that engine — pass e.g. `{ trino:
 * "mongodb" }` when `registrar`'s type has a live connector reachable through the docker-topology
 * host rewrite (see that function's own doc); omit an engine here for a type with no such
 * dependency (a `LAND`-only source, a file path identity-mounted into the container, etc.) — a
 * no-op call for a type with no connector is harmless either way (REQ-842), so default to calling
 * it for every engine unless the caller has a reason not to.
 *
 * One registrar per test, matching runSwapCase's own "one type = one test" contract — see this
 * file's module doc for why that split matters (isolating a single type's failure, targeted
 * reruns). This is the primitive to reuse when extending REQ-1730 scenario 1 to more source
 * types: `test("<type>: ...", ({ page }) => runRebootCase(page, () =>
 * register<Type>(page), { trino: "<type>" }))`. */
export async function runRebootCase(
  page: Page,
  registrar: (page: Page) => Promise<Registration>,
  hostRewriteTypes: Partial<Record<RebootEngineKind, string>> = { trino: "" },
  engineSequence: RebootEngineKind[] = ["trino"],
  // kafka (REQ-1730, see registerKafka's own module doc): "the DuckDB leg is proven by
  // registration alone... only the Trino leg's assertion actually reads the message back, via a
  // REAL live scan of the topic, not a materialized copy" — DuckDB was NEVER meant to answer this
  // type's query at all, unlike every other type here. runSwapCase (the original harness) never
  // queries DuckDB for ANY type, so this gap was invisible until scenario 1 added a DuckDB
  // baseline query as its OWN starting assertion. Reproduced live: kafka's baseline query came
  // back `200 OK, []` even after a 60s poll-retry — not a timing issue, DuckDB genuinely has no
  // row-level read path for kafka. Set true to skip the baseline query/assertion for a type with
  // the same property; the reboot-and-query-under-the-target-engine loop below still runs.
  skipDuckdbBaseline = false,
): Promise<void> {
  test.setTimeout(120000 + engineSequence.length * 180000);
  prepareRebootDataDir();
  let proc = await spawnRebootBackend("duckdb");
  try {
    await sweepRebootZombieSources();
    const routes = ["/admin", "/data", "/query", "/health"].map((prefix) => `${UI_URL}${prefix}**`);
    for (const pattern of routes) {
      await page.route(pattern, (route) => {
        route.continue({ url: route.request().url().replace(UI_URL, REBOOT_BACKEND_URL) });
      });
    }
    const registration = await withRebootApiRequest(page, () => registrar(page));
    // A CDC-listener/push-wired type (websocket) needs its own wiring to catch up after EVERY
    // reboot, not just after the original harness's replay — pollTimeoutMs is that type's own
    // declared tolerance for that (see rss/websocket's own comment). queryRebootBackend's own
    // retry loop only retries on an HTTP failure, never on a successful-but-EMPTY response — the
    // shape landing/CDC-wiring races actually take. When pollTimeoutMs is set, retry the WHOLE
    // query call (fresh HTTP request each time, like trySqlOnPage's own poll loop) until rows are
    // non-empty or the deadline passes.
    const pollFor = async (): Promise<string[][]> => {
      if (!registration.pollTimeoutMs) return queryRebootBackend(registration.sql);
      const deadline = Date.now() + registration.pollTimeoutMs;
      let rows: string[][] = [];
      for (;;) {
        rows = await queryRebootBackend(registration.sql);
        if (rows.length > 0 || Date.now() > deadline) return rows;
        await new Promise((r) => setTimeout(r, 3000));
      }
    };
    let rows: string[][] = [];
    if (!skipDuckdbBaseline) {
      rows = await pollFor();
      registration.assertRows(rows);
    }

    for (const engineKind of engineSequence) {
      const sourceType = hostRewriteTypes[engineKind];
      if (engineKind !== "duckdb" && sourceType !== undefined) {
        await rewriteHostForContainerizedEngine(registration.sourceId, sourceType || registration.label);
      }
      await killRebootBackend(proc);
      proc = await spawnRebootBackend(engineKind);
      rows = await pollFor();
      registration.assertRows(rows);
    }
    for (const pattern of routes) await page.unroute(pattern);
  } finally {
    await killRebootBackend(proc);
  }
}

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
export async function sweepZombieSwapSources(): Promise<void> {
  const res = await fetch(`${BACKEND_URL}/admin/graphql`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query: `{ sources { id } }` }),
  });
  const sources: Array<{ id: string }> = (await res.json()).data?.sources ?? [];
  // registerIngest's sourceId (engine-swap-registrars.ts) breaks the usual "_<stamp>" ending on
  // purpose (a real ingest-only physical-table-naming gap — see that registrar's own comment), so
  // the trailing underscore here is optional rather than required, matching both shapes.
  const zombies = sources.filter((s) => /^e2e_swap_[a-z0-9_]+_?\d{10,}$/.test(s.id));
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



/** One type's full proof: register under DuckDB, then answer identically under every engine its
 * Registration declares reachableOn. Shared body for every per-type test below — see the
 * file-scope afterAll above for how the source this creates gets cleaned up. */
export async function runSwapCase(page: Page, registrar: () => Promise<Registration>): Promise<void> {
  test.setTimeout((1 + 5 * ENGINES.length) * 60 * 1000);
  const registration = await registrar();
  for (const engine of ENGINES) {
    await requeryOnEngine(page, engine, [registration]);
  }
}

