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

import { execFileSync } from "node:child_process";
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
// request, which is exactly what makes rss (unlike websocket's one-shot push) tractable for this
// harness's "register once, swap engine, requery" pattern: a poll job started fresh on the
// Trino-bound backend after the swap re-polls and lands the identical items.
export const E2E_RSS_PORT = 33111;

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

