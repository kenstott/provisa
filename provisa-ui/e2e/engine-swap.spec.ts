// Copyright (c) 2026 Kenneth Stott
// Canary: 330c3fe8-08fc-4973-ac36-6f85d5d8fb8b
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
// graphql_remote/openapi need nothing engine-specific to reach under Trino. splunk goes through
// the full swap like the original 9 — DuckDB reaches it via the connector's bundled Calcite
// pgwire bridge (schema = the source id, `pgwire_replica.schema_name()`); Trino reaches it via a
// DIFFERENT, standalone `trino-splunk` plugin. That plugin used to always register its schema
// under the fixed string "splunk" regardless of configuration — verified live via
// `SCHEMA_NOT_FOUND: Schema '<source_id>' does not exist` before the fix — fixed upstream
// (calcite/splunk's SplunkDriver now honors a `schema` connection property,
// kenstott/calcite@28db96ca1) plus `TrinoSplunkConnector.details()` passing that same schema name
// (trino_connectors.py). sharepoint goes through the full swap too, but needed NO calcite-repo
// change — the trino-sharepoint plugin already had a fully working `schema` catalog property
// end to end; only `TrinoSharepointConnector.details()` needed to start passing it (verified by
// reading the plugin source before assuming the same upstream fix splunk needed):
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
import { execFileSync, spawn, type ChildProcess } from "node:child_process";
import fs from "node:fs";
import http from "node:http";
import path from "node:path";

import { WebSocketServer } from "ws";

import { test, expect, UI_URL } from "./coverage";
import { startDemoSources, removeDemoSources } from "./demo-source-containers";
import {
  CLOUD_WAREHOUSE_SEED,
  E2E_DRUID_BROKER_PORT,
  E2E_DRUID_COORD_PORT,
  E2E_EXASOL_FINGERPRINT_FILE,
  E2E_EXASOL_PORT,
  E2E_GRPC_REMOTE_PORT,
  DOCKER_NETWORK,
  E2E_KAFKA_PORT,
  E2E_PINOT_BROKER_PORT,
  E2E_PINOT_CONTROLLER_PORT,
  E2E_KAFKA_SCHEMA_REGISTRY_PORT,
  E2E_RSS_PORT,
  E2E_TRINO_SOURCE_PORT,
  E2E_WS_PORT,
  FILE_LAKE_HOST_DIR,
  GRPC_REMOTE_SERVER_MODULE,
  MAKE_FILE_LAKE_FIXTURES,
  PYTHON,
  REBOOT_GRAPHQL_DEMO_URL,
  REBOOT_PETSTORE_OPENAPI_URL,
  ROOT,
  RUNNING_IN_CI,
  SINGLESTORE_AVAILABLE,
  SNOWFLAKE_ENGINE_AVAILABLE,
  DATABRICKS_ENGINE_AVAILABLE,
  BIGQUERY_ENGINE_AVAILABLE,
  provisionRedshift,
  provisionSynapse,
  provisionSwapSource,
  runFreshEngineCase,
  runRebootCase,
  runSwapCase,
  sweepZombieSwapSources,
  waitForPort,
  waitForTrinoStable,
} from "./engine-swap-helpers";
import type { RedshiftConnection, SynapseConnection } from "./engine-swap-helpers";
import {
  RDB_WIDGETS_SOURCES,
  registerAirport,
  registerBigquery,
  registerCassandra,
  registerDatabricks,
  registerElasticsearch,
  registerExasol,
  registerFabric,
  registerFileLake,
  registerFiles,
  registerFirebird,
  registerGsheets,
  registerSoda,
  registerGreatExpectations,
  registerGovdata,
  registerGraphqlRemote,
  registerRss,
  registerWebsocket,
  registerGrpcRemote,
  registerIngest,
  registerKafka,
  registerMongodb,
  registerNeo4j,
  registerOpenapi,
  registerPrometheus,
  registerDruid,
  registerHive,
  registerHiveS3,
  registerPinot,
  registerRdbWidgets,
  registerRedis,
  registerRedshift,
  registerSynapse,
  registerSinglestore,
  registerSingleFile,
  registerSnowflake,
  registerSparql,
  registerSharepoint,
  registerSplunk,
  registerSqlite,
  registerDuckdbSource,
  registerTrinoSource,
  WIDGETS_DUCKDB_PATH,
} from "./engine-swap-registrars";

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

// Written ONCE per worker (like waitForTrinoStable above), under FILE_LAKE_HOST_DIR — the SAME
// fixed directory docker-compose.core.yml mounts into Trino, not a random per-run temp dir
// (source-to-query-file-lake.spec.ts's own os.tmpdir() approach doesn't work here precisely
// because it's random: a static docker-compose volume mount can't follow it). Each run gets its
// own never-before-seen subdirectory (a timestamp), and nothing under the mount is ever deleted:
// Docker Desktop's VirtioFS attaches the bind mount to the directory's inode at container start,
// and a delete+recreate cycle ANYWHERE beneath the mount point silently poisons its directory
// cache for the WHOLE mount, not just the touched subpath (verified live 2026-09-16: rmSync+
// mkdirSync of the mount root orphaned it outright; even rmSync+recreate of a child directory,
// with the mount root itself left alone, broke live sync for the entire tree minutes later,
// including previously-synced top-level files). Never rm anything here — old runs' subdirectories
// are simply abandoned (an accepted, bounded amount of test-only disk growth), not cleaned up.
let deltaTablePath = "";
let icebergTablePath = "";
test.beforeAll(() => {
  fs.mkdirSync(FILE_LAKE_HOST_DIR, { recursive: true });
  const runDir = path.join(FILE_LAKE_HOST_DIR, `run_${Date.now()}`);
  fs.mkdirSync(runDir, { recursive: true });
  const out = execFileSync(PYTHON, [MAKE_FILE_LAKE_FIXTURES, runDir], {
    encoding: "utf8",
  });
  [deltaTablePath, icebergTablePath] = out.trim().split("\n");
});


test.afterAll(sweepZombieSwapSources);

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

  test.describe("splunk", () => {
    // amd64-only image (demo/sources/splunk/compose.yml), slow cold boot under emulation
    // (~3 min native, longer emulated) — the same budget source-to-query.spec.ts's own splunk
    // case uses, well past playwright.config.ts's 90s global default.
    test.beforeAll(() => {
      test.setTimeout(900000);
      startDemoSources(["splunk"]);
    });
    test.afterAll(async () => {
      await removeDemoSources(["splunk"]);
      await sweepZombieSwapSources();
    });

    test("splunk: register once under DuckDB, answer identical queries under every other engine", async ({
      page,
    }) => {
      test.setTimeout(900000);
      await runSwapCase(page, () => registerSplunk(page));
    });
  });

  test.describe("sharepoint", () => {
    test.skip(
      !process.env.SP_SITE_URL,
      "no live SharePoint credentials: set the SP_* block in the root .env",
    );

    test("sharepoint: register once under DuckDB, answer identical queries under every other engine", async ({
      page,
    }) => {
      test.setTimeout(300000);
      await runSwapCase(page, () => registerSharepoint(page));
    });
  });

  // sqlite/graphql_remote/openapi need no container of their own (a local file / the already-
  // running graphql-demo and petstore-mock webServers respectively), so nothing to provision here.
  test("sqlite: register once under DuckDB, answer identical queries under every other engine", async ({
    page,
  }) => runSwapCase(page, () => registerSqlite(page)));

  test("graphql_remote: register once under DuckDB, answer identical queries under every other engine", async ({
    page,
  }) => runSwapCase(page, () => registerGraphqlRemote(page)));

  test("openapi: register once under DuckDB, answer identical queries under every other engine", async ({
    page,
  }) => runSwapCase(page, () => registerOpenapi(page)));

  // ingest needs no container/webServer either — a NO_CONNECTION_TYPES push receiver, POST is the
  // write itself (see registerIngest's own module doc in engine-swap-registrars.ts).
  test("ingest: register once under DuckDB, answer identical queries under every other engine", async ({
    page,
  }) => runSwapCase(page, () => registerIngest(page)));

  test.describe("govdata", () => {
    test.skip(
      !process.env.FREE_ASKAMERICA_KEY,
      "no live AskAmerica/govdata credentials: set FREE_ASKAMERICA_KEY in the root .env",
    );

    test("govdata: register once under DuckDB, answer identical queries under every other engine", async ({
      page,
    }) => runSwapCase(page, () => registerGovdata(page)));
  });

  // rss's feed fixture is a plain in-process Node http.Server (same shape as source-to-query-
  // streaming.spec.ts's own, distinct port — see E2E_RSS_PORT's own comment) that re-serves the
  // SAME static feed on every request, scoped to just this describe block per the file's own
  // "one source = one test" redesign.
  test.describe("rss", () => {
    // pubDate is REQUIRED, not decorative: rss_provider.py's _parse_date() maps a missing/
    // unparseable pubDate to datetime.min, and poll_once()'s own watermark defaults to
    // datetime.min for a fresh (per-call) provider too — `pub <= watermark` then reads
    // datetime.min <= datetime.min (True), silently filtering out every single item. Reproduced
    // live: an earlier version of this fixture omitted pubDate and the poll job wired, fired, and
    // "landed" 0 rows every time, with no error anywhere in the pipeline (store_writer.land() was
    // reached and executed a genuine 0-row replace) — this fixture must not regress that.
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

    test("rss: register once under DuckDB, answer identical queries under every other engine", async ({
      page,
    }) => runSwapCase(page, () => registerRss(page)));
  });

  // websocket's fixture (the `ws` package, same as source-to-query-streaming.spec.ts's own) sends
  // its fixed 2-event payload on every NEW connection, not just the first ever — what makes it
  // tractable the same way rss's re-pollable static feed is.
  test.describe("websocket", () => {
    let wsServer: WebSocketServer;
    test.beforeAll(() => {
      wsServer = new WebSocketServer({ port: E2E_WS_PORT });
      wsServer.on("connection", (socket) => {
        socket.send(JSON.stringify({ id: "ws-1", value: "hello" }));
        socket.send(JSON.stringify({ id: "ws-2", value: "world" }));
      });
    });
    test.afterAll(async () => {
      // Unlike source-to-query-streaming.spec.ts's own single-backend version of this fixture,
      // THIS harness has TWO backend processes (DuckDB-bound and Trino-bound) each holding their
      // own persistent push_wiring.py listener connection open to this server — WebSocketServer
      // .close() alone only stops accepting NEW connections; it does not force-close existing
      // client sockets, so it never resolves while either backend's listener is still connected
      // (reproduced live: the afterAll hook timed out at 90s). Terminate every open client first.
      for (const client of wsServer.clients) client.terminate();
      await new Promise<void>((resolve, reject) =>
        wsServer.close((err) => (err ? reject(err) : resolve())),
      );
    });

    test("websocket: register once under DuckDB, answer identical queries under every other engine", async ({
      page,
    }) => runSwapCase(page, () => registerWebsocket(page)));
  });

  // grpc_remote has no Trino connector — same landing path as graphql_remote/openapi above, just
  // gated on a Python subprocess this harness spawns itself rather than a container or an
  // already-running demo webServer (see demo/grpc_remote_server, REQ-1742).
  test.describe("grpc_remote", () => {
    let grpcServer: ChildProcess | null = null;

    test.beforeAll(async () => {
      grpcServer = spawn(PYTHON, ["-m", GRPC_REMOTE_SERVER_MODULE], {
        cwd: ROOT,
        env: { ...process.env, DEMO_GRPC_REMOTE_PORT: String(E2E_GRPC_REMOTE_PORT) },
        stdio: "pipe",
      });
      await waitForPort(E2E_GRPC_REMOTE_PORT, 30000);
    });
    test.afterAll(async () => {
      grpcServer?.kill();
      await sweepZombieSwapSources();
    });

    test("grpc_remote: register once under DuckDB, answer identical queries under every other engine", async ({
      page,
    }) => runSwapCase(page, () => registerGrpcRemote(page, E2E_GRPC_REMOTE_PORT)));
  });

  test.describe("firebird", () => {
    test.beforeAll(() => provisionSwapSource("firebird", "up"));
    test.afterAll(async () => {
      await provisionSwapSource("firebird", "down");
      await sweepZombieSwapSources();
    });

    test("firebird: register once under DuckDB, answer identical queries under every other engine", async ({
      page,
    }) => runSwapCase(page, () => registerFirebird(page)));
  });

  test.describe("airport", () => {
    test.beforeAll(() => provisionSwapSource("airport", "up"));
    test.afterAll(async () => {
      await provisionSwapSource("airport", "down");
      await sweepZombieSwapSources();
    });

    test("airport: register once under DuckDB, answer identical queries under every other engine", async ({
      page,
    }) => runSwapCase(page, () => registerAirport(page)));
  });

  test.describe("singlestore", () => {
    test.skip(
      !SINGLESTORE_AVAILABLE,
      "needs SINGLESTORE_HOST/USERNAME/PASSWORD/DATABASE for the SingleStore Cloud workspace",
    );

    test("singlestore: register once under DuckDB, answer identical queries under every other engine", async ({
      page,
    }) => runSwapCase(page, () => registerSinglestore(page)));
  });

  test.describe("exasol", () => {
    test.skip(
      !RUNNING_IN_CI,
      "exasol/docker-db needs privileged mode + several GB RAM + a multi-minute cold init, and " +
        "is amd64-only (unbootable under arm64 emulation) — runs for real in CI (ubuntu-latest " +
        "is a genuine amd64 host); see source-to-query-olap-lake.spec.ts's identical gate",
    );
    let exasolFingerprint = "";
    test.beforeAll(() => {
      if (!RUNNING_IN_CI) return;
      test.setTimeout(900000); // EXAStorage cold init genuinely takes minutes, not seconds
      if (fs.existsSync(E2E_EXASOL_FINGERPRINT_FILE)) fs.rmSync(E2E_EXASOL_FINGERPRINT_FILE);
      provisionSwapSource("exasol", "up", {
        PROVISA_DEMO_EXASOL_PORT: String(E2E_EXASOL_PORT),
        PROVISA_DEMO_EXASOL_FINGERPRINT_FILE: E2E_EXASOL_FINGERPRINT_FILE,
      });
      exasolFingerprint = fs.readFileSync(E2E_EXASOL_FINGERPRINT_FILE, "utf8").trim();
    });
    test.afterAll(async () => {
      if (!RUNNING_IN_CI) return;
      await provisionSwapSource("exasol", "down");
      if (fs.existsSync(E2E_EXASOL_FINGERPRINT_FILE)) fs.rmSync(E2E_EXASOL_FINGERPRINT_FILE);
      await sweepZombieSwapSources();
    });

    test("exasol: register once under DuckDB, answer identical queries under every other engine", async ({
      page,
    }) => runSwapCase(page, () => registerExasol(page, () => exasolFingerprint)));
  });

  test.describe("kafka", () => {
    test.beforeAll(() => {
      test.setTimeout(180000);
      provisionSwapSource(
        "kafka",
        "up",
        {
          PROVISA_DEMO_KAFKA_PORT: String(E2E_KAFKA_PORT),
          PROVISA_DEMO_KAFKA_SCHEMA_REGISTRY_PORT: String(E2E_KAFKA_SCHEMA_REGISTRY_PORT),
        },
        DOCKER_NETWORK,
      );
    });
    test.afterAll(async () => {
      await provisionSwapSource("kafka", "down");
      await sweepZombieSwapSources();
    });

    test("kafka: register once under DuckDB, answer identical queries under every other engine", async ({
      page,
    }) => runSwapCase(page, () => registerKafka(page)));
  });

  // Every demo/sources/<name> RDBMS below primes the identical widgets(id, name) + 3 rows shape
  // (verified live 2026-09-16) and has a native Trino connector (provisa/federation/
  // trino_connectors.py's TRINO_CONNECTORS/_TRINO_JDBC_TYPES) — the SAME class of Trino reach as
  // mongodb/cassandra/redis/elasticsearch above, not the adapter-fetch/materialize-only fallback
  // neo4j/sparql need. registerRdbWidgets(cfg) is the shared registrar; only the type/port/
  // credentials/schema differ (RDB_WIDGETS_SOURCES).
  for (const cfg of RDB_WIDGETS_SOURCES) {
    test.describe(cfg.type, () => {
      // No test.skip() signal inside beforeAll/afterAll (a plain early return instead) — the
      // per-test test.skip() below is what reports the actual skip; this only avoids
      // provisioning a fixture (SAP HANA Express, 5-15min cold init) nothing will use locally.
      test.beforeAll(() => {
        if (cfg.needsCi && !RUNNING_IN_CI) return;
        if (cfg.bootTimeoutMs) test.setTimeout(cfg.bootTimeoutMs);
        return provisionSwapSource(cfg.type, "up");
      });
      test.afterAll(async () => {
        if (cfg.needsCi && !RUNNING_IN_CI) return;
        if (cfg.bootTimeoutMs) test.setTimeout(cfg.bootTimeoutMs);
        await provisionSwapSource(cfg.type, "down");
        await sweepZombieSwapSources();
      });

      test(`${cfg.type}: register once under DuckDB, answer identical queries under every other engine`, async ({
        page,
      }) => {
        test.skip(
          cfg.needsCi === true && !RUNNING_IN_CI,
          "saplabs/hanaexpress's indexserver does not start under Docker Desktop's Apple " +
            "Silicon VM (verified live, not a config issue) — runs for real in CI " +
            "(ubuntu-latest is a genuine amd64 host)",
        );
        await runSwapCase(page, () => registerRdbWidgets(cfg)(page));
      });
    });
  }

  // delta_lake/iceberg: no container (registerFileLake reads the file-level fixtures written by
  // the beforeAll near waitForTrinoStable, above) — no per-type provisionSwapSource beforeAll/
  // afterAll needed, just the source cleanup runSwapCase/sweepZombieSwapSources already give
  // every other type here.
  test.describe("delta_lake", () => {
    test(`delta_lake: register once under DuckDB, answer identical queries under every other engine`, async ({
      page,
    }) => runSwapCase(page, () => registerFileLake("delta_lake", () => deltaTablePath)(page)));
  });

  test.describe("iceberg", () => {
    test(`iceberg: register once under DuckDB, answer identical queries under every other engine`, async ({
      page,
    }) => runSwapCase(page, () => registerFileLake("iceberg", () => icebergTablePath)(page)));
  });

  test.describe("snowflake", () => {
    test.skip(
      !(
        process.env.SNOWFLAKE_ACCOUNT &&
        process.env.SNOWFLAKE_USER &&
        process.env.SNOWFLAKE_PASSWORD
      ),
      "no live Snowflake credentials in this environment (SNOWFLAKE_ACCOUNT/SNOWFLAKE_USER/SNOWFLAKE_PASSWORD)",
    );
    test.beforeAll(() => {
      execFileSync(PYTHON, [CLOUD_WAREHOUSE_SEED, "snowflake", "up"], { stdio: "pipe" });
    });
    test.afterAll(async () => {
      execFileSync(PYTHON, [CLOUD_WAREHOUSE_SEED, "snowflake", "down"], { stdio: "pipe" });
      await sweepZombieSwapSources();
    });

    test(`snowflake: register once under DuckDB, answer identical queries under every other engine`, async ({
      page,
    }) => runSwapCase(page, () => registerSnowflake(page)));
  });

  test.describe("databricks", () => {
    test.skip(
      !(
        process.env.DATABRICKS_SERVER_HOSTNAME &&
        process.env.DATABRICKS_HTTP_PATH &&
        process.env.DATABRICKS_TOKEN
      ),
      "no live Databricks credentials in this environment (DATABRICKS_SERVER_HOSTNAME/DATABRICKS_HTTP_PATH/DATABRICKS_TOKEN)",
    );
    test.beforeAll(() => {
      execFileSync(PYTHON, [CLOUD_WAREHOUSE_SEED, "databricks", "up"], { stdio: "pipe" });
    });
    test.afterAll(async () => {
      execFileSync(PYTHON, [CLOUD_WAREHOUSE_SEED, "databricks", "down"], { stdio: "pipe" });
      await sweepZombieSwapSources();
    });

    test(`databricks: register once under DuckDB, answer identical queries under every other engine`, async ({
      page,
    }) => runSwapCase(page, () => registerDatabricks(page)));
  });

  test.describe("fabric", () => {
    test.skip(
      !(process.env.FABRIC_SQL_SERVER && process.env.FABRIC_DATABASE),
      "no live Fabric credentials in this environment (FABRIC_SQL_SERVER/FABRIC_DATABASE)",
    );
    // REQ-1775: a paused Fabric capacity rejects the SQL connection outright — needs the ARM
    // capacity identifiers to resume it, same gate source-to-query-cloud-warehouse.spec.ts's own
    // fabric case uses.
    test.skip(
      !(process.env.FABRIC_RESOURCE_GROUP && process.env.FABRIC_CAPACITY_NAME),
      "no FABRIC_RESOURCE_GROUP/FABRIC_CAPACITY_NAME configured — a Fabric capacity must be " +
        "created once (a real-money Azure resource), see tests/integration/fabric_capacity.py's " +
        "module docstring for the one-time az CLI command",
    );
    test.beforeAll(() => {
      execFileSync(PYTHON, [CLOUD_WAREHOUSE_SEED, "fabric", "up"], { stdio: "pipe" });
    });
    test.afterAll(async () => {
      execFileSync(PYTHON, [CLOUD_WAREHOUSE_SEED, "fabric", "down"], { stdio: "pipe" });
      await sweepZombieSwapSources();
    });

    test(`fabric: register once under DuckDB, answer identical queries under every other engine`, async ({
      page,
    }) => runSwapCase(page, () => registerFabric(page)));
  });

  test.describe("bigquery", () => {
    test.skip(
      !(process.env.GOOGLE_CLOUD_PROJECT && process.env.GOOGLE_APPLICATION_CREDENTIALS),
      "no live GCP credentials in this environment (GOOGLE_CLOUD_PROJECT/GOOGLE_APPLICATION_CREDENTIALS)",
    );
    test.beforeAll(() => {
      execFileSync(PYTHON, [CLOUD_WAREHOUSE_SEED, "bigquery", "up"], { stdio: "pipe" });
    });
    test.afterAll(async () => {
      execFileSync(PYTHON, [CLOUD_WAREHOUSE_SEED, "bigquery", "down"], { stdio: "pipe" });
      await sweepZombieSwapSources();
    });

    test(`bigquery: register once under DuckDB, answer identical queries under every other engine`, async ({
      page,
    }) => runSwapCase(page, () => registerBigquery(page)));
  });

  test.describe("csv", () => {
    test(`csv: register once under DuckDB, answer identical queries under every other engine`, async ({
      page,
    }) =>
      runSwapCase(page, () =>
        registerSingleFile(
          "csv",
          "demo/files/customers.csv",
          /CSV File Path/,
          (table) => `SELECT id, first_name, email FROM pet_store.${table} ORDER BY id`,
          (rows) => {
            expect(rows).toHaveLength(15);
            expect(rows[0]).toEqual(["1", "Alice", "alice@example.com"]);
          },
        )(page),
      ));
  });

  test.describe("parquet", () => {
    test(`parquet: register once under DuckDB, answer identical queries under every other engine`, async ({
      page,
    }) =>
      runSwapCase(page, () =>
        registerSingleFile(
          "parquet",
          "demo/files/products.parquet",
          /Parquet File Path/,
          (table) => `SELECT id, sku, name, price FROM pet_store.${table} ORDER BY id`,
          (rows) => {
            expect(rows).toHaveLength(15);
            expect(rows[0]).toEqual(["1", "WIDGET-A", "Widget Alpha", "9.99"]);
          },
        )(page),
      ));
  });

  test.describe("files", () => {
    test(`files: register once under DuckDB, answer identical queries under every other engine`, async ({
      page,
    }) => runSwapCase(page, () => registerFiles()(page)));
  });

  // REQ-1730: pinot/druid/hive_s3 each now have a real DuckDB row-fetch (provisa/pinot/,
  // provisa/druid/, provisa/hive/ — materialized, not a live ATTACH) — the standard
  // register-under-DuckDB-first flow every RDB/warehouse type above already uses, same as
  // mysql/oracle/databricks/clickhouse (no DuckDB attach connector either, still swap-eligible).
  // Materialized-vs-attach is irrelevant to whether runSwapCase applies; only that SOME
  // registration path exists under the DuckDB-backed engine.
  test.describe("pinot", () => {
    test.beforeAll(() => {
      test.setTimeout(300000);
      return provisionSwapSource("pinot", "up", {
        PROVISA_DEMO_PINOT_CONTROLLER_PORT: String(E2E_PINOT_CONTROLLER_PORT),
        PROVISA_DEMO_PINOT_BROKER_PORT: String(E2E_PINOT_BROKER_PORT),
        PROVISA_TRINO_NETWORK: DOCKER_NETWORK,
      });
    });
    test.afterAll(async () => {
      await provisionSwapSource("pinot", "down");
      await sweepZombieSwapSources();
    });

    test("pinot: register once under DuckDB, answer identical queries under every other engine", async ({
      page,
    }) => runSwapCase(page, () => registerPinot(page)));
  });

  test.describe("druid", () => {
    test.beforeAll(() => {
      test.setTimeout(900000);
      provisionSwapSource(
        "druid",
        "up",
        {
          PROVISA_DEMO_DRUID_COORD_PORT: String(E2E_DRUID_COORD_PORT),
          PROVISA_DEMO_DRUID_BROKER_PORT: String(E2E_DRUID_BROKER_PORT),
          PROVISA_TRINO_NETWORK: DOCKER_NETWORK,
        },
        DOCKER_NETWORK,
      );
      execFileSync(PYTHON, [path.join(ROOT, "demo", "sources", "druid", "prime.py")], {
        stdio: "inherit",
        env: {
          ...process.env,
          PROVISA_DEMO_DRUID_COORD_PORT: String(E2E_DRUID_COORD_PORT),
          PROVISA_DEMO_DRUID_BROKER_PORT: String(E2E_DRUID_BROKER_PORT),
        },
      });
    });
    test.afterAll(async () => {
      await provisionSwapSource("druid", "down");
      await sweepZombieSwapSources();
    });

    test("druid: register once under DuckDB, answer identical queries under every other engine", async ({
      page,
    }) => runSwapCase(page, () => registerDruid(page)));
  });

  test.describe("hive_s3", () => {
    test.beforeAll(async () => {
      test.setTimeout(300000);
      await provisionSwapSource("hive-s3", "up", {}, DOCKER_NETWORK);
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
    test.afterAll(async () => {
      await provisionSwapSource("hive-s3", "down");
      await sweepZombieSwapSources();
    });

    test("hive_s3: register once under DuckDB, answer identical queries under every other engine", async ({
      page,
    }) => runSwapCase(page, () => registerHiveS3(page)));
  });

  // duckdb-as-a-source and trino-as-a-source (REQ-994) are meta/self-referential SourceTypes —
  // both already have a generic DIRECT-driver connector on every engine (executor/drivers/
  // registry.py's `_make_duckdb`/`_make_trino`, FederationEngine.complete_reach(), REQ-947), so
  // federate() resolves both to Strategy.MATERIALIZED universally and the standard
  // register-under-DuckDB swap applies, same as duckdb-source's own reboot-suite test just below.
  test.describe("duckdb-source", () => {
    test.beforeAll(() => {
      execFileSync(
        PYTHON,
        [
          "-c",
          "import sys; sys.path.insert(0, 'demo/files'); " +
            "from create_demo_files import create_widgets_duckdb; create_widgets_duckdb()",
        ],
        { cwd: ROOT, stdio: "pipe" },
      );
    });
    test.afterAll(async () => {
      fs.rmSync(WIDGETS_DUCKDB_PATH, { force: true });
      await sweepZombieSwapSources();
    });

    test("duckdb-source: register once under DuckDB, answer identical queries under every other engine", async ({
      page,
    }) => runSwapCase(page, () => registerDuckdbSource(page)));
  });

  test.describe("trino-source", () => {
    test.beforeAll(() => {
      test.setTimeout(300000);
      return provisionSwapSource("trino", "up", {
        PROVISA_DEMO_TRINO_PORT: String(E2E_TRINO_SOURCE_PORT),
      });
    });
    test.afterAll(async () => {
      await provisionSwapSource("trino", "down");
      await sweepZombieSwapSources();
    });

    test("trino-source: register once under DuckDB, answer identical queries under every other engine", async ({
      page,
    }) => {
      test.setTimeout(300000);
      await runSwapCase(page, () => registerTrinoSource(page));
    });
  });

  test.describe("synapse", () => {
    // retries: 0 — a retry re-invokes beforeAll on a fresh worker without ever calling the failed
    // attempt's afterAll, leaking a billable Azure resource per retry (see redshift's own reboot-
    // suite comment for the live incident this pattern fixes). synapse_e2e.py's `up` also guards
    // against this independently (state-file reuse against a live `az group show`).
    test.describe.configure({ retries: 0 });
    let synapseConn: SynapseConnection | undefined;
    test.beforeAll(() => {
      test.setTimeout(1800000); // workspace+storage+RBAC-propagation create can run ~15-20min
      synapseConn = provisionSynapse("up");
    });
    test.afterAll(() => {
      provisionSynapse("down");
    });

    test("synapse: register once under DuckDB, answer identical queries under every other engine", async ({
      page,
    }) =>
      runSwapCase(page, () =>
        registerSynapse(page, synapseConn!.sql_server, synapseConn!.database, synapseConn!.adls_url),
      ));
  });

  test.describe("google_sheets", () => {
    test("google_sheets: register once under DuckDB, answer identical queries under every other engine", async ({
      page,
    }) => {
      test.skip(
        !process.env.GOOGLE_APPLICATION_CREDENTIALS || !process.env.GSHEETS_TEST_SHEET_ID,
        "GOOGLE_APPLICATION_CREDENTIALS / GSHEETS_TEST_SHEET_ID not set to the durable fixture " +
          "sheet (see gsheets-e2e-fixture project memory) — the service account has zero Drive " +
          "quota so no throwaway sheet can be created; this test reads the existing shared fixture.",
      );
      await runSwapCase(page, () => registerGsheets(page));
    });
  });

  test.describe("soda", () => {
    test("soda: register once under DuckDB, answer identical queries under every other engine", async ({
      page,
    }) => runSwapCase(page, () => registerSoda(page)));
  });

  test.describe("great_expectations", () => {
    test("great_expectations: register once under DuckDB, answer identical queries under every other engine", async ({
      page,
    }) => runSwapCase(page, () => registerGreatExpectations(page)));
  });

  // hive (plain, non-S3) is excluded here, unlike pinot/druid/hive_s3 above: its warehouse lives
  // in a Docker-volume-only path with no host-visible location, so DuckDB has NEITHER a live
  // ATTACH connector NOR a DIRECT/FETCH row-fetch driver for it — genuinely UnreachableSource from
  // the DuckDB backend, not merely materialized (provisa/federation/strategy.py:130-137). Proven
  // reachable only by starting the very first process on Trino (see the reboot suite's own "hive"
  // case just below, `runRebootCase(..., ["trino"], false, "trino")`) — runSwapCase has no such
  // start-engine override, so a DuckDB-first swap test for it cannot exist.

  test.describe("redshift", () => {
    // retries: 0 — see the reboot suite's own redshift block for the live incident this guards
    // (a retry re-invokes beforeAll on a fresh worker without calling the failed attempt's
    // afterAll, leaking a billable Serverless workgroup).
    test.describe.configure({ retries: 0 });
    test.skip(
      !process.env.REDSHIFT_AWS_ACCESS_KEY_ID || !process.env.REDSHIFT_AWS_SECRET_ACCESS_KEY,
      "no AWS credentials for the ephemeral Redshift Serverless lane (REDSHIFT_AWS_* in .env) — " +
        "see scripts/redshift_e2e.py's own module doc",
    );
    let redshiftConn: RedshiftConnection | undefined;
    test.beforeAll(() => {
      test.setTimeout(900000); // Serverless namespace+workgroup create + TCP-ready can run ~10min
      redshiftConn = provisionRedshift("up");
    });
    test.afterAll(() => {
      provisionRedshift("down");
    });

    // Was previously excluded here: redshift's DIRECT driver is the generic SQLAlchemyDriver
    // (registry.py's _SQLALCHEMY_FALLBACK), and introspect.py's native_schemas/_native_tables_rdbms/
    // native_columns dispatch tables had no "redshift" branch at all, so every one fell through to
    // `return None` and available_schemas' engine-catalog fallback silently returned [] pre-
    // registration — the Register Table schema picker was empty under DuckDB. Root-caused and
    // fixed (introspect.py: added a dedicated schema branch with Redshift's own pg_internal
    // exclusion, and added "redshift" to the postgres-wire table/column dispatch tuples) rather
    // than working around it here.
    test("redshift: register once under DuckDB, answer identical queries under every other engine", async ({
      page,
    }) => runSwapCase(page, () => registerRedshift(redshiftConn!)(page)));
  });
});

// REQ-1730 scenario 1: "if you changed engines, it required you to reboot the backend" (the
// user's own design intent) — a source registered purely through the UI (no `sources:` YAML
// entry) must survive an engine change + a genuine process reboot and remain queryable, with no
// replay of its own createSource mutation. This is deliberately NOT the reload-in-place trick
// every type in the describe block above uses (reloadEngineBackend/reprovisionSourceOnEngine PUT
// /admin/config against an already-running, already-differently-engined process) — that proxy
// already found one real gap (extra_sources never reaching _replace_mode_cleanup) and, on its own
// admission, is not what "reboot" means: it exercises one process's in-place reload path, never a
// cold start. `runRebootCase` (engine-swap-helpers.ts) owns ONE dedicated backend process per
// test, genuinely killed and respawned with PROVISA_ENGINE flipped; same port, same data dir, same
// control-plane org row — as close to a real desktop engine-swap-then-restart as this harness
// gets. One type per test (mirrors the "one source = one type" contract above) — add a type by
// adding a test here, reusing its own existing register* function unchanged.
test.describe("scenario 1: same source survives an engine change + reboot (REQ-1730)", () => {
  // A genuine kill+respawn leaves a real (sub-second to low-single-digit-second) window where
  // nothing listens on REBOOT_HTTP_PORT — any in-flight browser request from the STILL-MOUNTED
  // page (e.g. an Apollo/schema-version poll) that lands in that window gets a real
  // ERR_CONNECTION_REFUSED. That is exactly what "reboot" means here, not a bug to chase —
  // reproduced live (rss's own case failed this way, coverage.ts's own blanket "no uncaught
  // browser errors" check otherwise fails the whole test on it).
  test.use({ allowedBrowserErrors: ["ERR_CONNECTION_REFUSED"] });

  test.describe("mongodb", () => {
    test.beforeAll(() => startDemoSources(["mongodb"]));
    test.afterAll(() => removeDemoSources(["mongodb"]));

    test("mongodb registered once under DuckDB resolves under every rebooted engine, no replay", async ({
      page,
    }) => runRebootCase(page, (p) => registerMongodb(p), { trino: "mongodb" }));
  });

  test.describe("neo4j", () => {
    test.beforeAll(() => startDemoSources(["neo4j"]));
    test.afterAll(() => removeDemoSources(["neo4j"]));

    test("neo4j registered once under DuckDB resolves under every rebooted engine, no replay", async ({
      page,
    }) => runRebootCase(page, (p) => registerNeo4j(p), { trino: "neo4j" }));
  });

  test.describe("elasticsearch", () => {
    test.beforeAll(() => startDemoSources(["elasticsearch"]));
    test.afterAll(() => removeDemoSources(["elasticsearch"]));

    test("elasticsearch registered once under DuckDB resolves under every rebooted engine, no replay", async ({
      page,
    }) => runRebootCase(page, (p) => registerElasticsearch(p), { trino: "elasticsearch" }));
  });

  test.describe("redis", () => {
    test.beforeAll(() => startDemoSources(["redis"]));
    test.afterAll(() => removeDemoSources(["redis"]));

    test("redis registered once under DuckDB resolves under every rebooted engine, no replay", async ({
      page,
    }) => runRebootCase(page, (p) => registerRedis(p), { trino: "redis" }));
  });

  test.describe("cassandra", () => {
    test.beforeAll(() => startDemoSources(["cassandra"]));
    test.afterAll(() => removeDemoSources(["cassandra"]));

    test("cassandra registered once under DuckDB resolves under every rebooted engine, no replay", async ({
      page,
    }) => runRebootCase(page, (p) => registerCassandra(p), { trino: "cassandra" }));
  });

  test.describe("sparql", () => {
    test.beforeAll(() => startDemoSources(["sparql"]));
    test.afterAll(() => removeDemoSources(["sparql"]));

    test("sparql registered once under DuckDB resolves under every rebooted engine, no replay", async ({
      page,
    }) => runRebootCase(page, (p) => registerSparql(p), { trino: "sparql" }));
  });

  test.describe("prometheus", () => {
    test.beforeAll(() => startDemoSources(["prometheus"]));
    test.afterAll(() => removeDemoSources(["prometheus"]));

    test("prometheus registered once under DuckDB resolves under every rebooted engine, no replay", async ({
      page,
    }) => runRebootCase(page, (p) => registerPrometheus(p), { trino: "prometheus" }));
  });

  // pinot/hive_s3/hive/druid: NO direct DuckDB driver at all (register_source() is a documented
  // no-op for them on the native engine — see source-to-query-olap-lake-trino.spec.ts's own module
  // doc) — the standard "register under DuckDB first" shape every OTHER type here uses cannot even
  // run against them. runRebootCase's `startEngine` param (added for this) starts the reboot
  // process already on Trino instead, so the registration+baseline query above IS the Trino leg,
  // and the kill+respawn loop that follows still proves the thing REQ-1730 is actually about: does
  // a control-plane-only source on a connector-only type survive a genuine restart. No host-rewrite
  // map is needed (empty {}) — these registrars already use the container-network alias directly
  // (e.g. host "pinot", not "localhost"), unlike every type registered against the DuckDB-bound
  // process first.
  test.describe("pinot", () => {
    test.beforeAll(() => {
      test.setTimeout(300000);
      return provisionSwapSource("pinot", "up", {
        PROVISA_DEMO_PINOT_CONTROLLER_PORT: String(E2E_PINOT_CONTROLLER_PORT),
        PROVISA_DEMO_PINOT_BROKER_PORT: String(E2E_PINOT_BROKER_PORT),
        PROVISA_TRINO_NETWORK: DOCKER_NETWORK,
      });
    });
    test.afterAll(async () => {
      await provisionSwapSource("pinot", "down");
      await sweepZombieSwapSources();
    });

    // REQ-1730: pinot now has a real DuckDB row-fetch (make_pinot_loader/_native_tables_pinot,
    // provisa/pinot/) — the standard register-under-DuckDB-first flow every RDB/warehouse type
    // uses, no `startEngine` override needed anymore.
    test("pinot registered once under DuckDB resolves under every rebooted engine, no replay", async ({
      page,
    }) => runRebootCase(page, (p) => registerPinot(p), { trino: "pinot" }));
  });

  test.describe("hive_s3", () => {
    test.beforeAll(async () => {
      test.setTimeout(300000);
      await provisionSwapSource("hive-s3", "up", {}, DOCKER_NETWORK);
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
    test.afterAll(async () => {
      await provisionSwapSource("hive-s3", "down");
      await sweepZombieSwapSources();
    });

    // REQ-1730: hive_s3 now has a real DuckDB row-fetch (make_hive_s3_loader/
    // _native_tables_hive_s3, provisa/hive/) — the standard register-under-DuckDB-first flow, no
    // `startEngine` override needed anymore.
    test("hive_s3 registered once under DuckDB resolves under every rebooted engine, no replay", async ({
      page,
    }) => runRebootCase(page, (p) => registerHiveS3(p), { trino: "hive_s3" }));
  });

  test.describe("hive", () => {
    let hiveWarehouseVolume: string | null = null;

    test.beforeAll(() => {
      test.setTimeout(180000);
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
      hiveWarehouseVolume = mount.Name;
      return provisionSwapSource("hive", "up", { PROVISA_HIVE_WAREHOUSE_VOLUME: hiveWarehouseVolume }, DOCKER_NETWORK);
    });
    test.afterAll(async () => {
      if (!hiveWarehouseVolume) return;
      await provisionSwapSource("hive", "down");
      await sweepZombieSwapSources();
    });

    test("hive registered once directly on a rebooted Trino resolves after a genuine restart, no replay", async ({
      page,
    }) => {
      test.setTimeout(300000);
      await runRebootCase(page, (p) => registerHive(p), {}, ["trino"], false, "trino");
    });
  });

  // druid is CI-only: apache/druid is amd64-only, unbootable under arm64 emulation (same gate
  // source-to-query-olap-lake-trino.spec.ts's own druid case uses) — skips locally, runs for real
  // on ui-e2e-trino.yml's ubuntu-latest runner.
  // REQ-1730: the RUNNING_IN_CI skip this describe block used to have (see git history) was a
  // stale, never-rechecked assumption — apache/druid:30.0.0 is genuinely single-arch (amd64,
  // confirmed via `docker manifest inspect`), but was verified LIVE this session to boot and
  // answer real queries under arm64 (QEMU) emulation on Docker Desktop on this exact host (all 6
  // services reached "Healthy", `provisa.druid.fetch` queried it directly and got real rows back)
  // — unlike exasol's own confirmed-genuine emulation failure (see that type's own comment). Not
  // gated on CI anymore; runs everywhere docker-compose.core.yml's own webServer already does.
  test.describe("druid", () => {
    test.beforeAll(() => {
      test.setTimeout(900000);
      provisionSwapSource(
        "druid",
        "up",
        {
          PROVISA_DEMO_DRUID_COORD_PORT: String(E2E_DRUID_COORD_PORT),
          PROVISA_DEMO_DRUID_BROKER_PORT: String(E2E_DRUID_BROKER_PORT),
          PROVISA_TRINO_NETWORK: DOCKER_NETWORK,
        },
        DOCKER_NETWORK,
      );
      execFileSync(PYTHON, [path.join(ROOT, "demo", "sources", "druid", "prime.py")], {
        stdio: "inherit",
        env: {
          ...process.env,
          PROVISA_DEMO_DRUID_COORD_PORT: String(E2E_DRUID_COORD_PORT),
          PROVISA_DEMO_DRUID_BROKER_PORT: String(E2E_DRUID_BROKER_PORT),
        },
      });
    });
    test.afterAll(async () => {
      await provisionSwapSource("druid", "down");
      await sweepZombieSwapSources();
    });

    // REQ-1730: druid now has a real DuckDB row-fetch (make_druid_loader/_native_tables_druid,
    // provisa/druid/) — the standard register-under-DuckDB-first flow, no `startEngine` override.
    test("druid registered once under DuckDB resolves under every rebooted engine, no replay", async ({
      page,
    }) => runRebootCase(page, (p) => registerDruid(p), { trino: "druid" }));
  });

  // Same RDB_WIDGETS_SOURCES config list the original describe block above loops over — every
  // type here has a native Trino connector (reachableOn: ["trino"] unless overridden), so
  // rebooting into Trino is meaningful; hiveserver2/saphana (reachableOn: []) are skipped — no
  // Trino leg exists for them to prove a reboot restores.
  for (const cfg of RDB_WIDGETS_SOURCES) {
    if ((cfg.reachableOn ?? ["trino"]).length === 0) continue;
    test.describe(cfg.type, () => {
      test.beforeAll(() => {
        if (cfg.needsCi && !RUNNING_IN_CI) return;
        if (cfg.bootTimeoutMs) test.setTimeout(cfg.bootTimeoutMs);
        return provisionSwapSource(cfg.type, "up");
      });
      test.afterAll(async () => {
        if (cfg.needsCi && !RUNNING_IN_CI) return;
        if (cfg.bootTimeoutMs) test.setTimeout(cfg.bootTimeoutMs);
        await provisionSwapSource(cfg.type, "down");
      });

      test(`${cfg.type} registered once under DuckDB resolves under every rebooted engine, no replay`, async ({
        page,
      }) => {
        test.skip(
          cfg.needsCi === true && !RUNNING_IN_CI,
          "saplabs/hanaexpress's indexserver does not start under Docker Desktop's Apple " +
            "Silicon VM (verified live, not a config issue) — runs for real in CI " +
            "(ubuntu-latest is a genuine amd64 host)",
        );
        await runRebootCase(page, registerRdbWidgets(cfg), { trino: cfg.type });
      });
    });
  }

  test.describe("splunk", () => {
    test.beforeAll(() => {
      test.setTimeout(900000);
      return startDemoSources(["splunk"]);
    });
    test.afterAll(() => removeDemoSources(["splunk"]));

    test("splunk registered once under DuckDB resolves under every rebooted engine, no replay", async ({
      page,
    }) => {
      test.setTimeout(900000 + 180000);
      await runRebootCase(page, (p) => registerSplunk(p), { trino: "splunk" });
    });
  });

  test.describe("sharepoint", () => {
    test.skip(
      !process.env.SP_SITE_URL,
      "no live SharePoint credentials: set the SP_* block in the root .env",
    );

    test("sharepoint registered once under DuckDB resolves under every rebooted engine, no replay", async ({
      page,
    }) => {
      test.setTimeout(300000 + 180000);
      await runRebootCase(page, (p) => registerSharepoint(p), { trino: "sharepoint" });
    });
  });

  test("graphql_remote registered once under DuckDB resolves under every rebooted engine, no replay", async ({
    page,
  }) =>
    runRebootCase(page, (p) => registerGraphqlRemote(p, REBOOT_GRAPHQL_DEMO_URL), {
      trino: "graphql_remote",
    }));

  test("openapi registered once under DuckDB resolves under every rebooted engine, no replay", async ({
    page,
  }) =>
    runRebootCase(page, (p) => registerOpenapi(p, REBOOT_PETSTORE_OPENAPI_URL), {
      trino: "openapi",
    }));

  test("ingest registered once under DuckDB resolves under every rebooted engine, no replay", async ({
    page,
  }) => runRebootCase(page, (p) => registerIngest(p), { trino: "ingest" }));

  test.describe("govdata", () => {
    test.skip(
      !process.env.FREE_ASKAMERICA_KEY,
      "no live AskAmerica/govdata credentials: set FREE_ASKAMERICA_KEY in the root .env",
    );

    test("govdata registered once under DuckDB resolves under every rebooted engine, no replay", async ({
      page,
    }) => runRebootCase(page, (p) => registerGovdata(p), { trino: "govdata" }));
  });

  test.describe("rss", () => {
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

    test("rss registered once under DuckDB resolves under every rebooted engine, no replay", async ({
      page,
    }) => runRebootCase(page, (p) => registerRss(p), { trino: "rss" }));
  });

  test.describe("websocket", () => {
    let wsServer: WebSocketServer;
    test.beforeAll(() => {
      wsServer = new WebSocketServer({ port: E2E_WS_PORT });
      wsServer.on("connection", (socket) => {
        socket.send(JSON.stringify({ id: "ws-1", value: "hello" }));
        socket.send(JSON.stringify({ id: "ws-2", value: "world" }));
      });
    });
    test.afterAll(async () => {
      for (const client of wsServer.clients) client.terminate();
      await new Promise<void>((resolve, reject) =>
        wsServer.close((err) => (err ? reject(err) : resolve())),
      );
    });

    test("websocket registered once under DuckDB resolves under every rebooted engine, no replay", async ({
      page,
    }) => runRebootCase(page, (p) => registerWebsocket(p), { trino: "websocket" }));
  });

  test.describe("grpc_remote", () => {
    let grpcServer: ChildProcess | null = null;

    test.beforeAll(async () => {
      grpcServer = spawn(PYTHON, ["-m", GRPC_REMOTE_SERVER_MODULE], {
        cwd: ROOT,
        env: { ...process.env, DEMO_GRPC_REMOTE_PORT: String(E2E_GRPC_REMOTE_PORT) },
        stdio: "pipe",
      });
      await waitForPort(E2E_GRPC_REMOTE_PORT, 30000);
    });
    test.afterAll(async () => {
      grpcServer?.kill();
    });

    test("grpc_remote registered once under DuckDB resolves under every rebooted engine, no replay", async ({
      page,
    }) => runRebootCase(page, (p) => registerGrpcRemote(p, E2E_GRPC_REMOTE_PORT), { trino: "grpc_remote" }));
  });

  test.describe("singlestore", () => {
    test.skip(
      !SINGLESTORE_AVAILABLE,
      "needs SINGLESTORE_HOST/USERNAME/PASSWORD/DATABASE for the SingleStore Cloud workspace",
    );

    test("singlestore registered once under DuckDB resolves under every rebooted engine, no replay", async ({
      page,
    }) => runRebootCase(page, (p) => registerSinglestore(p), { trino: "singlestore" }));
  });

  test.describe("exasol", () => {
    test.skip(
      !RUNNING_IN_CI,
      "exasol/docker-db needs privileged mode + several GB RAM + a multi-minute cold init, and " +
        "is amd64-only (unbootable under arm64 emulation) — runs for real in CI (ubuntu-latest " +
        "is a genuine amd64 host); see source-to-query-olap-lake.spec.ts's identical gate",
    );
    let exasolFingerprint = "";
    test.beforeAll(() => {
      if (!RUNNING_IN_CI) return;
      test.setTimeout(900000);
      if (fs.existsSync(E2E_EXASOL_FINGERPRINT_FILE)) fs.rmSync(E2E_EXASOL_FINGERPRINT_FILE);
      provisionSwapSource("exasol", "up", {
        PROVISA_DEMO_EXASOL_PORT: String(E2E_EXASOL_PORT),
        PROVISA_DEMO_EXASOL_FINGERPRINT_FILE: E2E_EXASOL_FINGERPRINT_FILE,
      });
      exasolFingerprint = fs.readFileSync(E2E_EXASOL_FINGERPRINT_FILE, "utf8").trim();
    });
    test.afterAll(async () => {
      if (!RUNNING_IN_CI) return;
      await provisionSwapSource("exasol", "down");
      if (fs.existsSync(E2E_EXASOL_FINGERPRINT_FILE)) fs.rmSync(E2E_EXASOL_FINGERPRINT_FILE);
    });

    test("exasol registered once under DuckDB resolves under every rebooted engine, no replay", async ({
      page,
    }) => runRebootCase(page, (p) => registerExasol(p, () => exasolFingerprint), { trino: "exasol" }));
  });

  test.describe("kafka", () => {
    test.beforeAll(() => {
      test.setTimeout(180000);
      return provisionSwapSource(
        "kafka",
        "up",
        {
          PROVISA_DEMO_KAFKA_PORT: String(E2E_KAFKA_PORT),
          PROVISA_DEMO_KAFKA_SCHEMA_REGISTRY_PORT: String(E2E_KAFKA_SCHEMA_REGISTRY_PORT),
        },
        DOCKER_NETWORK,
      );
    });
    test.afterAll(() => provisionSwapSource("kafka", "down"));

    test("kafka registered once under DuckDB resolves under every rebooted engine, no replay", async ({
      page,
    }) => {
      test.setTimeout(180000);
      // DuckDB DOES answer kafka through the materialize store (CDC landing, push_wiring.py) —
      // it just needs the registered table's primary key declared, which registerKafka's own
      // registrar was missing (fixed there, see its own comment) — no skipDuckdbBaseline needed.
      await runRebootCase(page, (p) => registerKafka(p), { trino: "kafka" });
    });
  });

  test.describe("snowflake", () => {
    test.skip(
      !(
        process.env.SNOWFLAKE_ACCOUNT &&
        process.env.SNOWFLAKE_USER &&
        process.env.SNOWFLAKE_PASSWORD
      ),
      "no live Snowflake credentials in this environment",
    );
    test.beforeAll(() => {
      execFileSync(PYTHON, [CLOUD_WAREHOUSE_SEED, "snowflake", "up"], { stdio: "pipe" });
    });
    test.afterAll(() => {
      execFileSync(PYTHON, [CLOUD_WAREHOUSE_SEED, "snowflake", "down"], { stdio: "pipe" });
    });

    test("snowflake registered once under DuckDB resolves under every rebooted engine, no replay", async ({
      page,
    }) => runRebootCase(page, (p) => registerSnowflake(p), { trino: "snowflake" }));
  });

  test.describe("databricks", () => {
    test.skip(
      !(
        process.env.DATABRICKS_SERVER_HOSTNAME &&
        process.env.DATABRICKS_HTTP_PATH &&
        process.env.DATABRICKS_TOKEN
      ),
      "no live Databricks credentials in this environment",
    );
    test.beforeAll(() => {
      execFileSync(PYTHON, [CLOUD_WAREHOUSE_SEED, "databricks", "up"], { stdio: "pipe" });
    });
    test.afterAll(() => {
      execFileSync(PYTHON, [CLOUD_WAREHOUSE_SEED, "databricks", "down"], { stdio: "pipe" });
    });

    test("databricks registered once under DuckDB resolves under every rebooted engine, no replay", async ({
      page,
    }) => runRebootCase(page, (p) => registerDatabricks(p), { trino: "databricks" }));
  });

  test.describe("fabric", () => {
    test.skip(
      !(process.env.FABRIC_SQL_SERVER && process.env.FABRIC_DATABASE),
      "no live Fabric credentials in this environment",
    );
    test.skip(
      !(process.env.FABRIC_RESOURCE_GROUP && process.env.FABRIC_CAPACITY_NAME),
      "no FABRIC_RESOURCE_GROUP/FABRIC_CAPACITY_NAME configured",
    );
    test.beforeAll(() => {
      execFileSync(PYTHON, [CLOUD_WAREHOUSE_SEED, "fabric", "up"], { stdio: "pipe" });
    });
    test.afterAll(() => {
      execFileSync(PYTHON, [CLOUD_WAREHOUSE_SEED, "fabric", "down"], { stdio: "pipe" });
    });

    test("fabric registered once under DuckDB resolves under every rebooted engine, no replay", async ({
      page,
    }) => runRebootCase(page, (p) => registerFabric(p), { trino: "fabric" }));
  });

  test.describe("bigquery", () => {
    test.skip(
      !(process.env.GOOGLE_CLOUD_PROJECT && process.env.GOOGLE_APPLICATION_CREDENTIALS),
      "no live GCP credentials in this environment",
    );
    test.beforeAll(() => {
      execFileSync(PYTHON, [CLOUD_WAREHOUSE_SEED, "bigquery", "up"], { stdio: "pipe" });
    });
    test.afterAll(() => {
      execFileSync(PYTHON, [CLOUD_WAREHOUSE_SEED, "bigquery", "down"], { stdio: "pipe" });
    });

    test("bigquery registered once under DuckDB resolves under every rebooted engine, no replay", async ({
      page,
    }) => runRebootCase(page, (p) => registerBigquery(p), { trino: "bigquery" }));
  });

  test.describe("redshift", () => {
    // retries: 0 overrides playwright.config.ts's global `retries: 1` — reproduced live, a retry
    // re-invokes beforeAll on a FRESH worker without ever calling the failed attempt's afterAll,
    // leaking a full billable Serverless workgroup per retry. redshift_e2e.py's `up` also now
    // guards against this independently (state-file reuse), but never rely on a single layer for
    // a real-money resource — see that script's own comment for the live incident this fixed.
    test.describe.configure({ retries: 0 });
    test.skip(
      !process.env.REDSHIFT_AWS_ACCESS_KEY_ID || !process.env.REDSHIFT_AWS_SECRET_ACCESS_KEY,
      "no AWS credentials for the ephemeral Redshift Serverless lane (REDSHIFT_AWS_* in .env) — " +
        "see scripts/redshift_e2e.py's own module doc",
    );
    let redshiftConn: RedshiftConnection | undefined;
    test.beforeAll(() => {
      test.setTimeout(900000); // Serverless namespace+workgroup create + TCP-ready can run ~10min
      redshiftConn = provisionRedshift("up");
    });
    test.afterAll(() => {
      provisionRedshift("down");
    });

    // REQ-1730: redshift has NO entry in executor/drivers/registry.py's _DRIVER_FACTORIES at all
    // (confirmed live: test_redshift_source_e2e.py's own module doc already documents this —
    // "reachable ONLY through the federation engine's Trino redshift catalog") — DuckDB cannot
    // introspect it, so the Register Table form's schema picker comes back empty when DuckDB is
    // the active engine (reproduced live: `toHaveCount(1)` on the "public" schema option timed
    // out at 0). Same shape as pinot/druid/hive/hive_s3 just above: start the FIRST spawn already
    // on Trino (startEngine="trino") so registration itself succeeds, and the reboot loop still
    // proves the real thing this harness is about — does a control-plane-only source survive a
    // genuine kill+respawn, not whether DuckDB can see it.
    test("redshift registered once under Trino resolves after a genuine reboot, no replay", async ({
      page,
    }) => runRebootCase(page, (p) => registerRedshift(redshiftConn!)(p), {}, ["trino"], false, "trino"));
  });

  test.describe("synapse", () => {
    // retries: 0 overrides playwright.config.ts's global `retries: 1` — see redshift's own
    // comment just above for the live incident this pattern fixes (a retry re-invokes beforeAll
    // on a FRESH worker without ever calling the failed attempt's afterAll, leaking a billable
    // resource per retry). synapse_e2e.py's `up` also guards against this independently
    // (state-file reuse against a live `az group show`), same defense-in-depth as redshift.
    test.describe.configure({ retries: 0 });
    let synapseConn: SynapseConnection | undefined;
    test.beforeAll(() => {
      test.setTimeout(1800000); // workspace+storage+RBAC-propagation create can run ~15-20min
      synapseConn = provisionSynapse("up");
    });
    test.afterAll(() => {
      provisionSynapse("down");
    });

    test("synapse registered once under DuckDB resolves under every rebooted engine, no replay", async ({
      page,
    }) =>
      runRebootCase(
        page,
        (p) =>
          registerSynapse(p, synapseConn!.sql_server, synapseConn!.database, synapseConn!.adls_url),
        { trino: "synapse" },
      ));
  });

  test("csv registered once under DuckDB resolves under every rebooted engine, no replay", async ({
    page,
  }) =>
    runRebootCase(
      page,
      (p) =>
        registerSingleFile(
          "csv",
          "demo/files/customers.csv",
          /CSV File Path/,
          (table) => `SELECT id, first_name, email FROM pet_store.${table} ORDER BY id`,
          (rows) => {
            expect(rows).toHaveLength(15);
            expect(rows[0]).toEqual(["1", "Alice", "alice@example.com"]);
          },
        )(p),
      { trino: "csv" },
    ));

  test("parquet registered once under DuckDB resolves under every rebooted engine, no replay", async ({
    page,
  }) =>
    runRebootCase(
      page,
      (p) =>
        registerSingleFile(
          "parquet",
          "demo/files/products.parquet",
          /Parquet File Path/,
          (table) => `SELECT id, sku, name, price FROM pet_store.${table} ORDER BY id`,
          (rows) => {
            expect(rows).toHaveLength(15);
            expect(rows[0]).toEqual(["1", "WIDGET-A", "Widget Alpha", "9.99"]);
          },
        )(p),
      { trino: "parquet" },
    ));

  test("files registered once under DuckDB resolves under every rebooted engine, no replay", async ({
    page,
  }) => runRebootCase(page, (p) => registerFiles()(p), { trino: "files" }));

  test("delta_lake registered once under DuckDB resolves under every rebooted engine, no replay", async ({
    page,
  }) =>
    runRebootCase(page, (p) => registerFileLake("delta_lake", () => deltaTablePath)(p), {
      trino: "delta_lake",
    }));

  test("iceberg registered once under DuckDB resolves under every rebooted engine, no replay", async ({
    page,
  }) =>
    runRebootCase(page, (p) => registerFileLake("iceberg", () => icebergTablePath)(p), {
      trino: "iceberg",
    }));

  test.describe("soda", () => {
    test("checker source scan (soda) survives an engine change + reboot, no replay", async ({
      page,
    }) => runRebootCase(page, (p) => registerSoda(p), { trino: "soda" }));
  });

  test.describe("great_expectations", () => {
    test("checker source scan (great_expectations) survives an engine change + reboot, no replay", async ({
      page,
    }) => runRebootCase(page, (p) => registerGreatExpectations(p), { trino: "great_expectations" }));
  });

  // REQ-1730: sqlite/firebird/airport were previously excluded from this whole scenario ("no
  // Trino leg, structurally excluded") — wrong. strategy.py's federate() raises UnreachableSource
  // for a type with no engine connector UNLESS it's in _MATERIALIZE_ONLY/_CONNECTOR_PGWIRE_REPLICA
  // — none of these three were, which was itself the real (now-fixed) product gap, not a
  // structural ceiling. sqlite already had a working, engine-independent row-fetch
  // (make_sqlite_loader); firebird/airport needed new ones (make_firebird_loader/
  // make_airport_loader, both added this session — a scratch DuckDB connection ATTACHed through
  // the same community extension the live engine itself uses).
  test.describe("sqlite", () => {
    // sqlite has no Trino connector but DOES reach "pg" (SqliteFdwConnector) — the one type in
    // this whole harness that needs a non-Trino swap target. No container: a local file.
    test("sqlite registered once under DuckDB resolves under a rebooted pg engine, no replay", async ({
      page,
    }) => {
      test.setTimeout(300000);
      await runRebootCase(page, (p) => registerSqlite(p), {}, ["pg"]);
    });
  });

  test.describe("firebird", () => {
    test.beforeAll(() => provisionSwapSource("firebird", "up"));
    test.afterAll(async () => {
      await provisionSwapSource("firebird", "down");
      await sweepZombieSwapSources();
    });

    test("firebird registered once under DuckDB resolves under every rebooted engine, no replay", async ({
      page,
    }) => runRebootCase(page, (p) => registerFirebird(p), { trino: "firebird" }));
  });

  test.describe("airport", () => {
    test.beforeAll(() => provisionSwapSource("airport", "up"));
    test.afterAll(async () => {
      await provisionSwapSource("airport", "down");
      await sweepZombieSwapSources();
    });

    test("airport registered once under DuckDB resolves under every rebooted engine, no replay", async ({
      page,
    }) => runRebootCase(page, (p) => registerAirport(p), { trino: "airport" }));
  });

  // REQ-1730: duckdb-as-a-source and trino-as-a-source are meta/self-referential SourceTypes
  // (a SECOND local .duckdb file / a remote Trino coordinator, both distinct from either engine
  // this harness swaps between) — NOT structurally excluded, despite an earlier wrong claim to
  // that effect this session. Both already have a generic DIRECT-driver connector on every
  // engine (executor/drivers/registry.py's `_make_duckdb`/`_make_trino`, projected onto every
  // engine by FederationEngine.complete_reach(), REQ-947) — federate() resolves both to
  // Strategy.MATERIALIZED universally, so a real Trino reboot (not just a same-engine one) is the
  // right test for both.
  test.describe("duckdb-source", () => {
    test.beforeAll(() => {
      execFileSync(
        PYTHON,
        [
          "-c",
          "import sys; sys.path.insert(0, 'demo/files'); " +
            "from create_demo_files import create_widgets_duckdb; create_widgets_duckdb()",
        ],
        { cwd: ROOT, stdio: "pipe" },
      );
    });
    test.afterAll(async () => {
      fs.rmSync(WIDGETS_DUCKDB_PATH, { force: true });
      await sweepZombieSwapSources();
    });

    test("duckdb-as-a-source registered once under DuckDB resolves under every rebooted engine, no replay", async ({
      page,
    }) => runRebootCase(page, (p) => registerDuckdbSource(p), { trino: "duckdb-source" }));
  });

  test.describe("trino-source", () => {
    test.beforeAll(() => {
      test.setTimeout(300000);
      return provisionSwapSource("trino", "up", {
        PROVISA_DEMO_TRINO_PORT: String(E2E_TRINO_SOURCE_PORT),
      });
    });
    test.afterAll(async () => {
      await provisionSwapSource("trino", "down");
      await sweepZombieSwapSources();
    });

    test("trino-as-a-source registered once under DuckDB resolves under every rebooted engine, no replay", async ({
      page,
    }) => {
      test.setTimeout(300000);
      await runRebootCase(page, (p) => registerTrinoSource(p), { trino: "trino-source" });
    });
  });
});

// REQ-1730 scenario 1, reversed direction: the opposite starting engine from the block above —
// register Trino-primary (the real Sources-form UI, host.docker.internal from the very first
// call, same as scenario 2's own mongodb case), then genuinely kill+respawn the SAME reboot-
// harness process back onto DuckDB and requery, with no replay of the registration mutation
// either way. Proves the swap is symmetric: it is not only "register on DuckDB, survive a swap to
// Trino" (every case above) but also "register on Trino, survive a swap back to DuckDB" — the
// direction a user who set up a source under a Trino-primary deployment and later downgraded to
// DuckDB would actually hit. `runRebootCase`'s `startEngine`/direction-aware
// `rewriteHostForContainerizedEngine` (see its own doc) already generalize to this; only
// `hostRewriteTypes` needs to key the REWRITE OFF the engine being entered ("duckdb" here, not
// "trino") to trigger the containerized → native inverse. Four MATERIALIZE_ONLY types piloted
// here (mongodb/redis/cassandra/elasticsearch — the ones whose own register* already exposes a
// `host` param and whose row-landing mechanism this file's own module doc already establishes as
// "nothing engine-specific", i.e. independent of which engine's own Python driver does the
// fetch) — widen to more types only after confirming a given type's registrar tolerates being
// driven against a containerized backend from its very first call, same caveat scenario 2's own
// module doc raises for cold-start-primary registration in general.
test.describe("scenario 1 reversed: same source survives an engine change + reboot, Trino first (REQ-1730)", () => {
  // See the forward-direction describe block's own comment for why this specific browser error is
  // allowed — identical reboot-window race, same cause.
  test.use({ allowedBrowserErrors: ["ERR_CONNECTION_REFUSED"] });

  test.describe("mongodb", () => {
    test.beforeAll(() => startDemoSources(["mongodb"]));
    test.afterAll(() => removeDemoSources(["mongodb"]));

    test("mongodb registered once under Trino resolves after rebooting into DuckDB, no replay", async ({
      page,
    }) =>
      runRebootCase(
        page,
        (p) => registerMongodb(p, "", "host.docker.internal"),
        { duckdb: "mongodb" },
        ["duckdb"],
        false,
        "trino",
      ));
  });

  test.describe("redis", () => {
    test.beforeAll(() => startDemoSources(["redis"]));
    test.afterAll(() => removeDemoSources(["redis"]));

    test("redis registered once under Trino resolves after rebooting into DuckDB, no replay", async ({
      page,
    }) =>
      runRebootCase(
        page,
        (p) => registerRedis(p, "host.docker.internal"),
        { duckdb: "redis" },
        ["duckdb"],
        false,
        "trino",
      ));
  });

  test.describe("cassandra", () => {
    test.beforeAll(() => startDemoSources(["cassandra"]));
    test.afterAll(() => removeDemoSources(["cassandra"]));

    test("cassandra registered once under Trino resolves after rebooting into DuckDB, no replay", async ({
      page,
    }) =>
      runRebootCase(
        page,
        (p) => registerCassandra(p, "host.docker.internal"),
        { duckdb: "cassandra" },
        ["duckdb"],
        false,
        "trino",
      ));
  });

  test.describe("elasticsearch", () => {
    test.beforeAll(() => startDemoSources(["elasticsearch"]));
    test.afterAll(() => removeDemoSources(["elasticsearch"]));

    test("elasticsearch registered once under Trino resolves after rebooting into DuckDB, no replay", async ({
      page,
    }) =>
      runRebootCase(
        page,
        (p) => registerElasticsearch(p, "host.docker.internal"),
        { duckdb: "elasticsearch" },
        ["duckdb"],
        false,
        "trino",
      ));
  });
});

// REQ-1730 extended: engines beyond duckdb/trino. provisa/federation/engine.py's `_ENGINE_BUILDERS`
// registers ~30 selectable `PROVISA_ENGINE` values — Snowflake/Databricks/BigQuery/mssql among them
// are first-class SELF_ONLY/PARTIAL warehouse engines, not merely SOURCE TYPES (registerSnowflake/
// registerDatabricks/registerBigquery above register them as a federated SOURCE reached via
// duckdb/trino — a different model, even though the physical driver underneath is the same product
// — see rebootEngineExtraEnv's own doc for the DSN each engine's OWN config needs, distinct from
// SourceInput's fields). This block proves the reboot harness generalizes to these too: register
// once under DuckDB (the standard start), reboot the SAME process into the warehouse engine, requery
// with no replay. No host rewrite applies (none of these are containerized from this app process's
// perspective — snowflake/databricks/bigquery are external SaaS APIs, mssql is a plain published
// TCP port) — `hostRewriteTypes: {}` throughout. One MATERIALIZE_ONLY type piloted per engine
// (reusing this file's own established "nothing engine-specific to land" types); snowflake/
// databricks/bigquery gate on the same live-credential env vars source-to-query-cloud-warehouse.spec
// .ts's own tests already require, mssql needs only the local demo/sources/sqlserver fixture.
test.describe("scenario 1 extended: same source survives an engine change to a warehouse engine (REQ-1730)", () => {
  test.use({ allowedBrowserErrors: ["ERR_CONNECTION_REFUSED"] });

  test.describe("mongodb -> snowflake", () => {
    test.skip(!SNOWFLAKE_ENGINE_AVAILABLE, "no live Snowflake credentials (SNOWFLAKE_ACCOUNT/USER/PASSWORD)");
    test.beforeAll(() => startDemoSources(["mongodb"]));
    test.afterAll(() => removeDemoSources(["mongodb"]));

    test("mongodb registered once under DuckDB resolves after rebooting into Snowflake, no replay", async ({
      page,
    }) => {
      test.setTimeout(300000);
      await runRebootCase(page, (p) => registerMongodb(p), {}, ["snowflake"]);
    });
  });

  test.describe("redis -> databricks", () => {
    test.skip(
      !DATABRICKS_ENGINE_AVAILABLE,
      "no live Databricks credentials (DATABRICKS_SERVER_HOSTNAME/HTTP_PATH/TOKEN)",
    );
    test.beforeAll(() => startDemoSources(["redis"]));
    test.afterAll(() => removeDemoSources(["redis"]));

    test("redis registered once under DuckDB resolves after rebooting into Databricks, no replay", async ({
      page,
    }) => {
      test.setTimeout(300000);
      await runRebootCase(page, (p) => registerRedis(p), {}, ["databricks"]);
    });
  });

  test.describe("cassandra -> bigquery", () => {
    test.skip(
      !BIGQUERY_ENGINE_AVAILABLE,
      "no live GCP credentials (GOOGLE_CLOUD_PROJECT/GOOGLE_APPLICATION_CREDENTIALS)",
    );
    test.beforeAll(() => startDemoSources(["cassandra"]));
    test.afterAll(() => removeDemoSources(["cassandra"]));

    test("cassandra registered once under DuckDB resolves after rebooting into BigQuery, no replay", async ({
      page,
    }) => {
      test.setTimeout(300000);
      await runRebootCase(page, (p) => registerCassandra(p), {}, ["bigquery"]);
    });
  });

  test.describe("elasticsearch -> mssql", () => {
    test.beforeAll(() => provisionSwapSource("sqlserver", "up"));
    test.afterAll(async () => {
      await provisionSwapSource("sqlserver", "down");
    });
    test.beforeAll(() => startDemoSources(["elasticsearch"]));
    test.afterAll(() => removeDemoSources(["elasticsearch"]));

    test("elasticsearch registered once under DuckDB resolves after rebooting into mssql, no replay", async ({
      page,
    }) => {
      test.setTimeout(300000);
      await runRebootCase(page, (p) => registerElasticsearch(p), {}, ["mssql"]);
    });
  });
});

// REQ-1730 scenario 2: does the real Sources-form UI — create source, register table, query —
// work when engine X is primary from a COLD START, not just after a DuckDB registration replayed
// onto an already-running Trino process (every type above)? Deliberately NOT parameterized across
// every source type: `runFreshEngineCase` already generalizes over (engine, registrar), so adding
// a type here is a one-line addition, but each case pays a full Trino cold boot (minutes, see
// spawnRebootBackend/waitForTrinoStable's own comments) for coverage `reprovisionSourceOnEngine`
// already gets for free (it replays the SAME createSource mutation live against Trino for every
// passing type) — the only thing a genuine cold start adds is UI-affordance behavior that only
// shows up when Trino, not DuckDB, answers the very first schema/table introspection call. One
// representative case (mongodb) is kept here to prove the mechanism works; add more only for a
// type actually suspected of a first-boot-specific UI gap, not as a routine sweep.
test.describe("scenario 2: real UI flow works when a non-default engine is primary from boot (REQ-1730)", () => {
  test.beforeAll(() => startDemoSources(["mongodb"]));
  test.afterAll(() => removeDemoSources(["mongodb"]));

  test("mongodb: create source, register table, query — Trino primary from a cold start", async ({
    page,
  }) => {
    test.setTimeout(240000);
    // "host.docker.internal", not "localhost" — Trino is primary from the very first
    // schema-introspection call here, made from INSIDE its own container.
    await runFreshEngineCase(page, "trino", (p) =>
      registerMongodb(p, "", "host.docker.internal"),
    );
  });
});
