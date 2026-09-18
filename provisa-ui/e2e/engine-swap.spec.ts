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
  E2E_EXASOL_FINGERPRINT_FILE,
  E2E_EXASOL_PORT,
  E2E_GRPC_REMOTE_PORT,
  DOCKER_NETWORK,
  E2E_KAFKA_PORT,
  E2E_KAFKA_SCHEMA_REGISTRY_PORT,
  E2E_RSS_PORT,
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
  provisionSwapSource,
  runFreshEngineCase,
  runRebootCase,
  runSwapCase,
  sweepZombieSwapSources,
  waitForPort,
  waitForTrinoStable,
} from "./engine-swap-helpers";
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
  registerRdbWidgets,
  registerRedis,
  registerSinglestore,
  registerSingleFile,
  registerSnowflake,
  registerSparql,
  registerSharepoint,
  registerSplunk,
  registerSqlite,
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
  test("sqlite: register once under DuckDB (no Trino leg — REQ-1726)", async ({ page }) =>
    runSwapCase(page, () => registerSqlite(page)));

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
        return provisionSwapSource(cfg.type, "up");
      });
      test.afterAll(async () => {
        if (cfg.needsCi && !RUNNING_IN_CI) return;
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

  // Same RDB_WIDGETS_SOURCES config list the original describe block above loops over — every
  // type here has a native Trino connector (reachableOn: ["trino"] unless overridden), so
  // rebooting into Trino is meaningful; hiveserver2/saphana (reachableOn: []) are skipped — no
  // Trino leg exists for them to prove a reboot restores.
  for (const cfg of RDB_WIDGETS_SOURCES) {
    if ((cfg.reachableOn ?? ["trino"]).length === 0) continue;
    test.describe(cfg.type, () => {
      test.beforeAll(() => {
        if (cfg.needsCi && !RUNNING_IN_CI) return;
        return provisionSwapSource(cfg.type, "up");
      });
      test.afterAll(async () => {
        if (cfg.needsCi && !RUNNING_IN_CI) return;
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
      "needs SINGLESTORE_LICENSE and an amd64 host (singlestoredb-dev publishes no arm64 manifest) " +
        "— see the module doc",
    );
    test.beforeAll(() => provisionSwapSource("singlestore", "up"));
    test.afterAll(() => provisionSwapSource("singlestore", "down"));

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
