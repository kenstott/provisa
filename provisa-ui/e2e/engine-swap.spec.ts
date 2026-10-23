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

import { test, expect } from "./coverage";
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
  GRPC_REMOTE_SERVER_MODULE,
  PYTHON,
  ROOT,
  RUNNING_IN_CI,
  SINGLESTORE_AVAILABLE,
  provisionRedshift,
  provisionSynapse,
  provisionSwapSource,
  runSwapCase,
  sweepZombieSwapSources,
  waitForPort,
} from "./engine-swap-helpers";
import type { RedshiftConnection, SynapseConnection } from "./engine-swap-helpers";
import {
  RDB_WIDGETS_SOURCES,
  registerBigquery,
  registerDatabricks,
  registerFabric,
  registerFileLake,
  registerFiles,
  registerGsheets,
  registerSoda,
  registerGreatExpectations,
  registerMongodb,
  registerNeo4j,
  registerRdbWidgets,
  registerRedshift,
  registerSingleFile,
  registerSnowflake,
  WIDGETS_DUCKDB_PATH,
} from "./engine-swap-registrars";
import {
  registerCassandra,
  registerElasticsearch,
  registerGovdata,
  registerGraphqlRemote,
  registerRss,
  registerWebsocket,
  registerGrpcRemote,
  registerIngest,
  registerOpenapi,
  registerPrometheus,
  registerRedis,
  registerSparql,
  registerSharepoint,
  registerSplunk,
  registerSqlite,
} from "./engine-swap-registrars-services";
import {
  registerAirport,
  registerExasol,
  registerFirebird,
  registerKafka,
  registerDruid,
  registerHiveS3,
  registerPinot,
  registerSynapse,
  registerSinglestore,
  registerDuckdbSource,
  registerTrinoSource,
} from "./engine-swap-registrars-olap";

import { registerSwapFileHooks } from "./engine-swap-setup";

const lake = registerSwapFileHooks();

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
    }) => runSwapCase(page, () => registerFileLake("delta_lake", lake.deltaTablePath)(page)));
  });

  test.describe("iceberg", () => {
    test(`iceberg: register once under DuckDB, answer identical queries under every other engine`, async ({
      page,
    }) => runSwapCase(page, () => registerFileLake("iceberg", lake.icebergTablePath)(page)));
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
