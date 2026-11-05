// Copyright (c) 2026 Kenneth Stott
// Canary: e689f38b-2420-4697-a72e-5f0cea0f85e4
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

// REQ-1730 scenario 1: a source survives an engine change and a genuine reboot. Split from
// engine-swap.spec.ts to stay under the max-lines lint rule; see that file's module doc for the
// harness's design and invocation.
import { execFileSync, spawn, type ChildProcess } from "node:child_process";
import fs from "node:fs";
import http from "node:http";
import path from "node:path";

import { WebSocketServer } from "ws";

import { test, expect } from "./coverage";
import { seedWarehouse } from "./cloud-warehouse";
import { startDemoSources, removeDemoSources } from "./demo-source-containers";
import {
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
  REBOOT_GRAPHQL_DEMO_URL,
  REBOOT_PETSTORE_OPENAPI_URL,
  ROOT,
  RUNNING_IN_CI,
  SINGLESTORE_AVAILABLE,
  provisionRedshift,
  provisionSynapse,
  provisionSwapSource,
  runRebootCase,
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
  registerHive,
  registerHiveS3,
  registerPinot,
  registerSynapse,
  registerSinglestore,
  registerDuckdbSource,
  registerTrinoSource,
} from "./engine-swap-registrars-olap";

import { registerSwapFileHooks } from "./engine-swap-setup";

const lake = registerSwapFileHooks();

// REQ-1730 scenario 1: "if you changed engines, it required you to reboot the backend" (the
// user's own design intent) — a source registered purely through the UI (no `sources:` YAML
// entry) must survive an engine change + a genuine process reboot and remain queryable, with no
// replay of its own createSource mutation. This is deliberately NOT the reload-in-place trick
// every type in engine-swap.spec.ts's describe block uses (reloadEngineBackend/reprovisionSourceOnEngine PUT
// /admin/config against an already-running, already-differently-engined process) — that proxy
// already found one real gap (extra_sources never reaching _replace_mode_cleanup) and, on its own
// admission, is not what "reboot" means: it exercises one process's in-place reload path, never a
// cold start. `runRebootCase` (engine-swap-helpers.ts) owns ONE dedicated backend process per
// test, genuinely killed and respawned with PROVISA_ENGINE flipped; same port, same data dir, same
// control-plane org row — as close to a real desktop engine-swap-then-restart as this harness
// gets. One type per test (mirrors the "one source = one type" contract in engine-swap.spec.ts) — add a type by
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

  // Same RDB_WIDGETS_SOURCES config list engine-swap.spec.ts's describe block loops over — every
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
      seedWarehouse("snowflake", "up");
    });
    test.afterAll(() => {
      seedWarehouse("snowflake", "down");
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
      seedWarehouse("databricks", "up");
    });
    test.afterAll(() => {
      seedWarehouse("databricks", "down");
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
      seedWarehouse("fabric", "up");
    });
    test.afterAll(() => {
      seedWarehouse("fabric", "down");
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
      seedWarehouse("bigquery", "up");
    });
    test.afterAll(() => {
      seedWarehouse("bigquery", "down");
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
    runRebootCase(page, (p) => registerFileLake("delta_lake", lake.deltaTablePath)(p), {
      trino: "delta_lake",
    }));

  test("iceberg registered once under DuckDB resolves under every rebooted engine, no replay", async ({
    page,
  }) =>
    runRebootCase(page, (p) => registerFileLake("iceberg", lake.icebergTablePath)(p), {
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
