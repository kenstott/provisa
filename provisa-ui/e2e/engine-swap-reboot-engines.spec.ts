// Copyright (c) 2026 Kenneth Stott
// Canary: aa1a0080-8f58-4751-80b5-82bd74329a45
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

// REQ-1730: scenario 1 reversed and extended to warehouse and other primary engines, and
// scenario 2 (a non-default primary engine from boot). Split from engine-swap.spec.ts to stay
// under the max-lines lint rule; see that file's module doc for the harness's design and
// invocation.

import { test } from "./coverage";
import { startDemoSources, removeDemoSources } from "./demo-source-containers";
import {
  SNOWFLAKE_ENGINE_AVAILABLE,
  DATABRICKS_ENGINE_AVAILABLE,
  BIGQUERY_ENGINE_AVAILABLE,
  provisionSwapSource,
  runFreshEngineCase,
  runRebootCase,
  sweepZombieSwapSources,
} from "./engine-swap-helpers";
import {
  RDB_WIDGETS_SOURCES,
  registerMongodb,
  registerRdbWidgets,
} from "./engine-swap-registrars";
import {
  registerCassandra,
  registerElasticsearch,
  registerRedis,
} from "./engine-swap-registrars-services";

import { registerSwapFileHooks } from "./engine-swap-setup";

registerSwapFileHooks();

// REQ-1730 scenario 1, reversed direction: the opposite starting engine from scenario 1 —
// register Trino-primary (the real Sources-form UI, host.docker.internal from the very first
// call, same as scenario 2's own mongodb case), then genuinely kill+respawn the SAME reboot-
// harness process back onto DuckDB and requery, with no replay of the registration mutation
// either way. Proves the swap is symmetric: it is not only "register on DuckDB, survive a swap to
// Trino" (every case in engine-swap-reboot.spec.ts) but also "register on Trino, survive a swap back to DuckDB" — the
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
// registerDatabricks/registerBigquery (engine-swap-registrars.ts) register them as a federated SOURCE reached via
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

  test.describe("elasticsearch -> clickhouse", () => {
    test.beforeAll(() => provisionSwapSource("clickhouse", "up"));
    test.afterAll(async () => {
      await provisionSwapSource("clickhouse", "down");
    });
    test.beforeAll(() => startDemoSources(["elasticsearch"]));
    test.afterAll(() => removeDemoSources(["elasticsearch"]));

    test("elasticsearch registered once under DuckDB resolves after rebooting into clickhouse, no replay", async ({
      page,
    }) => {
      test.setTimeout(300000);
      await runRebootCase(page, (p) => registerElasticsearch(p), {}, ["clickhouse"]);
    });
  });

  test.describe("elasticsearch -> fabric", () => {
    test.skip(
      !(process.env.FABRIC_SQL_SERVER && process.env.FABRIC_DATABASE),
      "no live Fabric credentials: set FABRIC_SQL_SERVER/FABRIC_DATABASE in the root .env",
    );
    test.beforeAll(() => startDemoSources(["elasticsearch"]));
    test.afterAll(() => removeDemoSources(["elasticsearch"]));

    test("elasticsearch registered once under DuckDB resolves after rebooting into fabric, no replay", async ({
      page,
    }) => {
      test.setTimeout(300000);
      await runRebootCase(page, (p) => registerElasticsearch(p), {}, ["fabric"]);
    });
  });

  test.describe("elasticsearch -> oracle", () => {
    // gvenzl/oracle-free creates its database from scratch on first boot (demo/sources/oracle's
    // own documented start_period/retries) — several minutes, same reason RDB_WIDGETS_SOURCES's
    // own oracle entry carries a bootTimeoutMs of 480000 rather than the others' default.
    test.beforeAll(() => provisionSwapSource("oracle", "up"));
    test.afterAll(async () => {
      await provisionSwapSource("oracle", "down");
    });
    test.beforeAll(() => startDemoSources(["elasticsearch"]));
    test.afterAll(() => removeDemoSources(["elasticsearch"]));

    test("elasticsearch registered once under DuckDB resolves after rebooting into oracle, no replay", async ({
      page,
    }) => {
      test.setTimeout(600000);
      await runRebootCase(page, (p) => registerElasticsearch(p), {}, ["oracle"]);
    });
  });
});

// REQ-1730 extended, continued: mssql and pg as the PRIMARY (registration) engine — the reverse
// direction of the block above, completing the matrix cell every other engine in this file already
// has both ways (duckdb/trino). Neither mssql nor pg is containerized from this app process's own
// perspective (mssql is a plain published TCP port same as any other native-process target; pg is
// the SAME control-plane Postgres this whole harness already runs on — PgBackend._new_runtime falls
// back to it when no $PROVISA_ENGINE_URL is set, per rebootControlPlaneEnv's own comment), so no
// host rewrite applies in either direction — `hostRewriteTypes: {}` throughout, `host="localhost"`
// (the registrar's own default) needs no override either. pg is a per-source-catalog FEDERATOR
// (`_PER_SOURCE_CATALOG_ENGINES` in engine.py), the same class as duckdb/trino, not a self-only
// warehouse like mssql/snowflake/databricks/bigquery — piloted here for the first time in this
// harness to find out whether its own landing/reconcile path for a MATERIALIZE_ONLY source (no FDW
// reaches elasticsearch) already works or has the same gap mssql needed five fixes for.
test.describe("scenario 1 extended: mssql, pg and bigquery as the primary engine (REQ-1730)", () => {
  test.use({ allowedBrowserErrors: ["ERR_CONNECTION_REFUSED"] });

  test.describe("mssql -> duckdb", () => {
    test.beforeAll(() => provisionSwapSource("sqlserver", "up"));
    test.afterAll(async () => {
      await provisionSwapSource("sqlserver", "down");
    });
    test.beforeAll(() => startDemoSources(["elasticsearch"]));
    test.afterAll(() => removeDemoSources(["elasticsearch"]));

    test("elasticsearch registered once under mssql resolves after rebooting into DuckDB, no replay", async ({
      page,
    }) => {
      test.setTimeout(300000);
      await runRebootCase(page, (p) => registerElasticsearch(p), {}, ["duckdb"], false, "mssql");
    });
  });

  test.describe("pg -> duckdb", () => {
    test.beforeAll(() => startDemoSources(["elasticsearch"]));
    test.afterAll(() => removeDemoSources(["elasticsearch"]));

    test("elasticsearch registered once under pg resolves after rebooting into DuckDB, no replay", async ({
      page,
    }) => {
      test.setTimeout(300000);
      await runRebootCase(page, (p) => registerElasticsearch(p), {}, ["duckdb"], false, "pg");
    });
  });

  test.describe("duckdb -> pg", () => {
    test.beforeAll(() => startDemoSources(["elasticsearch"]));
    test.afterAll(() => removeDemoSources(["elasticsearch"]));

    test("elasticsearch registered once under DuckDB resolves after rebooting into pg, no replay", async ({
      page,
    }) => {
      test.setTimeout(300000);
      await runRebootCase(page, (p) => registerElasticsearch(p), {}, ["pg"]);
    });
  });

  test.describe("bigquery -> duckdb", () => {
    test.beforeAll(() => startDemoSources(["elasticsearch"]));
    test.afterAll(() => removeDemoSources(["elasticsearch"]));

    test("elasticsearch registered once under BigQuery resolves after rebooting into DuckDB, no replay", async ({
      page,
    }) => {
      test.setTimeout(300000);
      await runRebootCase(page, (p) => registerElasticsearch(p), {}, ["duckdb"], false, "bigquery");
    });
  });

  // LIVE ATTACH, not materialize-only: pg's own postgres_fdw reaches a real postgresql source
  // in place (per pg's `_PER_SOURCE_CATALOG_ENGINES` federator class — the same reach model as
  // duckdb/trino's own native connectors, unlike mssql/snowflake/databricks/bigquery's self-only
  // land-everything model). The elasticsearch cases above prove pg's MATERIALIZE_ONLY landing
  // path; this proves the DIFFERENT, opposite path — a source pg can actually scan live through
  // its own FDW, no landing involved at all — reusing the same postgresql RDB widget fixture
  // duckdb/trino's own native-connector tests already use.
  test.describe("postgresql (live attach via postgres_fdw) -> pg", () => {
    const _pgCfg = RDB_WIDGETS_SOURCES.find((c) => c.type === "postgresql")!;
    test.beforeAll(() => provisionSwapSource(_pgCfg.type, "up"));
    test.afterAll(async () => {
      await provisionSwapSource(_pgCfg.type, "down");
      await sweepZombieSwapSources();
    });

    test("postgresql registered once under DuckDB resolves after rebooting into pg via live ATTACH, no replay", async ({
      page,
    }) => {
      test.setTimeout(300000);
      await runRebootCase(page, (p) => registerRdbWidgets(_pgCfg)(p), {}, ["pg"]);
    });
  });
});

// REQ-1730 scenario 2: does the real Sources-form UI — create source, register table, query —
// work when engine X is primary from a COLD START, not just after a DuckDB registration replayed
// onto an already-running Trino process (every type in engine-swap.spec.ts)? Deliberately NOT parameterized across
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
