// Copyright (c) 2026 Kenneth Stott
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
// Scope: 9 of cat1+cat2's 13 types. Both categories land MATERIALIZED sources through the SAME
// mechanism regardless of engine — the app process's own Python driver fetches the source and
// writes the replica into whichever engine's store is active (REQ-826's `_MATERIALIZE_ONLY`
// set) — so neo4j/mongodb/elasticsearch/redis/cassandra/sparql/prometheus/graphql_remote/openapi
// need nothing engine-specific to reach under Trino. Three are excluded from THIS harness, not
// because they're broken, but because they reach through a DIFFERENT mechanism whose
// register-once/swap-engine behavior is unproven and out of scope here:
//   - splunk/sharepoint ATTACH through the connector's bundled Calcite pgwire server as a real
//     Trino catalog (TrinoBackend.register_source creates it; DuckDB's is a no-op) — that catalog
//     creation happens INSIDE the registration mutation, which this harness only ever runs
//     against DuckDB, so Trino never gets it built.
//   - sqlite has no Trino connector or FDW path at all (only DuckDB natively and pg via
//     sqlite_fdw per REQ-1726) — registered here to prove the DuckDB leg, never requeried.
//
// Run: PROVISA_E2E_LANE=all PROVISA_E2E_CONTROL_PLANE=postgres PROVISA_E2E_WORKERS=1 \
//      PROVISA_E2E_ORG_ID=e2e_swap PROVISA_E2E_TRINO_ORG_ID=e2e_swap \
//      npx playwright test --project=swap
// (The two env overrides put the DuckDB and Trino backends on the SAME org_<id> Postgres schema —
// normally kept apart to prevent collision — which is exactly the sharing this harness needs.)

import fs from "node:fs";

import { test, expect, TRINO_BACKEND_URL, UI_URL } from "./coverage";
import {
  E2E_CASSANDRA_PORT,
  E2E_ES_PORT,
  E2E_MONGO_PORT,
  E2E_NEO4J_HTTP_PORT,
  E2E_PROMETHEUS_PORT,
  E2E_REDIS_PORT,
  E2E_SPARQL_PORT,
} from "./demo-source-containers";
import {
  existingSourcePath,
  openRegisterForm,
  openSourcesForm,
  pickSchemaAndTable,
  registeredTableNames,
  runSqlOnPage,
  submitRegisterAndExpectListed,
  submitSourceAndExpectListed,
} from "./source-to-query-helpers";
import type { Page } from "./coverage";

/** One source+table's proof: the SQL that reads it back, and the assertion every engine must
 * satisfy identically. An empty `reachableOn` registers the source (proving the DuckDB leg) but
 * skips every engine's requery — see the module doc for why (sqlite). */
interface Registration {
  label: string;
  sourceId: string;
  sql: string;
  assertRows: (rows: string[][]) => void;
  /** Engine names (EngineTarget.name below) this registration's source has a live reach path
   * for. Absence means "registered to prove the DuckDB leg, never requeried on that engine" —
   * e.g. sqlite, which has no Trino connector or FDW path at all (REQ-1726). */
  reachableOn: string[];
}

/** One already-running backend process this harness can requery against, sharing the DuckDB
 * backend's Postgres control-plane org schema. Add an entry here (and to ENGINES below) to bring
 * a new engine into the swap — nothing else in this file names an engine by hand. */
interface EngineTarget {
  name: string;
  backendUrl: string;
  /** Env var naming this engine's own config file on disk — PUT back verbatim to force a live,
   * in-process reload (REQ-1729's DB-driven backfill), no OS-level restart needed. */
  configPathEnv: string;
}

const ENGINES: EngineTarget[] = [
  { name: "trino", backendUrl: TRINO_BACKEND_URL, configPathEnv: "PROVISA_E2E_TRINO_CONFIG" },
  // pg: add { name: "pg", backendUrl: PG_BACKEND_URL, configPathEnv: "PROVISA_E2E_PG_CONFIG" }
  // once a pg-engine e2e backend exists (the `pgserver` embedded-Postgres package is the planned
  // route — no Docker image to stand up, unlike Trino's JVM cluster).
];

async function registerNeo4j(page: Page): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_neo4j_${stamp}`;
  const tableName = `adopter_${stamp}`;
  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("neo4j");
  await page.getByTestId("neo4j-host-input").fill("localhost");
  await page.getByTestId("neo4j-port-input").fill(String(E2E_NEO4J_HTTP_PORT));
  await page.getByTestId("neo4j-database-input").fill("neo4j");
  await submitSourceAndExpectListed(page, sourceId);

  await openRegisterForm(page, sourceId);
  await expect(page.getByTestId("register-table-schema-select")).toHaveCount(0);
  await page.getByTestId("register-table-neo4j-table-name").fill(tableName);
  await page
    .getByTestId("register-table-neo4j-cypher")
    .fill(
      "MATCH (a:Adopter) RETURN a.adopter_id AS adopter_id, a.name AS name, a.city AS city " +
        "ORDER BY a.adopter_id",
    );
  await page.getByTestId("register-table-neo4j-preview").click();
  await expect(page.getByTestId("register-table-neo4j-preview-rows")).toBeVisible({
    timeout: 60000,
  });
  const registered = await submitRegisterAndExpectListed(page, sourceId);

  return {
    label: "neo4j",
    sourceId,
    sql: `SELECT adopter_id, name, city FROM pet_store.${registered} ORDER BY adopter_id`,
    assertRows: (rows) => {
      expect(rows).toHaveLength(8);
      expect(rows[0]).toEqual(["1", "Sara Kim", "Portland"]);
      expect(rows[7]).toEqual(["8", "Mark Torres", "Tacoma"]);
    },
    reachableOn: ["trino"],
  };
}

async function registerMongodb(page: Page): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_mongodb_${stamp}`;
  const tableName = "product_reviews";
  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("mongodb");
  await page.getByLabel(/^Host/).fill("localhost");
  await page.getByLabel(/^Port/).fill(String(E2E_MONGO_PORT));
  await page.getByLabel(/^Database/).fill("provisa");
  await submitSourceAndExpectListed(page, sourceId);

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, "provisa", tableName);
  await expect(page.getByTestId("register-table-col-selected-reviewer")).toBeVisible({
    timeout: 60000,
  });
  const registered = await submitRegisterAndExpectListed(page, sourceId);

  return {
    label: "mongodb",
    sourceId,
    sql: `SELECT product_id, reviewer, rating FROM pet_store.${registered} ORDER BY reviewer`,
    assertRows: (rows) => {
      expect(rows).toHaveLength(10);
      expect(rows[0]).toEqual(["1", "alice", "5"]);
      expect(rows[9]).toEqual(["7", "jack", "1"]);
    },
    reachableOn: ["trino"],
  };
}

async function registerElasticsearch(page: Page): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_es_${stamp}`;
  const tableName = "support_tickets";
  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("elasticsearch");
  await page.getByLabel(/^Host/).fill("localhost");
  await page.getByLabel(/^Port/).fill(String(E2E_ES_PORT));
  await submitSourceAndExpectListed(page, sourceId);

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, "default", tableName);
  await expect(page.getByTestId("register-table-col-selected-ticket_id")).toBeVisible({
    timeout: 60000,
  });
  const registered = await submitRegisterAndExpectListed(page, sourceId);

  return {
    label: "elasticsearch",
    sourceId,
    sql: `SELECT ticket_id, status, priority FROM pet_store.${registered} ORDER BY ticket_id`,
    assertRows: (rows) => {
      expect(rows).toHaveLength(6);
      expect(rows[0]).toEqual(["T-1001", "open", "2"]);
      expect(rows[5]).toEqual(["T-1006", "open", "2"]);
    },
    reachableOn: ["trino"],
  };
}

async function registerRedis(page: Page): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_redis_${stamp}`;
  const tableName = "support_agent";
  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("redis");
  await page.getByLabel(/^Host/).fill("localhost");
  await page.getByLabel(/^Port/).fill(String(E2E_REDIS_PORT));
  await page.getByLabel(/^Database/).fill("0");
  await submitSourceAndExpectListed(page, sourceId);

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, "default", tableName);
  await expect(page.getByTestId("register-table-col-selected-name")).toBeVisible({
    timeout: 60000,
  });
  const registered = await submitRegisterAndExpectListed(page, sourceId);

  return {
    label: "redis",
    sourceId,
    sql: `SELECT agent_id, name, team FROM pet_store.${registered} ORDER BY agent_id`,
    assertRows: (rows) => {
      expect(rows).toHaveLength(5);
      expect(rows[0]).toEqual(["1", "Ann Lee", "tier1"]);
      expect(rows[4]).toEqual(["5", "Eli Stone", "escalations"]);
    },
    reachableOn: ["trino"],
  };
}

async function registerCassandra(page: Page): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_cassandra_${stamp}`;
  const tableName = "intake_events";
  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("cassandra");
  await page.getByLabel(/^Host/).fill("localhost");
  await page.getByLabel(/^Port/).fill(String(E2E_CASSANDRA_PORT));
  await page.getByLabel(/^Database/).fill("shelter_ops");
  await submitSourceAndExpectListed(page, sourceId);

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, "shelter_ops", tableName);
  await expect(page.getByTestId("register-table-col-selected-event_id")).toBeVisible({
    timeout: 60000,
  });
  const registered = await submitRegisterAndExpectListed(page, sourceId);

  return {
    label: "cassandra",
    sourceId,
    sql: `SELECT event_id, event_type, animal_name FROM pet_store.${registered} ORDER BY event_id`,
    assertRows: (rows) => {
      expect(rows).toHaveLength(7);
      expect(rows[0]).toEqual(["1", "intake", "Buddy"]);
      expect(rows[6]).toEqual(["7", "adoption", "Mittens"]);
    },
    reachableOn: ["trino"],
  };
}

async function registerSparql(page: Page): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_sparql_${stamp}`;
  const tableName = `volunteer_${stamp}`;
  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("sparql");
  await page
    .getByTestId("sparql-endpoint-input")
    .fill(`http://localhost:${E2E_SPARQL_PORT}/provisa/query`);
  await submitSourceAndExpectListed(page, sourceId);

  await openRegisterForm(page, sourceId);
  await expect(page.getByTestId("register-table-schema-select")).toHaveCount(0);
  await page.getByTestId("register-table-sparql-table-name").fill(tableName);
  await page
    .getByTestId("register-table-sparql-query")
    .fill(
      "PREFIX s: <http://provisa.dev/shelter#> SELECT ?volunteer_id ?name ?program " +
        "WHERE { ?v a s:Volunteer ; s:id ?volunteer_id ; s:name ?name ; s:program ?program } " +
        "ORDER BY ?volunteer_id",
    );
  await page.getByTestId("register-table-sparql-preview").click();
  await expect(page.getByTestId("register-table-sparql-preview-rows")).toBeVisible({
    timeout: 60000,
  });
  const registered = await submitRegisterAndExpectListed(page, sourceId);

  return {
    label: "sparql",
    sourceId,
    sql: `SELECT volunteer_id, name, program FROM pet_store.${registered} ORDER BY volunteer_id`,
    assertRows: (rows) => {
      expect(rows).toHaveLength(6);
      expect(rows[0]).toEqual(["V-01", "Grace Hall", "adoption"]);
      expect(rows[5]).toEqual(["V-06", "Noah Bryce", "fostering"]);
    },
    reachableOn: ["trino"],
  };
}

async function registerPrometheus(page: Page): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_prometheus_${stamp}`;
  const tableName = "up";
  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("prometheus");
  await page.getByTestId("prometheus-url-input").fill(`http://localhost:${E2E_PROMETHEUS_PORT}`);
  await submitSourceAndExpectListed(page, sourceId);

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, "default", tableName);
  await expect(page.getByTestId("register-table-col-selected-job")).toBeVisible({
    timeout: 60000,
  });
  const registered = await submitRegisterAndExpectListed(page, sourceId);

  return {
    label: "prometheus",
    sourceId,
    // "up" is a boolean gauge (always 1 when scraped), not a growing counter — identical under
    // any engine at any time, unlike a counter metric would be.
    sql: `SELECT job, CAST(MAX(value) AS INTEGER) AS healthy FROM pet_store.${registered} GROUP BY job ORDER BY job`,
    assertRows: (rows) => expect(rows).toEqual([["prometheus", "1"]]),
    reachableOn: ["trino"],
  };
}

async function registerSqlite(page: Page): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_sqlite_${stamp}`;
  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("sqlite");
  await page.getByLabel(/SQLite File Path/).fill("./demo/files/inquiries.sqlite");
  await submitSourceAndExpectListed(page, sourceId);

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, "main", "users");
  await expect(page.getByTestId("register-table-col-selected-name")).toBeVisible({
    timeout: 60000,
  });
  const registered = await submitRegisterAndExpectListed(page, sourceId);

  return {
    label: "sqlite",
    sourceId,
    sql: `SELECT id, name, email FROM pet_store.${registered} ORDER BY id`,
    assertRows: (rows) => {
      expect(rows).toHaveLength(10);
      expect(rows[0]).toEqual(["1", "Alice Nguyen", "alice@example.com"]);
    },
    reachableOn: [], // REQ-1726: no Trino connector or FDW path for sqlite
  };
}

async function registerGraphqlRemote(page: Page): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_gql_${stamp}`;
  const namespace = `e2e_swap_gql_${stamp}`;
  const endpoint = await existingSourcePath(page, "graphql-demo");

  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("graphql");
  await page.getByTestId("graphql-endpoint-input").fill(endpoint);
  await page.getByTestId("graphql-namespace-input").fill(namespace);
  await submitSourceAndExpectListed(page, sourceId);

  const tableNames = await registeredTableNames(page, sourceId);
  const breedTable = tableNames.find((n) => n.includes("animal_breed"));
  expect(breedTable, `no animal_breeds table registered for ${sourceId}`).toBeTruthy();
  const grant = await page.request.post("/admin/graphql", {
    data: {
      query: `mutation($t: TableInput!) { updateTable(input: $t) { success message } }`,
      variables: {
        t: {
          sourceId,
          domainId: "",
          schemaName: "graphql",
          tableName: breedTable,
          columns: [
            { name: "name", visibleTo: ["*"] },
            { name: "species", visibleTo: ["*"] },
          ],
        },
      },
    },
  });
  expect(grant.ok(), await grant.text()).toBeTruthy();
  const grantJson = await grant.json();
  expect(grantJson.errors, JSON.stringify(grantJson.errors)).toBeUndefined();
  expect(grantJson.data.updateTable.success, grantJson.data.updateTable.message).toBeTruthy();

  return {
    label: "graphql_remote",
    sourceId,
    sql: `SELECT name, species FROM graphql.${breedTable} ORDER BY name`,
    assertRows: (rows) => {
      expect(rows).toHaveLength(6);
      expect(rows.map((r) => r[0])).toEqual([
        "African Lion",
        "Barbary Lion",
        "Golden Retriever",
        "Holland Lop",
        "Maine Coon",
        "Siamese",
      ]);
    },
    reachableOn: ["trino"],
  };
}

async function registerOpenapi(page: Page): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_openapi_${stamp}`;
  const specUrl = await existingSourcePath(page, "petstore-api");
  const baseUrl = specUrl.replace(/\/openapi\.json$/, "");

  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("openapi");
  await page.getByTestId("openapi-spec-path-input").fill(specUrl);
  await page.getByTestId("openapi-base-url-input").fill(baseUrl);
  await submitSourceAndExpectListed(page, sourceId);

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, "openapi", "getInventory");
  const registered = await submitRegisterAndExpectListed(page, sourceId);

  return {
    label: "openapi",
    sourceId,
    sql: `SELECT * FROM pet_store.${registered}`,
    assertRows: (rows) => expect(rows.length).toBeGreaterThan(0),
    reachableOn: ["trino"],
  };
}

/** Force an already-running engine backend to pick up rows the DuckDB backend just registered
 * into their SHARED Postgres control-plane schema. `PUT /admin/config` re-enters the exact
 * boot-time DB-driven backfill (introspect_tables/_rebuild_schemas/reconcile_landed_tables —
 * see REQ-1729) — no OS-level restart needed, since it is already a live in-process reload. */
async function reloadEngineBackend(engine: EngineTarget) {
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
async function reprovisionSourceOnEngine(engine: EngineTarget, sourceId: string) {
  // Raw fetch() against the engine's own URL, NOT page.request — page.request is a separate
  // APIRequestContext that page.route() never intercepts (only browser-initiated requests are
  // routed), so this call would otherwise silently keep hitting the DuckDB backend regardless of
  // the route rewrite below, exactly as it did before this was caught.
  const res = await fetch(`${engine.backendUrl}/admin/graphql`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      query: "{ sources { id type host port database username path description } }",
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
    path: string | null;
    description: string;
  }>;
  const src = sources.find((s) => s.id === sourceId);
  expect(src, `source ${sourceId} not found on this engine's control-plane schema`).toBeTruthy();
  const mutation = await fetch(`${engine.backendUrl}/admin/graphql`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({
      query: `mutation($s: SourceInput!) { createSource(input: $s) { success message } }`,
      variables: { s: src },
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
async function requeryOnEngine(page: Page, engine: EngineTarget, registrations: Registration[]) {
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
    const rows = await runSqlOnPage(page, reg.sql);
    reg.assertRows(rows);
  }
  for (const pattern of routes) await page.unroute(pattern);
}

test.describe("engine swap: one registration answers every engine (REQ-1730)", () => {
  test("cat1+cat2 sources register once under DuckDB, then answer identical queries under every other engine", async ({
    page,
  }) => {
    // 10 registrations + one reload/requery pass per entry in ENGINES.
    test.setTimeout((10 + 5 * ENGINES.length) * 60 * 1000);

    const registrars = [
      registerNeo4j,
      registerMongodb,
      registerElasticsearch,
      registerRedis,
      registerCassandra,
      registerSparql,
      registerPrometheus,
      registerSqlite,
      registerGraphqlRemote,
      registerOpenapi,
    ];

    const registrations: Registration[] = [];
    for (const registrar of registrars) {
      registrations.push(await registrar(page));
    }

    for (const engine of ENGINES) {
      await requeryOnEngine(page, engine, registrations);
    }
  });
});
