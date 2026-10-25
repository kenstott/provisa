// Copyright (c) 2026 Kenneth Stott
// Canary: 0a908bae-31c8-4725-ac23-7d22d4617b98
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

// REQ-1730: engine-swap.spec.ts's registrars for search, key-value, wide-column, graph-query,
// metrics, SaaS, API and streaming-push sources (elasticsearch through websocket). Split from
// engine-swap-registrars.ts to stay under the max-lines lint rule; see that module's doc.

import path from "node:path";

import { expect, test } from "./coverage";
import type { Page } from "./coverage";
import {
  E2E_CASSANDRA_PORT,
  E2E_ES_PORT,
  E2E_PROMETHEUS_PORT,
  E2E_REDIS_PORT,
  E2E_SPARQL_PORT,
  E2E_SPLUNK_PORT,
} from "./demo-source-containers";
import {
  expectDiscoveredColumns,
  existingSourcePath,
  openRegisterForm,
  openSourcesForm,
  pickSchemaAndTable,
  registerOfferedTable,
  submitRegisterAndExpectListed,
  ASKAMERICA_SERVING_BUDGET_MS,
  waitForSourceServing,
  FORM_FIELD,
} from "./source-to-query-helpers";
import {
  E2E_RSS_PORT,
  E2E_WS_PORT,
  ROOT,
  SWAP_REGISTER_TIMEOUT_MS,
} from "./engine-swap-helpers";
import type { Registration } from "./engine-swap-helpers";
import { submitSwapSource } from "./engine-swap-registrars";

export async function registerElasticsearch(
  page: Page,
  // See registerMongodb's own `host` param doc — same Trino-primary-from-boot need.
  host = "localhost",
): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_es_${stamp}`;
  const tableName = "support_tickets";
  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("elasticsearch");
  await page.getByLabel(/^Host/).and(page.locator(FORM_FIELD)).fill(host);
  await page.getByLabel(/^Port/).and(page.locator(FORM_FIELD)).fill(String(E2E_ES_PORT));
  await submitSwapSource(page, sourceId);

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, "default", tableName);
  await expectDiscoveredColumns(page, ["ticket_id"], 60000);
  const registered = await submitRegisterAndExpectListed(page, sourceId, SWAP_REGISTER_TIMEOUT_MS);

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

export async function registerRedis(
  page: Page,
  // See registerMongodb's own `host` param doc — same Trino-primary-from-boot need.
  host = "localhost",
): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_redis_${stamp}`;
  const tableName = "support_agent";
  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("redis");
  await page.getByLabel(/^Host/).and(page.locator(FORM_FIELD)).fill(host);
  await page.getByLabel(/^Port/).and(page.locator(FORM_FIELD)).fill(String(E2E_REDIS_PORT));
  await page.getByLabel(/^Database/).and(page.locator(FORM_FIELD)).fill("0");
  await submitSwapSource(page, sourceId);

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, "default", tableName);
  await expectDiscoveredColumns(page, ["name"], 60000);
  const registered = await submitRegisterAndExpectListed(page, sourceId, SWAP_REGISTER_TIMEOUT_MS);

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

export async function registerCassandra(
  page: Page,
  // See registerMongodb's own `host` param doc — same Trino-primary-from-boot need.
  host = "localhost",
): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_cassandra_${stamp}`;
  const tableName = "intake_events";
  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("cassandra");
  await page.getByLabel(/^Host/).and(page.locator(FORM_FIELD)).fill(host);
  await page.getByLabel(/^Port/).and(page.locator(FORM_FIELD)).fill(String(E2E_CASSANDRA_PORT));
  await page.getByLabel(/^Database/).and(page.locator(FORM_FIELD)).fill("shelter_ops");
  await submitSwapSource(page, sourceId);

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, "shelter_ops", tableName);
  await expectDiscoveredColumns(page, ["event_id"], 60000);
  const registered = await submitRegisterAndExpectListed(page, sourceId, SWAP_REGISTER_TIMEOUT_MS);

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

export async function registerSparql(page: Page): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_sparql_${stamp}`;
  const tableName = `volunteer_${stamp}`;
  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("sparql");
  await page
    .getByTestId("sparql-endpoint-input")
    .fill(`http://localhost:${E2E_SPARQL_PORT}/provisa/query`);
  await submitSwapSource(page, sourceId);

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
  const registered = await submitRegisterAndExpectListed(page, sourceId, SWAP_REGISTER_TIMEOUT_MS);

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

export async function registerPrometheus(page: Page): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_prometheus_${stamp}`;
  const tableName = "up";
  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("prometheus");
  await page.getByTestId("prometheus-url-input").fill(`http://localhost:${E2E_PROMETHEUS_PORT}`);
  await submitSwapSource(page, sourceId);

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, "default", tableName);
  await expectDiscoveredColumns(page, ["job"], 60000);
  const registered = await submitRegisterAndExpectListed(page, sourceId, SWAP_REGISTER_TIMEOUT_MS);

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

/** splunk: DuckDB reaches it via the connector's bundled Calcite pgwire bridge, a per-source JVM
 * whose schema is the sql-normalized SOURCE ID (`pgwire_replica.schema_name()`). Trino reaches it
 * via a genuinely different, standalone plugin (`trino/plugins/trino-splunk/`, also wrapping
 * Calcite's splunk adapter) that — until now — always registered its schema under the FIXED
 * string `"splunk"` regardless of configuration, verified live via a real
 * `FederationError(..., SCHEMA_NOT_FOUND, "Schema '<source_id>' does not exist")`: a table
 * registered under DuckDB records schema=<source_id>, and swapped to Trino the compiler emits
 * that SAME physical schema name, which the plugin's hardcoded schema never had. NOT the
 * registration-timing gap this harness's module doc originally guessed
 * (`reprovisionSourceOnEngine`'s createSource replay runs fine and does create the Trino catalog)
 * — a genuine physical-schema-naming mismatch, fixed upstream: `calcite/splunk`'s `SplunkDriver`
 * now honors a `schema` connection property (kenstott/calcite@28db96ca1), and
 * `TrinoSplunkConnector.details()` (trino_connectors.py) passes the SAME
 * `pgwire_replica.schema_name()` value Trino sees under both engines. sharepoint has the
 * identical shape (tests/integration/test_sharepoint_source_e2e.py:28, fixed `"sharepoint"`
 * schema) — same root cause, not yet fixed upstream (its plugin is a separate codebase). */
export async function registerSplunk(page: Page): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_splunk_${stamp}`;
  const tableName = "shelter_alerts";

  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("splunk");
  await page.getByTestId("splunk-host-input").fill("localhost");
  await page.getByTestId("splunk-port-input").fill(String(E2E_SPLUNK_PORT));
  await page.getByTestId("splunk-auth-mode-select").click();
  await page.getByRole("option", { name: "Username / Password" }).click();
  await page.getByTestId("splunk-username-input").fill("admin");
  await page.getByTestId("splunk-password-input").fill("Provisa_2026!");
  await page.getByTestId("splunk-disable-ssl-checkbox").check();
  await submitSwapSource(page, sourceId);

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, sourceId, tableName);
  await expectDiscoveredColumns(page, ["alert_id"], 120000);
  await expectDiscoveredColumns(page, ["animal_name"]);
  const registered = await submitRegisterAndExpectListed(page, sourceId, SWAP_REGISTER_TIMEOUT_MS);

  return {
    label: "splunk",
    sourceId,
    sql: `SELECT alert_type, COUNT(*) AS alerts FROM pet_store.${registered} GROUP BY alert_type ORDER BY alert_type`,
    assertRows: (rows) => {
      expect(rows).toEqual([
        ["adoption_hold", "1"],
        ["intake", "2"],
        ["medical", "2"],
        ["transfer", "2"],
      ]);
    },
    reachableOn: ["trino"],
  };
}

/** sharepoint: a real live tenant (SP_* in .env), not a container — same cloud-warehouse-class
 * pattern as snowflake/databricks/fabric, just no seed/teardown step needed (reads an existing
 * document library, writes nothing). UI flow copied verbatim from source-to-query.spec.ts's own
 * proven sharepoint case (REQ-1747-adjacent). Unlike splunk (kenstott/calcite@28db96ca1/
 * @4a042e872, a genuine upstream bug), the underlying trino-sharepoint plugin ALREADY had a fully
 * working `schema` catalog property (verified by reading SharePointConfig.java/
 * SharePointClientModule.java/SharePointListDriver.java before assuming the same fix was needed —
 * see [[feedback-verify-old-exclusion-labels]]) — only TrinoSharepointConnector.details()
 * (trino_connectors.py) needed to start passing it. */
export async function registerSharepoint(page: Page): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_sharepoint_${stamp}`;
  const tableName = "documents";
  // The Calcite pgwire server runs with its bundle directory as cwd, so a relative SP_CERT_PATH
  // (.env authors it as ./sharepoint.pfx) has to be resolved here, same as source-to-query.spec.ts.
  const certPath = path.resolve(ROOT, process.env.SP_CERT_PATH!);

  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("sharepoint");
  await page.getByTestId("sharepoint-site-url-input").fill(process.env.SP_SITE_URL!);
  await page.getByTestId("sharepoint-tenant-id-input").fill(process.env.SP_TENANT_ID!);
  await page.getByTestId("sharepoint-auth-type-select").click();
  await page.getByRole("option", { name: "Certificate", exact: true }).click();
  await page.getByTestId("sharepoint-client-id-input").fill(process.env.SP_CLIENT_ID!);
  await page.getByTestId("sharepoint-cert-path-input").fill(certPath);
  await page.getByTestId("sharepoint-cert-password-input").fill(process.env.SP_CERT_PASSWORD ?? "");
  await submitSwapSource(page, sourceId);

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, sourceId, tableName);
  await expectDiscoveredColumns(page, ["id"], 120000);
  await expectDiscoveredColumns(page, ["title"]);
  const registered = await submitRegisterAndExpectListed(page, sourceId, SWAP_REGISTER_TIMEOUT_MS);

  return {
    label: "sharepoint",
    sourceId,
    // The tenant's document library contents are real and may legitimately be empty, so the
    // aggregate's SHAPE is the assertion (matches source-to-query.spec.ts's own case) — same
    // shape under both engines is exactly what this harness is proving, not a fixed row count.
    sql: `SELECT COUNT(*) AS document_count FROM pet_store.${registered}`,
    assertRows: (rows) => {
      expect(rows).toHaveLength(1);
      expect(rows[0]).toHaveLength(1);
      expect(Number(rows[0][0])).toBeGreaterThanOrEqual(0);
    },
    reachableOn: ["trino"],
  };
}

export async function registerSqlite(page: Page): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_sqlite_${stamp}`;
  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("sqlite");
  await page.getByLabel(/SQLite File Path/).fill("./demo/files/inquiries.sqlite");
  await submitSwapSource(page, sourceId);

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, "main", "users");
  await expectDiscoveredColumns(page, ["name"], 60000);
  const registered = await submitRegisterAndExpectListed(page, sourceId, SWAP_REGISTER_TIMEOUT_MS);

  return {
    label: "sqlite",
    sourceId,
    sql: `SELECT id, name, email FROM pet_store.${registered} ORDER BY id`,
    assertRows: (rows) => {
      expect(rows).toHaveLength(10);
      expect(rows[0]).toEqual(["1", "Alice Nguyen", "alice@example.com"]);
    },
    // REQ-1730: sqlite has no Trino connector, but IS reachable on Trino via landing (now that
    // sqlite is in strategy.py's _MATERIALIZE_ONLY — events/source_loader.py's make_sqlite_loader
    // already had a working, engine-independent row-fetch). Reachable on pg too, via
    // SqliteFdwConnector — see the dedicated scenario-1 pg-reboot test in engine-swap.spec.ts.
    reachableOn: ["trino"],
  };
}

export async function registerGraphqlRemote(page: Page, endpointOverride?: string): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_gql_${stamp}`;
  const namespace = `e2e_swap_gql_${stamp}`;
  // REQ-1730 scenario 1 (test-only): the reboot harness's minimal config has no baked-in
  // "graphql-demo" source for existingSourcePath to read a path off of — pass the already-running
  // demo server's URL directly (spawnRebootBackend's own GRAPHQL_DEMO_URL env, same default port).
  const endpoint = endpointOverride ?? (await existingSourcePath(page, "graphql-demo"));

  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("graphql");
  await page.getByTestId("graphql-endpoint-input").fill(endpoint);
  await page.getByTestId("graphql-namespace-input").fill(namespace);
  await submitSwapSource(page, sourceId);

  // REQ-308: adding the source registers nothing; the table is registered on its own.
  const breedTable = await registerOfferedTable(page, sourceId, "graphql", "animal_breed", [
    "name",
    "species",
  ]);

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

// grpc_remote has no Trino connector — reachability runs through the same landing path as
// graphql_remote/openapi (rows written into the Postgres-backed materialize store Trino reads via
// its provisa_admin catalog), which REQ-1730's ensure_cache_schema fix made work. See
// demo/grpc_remote_server for the proto-based fixture this registers against (REQ-1742).
export async function registerGrpcRemote(page: Page, port: number): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_grpc_${stamp}`;
  const namespace = `e2e_swap_grpc_${stamp}`;
  const protoPath = path.join(ROOT, "demo", "grpc_remote_server", "animal_catalog.proto");

  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("grpc");
  await page.getByTestId("grpc-proto-path-input").fill(protoPath);
  await page.getByTestId("grpc-server-address-input").fill(`localhost:${port}`);
  await page.getByTestId("grpc-namespace-input").fill(namespace);
  await submitSwapSource(page, sourceId);

  // REQ-322: adding the source registers nothing; the table is registered on its own.
  const breedTable = await registerOfferedTable(page, sourceId, "grpc_remote", "ListBreeds", [
    "name",
    "species",
    "avg_lifespan_years",
  ]);

  return {
    label: "grpc_remote",
    sourceId,
    sql: `SELECT name, species FROM grpc_remote.${breedTable} ORDER BY name`,
    assertRows: (rows) => {
      expect(rows).toHaveLength(3);
      expect(rows.map((r) => r[0])).toEqual(["Holland Lop", "Labrador Retriever", "Siamese"]);
    },
    reachableOn: ["trino"],
  };
}

// ingest (REQ-1730): a NO_CONNECTION_TYPES push receiver (POST /data/ingest/<source>/<table>,
// provisa/ingest/router.py) — no Sources-form connection fields, no adapter fetch. Rows land
// SYNCHRONOUSLY into the SAME control-plane DB state.tenant_db mirrors (provisa/ingest/engine.py's
// _init_ingest_engines), which under this harness's Postgres control plane is the identical
// Postgres schema Trino's own `provisa_admin` catalog reads live — unlike grpc_remote/graphql_remote
// (a materialize-store copy reconcile_landed_tables writes asynchronously), there is no landing
// delay to wait out: the POST itself is the write. source-to-query-streaming.spec.ts's own proven
// ingest case (REQ-1739/1771, 4 real bugs fixed there) is this registrar's exact template —
// "default"-schema pick (source_id as the one table, REQ-1745), placeholder ext_id/value columns
// with per-column JSON-path extraction (id/value), pet_store.<registered> as the query surface
// (register_table already overrides ingest's stored schema_name to the real control-plane schema
// at registration time — REAL BUG #3 in that file's own comment — so pet_store here is the same
// logical-domain surface every plain pickSchemaAndTable registrar uses, not a literal physical one).
//
// sourceId uses "_id<stamp>" rather than "_<stamp>". It once had to: the SQL name dropped an
// underscore before a digit, so an ingest table created under its registered name was read by
// another. That is fixed (compiler naming._to_pascal_case keeps the boundary); the form is kept.
// A hyphen is not an option: the "default"-schema picker's physical table name IS the source id verbatim (REQ-1745), and an unquoted hyphen in
// `CREATE TABLE IF NOT EXISTS <name>` parses as subtraction — a SQL syntax error, not a naming
// mismatch — so the source id must stay a single valid unquoted SQL identifier throughout.
export async function registerIngest(page: Page): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_ingest_id${stamp}`;

  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("ingest");
  await submitSwapSource(page, sourceId);

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, "default", sourceId);
  await expectDiscoveredColumns(page, ["ext_id"], 30000);
  // The JSON paths are set in the editor the short register step opens on the draft.
  const registered = await submitRegisterAndExpectListed(
    page,
    sourceId,
    SWAP_REGISTER_TIMEOUT_MS,
    "",
    { paths: { ext_id: "id", value: "value" } },
  );

  const res = await page.request.post(`/data/ingest/${sourceId}/${sourceId}`, {
    data: { id: "abc-1", value: "hello-ingest" },
  });
  expect(res.ok(), await res.text()).toBeTruthy();

  return {
    label: "ingest",
    sourceId,
    sql: `SELECT ext_id, value FROM pet_store.${registered} ORDER BY ext_id`,
    assertRows: (rows) => {
      expect(rows).toEqual([["abc-1", "hello-ingest"]]);
    },
    reachableOn: ["trino"],
  };
}

export async function registerOpenapi(page: Page, specUrlOverride?: string): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_openapi_${stamp}`;
  // REQ-1730 scenario 1 (test-only): see registerGraphqlRemote's identical comment.
  const specUrl = specUrlOverride ?? (await existingSourcePath(page, "petstore-api"));
  const baseUrl = specUrl.replace(/\/openapi\.json$/, "");

  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("openapi");
  await page.getByTestId("openapi-spec-path-input").fill(specUrl);
  await page.getByTestId("openapi-base-url-input").fill(baseUrl);
  await submitSwapSource(page, sourceId);

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, "openapi", "getInventory");
  const registered = await submitRegisterAndExpectListed(page, sourceId, SWAP_REGISTER_TIMEOUT_MS);

  return {
    label: "openapi",
    sourceId,
    sql: `SELECT * FROM pet_store.${registered}`,
    assertRows: (rows) => expect(rows.length).toBeGreaterThan(0),
    reachableOn: ["trino"],
  };
}

// govdata (REQ-540): AskAmerica, read through its bundled pgwire server like the SharePoint and
// Salesforce sources — DuckDB attaches the server, and Trino, which has no connector for it,
// reads a replica landed through it. A REAL external API (US government open data), no offline
// mock — same credential gate source-to-query-special-cases.spec.ts's own govdata case uses
// (FREE_ASKAMERICA_KEY in .env). The server mounts its schemas before it listens, so the
// source answers discovery as still starting until it does (waitForSourceServing).
export async function registerGovdata(page: Page): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_govdata_${stamp}`;
  const apiKey = process.env.FREE_ASKAMERICA_KEY ?? "";

  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("govdata");
  await page.getByTestId("govdata-subject-WEATHER").check();
  await page.getByTestId("govdata-api-key-input").fill(apiKey);
  await submitSwapSource(page, sourceId);
  // The server's start has its own budget, on top of the case's.
  test.setTimeout(test.info().timeout + ASKAMERICA_SERVING_BUDGET_MS);
  expect(
    await waitForSourceServing(page, sourceId, "weather", ASKAMERICA_SERVING_BUDGET_MS),
  ).toContain("nws_stations");

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, "weather", "nws_stations");
  const registered = await submitRegisterAndExpectListed(page, sourceId, SWAP_REGISTER_TIMEOUT_MS);

  return {
    label: "govdata",
    sourceId,
    sql: `SELECT * FROM pet_store.${registered} LIMIT 5`,
    assertRows: (rows) => expect(rows.length).toBeGreaterThan(0),
    reachableOn: ["trino"],
  };
}

// rss (REQ-1730): no Trino connector (strategy.py's _MATERIALIZE_ONLY) — but UNLIKE every other
// type in this set, rss lands through a background POLL job (wire_new_poll_jobs, REQ-1770), not
// synchronously on the query itself. Both DuckDB's own registration AND the Trino-side replay
// (reprovisionSourceOnEngine's createSource call) trigger _rebuild_schemas, which starts a FRESH
// poll job for whichever backend runs it — and since the feed fixture (engine-swap.spec.ts's own
// beforeAll/afterAll, port E2E_RSS_PORT) re-serves the SAME static XML on every request, a poll
// job started on Trino's backend AFTER the swap lands the identical items just fine. This is what
// makes rss tractable where websocket (a one-shot push, never replayed) is not. `pollTimeoutMs` on
// the returned Registration (requeryOnEngine, engine-swap-helpers.ts) retries the whole query
// every 5s until the poll job's first tick lands something, instead of the single attempt every
// other registration here gets.
// sourceId uses "_id<stamp>", as registerIngest's does (see its comment).
export async function registerRss(page: Page): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_rss_id${stamp}`;

  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("rss");
  await page.getByLabel(/^Host/).and(page.locator(FORM_FIELD)).fill("localhost");
  await page.getByLabel(/^Port/).and(page.locator(FORM_FIELD)).fill(String(E2E_RSS_PORT));
  await page.getByTestId("rss-use-ssl-checkbox").uncheck();
  await submitSwapSource(page, sourceId);

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, "default", sourceId);
  await expectDiscoveredColumns(page, ["id"], 30000);
  const registered = await submitRegisterAndExpectListed(page, sourceId, SWAP_REGISTER_TIMEOUT_MS);

  // Tables page: give the poll job an actual cadence (RegisterTableForm has no Cache TTL field,
  // only TableEditForm's own) — the same step source-to-query-streaming.spec.ts's own rss case
  // uses, and the reason wire_new_poll_jobs (REQ-1770) exists: saving here re-runs
  // _rebuild_schemas on an already-running runtime, which is exactly the path that needs to start
  // (or, on Trino's side after the swap, RE-start) this table's poll job.
  await page.goto("/tables");
  await page.waitForSelector(".page-header", { timeout: 15000 });
  const row = page.locator(".data-table tbody tr").filter({ hasText: sourceId }).first();
  await row.waitFor({ timeout: 15000 });
  // Click the FIRST cell, not the row's own (default-center) point: TablesPage.tsx renders an
  // actions column with its own `<Table.Td onClick={(e) => e.stopPropagation()}>` guard (so an
  // action button click doesn't also toggle the row), and a row wide enough for that column to
  // sit at the row's horizontal midpoint swallows a plain `row.click()` there — reproduced live
  // (the row locator resolves and clicks with no error, but the row never expands).
  await row.locator("td").first().click();
  const editBtn = page.getByTestId("table-read-view-edit").first();
  await editBtn.waitFor({ timeout: 10000 });
  await editBtn.click();
  // Cache TTL is on the table's "Load and timeliness" section.
  await page.getByTestId("table-section-load").click();
  await page.getByLabel(/^Cache TTL/).fill("5");
  await page.getByTestId("table-edit-save").click();
  await expect(page.getByTestId("table-edit-save")).toBeHidden({ timeout: 15000 });

  return {
    label: "rss",
    sourceId,
    sql: `SELECT id, title FROM pet_store.${registered} ORDER BY id`,
    assertRows: (rows) => {
      expect(rows).toEqual([
        ["item-1", "First item"],
        ["item-2", "Second item"],
      ]);
    },
    reachableOn: ["trino"],
    pollTimeoutMs: 120000,
  };
}

// websocket (REQ-1730): no Trino connector (strategy.py's _MATERIALIZE_ONLY) — lands through the
// SAME REQ-1733 CDC-listener mechanism as kafka (push_wiring.py's wire_push_listeners), unlike
// rss's poll cadence. Tractable for this harness for the same reason rss is: the fixture (the
// `ws` package, source-to-query-streaming.spec.ts's own pattern) sends its fixed 2-event payload
// on EVERY new connection, not just the first ever, so a fresh listener wire_push_listeners opens
// on the Trino-bound backend after the swap (triggered the same way as rss's poll job — every
// _rebuild_schemas call, including the createSource replay itself) receives the same 2 events
// again. Unlike rss, wire_push_listeners' own per-node idempotency (state.push_listener_
// disconnects) only marks a node visited on an ACTUAL successful listener start, not eagerly on
// every attempt (no wire_new_poll_jobs-style permanent-exclusion risk) — but pollTimeoutMs is
// still used since opening a live connection and landing its first events is asynchronous either
// way. CDC landing hard-requires a declared primary key (set on the draft's Columns), unlike rss.
export async function registerWebsocket(page: Page): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_ws_id${stamp}`;

  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("websocket");
  await page.getByLabel(/^Host/).and(page.locator(FORM_FIELD)).fill("localhost");
  await page.getByLabel(/^Port/).and(page.locator(FORM_FIELD)).fill(String(E2E_WS_PORT));
  await submitSwapSource(page, sourceId);

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, "default", sourceId);
  await expectDiscoveredColumns(page, ["id"], 30000);
  // The primary key is declared in the editor the short register step opens on the draft.
  const registered = await submitRegisterAndExpectListed(
    page,
    sourceId,
    SWAP_REGISTER_TIMEOUT_MS,
    "",
    { primaryKey: ["id"] },
  );

  return {
    label: "websocket",
    sourceId,
    sql: `SELECT id, value FROM pet_store.${registered} ORDER BY id`,
    assertRows: (rows) => {
      expect(rows).toEqual([
        ["ws-1", "hello"],
        ["ws-2", "world"],
      ]);
    },
    reachableOn: ["trino"],
    pollTimeoutMs: 120000,
  };
}
