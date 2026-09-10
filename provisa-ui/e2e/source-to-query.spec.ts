// Copyright (c) 2026 Kenneth Stott
// Canary: b3aa630c-b223-4716-be3d-bb17006a16fb
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1671: a source configured through the UI serves a query — per source type, on a live
// instance. Three screens, no API shortcuts: the Sources form creates the source, the Register
// Table form registers a table on it, and the SQL page runs a SELECT against the registered name
// and shows the rows the seed put in the source. The containers come from demo/sources/<name>
// (see demo-source-containers.ts), so the seed and the expected rows are defined once for the
// demo flag and this test.
//
// Why this exists: the integration harness drives createSource/registerTable as GraphQL mutations
// and proves the pipeline; nothing before this drove the forms. The Neo4j form had no path to a
// table at all until REQ-1670, and no test could have said so.

import path from "node:path";
import { fileURLToPath } from "node:url";
import { test, expect } from "./coverage";
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
  openRegisterForm,
  openSourcesForm,
  pickSchemaAndTable,
  runSqlOnPage,
  submitRegisterAndExpectListed,
  submitSourceAndExpectListed,
} from "./source-to-query-helpers";

test.describe("source to query through the UI (REQ-1671)", () => {
  test("neo4j: add the source, register a Cypher table, query it on the SQL page", async ({
    page,
  }) => {
    test.setTimeout(300000);
    const stamp = Date.now();
    const sourceId = `e2e_neo4j_${stamp}`;
    const tableName = `adopter_${stamp}`;

    // 1. Sources form
    await openSourcesForm(page);
    await page.getByTestId("sources-id-input").fill(sourceId);
    await page.getByTestId("sources-type-select").selectOption("neo4j");
    await page.getByTestId("neo4j-host-input").fill("localhost");
    await page.getByTestId("neo4j-port-input").fill(String(E2E_NEO4J_HTTP_PORT));
    await page.getByTestId("neo4j-database-input").fill("neo4j");
    await submitSourceAndExpectListed(page, sourceId);

    // 2. Register Table form: name + Cypher, preview, submit
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
    await expect(page.getByTestId("register-table-col-datatype-adopter_id")).toHaveValue("integer");
    await expect(page.getByTestId("register-table-col-datatype-name")).toHaveValue("text");
    const registered = await submitRegisterAndExpectListed(page, sourceId);

    // 3. SQL page: the seed graph (demo/sources/neo4j/seed.cypher) comes back through the table
    const rows = await runSqlOnPage(
      page,
      `SELECT adopter_id, name, city FROM pet_store.${registered} ORDER BY adopter_id`,
    );
    expect(rows).toHaveLength(8);
    expect(rows[0]).toEqual(["1", "Sara Kim", "Portland"]);
    expect(rows[7]).toEqual(["8", "Mark Torres", "Tacoma"]);
  });

  test("mongodb: add the source, register a collection, query it on the SQL page", async ({
    page,
  }) => {
    test.setTimeout(300000);
    const stamp = Date.now();
    const sourceId = `e2e_mongodb_${stamp}`;
    const tableName = "product_reviews";

    // 1. Sources form (host/port/database are the shared RDBMS-style inputs)
    await openSourcesForm(page);
    await page.getByTestId("sources-id-input").fill(sourceId);
    await page.getByTestId("sources-type-select").selectOption("mongodb");
    await page.getByLabel(/^Host/).fill("localhost");
    await page.getByLabel(/^Port/).fill(String(E2E_MONGO_PORT));
    await page.getByLabel(/^Database/).fill("provisa");
    await submitSourceAndExpectListed(page, sourceId);

    // 2. Register Table form: the engine's introspection lists the collection as a table
    await openRegisterForm(page, sourceId);
    await pickSchemaAndTable(page, "provisa", tableName);
    // Columns and their types come from the engine's view of the collection (db/mongo-init.js
    // seeds the _schema collection the connector types them from).
    await expect(page.getByTestId("register-table-col-selected-reviewer")).toBeVisible({
      timeout: 60000,
    });
    await expect(page.getByTestId("register-table-col-selected-rating")).toBeVisible();
    const registered = await submitRegisterAndExpectListed(page, sourceId);

    // 3. SQL page: the ten reviews db/mongo-init.js seeds
    const rows = await runSqlOnPage(
      page,
      `SELECT product_id, reviewer, rating FROM pet_store.${registered} ORDER BY reviewer`,
    );
    expect(rows).toHaveLength(10);
    expect(rows[0]).toEqual(["1", "alice", "5"]);
    expect(rows[9]).toEqual(["7", "jack", "1"]);
  });

  test("elasticsearch: add the source, register an index, query it on the SQL page", async ({
    page,
  }) => {
    test.setTimeout(300000);
    const stamp = Date.now();
    const sourceId = `e2e_es_${stamp}`;
    const tableName = "support_tickets";

    // 1. Sources form — read over HTTP by the native engine (REQ-1672)
    await openSourcesForm(page);
    await page.getByTestId("sources-id-input").fill(sourceId);
    await page.getByTestId("sources-type-select").selectOption("elasticsearch");
    await page.getByLabel(/^Host/).fill("localhost");
    await page.getByLabel(/^Port/).fill(String(E2E_ES_PORT));
    await submitSourceAndExpectListed(page, sourceId);

    // 2. Register Table form — the live indices list under "default"; columns come from the mapping
    await openRegisterForm(page, sourceId);
    await pickSchemaAndTable(page, "default", tableName);
    await expect(page.getByTestId("register-table-col-selected-ticket_id")).toBeVisible({
      timeout: 60000,
    });
    await expect(page.getByTestId("register-table-col-selected-status")).toBeVisible();
    const registered = await submitRegisterAndExpectListed(page, sourceId);

    // 3. SQL page: the six tickets demo/sources/elasticsearch/prime.py loads
    const rows = await runSqlOnPage(
      page,
      `SELECT ticket_id, status, priority FROM pet_store.${registered} ORDER BY ticket_id`,
    );
    expect(rows).toHaveLength(6);
    expect(rows[0]).toEqual(["T-1001", "open", "2"]);
    expect(rows[5]).toEqual(["T-1006", "open", "2"]);
  });

  test("redis: add the source, register a key prefix, query it on the SQL page", async ({
    page,
  }) => {
    test.setTimeout(300000);
    const stamp = Date.now();
    const sourceId = `e2e_redis_${stamp}`;
    const tableName = "support_agent";

    // 1. Sources form — read natively over redis-py (REQ-1675)
    await openSourcesForm(page);
    await page.getByTestId("sources-id-input").fill(sourceId);
    await page.getByTestId("sources-type-select").selectOption("redis");
    await page.getByLabel(/^Host/).fill("localhost");
    await page.getByLabel(/^Port/).fill(String(E2E_REDIS_PORT));
    await page.getByLabel(/^Database/).fill("0"); // the form requires one; Redis's db index
    await submitSourceAndExpectListed(page, sourceId);

    // 2. Register Table form — the key prefixes list under "default"; columns are the hash fields
    await openRegisterForm(page, sourceId);
    await pickSchemaAndTable(page, "default", tableName);
    await expect(page.getByTestId("register-table-col-selected-name")).toBeVisible({
      timeout: 60000,
    });
    await expect(page.getByTestId("register-table-col-selected-team")).toBeVisible();
    const registered = await submitRegisterAndExpectListed(page, sourceId);

    // 3. SQL page: the five agents demo/sources/redis/prime.py writes
    const rows = await runSqlOnPage(
      page,
      `SELECT agent_id, name, team FROM pet_store.${registered} ORDER BY agent_id`,
    );
    expect(rows).toHaveLength(5);
    expect(rows[0]).toEqual(["1", "Ann Lee", "tier1"]);
    expect(rows[4]).toEqual(["5", "Eli Stone", "escalations"]);
  });

  test("cassandra: add the source, register a keyspace table, query it on the SQL page", async ({
    page,
  }) => {
    test.setTimeout(300000);
    const stamp = Date.now();
    const sourceId = `e2e_cassandra_${stamp}`;
    const tableName = "intake_events";

    // 1. Sources form — read natively over CQL (REQ-1676)
    await openSourcesForm(page);
    await page.getByTestId("sources-id-input").fill(sourceId);
    await page.getByTestId("sources-type-select").selectOption("cassandra");
    await page.getByLabel(/^Host/).fill("localhost");
    await page.getByLabel(/^Port/).fill(String(E2E_CASSANDRA_PORT));
    await page.getByLabel(/^Database/).fill("shelter_ops"); // the form requires one; the keyspace
    await submitSourceAndExpectListed(page, sourceId);

    // 2. Register Table form — keyspaces are the schemas; columns come from the cluster metadata
    await openRegisterForm(page, sourceId);
    await pickSchemaAndTable(page, "shelter_ops", tableName);
    await expect(page.getByTestId("register-table-col-selected-event_id")).toBeVisible({
      timeout: 60000,
    });
    await expect(page.getByTestId("register-table-col-selected-animal_name")).toBeVisible();
    const registered = await submitRegisterAndExpectListed(page, sourceId);

    // 3. SQL page: the seven events demo/sources/cassandra/prime.py writes
    const rows = await runSqlOnPage(
      page,
      `SELECT event_id, event_type, animal_name FROM pet_store.${registered} ORDER BY event_id`,
    );
    expect(rows).toHaveLength(7);
    expect(rows[0]).toEqual(["1", "intake", "Buddy"]);
    expect(rows[6]).toEqual(["7", "adoption", "Mittens"]);
  });

  test("sparql: add the source, register a SELECT projection, query it on the SQL page", async ({
    page,
  }) => {
    test.setTimeout(300000);
    const stamp = Date.now();
    const sourceId = `e2e_sparql_${stamp}`;
    const tableName = `volunteer_${stamp}`;

    // 1. Sources form — the endpoint URL is the source (REQ-1683)
    await openSourcesForm(page);
    await page.getByTestId("sources-id-input").fill(sourceId);
    await page.getByTestId("sources-type-select").selectOption("sparql");
    await page
      .getByTestId("sparql-endpoint-input")
      .fill(`http://localhost:${E2E_SPARQL_PORT}/provisa/query`);
    await submitSourceAndExpectListed(page, sourceId);

    // 2. Register Table form: name + SPARQL, preview, submit
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
    await expect(page.getByTestId("register-table-col-datatype-name")).toHaveValue("text");
    const registered = await submitRegisterAndExpectListed(page, sourceId);

    // 3. SQL page: the six volunteers demo/sources/sparql/prime.py loads
    const rows = await runSqlOnPage(
      page,
      `SELECT volunteer_id, name, program FROM pet_store.${registered} ORDER BY volunteer_id`,
    );
    expect(rows).toHaveLength(6);
    expect(rows[0]).toEqual(["V-01", "Grace Hall", "adoption"]);
    expect(rows[5]).toEqual(["V-06", "Noah Bryce", "fostering"]);
  });

  test("prometheus: add the source, register a metric, query it on the SQL page", async ({
    page,
  }) => {
    test.setTimeout(300000);
    const stamp = Date.now();
    const sourceId = `e2e_prometheus_${stamp}`;
    const tableName = "up";

    // 1. Sources form — the server URL is the source (REQ-1689)
    await openSourcesForm(page);
    await page.getByTestId("sources-id-input").fill(sourceId);
    await page.getByTestId("sources-type-select").selectOption("prometheus");
    await page.getByTestId("prometheus-url-input").fill(`http://localhost:${E2E_PROMETHEUS_PORT}`);
    await submitSourceAndExpectListed(page, sourceId);

    // 2. Register Table form — the metrics list under "default"; columns come from the labels
    await openRegisterForm(page, sourceId);
    await pickSchemaAndTable(page, "default", tableName);
    await expect(page.getByTestId("register-table-col-selected-job")).toBeVisible({
      timeout: 60000,
    });
    await expect(page.getByTestId("register-table-col-selected-value")).toBeVisible();
    const registered = await submitRegisterAndExpectListed(page, sourceId);

    // 3. SQL page: the server scrapes itself, so `up` has one series, job="prometheus", value 1.
    // Sample counts grow with time; the assertion is per series.
    const rows = await runSqlOnPage(
      page,
      `SELECT job, CAST(MAX(value) AS INTEGER) AS healthy FROM pet_store.${registered} GROUP BY job ORDER BY job`,
    );
    expect(rows).toEqual([["prometheus", "1"]]);
  });

  // SharePoint is the one case with no container to seed: the source is a real Microsoft 365 site,
  // reached read-only with the certificate-auth app registration in the root .env (playwright.config
  // loads it into process.env). On this lane the engine is DuckDB, which reaches SharePoint through
  // the connector's bundled Calcite pgwire server (REQ-1690) rather than any engine-native
  // connector, so this case is the UI-level proof of that path.
  //
  // The site's built-in `Documents` library is the only list that exists on every SharePoint site,
  // so it is what gets registered — and the test never writes to the tenant. The library may hold
  // zero documents, so the query asserts the shape of the result, not a row count.
  test("sharepoint: add the source, register a list, query it on the SQL page", async ({
    page,
  }) => {
    test.skip(
      !process.env.SP_SITE_URL,
      "no live SharePoint credentials: set the SP_* block in the root .env",
    );
    test.setTimeout(300000);
    const stamp = Date.now();
    const sourceId = `e2e_sharepoint_${stamp}`;
    const tableName = "documents";
    // The Calcite pgwire server runs with its bundle directory as cwd, so a relative
    // SP_CERT_PATH (.env authors it as ./sharepoint.pfx) has to be resolved here (REQ-1693).
    const certPath = path.resolve(
      path.dirname(fileURLToPath(import.meta.url)),
      "..",
      "..",
      process.env.SP_CERT_PATH!,
    );

    // 1. Sources form — site URL + tenant, then the certificate-auth fields
    await openSourcesForm(page);
    await page.getByTestId("sources-id-input").fill(sourceId);
    await page.getByTestId("sources-type-select").selectOption("sharepoint");
    await page.getByTestId("sharepoint-site-url-input").fill(process.env.SP_SITE_URL!);
    await page.getByTestId("sharepoint-tenant-id-input").fill(process.env.SP_TENANT_ID!);
    await page.getByTestId("sharepoint-auth-type-select").click();
    await page.getByRole("option", { name: "Certificate", exact: true }).click();
    await page.getByTestId("sharepoint-client-id-input").fill(process.env.SP_CLIENT_ID!);
    await page.getByTestId("sharepoint-cert-path-input").fill(certPath);
    await page
      .getByTestId("sharepoint-cert-password-input")
      .fill(process.env.SP_CERT_PASSWORD ?? "");
    await submitSourceAndExpectListed(page, sourceId);

    // 2. Register Table form — the connector exposes the site's lists as tables under a schema
    // named for the sql-normalized source id (pgwire_replica.schema_name).
    await openRegisterForm(page, sourceId);
    await pickSchemaAndTable(page, sourceId, tableName);
    await expect(page.getByTestId("register-table-col-selected-id")).toBeVisible({
      timeout: 120000,
    });
    await expect(page.getByTestId("register-table-col-selected-title")).toBeVisible();
    const registered = await submitRegisterAndExpectListed(page, sourceId);

    // 3. SQL page — a live read of the document library. The library's contents are the tenant's
    // and may legitimately be empty, so the aggregate's SHAPE is the assertion: one row, one cell,
    // a non-negative integer that only a completed round trip to SharePoint can produce.
    const rows = await runSqlOnPage(
      page,
      `SELECT COUNT(*) AS document_count FROM pet_store.${registered}`,
    );
    expect(rows).toHaveLength(1);
    expect(rows[0]).toHaveLength(1);
    expect(Number(rows[0][0])).toBeGreaterThanOrEqual(0);
  });
});
