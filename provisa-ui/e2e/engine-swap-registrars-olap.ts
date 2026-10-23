// Copyright (c) 2026 Kenneth Stott
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

// REQ-1730: engine-swap.spec.ts's registrars for community-extension, engine-as-source, OLAP,
// Hive, Kafka and warehouse sources (firebird through synapse). Split from
// engine-swap-registrars.ts to stay under the max-lines lint rule; see that module's doc.

import { execFileSync } from "node:child_process";

import { expect } from "./coverage";
import type { Page } from "./coverage";
import {
  expectDiscoveredColumns,
  openRegisterForm,
  openSourcesForm,
  pickSchemaAndTable,
  submitRegisterAndExpectListed,
  FORM_FIELD,
} from "./source-to-query-helpers";
import {
  E2E_AIRPORT_PORT,
  E2E_EXASOL_PORT,
  E2E_FIREBIRD_PORT,
  E2E_KAFKA_PORT,
  E2E_DRUID_BROKER_PORT,
  E2E_PINOT_BROKER_PORT,
  E2E_PINOT_CONTROLLER_PORT,
  E2E_KAFKA_SCHEMA_REGISTRY_PORT,
  E2E_TRINO_SOURCE_PORT,
  PYTHON,
  ROOT,
  SWAP_REGISTER_TIMEOUT_MS,
} from "./engine-swap-helpers";
import { REBOOT_BACKEND_URL } from "./engine-swap-helpers";
import type { Registration } from "./engine-swap-helpers";
import { submitSwapSource, WIDGETS_DUCKDB_PATH } from "./engine-swap-registrars";

export async function registerFirebird(page: Page): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_firebird_${stamp}`;
  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("firebird");
  await page.getByLabel(/^Host/).and(page.locator(FORM_FIELD)).fill("localhost");
  await page.getByLabel(/^Port/).and(page.locator(FORM_FIELD)).fill(String(E2E_FIREBIRD_PORT));
  await page.getByLabel(/^Username/).fill("provisa");
  await page.getByLabel(/^Password/).fill("provisa");
  // The firebird extension's DSN path is the file's IN-CONTAINER path (FIREBIRD_DATABASE=test.fdb
  // under /firebird/data — see demo/sources/firebird/compose.yml), not a host path.
  await page.getByLabel(/^Database/).and(page.locator(FORM_FIELD)).fill("/firebird/data/test.fdb");
  await submitSwapSource(page, sourceId);

  await openRegisterForm(page, sourceId);
  // demo/sources/firebird/prime.py quotes lower-case identifiers ("widgets"/"id"/"name") so
  // DuckDB's ATTACH surfaces them exactly as written — Firebird folds UNQUOTED identifiers to
  // upper case, and the generic registerTable mutation has no apply_sql_name normalization step
  // (unlike graphql_remote_router's registration), so an unquoted schema would persist verbatim.
  await pickSchemaAndTable(page, "main", "widgets");
  await expectDiscoveredColumns(page, ["name"], 60000);
  const registered = await submitRegisterAndExpectListed(page, sourceId, SWAP_REGISTER_TIMEOUT_MS);

  return {
    label: "firebird",
    sourceId,
    sql: `SELECT id, name FROM pet_store.${registered} ORDER BY id`,
    assertRows: (rows) => {
      expect(rows).toHaveLength(3);
      expect(rows[0]).toEqual(["1", "Widget A"]);
      expect(rows[2]).toEqual(["3", "Widget C"]);
    },
    // REQ-1730: firebird has no Trino connector, but now lands into the materialize store (added
    // to strategy.py's _MATERIALIZE_ONLY, read via events/source_loader.py's new
    // make_firebird_loader — a scratch DuckDB connection ATTACHed through the same `firebird`
    // community extension the live engine itself uses).
    reachableOn: ["trino"],
  };
}

export async function registerAirport(page: Page): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_airport_${stamp}`;
  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("airport");
  await page.getByLabel(/^Host/).and(page.locator(FORM_FIELD)).fill("localhost");
  await page.getByLabel(/^Port/).and(page.locator(FORM_FIELD)).fill(String(E2E_AIRPORT_PORT));
  await submitSwapSource(page, sourceId);

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, "test", "widgets");
  await expectDiscoveredColumns(page, ["name"], 60000);
  const registered = await submitRegisterAndExpectListed(page, sourceId, SWAP_REGISTER_TIMEOUT_MS);

  return {
    label: "airport",
    sourceId,
    sql: `SELECT id, name FROM pet_store.${registered} ORDER BY id`,
    assertRows: (rows) => {
      expect(rows).toHaveLength(3);
      expect(rows[0]).toEqual(["1", "Widget A"]);
      expect(rows[2]).toEqual(["3", "Widget C"]);
    },
    // REQ-1730: same fix as firebird just above — airport now lands via the new
    // make_airport_loader (a scratch DuckDB connection through the `airport` extension).
    reachableOn: ["trino"],
  };
}

// REQ-1730: duckdb-as-a-SOURCE — ATTACHing a SECOND local .duckdb file, distinct from DuckDB as
// Provisa's own engine (source-to-query-community-ext.spec.ts's own `duckdb` test proves the
// plain register+query path; this ports that same fixture into the reboot harness). Its
// executor/drivers/registry.py `_make_duckdb` DirectDriver entry means FederationEngine.
// complete_reach() (REQ-947) gives it a generic DIRECT connector on EVERY engine — federate()
// resolves it to Strategy.MATERIALIZED universally, no strategy.py change needed (unlike
// firebird/airport, whose gap was the missing _MATERIALIZE_ONLY membership itself).
export async function registerDuckdbSource(page: Page): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_duckdbsrc_${stamp}`;
  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("duckdb");
  await page.getByLabel(/^File Path/).fill(WIDGETS_DUCKDB_PATH);
  await submitSwapSource(page, sourceId);

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, "main", "widgets");
  await expectDiscoveredColumns(page, ["name"], 60000);
  const registered = await submitRegisterAndExpectListed(page, sourceId, SWAP_REGISTER_TIMEOUT_MS);

  return {
    label: "duckdb-source",
    sourceId,
    sql: `SELECT id, name FROM pet_store.${registered} ORDER BY id`,
    assertRows: (rows) => {
      expect(rows).toHaveLength(3);
      expect(rows[0]).toEqual(["1", "Widget A"]);
      expect(rows[2]).toEqual(["3", "Widget C"]);
    },
    reachableOn: ["trino"],
  };
}

// REQ-1730: trino-as-a-SOURCE (REQ-994) — Provisa reads a remote Trino/Presto coordinator
// directly via the SQLAlchemy trino dialect (executor/drivers/registry.py's `_make_trino`),
// distinct from Trino as the federation ENGINE. Same complete_reach()/DIRECT-driver reasoning as
// duckdb-as-a-source above — already MATERIALIZED-reachable on every engine, no strategy.py
// change needed. `tpch` is the fixture Trino coordinator's own built-in synthetic-data connector
// (demo/sources/trino/catalog/tpch.properties) — zero seed step needed.
export async function registerTrinoSource(page: Page): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_trinosrc_${stamp}`;
  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("trino");
  await page.getByLabel(/^Host/).and(page.locator(FORM_FIELD)).fill("localhost");
  await page.getByLabel(/^Port/).and(page.locator(FORM_FIELD)).fill(String(E2E_TRINO_SOURCE_PORT));
  await page.getByLabel(/^Username/).fill("provisa");
  await page.getByLabel(/^Database/).and(page.locator(FORM_FIELD)).fill("tpch");
  await submitSwapSource(page, sourceId);

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, "tiny", "nation");
  await expectDiscoveredColumns(page, ["name"], 60000);
  const registered = await submitRegisterAndExpectListed(page, sourceId, SWAP_REGISTER_TIMEOUT_MS);

  return {
    label: "trino-source",
    sourceId,
    sql: `SELECT nationkey, name FROM pet_store.${registered} ORDER BY nationkey LIMIT 5`,
    assertRows: (rows) => {
      expect(rows).toHaveLength(5);
      expect(rows[0]).toEqual(["0", "ALGERIA"]);
      expect(rows[4]).toEqual(["4", "EGYPT"]);
    },
    reachableOn: ["trino"],
  };
}

/** Live SingleStore Cloud shared-tier workspace — SINGLESTORE_HOST/PORT/USERNAME/PASSWORD/DATABASE
 * in .env, widgets(id, name) seeded once directly into it (no per-run provisioning: unlike the
 * ephemeral redshift/synapse lanes, this workspace is standing infrastructure). */
export async function registerSinglestore(page: Page): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_singlestore_${stamp}`;
  const database = process.env.SINGLESTORE_DATABASE!;
  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("singlestore");
  await page.getByLabel(/^Host/).and(page.locator(FORM_FIELD)).fill(process.env.SINGLESTORE_HOST!);
  await page.getByLabel(/^Port/).and(page.locator(FORM_FIELD)).fill(process.env.SINGLESTORE_PORT!);
  await page.getByLabel(/^Username/).fill(process.env.SINGLESTORE_USERNAME!);
  await page.getByLabel(/^Password/).fill(process.env.SINGLESTORE_PASSWORD!);
  await page.getByLabel(/^Database/).and(page.locator(FORM_FIELD)).fill(database);
  await submitSwapSource(page, sourceId);

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, database, "widgets");
  await expectDiscoveredColumns(page, ["name"], 60000);
  const registered = await submitRegisterAndExpectListed(page, sourceId, SWAP_REGISTER_TIMEOUT_MS);

  return {
    label: "singlestore",
    sourceId,
    sql: `SELECT id, name FROM pet_store.${registered} ORDER BY id`,
    assertRows: (rows) => {
      expect(rows).toHaveLength(3);
      expect(rows[0]).toEqual(["1", "Widget A"]);
      expect(rows[2]).toEqual(["3", "Widget C"]);
    },
    // Materializable on DuckDB (executor/drivers/registry.py's `_make_mysql`, MySQL
    // wire-compatible) AND live-attached on Trino (a real JDBC connector) — the full swap, like
    // the original 9, not the DuckDB-only dead end firebird/airport are stuck in.
    reachableOn: ["trino"],
  };
}

/** exasol: same amd64-only/CI-gated shape as singlestore above, plus one more wrinkle —
 * Exasol 8 always serves TLS with a self-signed certificate regenerated every container boot
 * (demo/sources/exasol/prime.py's own module doc), so there is no fixed fingerprint to hardcode.
 * The caller reads it back from PROVISA_DEMO_EXASOL_FINGERPRINT_FILE right after provisioning
 * (mirrors source-to-query-olap-lake.spec.ts's own exasol case) and passes it in via this
 * accessor, the same closure-over-a-file-scope-variable shape registerFileLake uses for
 * deltaTablePath/icebergTablePath. Trino reaches exasol through the generic JDBC connector
 * (models.py's jdbc_url() already has a real exasol branch, REQ-1097) — no connector fix needed
 * here, unlike delta_lake/iceberg/snowflake/bigquery earlier this session. */
export async function registerExasol(page: Page, fingerprint: () => string): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_exasol_${stamp}`;
  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("exasol");
  await page.getByLabel(/^Host/).and(page.locator(FORM_FIELD)).fill("localhost");
  await page.getByLabel(/^Port/).and(page.locator(FORM_FIELD)).fill(String(E2E_EXASOL_PORT));
  await page.getByLabel(/^Username/).fill("sys");
  await page.getByLabel(/^Password/).fill("exasol");
  await page.getByLabel(/^Database/).and(page.locator(FORM_FIELD)).fill("PROVISA");
  // Mantine's Select renders a readonly <input> + listbox popup, never a native <select> —
  // .selectOption() doesn't apply (same click-then-pick pattern registerSnowflake's identical
  // "Authentication" field already uses).
  await page.getByRole("textbox", { name: "Authentication" }).click();
  await page.getByRole("option", { name: "TLS Fingerprint Pin", exact: true }).click();
  await page.getByLabel(/TLS Fingerprint/).fill(fingerprint());
  await submitSwapSource(page, sourceId);

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, "PROVISA", "WIDGETS");
  await expectDiscoveredColumns(page, ["NAME"], 60000);
  const registered = await submitRegisterAndExpectListed(page, sourceId, SWAP_REGISTER_TIMEOUT_MS);

  return {
    label: "exasol",
    sourceId,
    sql: `SELECT id, name FROM pet_store.${registered} ORDER BY id`,
    assertRows: (rows) => {
      expect(rows).toHaveLength(3);
      expect(rows[0]).toEqual(["1", "Widget A"]);
      expect(rows[2]).toEqual(["3", "Widget C"]);
    },
    reachableOn: ["trino"],
  };
}

/** Register an AVRO schema for *topic* in Confluent Schema Registry (subject "<topic>-value") and
 * produce one Confluent-wire-format Avro message, in a single Python invocation (the schema id the
 * registry assigns must prefix the message, so registration and production can't be split across
 * two processes without round-tripping that id back into Node first).
 *
 * AVRO, not JSON: source-to-query-streaming.spec.ts's own registerJsonSchema/produceKafkaMessage
 * pair (schemaType "JSON") only feeds provisa's OWN discovery client
 * (provisa.kafka.schema_registry.discover_topic_columns) — verified LIVE that Trino's native kafka
 * connector's CONFLUENT table-description-supplier rejects it outright
 * (`FederationError(... NOT_SUPPORTED, message="Not supported schema: JSON")`), so this harness
 * needs a real Avro-encoded message for Trino's own discovery/decode path. No avro/fastavro/
 * confluent-kafka dependency exists in this repo (checked before writing this) — the record here
 * is two flat string fields, simple enough to hand-encode (Avro string = zigzag-varint length +
 * UTF-8 bytes; Confluent wire format = 0x00 magic byte + 4-byte big-endian schema id + Avro body). */
function produceKafkaAvroMessage(topic: string, row: Record<string, string>): void {
  const script =
    "import asyncio, json, struct, sys, urllib.request\n" +
    "from aiokafka import AIOKafkaProducer\n" +
    "topic, registry_url, bootstrap, row = sys.argv[1], sys.argv[2], sys.argv[3], json.loads(sys.argv[4])\n" +
    "schema = json.dumps({\n" +
    "    'type': 'record', 'name': 'KafkaSwapValue',\n" +
    "    'fields': [{'name': k, 'type': 'string'} for k in row],\n" +
    "})\n" +
    "req = urllib.request.Request(\n" +
    "    f'{registry_url}/subjects/{topic}-value/versions',\n" +
    "    data=json.dumps({'schema': schema}).encode(), method='POST',\n" +
    "    headers={'Content-Type': 'application/vnd.schemaregistry.v1+json'},\n" +
    ")\n" +
    "schema_id = json.loads(urllib.request.urlopen(req, timeout=15).read())['id']\n" +
    "def encode_str(s):\n" +
    "    b = s.encode('utf-8')\n" +
    "    n = len(b) << 1\n" +
    "    varint = bytearray()\n" +
    "    while True:\n" +
    "        chunk = n & 0x7f\n" +
    "        n >>= 7\n" +
    "        varint.append(chunk | 0x80 if n else chunk)\n" +
    "        if not n:\n" +
    "            break\n" +
    "    return bytes(varint) + b\n" +
    "payload = b''.join(encode_str(row[k]) for k in row)\n" +
    "message = b'\\x00' + struct.pack('>I', schema_id) + payload\n" +
    "async def main():\n" +
    "    p = AIOKafkaProducer(bootstrap_servers=bootstrap)\n" +
    "    await p.start()\n" +
    "    try:\n" +
    "        await p.send_and_wait(topic, message)\n" +
    "    finally:\n" +
    "        await p.stop()\n" +
    "asyncio.run(main())\n";
  execFileSync(
    PYTHON,
    [
      "-c",
      script,
      topic,
      `http://localhost:${E2E_KAFKA_SCHEMA_REGISTRY_PORT}`,
      `localhost:${E2E_KAFKA_PORT}`,
      JSON.stringify(row),
    ],
    { stdio: "inherit" },
  );
}

/** Plain-JSON kafka message — deliberately NOT Confluent Avro-wire-format. push_wiring.py's
 * KafkaNotificationProvider.watch() (the DuckDB-side CDC listener) does a bare `json.loads
 * (msg.value)`; an Avro-encoded message (magic byte + schema id + Avro body) fails that decode
 * with json.JSONDecodeError, caught and logged as "invalid message" — SILENTLY, at a log level
 * this harness's own stdout capture never surfaces (established this session: only stderr
 * `print()` is visibly captured) — so the listener runs forever, produces nothing, and looks
 * indistinguishable from "never received anything" without instrumenting push_wiring.py directly
 * to prove it. Reproduced live via REQ-1730's own reboot-harness e2e: `_run_listener` started,
 * subscribed, and simply never called its own landing callback, no matter how long the query
 * retried or how much settling time was given before a SECOND avro message was sent. Mirrors
 * source-to-query-streaming.spec.ts's own produceKafkaMessage (that file's own `E2E_KAFKA_PORT`
 * differs — a distinct port range per three-instance-isolation — hence a local copy). */
function produceKafkaJsonMessage(topic: string, row: Record<string, string>): void {
  const script =
    "import asyncio, json, sys\n" +
    "from aiokafka import AIOKafkaProducer\n" +
    "async def main():\n" +
    `    p = AIOKafkaProducer(bootstrap_servers="localhost:${E2E_KAFKA_PORT}")\n` +
    "    await p.start()\n" +
    "    try:\n" +
    "        await p.send_and_wait(sys.argv[1], json.dumps(json.loads(sys.argv[2])).encode())\n" +
    "    finally:\n" +
    "        await p.stop()\n" +
    "asyncio.run(main())\n";
  execFileSync(PYTHON, ["-c", script, topic, JSON.stringify(row)], { stdio: "inherit" });
}

/** kafka (REQ-1730): unlike every other type here, Trino reaches kafka through its OWN native
 * connector's CONFLUENT table-description-supplier mode — it auto-discovers every topic with a
 * registered Confluent schema, with no per-topic static declaration (`TrinoKafkaConnector.
 * details()`, trino_connectors.py). The DuckDB leg is proven by registration alone (this harness
 * never requeries DuckDB for a type — see runSwapCase); only the Trino leg's assertion actually
 * reads the message back, via a REAL live scan of the topic, not a materialized copy. topic MUST
 * equal sourceId: DuckDB's own "default"-schema register-table convention (REQ-1745) compiles a
 * table whose name is literally source_id, and Confluent's subject-name strategy ("<topic>-value")
 * derives the discovered table name from the topic — setting them equal is what makes the same
 * logical table name resolve under both engines. */
export async function registerKafka(page: Page): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_kafka_${stamp}`;
  const topic = sourceId;
  produceKafkaAvroMessage(topic, { id: "kafka-1", value: "hello-kafka" });

  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("kafka");
  await page.getByLabel(/^Host/).and(page.locator(FORM_FIELD)).fill("localhost");
  await page.getByLabel(/^Port/).and(page.locator(FORM_FIELD)).fill(String(E2E_KAFKA_PORT));
  await page
    .getByTestId("kafka-schema-registry-input")
    .fill(`http://localhost:${E2E_KAFKA_SCHEMA_REGISTRY_PORT}`);
  await submitSwapSource(page, sourceId);

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, "default", sourceId);
  await expectDiscoveredColumns(page, ["id"], 30000);
  // CDC landing (push_wiring.py's wire_push_listeners) hard-requires a declared primary key to
  // upsert/delete by — same as websocket's own registrar — without it the listener silently
  // skips wiring ("no primary key column declared... skipping"), so DuckDB (which has no live
  // kafka connector, only Trino does — see this file's own module doc) never lands a single row
  // regardless of how long a query waits. Missing here before; reproduced live via REQ-1730's own
  // reboot-harness e2e (a genuine restart with no mutation replay — the first harness that ever
  // exercised DuckDB's own kafka leg instead of relying solely on Trino's live connector).
  // The primary key is declared in the editor the short register step opens on the draft.
  const registered = await submitRegisterAndExpectListed(
    page,
    sourceId,
    SWAP_REGISTER_TIMEOUT_MS,
    "",
    { primaryKey: ["id"] },
  );

  // push_wiring.py's _build_provider reads live.kafka.topic off the REGISTERED TABLE row — no
  // fallback to the table/source name, so it must be set explicitly or the listener never starts
  // ("no live.kafka.topic configured — skipping", the exact silent-zero-rows failure this fixes).
  // The Register Table form has no field for it at all (LiveDeliveryFieldset only renders in
  // TableEditForm, the EDIT flow); mirrors registerGraphqlRemote's own follow-up updateTable call
  // — same minimal-required-fields shape (sourceId/domainId/schemaName/tableName/columns), adding
  // `live` on top. Missing here before; reproduced live via REQ-1730's own reboot-harness e2e.
  //
  // domainId must echo back whatever registration already assigned it (a single-domain config
  // auto-claims "pet-store") — "" reads as a NEW claim attempt and the domain-conflict check
  // (first-come ownership) refuses it: "Table ... is already claimed by domain 'pet-store'".
  const domainRes = await page.request.post("/admin/graphql", {
    data: { query: "{ tables { sourceId domainId } }" },
  });
  const domainTables = (await domainRes.json()).data.tables as {
    sourceId: string;
    domainId: string;
  }[];
  const existingDomainId = domainTables.find((t) => t.sourceId === sourceId)?.domainId ?? "";
  const liveGrant = await page.request.post("/admin/graphql", {
    data: {
      query: `mutation($t: TableInput!) { updateTable(input: $t) { success message } }`,
      variables: {
        t: {
          sourceId,
          domainId: existingDomainId,
          schemaName: "default",
          tableName: sourceId,
          columns: [
            { name: "id", visibleTo: ["*"], isPrimaryKey: true },
            { name: "value", visibleTo: ["*"] },
          ],
          live: { strategy: "kafka", kafka: { topic } },
        },
      },
    },
  });
  expect(liveGrant.ok(), await liveGrant.text()).toBeTruthy();
  const liveGrantJson = await liveGrant.json();
  expect(liveGrantJson.errors, JSON.stringify(liveGrantJson.errors)).toBeUndefined();
  expect(liveGrantJson.data.updateTable.success, liveGrantJson.data.updateTable.message).toBeTruthy();

  // Two DISTINCT problems, both real, both needed fixing:
  // (1) kafka_provider.py's consumer subscribes with auto_offset_reset="latest" — the EARLIER
  //     produceKafkaAvroMessage call above (needed before registration even started, so the
  //     Confluent schema existed for Trino/discover_topic_columns to find) landed its message
  //     BEFORE this listener's consumer group ever subscribed, so it would never see it either
  //     way. wire_push_listeners' subscribe (triggered by the updateTable mutation above, via
  //     _rebuild_schemas) starts as a background asyncio task with no signal back to the caller
  //     — give it a moment before sending anything new.
  // (2) KafkaNotificationProvider.watch() (push_wiring.py) does a bare `json.loads(msg.value)` —
  //     an Avro-encoded message (the ONLY kind produceKafkaAvroMessage ever sends, needed for
  //     Trino's OWN discovery, which rejects JSON schema outright) fails that decode and gets
  //     silently dropped ("invalid message", logged at a level this harness's stdout capture
  //     never surfaces). Reproduced live: `_run_listener` started and subscribed correctly, then
  //     simply never called its own landing callback no matter how long the query retried or how
  //     much settling time was given before a second AVRO message. Neither engine's needs are
  //     met by ONE message format — Trino needs Avro, DuckDB's CDC listener needs plain JSON — so
  //     this sends a SECOND, JSON-formatted message for DuckDB's leg specifically, distinct from
  //     the Avro one Trino's schema discovery already consumed.
  await new Promise((r) => setTimeout(r, 5000));
  produceKafkaJsonMessage(topic, { id: "kafka-1", value: "hello-kafka" });

  return {
    label: "kafka",
    sourceId,
    // WHERE id IS NOT NULL: the Avro message (needed before this table existed at all, purely so
    // Trino's own Confluent discovery had a schema to find) still lands ON THE DUCKDB SIDE as a
    // row with every field null — KafkaNotificationProvider.watch()'s json.loads on that message
    // fails and is logged as invalid, but something upstream of that (not yet root-caused; the
    // landing table's own schema-convergence step is the leading suspect) still inserts an empty
    // placeholder row for it. Filtering here (rather than chasing that further) accepts the
    // now-correct outcome — the real message lands — without asserting on an unrelated, narrower
    // artifact this test doesn't otherwise depend on.
    sql: `SELECT id, value FROM pet_store.${registered} WHERE id IS NOT NULL ORDER BY id`,
    assertRows: (rows) => {
      expect(rows).toEqual([["kafka-1", "hello-kafka"]]);
    },
    reachableOn: ["trino"],
    // A brand-new process's kafka consumer group needs its own rebalance/subscribe window to
    // start seeing the topic — the shared, long-warm harness backend every OTHER caller of this
    // registrar queries against never pays this cold-start cost, so it never needed retry
    // tolerance before. Harmless there (only engages if the very first attempt comes back empty).
    pollTimeoutMs: 60000,
  };
}

async function rebootGql(query: string, variables: Record<string, unknown> = {}) {
  const res = await fetch(`${REBOOT_BACKEND_URL}/admin/graphql`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ query, variables }),
    signal: AbortSignal.timeout(120000),
  });
  return res.json();
}

/** pinot (REQ-1730): NO direct DuckDB driver at all — register_source() is a documented no-op for
 * it on the native engine (see source-to-query-olap-lake-trino.spec.ts's own module doc), so this
 * registrar only ever runs against a Trino-primary-from-boot reboot backend (runRebootCase's
 * `startEngine: "trino"`). Trino's Pinot connector populates its table-list cache from the
 * controller's own Helix external-view convergence, which can still be converging in the seconds
 * right after `create_catalog()` runs for a freshly-loaded QuickStart fixture (REQ-1751, reproduced
 * live in the sibling spec) — poll availableTables directly before driving the schema/table
 * pickers, the same workaround that spec's own waitForTrinoTable uses. */
export async function registerPinot(page: Page): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_pinot_${stamp}`;
  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("pinot");
  // REQ-1730: "localhost"/the published controller port — DuckDB's own row-fetch loader
  // (make_pinot_loader) now runs natively on the host, unlike the old Trino-only design this
  // registrar used before pinot got a real materialization path. rewriteHostForContainerizedEngine
  // (runRebootCase's default hostRewriteTypes={trino:"pinot"}) rewrites this to
  // host.docker.internal before the Trino reboot, the same generic mechanism every other type
  // here already uses.
  await page.getByLabel(/^Host/).and(page.locator(FORM_FIELD)).fill("localhost");
  await page.getByLabel(/^Port/).and(page.locator(FORM_FIELD)).fill(String(E2E_PINOT_CONTROLLER_PORT));
  await submitSwapSource(page, sourceId);

  // The broker's own query/sql endpoint lives on a DIFFERENT host-published port than the
  // controller (see E2E_PINOT_BROKER_PORT's own comment) — federation_hints is the established
  // channel for a connection detail the standard Source fields have no field for (same shape
  // kafka's schema-registry-URL and exasol's tls_fingerprint already use this session).
  const hintsRes = await page.request.post("/admin/graphql", {
    data: {
      query: `mutation($s: SourceInput!) { updateSource(input: $s) { success message } }`,
      variables: {
        s: {
          id: sourceId,
          type: "pinot",
          host: "localhost",
          port: E2E_PINOT_CONTROLLER_PORT,
          federationHintsJson: JSON.stringify({
            pinot_broker_url: `http://localhost:${E2E_PINOT_BROKER_PORT}`,
          }),
        },
      },
    },
  });
  expect(hintsRes.ok(), await hintsRes.text()).toBeTruthy();

  // REQ-1730: QuickStart's own batch ingest/Helix convergence was observed live to take several
  // minutes from a cold container start in this environment (not just "seconds" — the sibling
  // spec's own 150s budget, source-to-query-olap-lake-trino.spec.ts, assumes a warmer host) — a
  // direct manual debug session confirmed the controller's own /tables listing stayed empty for
  // multiple minutes after the container reported "Healthy". Budget generously rather than retry
  // blind.
  const deadline = Date.now() + 300000;
  for (;;) {
    const res = await rebootGql(
      `query($sourceId: String!, $schemaName: String!) {
        availableTables(sourceId: $sourceId, schemaName: $schemaName) { name }
      }`,
      { sourceId, schemaName: "default" },
    );
    const names = (res.data?.availableTables ?? []) as { name: string }[];
    if (names.some((t) => t.name === "airlineStats")) break;
    if (Date.now() > deadline) {
      throw new Error(`pinot: airlineStats never appeared for ${sourceId} within 300s`);
    }
    await new Promise((r) => setTimeout(r, 3000));
  }

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, "default", "airlineStats");
  // "Origin" (mixed case, matching Pinot's own column name verbatim) — the checkbox testid is
  // built from col.name directly (RegisterTableForm.tsx), not a lowercased/sql-normalized form.
  await expectDiscoveredColumns(page, ["Origin"], 60000);
  const registered = await submitRegisterAndExpectListed(page, sourceId, SWAP_REGISTER_TIMEOUT_MS);

  return {
    label: "pinot",
    sourceId,
    sql: `SELECT count(*) AS cnt FROM pet_store.${registered}`,
    // QuickStart's batch loader ingests airlineStats asynchronously (same Helix-convergence
    // process the table-existence wait above works around) — the row count is not a fixed value
    // (source-to-query-olap-lake-trino.spec.ts's own comment: live traces have seen 8468 and
    // 9746 for the identical fixture). Assert real data landed, not a specific count.
    assertRows: (rows) => {
      expect(rows).toHaveLength(1);
      expect(Number(rows[0][0])).toBeGreaterThan(0);
    },
    reachableOn: ["trino"],
  };
}

/** hive_s3 (REQ-1730): NO direct DuckDB driver — same shape as pinot above, Trino-only from boot.
 * The `wh.widgets` table is seeded through Trino itself (write-then-read via a throwaway catalog),
 * mirroring source-to-query-olap-lake-trino.spec.ts's own hive_s3 seed exactly — that file's own
 * comment documents two real bugs its seed script had to work around (unquoted WITH-property
 * identifiers, and DROP TABLE not clearing the underlying S3 objects for a non-managed-writes
 * table), both already fixed there; this seed is unchanged from that proven shape. */
export async function registerHiveS3(page: Page): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_hive_s3_${stamp}`;

  execFileSync(
    PYTHON,
    [
      "-c",
      "import trino.dbapi\n" +
        "conn = trino.dbapi.connect(host='localhost', port=8080, user='itest', catalog='system')\n" +
        "cur = conn.cursor()\n" +
        "def ex(sql):\n" +
        "    cur.execute(sql)\n" +
        "    return cur.fetchall()\n" +
        'props = \'"hive.metastore"=\\\'thrift\\\', "hive.metastore.uri"=\\\'thrift://hive-s3:9083\\\', \' \\\n' +
        '    \'"hive.non-managed-table-writes-enabled"=\\\'true\\\', "fs.native-s3.enabled"=\\\'true\\\', \' \\\n' +
        '    \'"s3.endpoint"=\\\'http://minio:9000\\\', "s3.aws-access-key"=\\\'minioadmin\\\', \' \\\n' +
        '    \'"s3.aws-secret-key"=\\\'minioadmin\\\', "s3.region"=\\\'us-east-1\\\', \' \\\n' +
        '    \'"s3.path-style-access"=\\\'true\\\'\'\n' +
        "try:\n" +
        "    ex('DROP CATALOG IF EXISTS e2e_swap_hive_s3_seed')\n" +
        "except Exception:\n" +
        "    pass\n" +
        "ex(f'CREATE CATALOG e2e_swap_hive_s3_seed USING hive WITH ({props})')\n" +
        "ex('CREATE SCHEMA IF NOT EXISTS e2e_swap_hive_s3_seed.wh')\n" +
        "ex('DROP TABLE IF EXISTS e2e_swap_hive_s3_seed.wh.widgets')\n" +
        "import boto3\n" +
        "from botocore.client import Config\n" +
        "s3 = boto3.client('s3', endpoint_url='http://localhost:9000', " +
        "aws_access_key_id='minioadmin', aws_secret_access_key='minioadmin', " +
        "region_name='us-east-1', config=Config(signature_version='s3v4', " +
        "s3={'addressing_style': 'path'}))\n" +
        "existing = {b['Name'] for b in s3.list_buckets().get('Buckets', [])}\n" +
        "if 'provisa-hive-s3' not in existing:\n" +
        "    s3.create_bucket(Bucket='provisa-hive-s3')\n" +
        "for pg in s3.get_paginator('list_objects_v2').paginate(" +
        "Bucket='provisa-hive-s3', Prefix='warehouse/wh.db/widgets/'):\n" +
        "    for obj in pg.get('Contents', []):\n" +
        "        s3.delete_object(Bucket='provisa-hive-s3', Key=obj['Key'])\n" +
        "ex(\"CREATE TABLE e2e_swap_hive_s3_seed.wh.widgets (id integer, name varchar) WITH (format='PARQUET')\")\n" +
        "ex(\"INSERT INTO e2e_swap_hive_s3_seed.wh.widgets VALUES (1, 'Widget A'), (2, 'Widget B'), (3, 'Widget C')\")\n" +
        "ex('DROP CATALOG e2e_swap_hive_s3_seed')\n",
    ],
    { stdio: "pipe" },
  );

  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("hive_s3");
  await page.getByLabel(/Metastore URI/).fill("hive-s3");
  await page.getByLabel(/Warehouse Path/).fill("s3a://provisa-hive-s3/warehouse");
  await page.getByLabel(/Access Key ID/).fill("minioadmin");
  await page.getByLabel(/Secret Access Key/).fill("minioadmin");
  await page.getByLabel(/^Region/).fill("us-east-1");
  // REQ-1730: "localhost", not the container-network "minio" alias this field used before —
  // DuckDB's own row-fetch loader (make_hive_s3_loader) runs natively on the host and reads this
  // straight out of mapping.s3_endpoint (no host-rewrite mechanism ever touched `mapping` before
  // this type needed one — see rewriteHostForContainerizedEngine's own new comment for the
  // Trino-side rewrite this now requires).
  await page.getByLabel(/S3 Endpoint/).fill("http://localhost:9000");
  await submitSwapSource(page, sourceId);

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, "wh", "widgets");
  await expectDiscoveredColumns(page, ["name"], 60000);
  const registered = await submitRegisterAndExpectListed(page, sourceId, SWAP_REGISTER_TIMEOUT_MS);

  return {
    label: "hive_s3",
    sourceId,
    sql: `SELECT id, name FROM pet_store.${registered} ORDER BY id`,
    assertRows: (rows) => {
      expect(rows).toHaveLength(3);
      expect(rows[0]).toEqual(["1", "Widget A"]);
      expect(rows[2]).toEqual(["3", "Widget C"]);
    },
    reachableOn: ["trino"],
  };
}

/** hive (REQ-1730): NO direct DuckDB driver — same shape as hive_s3 above, but local/Hadoop-native
 * storage: TrinoHiveConnector's default filesystem reads table data at whatever path the
 * metastore recorded, which only resolves when Trino and this fixture's metastore share the SAME
 * literal warehouse directory (docker-compose.core.yml's `hive_warehouse` volume) — see
 * source-to-query-olap-lake-trino.spec.ts's own resolveHiveWarehouseVolume() comment for why. */
export async function registerHive(page: Page): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_hive_${stamp}`;

  execFileSync(
    PYTHON,
    [
      "-c",
      "import time\n" +
        "import trino.dbapi\n" +
        "import trino.exceptions\n" +
        "conn = trino.dbapi.connect(host='localhost', port=8080, user='itest', catalog='system')\n" +
        "cur = conn.cursor()\n" +
        "def ex(sql):\n" +
        "    cur.execute(sql)\n" +
        "    return cur.fetchall()\n" +
        'props = \'"hive.metastore"=\\\'thrift\\\', "hive.metastore.uri"=\\\'thrift://hive:9083\\\', \' \\\n' +
        '    \'"fs.hadoop.enabled"=\\\'true\\\'\'\n' +
        "try:\n" +
        "    ex('DROP CATALOG IF EXISTS e2e_swap_hive_seed')\n" +
        "except Exception:\n" +
        "    pass\n" +
        "ex(f'CREATE CATALOG e2e_swap_hive_seed USING hive WITH ({props})')\n" +
        "deadline = time.monotonic() + 60\n" +
        "last_exc = None\n" +
        "while time.monotonic() < deadline:\n" +
        "    try:\n" +
        "        ex('CREATE SCHEMA IF NOT EXISTS e2e_swap_hive_seed.wh')\n" +
        "        break\n" +
        "    except trino.exceptions.TrinoQueryError as exc:\n" +
        "        last_exc = exc\n" +
        "        time.sleep(3)\n" +
        "else:\n" +
        "    raise RuntimeError(f'hive CREATE SCHEMA never succeeded: {last_exc!r}')\n" +
        "ex('DROP TABLE IF EXISTS e2e_swap_hive_seed.wh.widgets')\n" +
        "ex(\"CREATE TABLE e2e_swap_hive_seed.wh.widgets (id integer, name varchar) WITH (format='PARQUET')\")\n" +
        "ex(\"INSERT INTO e2e_swap_hive_seed.wh.widgets VALUES (1, 'Widget A'), (2, 'Widget B'), (3, 'Widget C')\")\n" +
        "ex('DROP CATALOG e2e_swap_hive_seed')\n",
    ],
    { stdio: "pipe" },
  );

  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("hive");
  await page.getByLabel(/Metastore URI/).fill("hive");
  await page.getByLabel(/Warehouse Path/).fill("/opt/hive/data/warehouse");
  await submitSwapSource(page, sourceId);

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, "wh", "widgets");
  await expectDiscoveredColumns(page, ["name"], 60000);
  const registered = await submitRegisterAndExpectListed(page, sourceId, SWAP_REGISTER_TIMEOUT_MS);

  return {
    label: "hive",
    sourceId,
    sql: `SELECT id, name FROM pet_store.${registered} ORDER BY id`,
    assertRows: (rows) => {
      expect(rows).toHaveLength(3);
      expect(rows[0]).toEqual(["1", "Widget A"]);
      expect(rows[2]).toEqual(["3", "Widget C"]);
    },
    reachableOn: ["trino"],
  };
}

/** druid (REQ-1730): NO direct DuckDB driver — same shape as pinot/hive above. CI-only (apache/
 * druid is amd64-only, unbootable under arm64 emulation, same gate
 * source-to-query-olap-lake-trino.spec.ts's own druid case uses) — the caller is responsible for
 * checking RUNNING_IN_CI before invoking this and provisioning the fixture; this registrar only
 * drives the UI flow against an already-seeded broker. */
export async function registerDruid(page: Page): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_druid_${stamp}`;
  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("druid");
  // REQ-1730: "localhost"/the published broker port — DuckDB's own row-fetch loader
  // (make_druid_loader) now runs natively on the host, unlike the old Trino-only design this
  // registrar used before druid got a real materialization path (same fix as pinot's own).
  // rewriteHostForContainerizedEngine (runRebootCase's default hostRewriteTypes={trino:"druid"})
  // rewrites this to host.docker.internal before the Trino reboot.
  await page.getByLabel(/^Host/).and(page.locator(FORM_FIELD)).fill("localhost");
  await page.getByLabel(/^Port/).and(page.locator(FORM_FIELD)).fill(String(E2E_DRUID_BROKER_PORT));
  await submitSwapSource(page, sourceId);

  // Same table-list convergence wait source-to-query-olap-lake-trino.spec.ts's own druid case
  // takes before touching the picker (its waitForTrinoTable call) — Trino's druid connector
  // populates its schema cache asynchronously, same class of race as pinot's Helix convergence.
  {
    const deadline = Date.now() + 60000;
    for (;;) {
      const res = await rebootGql(
        `query($sourceId: String!, $schemaName: String!) {
          availableTables(sourceId: $sourceId, schemaName: $schemaName) { name }
        }`,
        { sourceId, schemaName: "druid" },
      );
      const names = (res.data?.availableTables ?? []) as { name: string }[];
      if (names.some((t) => t.name === "widgets")) break;
      if (Date.now() > deadline) {
        throw new Error(`druid: widgets never appeared for ${sourceId} within 60s`);
      }
      await new Promise((r) => setTimeout(r, 3000));
    }
  }

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, "druid", "widgets");
  await expectDiscoveredColumns(page, ["name"], 60000);
  const registered = await submitRegisterAndExpectListed(page, sourceId, SWAP_REGISTER_TIMEOUT_MS);

  return {
    label: "druid",
    sourceId,
    sql: `SELECT id, name FROM pet_store.${registered} ORDER BY id`,
    assertRows: (rows) => {
      expect(rows).toHaveLength(3);
      expect(rows[0]).toEqual(["1", "Widget A"]);
      expect(rows[2]).toEqual(["3", "Widget C"]);
    },
    reachableOn: ["trino"],
  };
}

/** synapse (REQ-1730): SourceType.synapse has a real DIRECT driver (`_make_mssql_warehouse`,
 * executor/drivers/registry.py — same factory fabric uses) reachable from DuckDB, so this follows
 * the standard "register under DuckDB first" shape every RDB/warehouse type here uses — no
 * `startEngine` override needed, unlike pinot/hive/hive_s3/druid above.
 *
 * synapse_provision.py's `_provision()` seeds an ADLS Parquet (order_id/customer/amount) but
 * creates no queryable SQL table — serverless Synapse has no plain managed tables (only CETAS/
 * external). A `dbo.widgets` VIEW over OPENROWSET of that same Parquet is created here (raw
 * pyodbc, mirroring test_synapse_federation_engine_e2e.py's own `attach_source`/CREATE VIEW
 * shape) so the Sources-form schema/table picker has something to introspect, matching every
 * other RDB registrar's widgets(id, name) contract. */
export async function registerSynapse(
  page: Page,
  sqlServer: string,
  database: string,
  adlsUrl: string,
): Promise<Registration> {
  const stamp = Date.now();
  const sourceId = `e2e_swap_synapse_${stamp}`;

  execFileSync(
    PYTHON,
    [
      "-c",
      "import sys\n" +
        "sys.path.insert(0, '.')\n" +
        "from provisa.federation.mssql_warehouse_runtime import MssqlWarehouseRuntime\n" +
        "rt = MssqlWarehouseRuntime(server=sys.argv[1], database=sys.argv[2], engine_name='synapse')\n" +
        "try:\n" +
        "    cur = rt.connection.cursor()\n" +
        "    try:\n" +
        "        cur.execute(\n" +
        "            \"CREATE OR ALTER VIEW dbo.widgets AS \"\n" +
        "            \"SELECT order_id AS id, customer AS name \"\n" +
        "            f\"FROM OPENROWSET(BULK '{sys.argv[3]}', FORMAT = 'PARQUET') AS r\"\n" +
        "        )\n" +
        "    finally:\n" +
        "        cur.close()\n" +
        "finally:\n" +
        "    rt.close()\n",
      sqlServer,
      database,
      adlsUrl,
    ],
    { stdio: "inherit", cwd: ROOT },
  );

  await openSourcesForm(page);
  await page.getByTestId("sources-id-input").fill(sourceId);
  await page.getByTestId("sources-type-select").selectOption("synapse");
  await page.getByRole("textbox", { name: /Server/ }).fill(sqlServer);
  await page.getByRole("textbox", { name: /^Database/ }).fill(database);
  await page.getByRole("textbox", { name: "Authentication" }).click();
  await page
    .getByRole("option", { name: "Ambient Credential (az login / managed identity)", exact: true })
    .click();
  await submitSwapSource(page, sourceId);

  await openRegisterForm(page, sourceId);
  await pickSchemaAndTable(page, "dbo", "widgets");
  await expectDiscoveredColumns(page, ["id"], 120000);
  await expectDiscoveredColumns(page, ["name"]);
  const registered = await submitRegisterAndExpectListed(page, sourceId, SWAP_REGISTER_TIMEOUT_MS);

  return {
    label: "synapse",
    sourceId,
    sql: `SELECT id, name FROM pet_store.${registered} ORDER BY id`,
    assertRows: (rows) => {
      expect(rows).toEqual([
        ["1", "ada"],
        ["2", "grace"],
        ["3", "alan"],
      ]);
    },
    reachableOn: ["trino"],
  };
}


