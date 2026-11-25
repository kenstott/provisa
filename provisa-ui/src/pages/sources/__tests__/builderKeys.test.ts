// Copyright (c) 2026 Kenneth Stott
// Canary: 3b8e6f21-9a4d-4c7e-8f15-6d2a0c9e7b43
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2114 (amended 2026-11-24): each source type's settings are defined once, in the server's
// descriptor (provisa/api/admin/source_types.py, src/generated/sourceTypes.json). Every form
// builder is run here with every field it reads filled, in each of its choices, and fails when it
// writes a mapping or hint key the descriptor does not list for that type.

import { describe, expect, it } from "vitest";

import type { Connector } from "../../../api/connectors";
import { athenaHints } from "../athena";
import { CLOUDOPS_CLOUDS, cloudopsMappingJson } from "../cloudops";
import { codeMappingJson } from "../codeSource";
import { connectorMappingJson, requiredField } from "../connector";
import { cosmosHints, COSMOS_ENTRA, COSMOS_KEY } from "../cosmosdb";
import { hintKeys, mappingKeys } from "../descriptor";
import { filesMappingJson, withExcelHints } from "../excelHints";
import { gwMapping } from "../googleWorkspace";
import {
  ICEBERG_CATALOG_FIELDS,
  ICEBERG_CATALOG_TYPES,
  icebergCatalogHints,
} from "../icebergCatalog";
import { jdbcSourceHints, JDBC_CLASS_KEY, JDBC_JAR_KEY } from "../jdbcSource";
import { KAFKA_SECURITY_PROTOCOLS, kafkaHints } from "../kafka";
import { LOGS_REGEX, logsMapping } from "../logs";
import { m365Mapping } from "../microsoft365";
import { OBJECT_STORE_HINT_KEYS, objectStoreHints, type ObjectStore } from "../objectStoreHints";
import { PROFILER_DEFAULTS, profilerMappingJson } from "../profilerMapping";
import { qdrantHints } from "../qdrant";
import { quickbooksMappingJson } from "../quickbooks";
import {
  SALESFORCE_AUTH_FIELDS,
  SALESFORCE_AUTH_TYPES,
  salesforceMappingJson,
} from "../salesforce";
import { sparqlMappingJson } from "../sparqlReasoner";
import { wikidataHints } from "../wikidata";
import { wikipediaHints } from "../wikipedia";
import { snowflakeHints } from "../snowflake";
import { databricksHints, REACH_SCAN } from "../databricks";
import { bigqueryHints, BIGQUERY_SERVICE_ACCOUNT } from "../bigquery";
import { exasolHints, EXASOL_TLS_FINGERPRINT } from "../exasol";
import { sqlserverHints, SQLSERVER_TRUST_CERTIFICATE } from "../sqlserver";
import { MSSQL_WAREHOUSES, mssqlWarehouseHints, SERVICE_PRINCIPAL } from "../mssqlWarehouse";
import { hiveserver2Hints } from "../hiveserver2";
import { websocketHints } from "../websocket";
import { rssHints } from "../rss";
import { sparqlHints } from "../sparqlGraph";
import { SHAREPOINT_AUTH_TYPES, sharepointMapping } from "../sharepoint";
import { splunkMapping } from "../splunk";
import { googleSheetsMapping } from "../googleSheets";
import { HIVE, HIVE_S3, hiveMapping, hiveSeed } from "../hiveStorage";
import { fieldsWhen } from "../descriptor";
import { seedSource } from "../sourceSeed";
import type { Source } from "../../../types/admin";

/** Whether the descriptor lists `key`: by name, as a part of a dotted name (`settings` of
 * `settings.cluster`), or under a placeholder (`capabilities.duckdb` of `capabilities.<engine>`). */
function listed(keys: string[], key: string): boolean {
  return keys.some(
    (name) =>
      name === key ||
      name.startsWith(`${key}.`) ||
      new RegExp(`^${name.replace(/[.]/g, "\\.").replace(/<[^>]+>/g, "[^.]+")}$`).test(key),
  );
}

function expectListed(keys: string[], written: Record<string, unknown>, what: string) {
  expect(Object.keys(written).length, `${what} writes nothing`).toBeGreaterThan(0);
  for (const key of Object.keys(written)) {
    expect(listed(keys, key), `${what} writes ${key}, which the descriptor does not list`).toBe(
      true,
    );
  }
}

const mapping = (type: string, json: string | Record<string, unknown>, what = type) =>
  expectListed(
    mappingKeys(type),
    typeof json === "string" ? (JSON.parse(json) as Record<string, unknown>) : json,
    what,
  );
const hints = (type: string, written: Record<string, unknown>, what = type) =>
  expectListed(hintKeys(type), written, what);

/** Every key filled with a value of its own. */
const filled = (keys: Iterable<string>, extra: Record<string, string> = {}) => ({
  ...Object.fromEntries([...keys].map((k) => [k, `v-${k}`])),
  ...extra,
});

describe("each builder writes only the descriptor's keys", () => {
  it("athena", () => {
    hints("athena", athenaHints(filled(hintKeys("athena"))));
  });

  it("an object store's credentials, on every file type read from one", () => {
    for (const type of ["csv", "parquet", "sqlite", "duckdb"])
      for (const store of Object.keys(OBJECT_STORE_HINT_KEYS) as ObjectStore[])
        hints(
          type,
          objectStoreHints(store, filled(OBJECT_STORE_HINT_KEYS[store])),
          `${type} ${store}`,
        );
  });

  it("an iceberg catalog, of every type", () => {
    for (const type of ICEBERG_CATALOG_TYPES) {
      const keys = ["iceberg_table_id", ...ICEBERG_CATALOG_FIELDS[type].map((f) => f.key)];
      hints("iceberg", icebergCatalogHints(filled(keys, { iceberg_catalog_type: type })), type);
    }
  });

  it("cloud inventory, every cloud", () => {
    const keys = Object.values(CLOUDOPS_CLOUDS).flatMap((c) => c.map((f) => f.key));
    mapping("cloudops", cloudopsMappingJson(filled(keys, { cache_ttl_minutes: "15" })));
  });

  it("salesforce, every sign-in", () => {
    for (const type of SALESFORCE_AUTH_TYPES) {
      const fields = filled(SALESFORCE_AUTH_FIELDS[type], { auth_type: type, api_version: "62.0" });
      mapping("salesforce", salesforceMappingJson(fields), type);
    }
  });

  it("quickbooks", () => {
    mapping(
      "quickbooks",
      quickbooksMappingJson({ environment: "production", refresh_token: "${secret:qb}" }),
    );
  });

  it("kafka, every protocol", () => {
    for (const protocol of KAFKA_SECURITY_PROTOCOLS.filter((p) => p !== "PLAINTEXT")) {
      const fields = {
        kafka_security_protocol: protocol,
        kafka_sasl_mechanism: "PLAIN",
        kafka_ssl_ca: "-----BEGIN CERTIFICATE-----",
      };
      hints("kafka", kafkaHints(fields), protocol);
    }
  });

  it("logs, a pattern format and another", () => {
    const fields = {
      logs_glob: "/var/log/*.log",
      logs_pattern: "(?P<ts>\\S+)",
      logs_record_start: "^\\d",
    };
    mapping("logs", logsMapping({ ...fields, logs_format: LOGS_REGEX }), "logs regex");
    mapping("logs", logsMapping({ ...fields, logs_format: "nginx_combined" }), "logs nginx");
  });

  it("the data profiler", () => {
    const fields = { cron: "0 3 * * *", sample_above_cells: "1000000", ...PROFILER_DEFAULTS };
    mapping("data_profiler", profilerMappingJson(fields));
  });

  it("sparql, with a reasoner and each credential kind", () => {
    for (const auth of ["none", "bearer", "basic"])
      mapping(
        "sparql",
        sparqlMappingJson({ reasoner: "hermit", axioms: "SubClassOf(:A :B)" }, auth),
        `sparql ${auth}`,
      );
  });

  it("cosmos db, both sign-ins", () => {
    for (const auth of [COSMOS_KEY, COSMOS_ENTRA])
      hints(
        "cosmosdb",
        cosmosHints({ cosmos_auth: auth, cosmos_tenant_id: "t", cosmos_client_id: "c" }),
        auth,
      );
  });

  it("qdrant", () => {
    hints("qdrant", qdrantHints({ qdrant_grpc: "true" }));
  });

  it("wikidata and wikipedia", () => {
    hints("wikidata", wikidataHints());
    hints(
      "wikipedia",
      wikipediaHints({ wp_pages: "Ada Lovelace", wp_language: "en", wpc_request_delay: "2s" }),
    );
  });

  it("a jdbc source, by URL and by jar", () => {
    hints(
      "jdbc",
      jdbcSourceHints({
        url: "jdbc:acme://db:1/x",
        sql_dialect: "postgres",
        [JDBC_JAR_KEY]: "/drivers/acme.jar",
        [JDBC_CLASS_KEY]: "com.acme.Driver",
        "capabilities.duckdb": "filter,limit",
      }),
    );
  });

  it("google workspace, one mailbox and the organisation's", () => {
    const reads = {
      gw_resources: "mail,calendar,tasks",
      gw_mail_content: "full",
      gw_mail_labels: "INBOX",
      gw_mail_include_spam_trash: "true",
      gw_mail_search: "from:a",
      gw_mail_since: "2026-01-01",
      gw_send_updates: "true",
    };
    mapping(
      "google_workspace",
      gwMapping({ ...reads, gw_whose: "one", gw_account: "a@x.com", gw_earlier_approval: "rt" }),
      "one mailbox",
    );
    mapping(
      "google_workspace",
      gwMapping({ ...reads, gw_whose: "organisation", gw_choice: "everyone" }),
      "organisation",
    );
  });

  it("microsoft 365, one mailbox and the organisation's", () => {
    const reads = {
      m365_resources: "mail,calendar,tasks",
      m365_mail_since: "2026-01-01",
      m365_check_days: "7",
      m365_send_updates: "true",
    };
    mapping(
      "microsoft_365",
      m365Mapping({
        ...reads,
        m365_whose: "one",
        m365_account: "a@x.com",
        m365_earlier_approval: "rt",
      }),
      "one mailbox",
    );
    mapping(
      "microsoft_365",
      m365Mapping({ ...reads, m365_whose: "organisation", m365_choice: "group", m365_group: "g" }),
      "organisation",
    );
  });

  it("a code source, every language, origin and mode", () => {
    const common = {
      code_entry: "pkg",
      code_secrets: JSON.stringify({ db: null, api: "${secret:api}" }),
      code_llm_cap: "5",
      code_settings: "region = eu",
      code_workers: "3",
      lockfile_upload: "u1",
      lockfile_filename: "uv.lock",
    };
    const origins: Record<string, string>[] = [
      { code_origin: "package", code_package: "p", code_version: "1.0.0" },
      {
        code_origin: "git",
        code_repository: "https://github.com/a/b",
        code_commit: "a".repeat(40),
      },
      { code_origin: "directory", code_directory: "/srv/code" },
      { code_origin: "upload", code_upload: "u2", code_upload_filename: "code.zip" },
    ];
    for (const type of ["node", "python", "go", "rust"])
      for (const origin of origins) {
        mapping(
          type,
          codeMappingJson(type, { ...common, ...origin }),
          `${type} ${origin.code_origin}`,
        );
        mapping(
          type,
          codeMappingJson(type, {
            ...common,
            ...origin,
            code_mode: "built",
            code_remote: JSON.stringify({ host: "h" }),
            code_content_hash: "c".repeat(64),
            code_connector: "crm",
            code_connector_settings: JSON.stringify({ region: "eu" }),
          }),
          `${type} ${origin.code_origin} built`,
        );
      }
  });

  it("a source joined to a connector, run here and paired", () => {
    const connector: Connector = {
      name: "spark",
      git: "",
      ref: "",
      commit: "",
      language: "python",
      image: "",
      digest: "",
      code_hash: "",
      arch: "amd64",
      requires: [{ name: "token", secret: true }, { name: "cluster" }],
      description: "",
      icon: "",
      settings: [],
    };
    const fields = {
      [requiredField("token")]: "token",
      [requiredField("cluster")]: "c1",
      connector_host: "h",
      connector_port: "7000",
      connector_pairing_code: "123456",
    };
    mapping("spark", connectorMappingJson(fields, connector, ["replicate"]), "paired");
    mapping("spark", connectorMappingJson(fields, { ...connector, native: true }), "native");
  });

  it("snowflake", () => {
    hints("snowflake", snowflakeHints(filled(hintKeys("snowflake"))));
  });

  it("databricks, read in place and from a replica", () => {
    hints("databricks", databricksHints({ http_path: "/sql/1", reach: REACH_SCAN }), "scan");
    hints("databricks", databricksHints({ http_path: "/sql/1", reach: "replica" }), "replica");
  });

  it("bigquery, by a service account's key file", () => {
    hints(
      "bigquery",
      bigqueryHints(BIGQUERY_SERVICE_ACCOUNT, { credentials_json: "/keys/sa.json" }),
    );
  });

  it("exasol, sql server, and hiveserver2's security settings", () => {
    hints("exasol", exasolHints(EXASOL_TLS_FINGERPRINT, { tls_fingerprint: "AB:CD" }));
    hints("sqlserver", sqlserverHints(SQLSERVER_TRUST_CERTIFICATE));
    hints("hiveserver2", hiveserver2Hints({ auth_mechanism: "GSSAPI" }));
  });

  it("fabric and synapse, as a service principal", () => {
    for (const type of MSSQL_WAREHOUSES)
      hints(
        type,
        mssqlWarehouseHints(type, SERVICE_PRINCIPAL, filled(hintKeys(type), { other: "x" })),
        type,
      );
  });

  it("a websocket feed and an rss feed", () => {
    hints(
      "websocket",
      websocketHints({
        useSsl: true,
        subscribePayload: "{}",
        eventPath: "$.data",
        reconnectInterval: "5",
      }),
    );
    hints("rss", rssHints({ feedUrl: "https://x/feed", pollInterval: "60", useSsl: false }));
  });

  it("sparql's named graph", () => {
    hints("sparql", sparqlHints({ default_graph_uri: " https://g/1 " }));
  });

  it("sharepoint, every sign-in", () => {
    for (const authType of SHAREPOINT_AUTH_TYPES)
      mapping(
        "sharepoint",
        sharepointMapping({
          authType,
          certPath: "/c.pfx",
          certPassword: "${secret:c}",
          username: "u",
          password: "${secret:p}",
        }),
        authType,
      );
  });

  it("splunk, by token and by password", () => {
    mapping("splunk", splunkMapping({ authMode: "token", disableSsl: true }), "token");
    mapping("splunk", splunkMapping({ authMode: "userpass", disableSsl: false }), "userpass");
  });

  it("google sheets", () => {
    mapping("google_sheets", googleSheetsMapping({ credentials_json: "/keys/sa.json" }));
  });

  it("hive, every storage, writes only that storage's credentials", () => {
    // Every credential field the form has ever held, and one it never offers: a storage writes
    // only its own.
    const fields = filled([...mappingKeys(HIVE), "leftover"]);
    for (const authType of ["none", "aws", "azure", "gcs"]) {
      const written = hiveMapping(HIVE, authType, fields);
      mapping(HIVE, written, `hive ${authType}`);
      const own = fieldsWhen(HIVE, "mappingJson", "storage", written.storage).map((f) => f.name);
      expect(Object.keys(written).sort(), authType).toEqual(["storage", ...own].sort());
    }
    const s3 = hiveMapping(HIVE_S3, "aws", fields);
    mapping(HIVE_S3, s3, "hive_s3");
    expect(s3.storage).toBe("s3");
    expect(Object.keys(s3)).not.toContain("leftover");
  });

  it("a file crawler's workbook hints", () => {
    const fields = withExcelHints({}, { counts: { header_rows: 2 }, entries: [] });
    mapping("files", filesMappingJson(null, fields));
  });
});

/** A saved source of `type` holding `settings`, as the source list gives it. */
const saved = (type: string, settings: Partial<Source>): Source => ({
  id: `s-${type}`,
  type,
  host: "h",
  port: 1,
  database: "d",
  username: "",
  dialect: null,
  isSystem: false,
  withdrawn: false,
  cacheEnabled: true,
  cacheTtl: null,
  replicate: null,
  region: null,
  loadProtected: false,
  offPeakWindow: null,
  offPeakTz: "UTC",
  sentinelPath: null,
  downloadFrom: null,
  freshnessGate: false,
  maxLiveConcurrency: null,
  gqlNamingConvention: null,
  path: null,
  allowedDomains: [],
  description: "",
  passwordRef: null,
  changeSignal: "ttl",
  rowFetchByKeys: false,
  ...settings,
});

describe("a saved source opens with the descriptor's keys", () => {
  it("hive opens in its storage, holding only that storage's credentials", () => {
    const mappingJson = { storage: "adls", storage_account: "acct", sas_token: "t", stale: "x" };
    expect(hiveSeed(HIVE, mappingJson)).toEqual({
      authType: "azure",
      authFields: { storage_account: "acct", sas_token: "t" },
    });
    expect(hiveSeed(HIVE_S3, { storage: "s3", region: "eu", endpoint: "e" }).authType).toBe("aws");
  });

  it("a warehouse opens with its listed hints only", () => {
    const seed = seedSource(
      saved("snowflake", {
        federationHintsJson: JSON.stringify({ warehouse: "w", role: "r", stale: "x" }),
        passwordRef: "${secret:sf}",
      }),
      null,
    );
    expect(seed.authType).toBe("password");
    expect(seed.authFields).toEqual({ warehouse: "w", role: "r", password: "${secret:sf}" });
  });

  it("the feeds, sharepoint and splunk open as they were saved", () => {
    const ws = { useSsl: true, subscribePayload: "{}", eventPath: "$.d", reconnectInterval: "5" };
    expect(
      seedSource(
        saved("websocket", { federationHintsJson: JSON.stringify(websocketHints(ws)) }),
        null,
      ).websocket,
    ).toEqual(ws);
    const rss = { feedUrl: "https://x/feed", pollInterval: "60", useSsl: false };
    expect(
      seedSource(saved("rss", { federationHintsJson: JSON.stringify(rssHints(rss)) }), null).rss,
    ).toEqual(rss);
    const sp = {
      authType: "CERTIFICATE",
      certPath: "/c",
      certPassword: "p",
      username: "",
      password: "",
    };
    expect(
      seedSource(saved("sharepoint", { mappingJson: JSON.stringify(sharepointMapping(sp)) }), null)
        .sharepoint,
    ).toEqual(sp);
    const splunk = { authMode: "userpass" as const, disableSsl: true };
    expect(
      seedSource(saved("splunk", { mappingJson: JSON.stringify(splunkMapping(splunk)) }), null)
        .splunk,
    ).toEqual(splunk);
  });
});

describe("the check itself", () => {
  it("fails a key the descriptor does not list", () => {
    expect(listed(mappingKeys("quickbooks"), "not_a_key")).toBe(false);
    expect(listed(hintKeys("jdbc"), "capabilities.duckdb")).toBe(true);
    expect(listed(mappingKeys("spark"), "settings")).toBe(true);
  });
});
