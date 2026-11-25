// Copyright (c) 2026 Kenneth Stott
// Canary: f2c6c249-6536-4e43-b8e4-feed528c6861
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A registered source as the form's state: what its edit form opens with, and what its read view
// shows -- one state, so the two never differ.

import i18n from "i18next";
import { requestFailed } from "../../i18n/serverMessage";
import type { Source } from "../../types/admin";
import { sourceChangeSignals } from "../../liveCapability";
import type { CdcState, SourceFormState } from "./SourceFormFields";
import { getDefaultPort, parseFilesPath, sourceBrand, uiType } from "./sourceHelpers";
import { CLOUD_FILE_TYPES, objectStoreFields, objectStoreOf } from "./objectStoreHints";
import { profilerFieldsFromMapping } from "./profilerMapping";
import { salesforceFieldsFromMapping } from "./salesforce";
import { quickbooksFieldsFromMapping } from "./quickbooks";
import { cloudopsFieldsFromMapping } from "./cloudops";
import { sparqlCredentialFromSource, sparqlFieldsFromMapping } from "./sparqlReasoner";
import { GOOGLE_WORKSPACE, gwFieldsFromMapping } from "./googleWorkspace";
import { MICROSOFT_365, m365FieldsFromMapping } from "./microsoft365";
import { LOGS, logsFieldsFromMapping } from "./logs";
import { COSMOSDB, cosmosFieldsFromHints } from "./cosmosdb";
import { JDBC_SOURCE, jdbcSourceHints } from "./jdbcSource";
import { excelHintsFromMapping, withExcelHints } from "./excelHints";
import { KAFKA, kafkaFieldsFromSource } from "./kafka";
import { QDRANT, qdrantFieldsFromHints } from "./qdrant";
import { CODE_SOURCE_TYPES, codeFieldsFromMapping } from "./codeSource";
import { WIKIPEDIA, wikipediaFieldsFromHints } from "./wikipedia";
import { subjectsOfSchemas, type GovdataSubjectCatalog } from "./govdataSubjects";
import type { AuthSeed } from "./descriptorPick";
import { SNOWFLAKE, snowflakeSeed } from "./snowflake";
import { DATABRICKS, databricksSeed } from "./databricks";
import { BIGQUERY, bigquerySeed } from "./bigquery";
import { EXASOL, exasolSeed } from "./exasol";
import { SQLSERVER, sqlserverSeed } from "./sqlserver";
import { MSSQL_WAREHOUSES, mssqlWarehouseSeed } from "./mssqlWarehouse";
import { HIVESERVER2, hiveserver2Fields } from "./hiveserver2";
import { WEBSOCKET, websocketFromHints, type WebsocketSeed } from "./websocket";
import { RSS, rssFromHints, type RssSeed } from "./rss";
import { sparqlGraphFields } from "./sparqlGraph";
import {
  SHAREPOINT,
  sharepointFromMapping,
  EMPTY_SHAREPOINT,
  type SharepointSeed,
} from "./sharepoint";
import { SPLUNK, splunkFromMapping, type SplunkSeed } from "./splunk";
import { GOOGLE_SHEETS, googleSheetsFields } from "./googleSheets";
import { HIVE_TYPES, hiveSeed } from "./hiveStorage";

export { EMPTY_SHAREPOINT, type SharepointSeed };

export const EMPTY_CDC: CdcState = {
  bootstrapServers: "",
  topicPrefix: "",
  schemaRegistryUrl: "",
  schemaRegistryAuth: "none",
  schemaRegistryUsername: "",
  schemaRegistryPassword: "",
  schemaRegistryToken: "",
  schemaRegistryClientCert: "",
  schemaRegistryClientKey: "",
  schemaRegistryCa: "",
  consumerGroupId: "",
};

/** The create form's state before a type is picked: every field at its default, no source's. */
export function emptySourceForm(): SourceFormState {
  return {
    id: "",
    type: "postgresql",
    host: "",
    port: 5432,
    database: "",
    username: "",
    password: "",
    gqlNamingConvention: "",
    cacheTtl: "",
    cacheEnabled: true,
    replicate: null,
    region: null,
    loadProtected: false,
    offPeakWindow: "",
    offPeakTz: "UTC",
    changeSignal: "ttl",
    sentinelPath: "",
    freshnessGate: false,
    maxLiveConcurrency: "",
    path: "",
    allowedDomains: "",
    description: "",
  };
}

export interface SourceSeed {
  form: SourceFormState;
  filesTransport: string;
  cdc: CdcState;
  authType: string;
  authFields: Record<string, string>;
  sharepoint: SharepointSeed;
  splunk: SplunkSeed;
  websocket: WebsocketSeed;
  rss: RssSeed;
  /** A govdata source's subjects; null when it has schemas but the subject catalog is not loaded. */
  govdataSubjects: string[] | null;
}

function parsed<T>(json: string): T | null {
  try {
    return JSON.parse(json) as T;
  } catch {
    // Not JSON: the source keeps none of these settings, as when none were saved.
    return null;
  }
}

/** The warehouses whose settings beside host, port, database and credentials are their
 * federation hints: each one's hints, and its password field as held, into the form. */
const WAREHOUSE_SEEDS: Record<string, (hints: Record<string, string>, s: Source) => AuthSeed> = {
  [SNOWFLAKE]: (hints, s) => snowflakeSeed(hints, s.passwordRef),
  [DATABRICKS]: (hints, s) => databricksSeed(hints, s.passwordRef),
  [BIGQUERY]: (hints) => bigquerySeed(hints),
  [EXASOL]: (hints) => exasolSeed(hints),
  [SQLSERVER]: (hints) => sqlserverSeed(hints),
  ...Object.fromEntries(
    [...MSSQL_WAREHOUSES].map((type) => [
      type,
      (hints: Record<string, string>) => mssqlWarehouseSeed(type, hints),
    ]),
  ),
};

/** What the form holds for a registered source, read from the source alone. */
export function seedSource(s: Source, govdataCatalog: GovdataSubjectCatalog | null): SourceSeed {
  const hintsJson = s.federationHintsJson ?? null;
  const hints = hintsJson ? parsed<Record<string, string>>(hintsJson) : null;
  const mapping = s.mappingJson ? parsed<Record<string, string>>(s.mappingJson) : null;

  let authType = "none";
  let authFields: Record<string, string> = {};
  if (HIVE_TYPES.has(s.type) && mapping) {
    ({ authType, authFields } = hiveSeed(s.type, mapping));
  }
  // delta_lake/iceberg S3 creds and snowflake/databricks/bigquery warehouse extras all live in
  // federation_hints (never mapping). A credential field comes back as it was entered (the value,
  // or a reference someone typed) beside the other extras (warehouse/schema/role/http_path/
  // credentials_path/access_key_id/endpoint), so the form shows again what each field holds.
  // REQ-990: a CSV/Parquet file's object-store credentials come back into the form's fields for
  // the store its path names.
  const store = objectStoreOf(s.path);
  if (CLOUD_FILE_TYPES.has(s.type) && store && hintsJson) {
    authFields = hints ? objectStoreFields(store, hints) : {};
  }
  if (s.type in WAREHOUSE_SEEDS && hintsJson) {
    ({ authType, authFields } = hints
      ? WAREHOUSE_SEEDS[s.type](hints, s)
      : { authType: "none", authFields: {} });
  }
  if (["delta_lake", "iceberg", "athena"].includes(s.type) && hintsJson) {
    if (!hints) {
      authType = "none";
      authFields = {};
    } else {
      // REQ-1981: an Athena source's settings are its hints, shown again as saved.
      authType = s.type !== "athena" && hints.access_key_id ? "aws" : "none";
      authFields = { ...hints };
    }
  }
  // REQ-1731: auth_mechanism round-trips through federation_hints, so a non-default (GSSAPI/LDAP)
  // hiveserver2 source reopens as it was saved, not as PLAIN.
  if (s.type === HIVESERVER2 && hintsJson) {
    authFields = hiveserver2Fields(hints);
  }
  if (s.type === "data_profiler" && s.mappingJson) {
    // REQ-1934: the profiler's schedule and run default live in its mapping.
    authFields = profilerFieldsFromMapping(s.mappingJson);
  }
  if (s.type === "cloudops" && s.mappingJson) {
    authFields = cloudopsFieldsFromMapping(s.mappingJson); // REQ-1947
  }
  if (s.type === GOOGLE_WORKSPACE && s.mappingJson) {
    authFields = gwFieldsFromMapping(s.mappingJson); // REQ-1923
  }
  if (s.type === LOGS && s.mappingJson) {
    authFields = logsFieldsFromMapping(s.mappingJson); // REQ-1996
  }
  if (s.type === MICROSOFT_365 && s.mappingJson) {
    authFields = m365FieldsFromMapping(s.mappingJson); // REQ-1923
  }
  if (s.type === COSMOSDB) {
    authFields = cosmosFieldsFromHints(hintsJson); // REQ-1982
  }
  if (s.type === KAFKA) {
    authFields = kafkaFieldsFromSource(s); // REQ-147
  }
  if (s.type === JDBC_SOURCE && hintsJson) {
    authFields = jdbcSourceHints(JSON.parse(hintsJson)); // REQ-2191: its settings are its hints
  }
  if (s.type === QDRANT) {
    authFields = qdrantFieldsFromHints(hintsJson); // REQ-1983
  }
  if (s.type === "files") {
    // How the Excel reader finds the tables of its workbooks (mapping.excel_hints).
    authFields = withExcelHints(authFields, excelHintsFromMapping(s.mappingJson));
  }
  if (CODE_SOURCE_TYPES.has(s.type) && s.mappingJson) {
    authFields = codeFieldsFromMapping(s.mappingJson); // REQ-1984, REQ-1993
  }
  if (sourceBrand(hintsJson) === WIKIPEDIA && hintsJson) {
    // REQ-1960: the directory is the source's own path, shown as where its files land.
    authFields = {
      ...wikipediaFieldsFromHints(hintsJson),
      ...(s.path ? { wp_directory: s.path } : {}),
    };
  }
  if (s.type === "salesforce" && s.mappingJson) {
    authFields = salesforceFieldsFromMapping(s.mappingJson); // REQ-1946
  }
  if (s.type === "quickbooks" && s.mappingJson) {
    authFields = quickbooksFieldsFromMapping(s.mappingJson); // REQ-2197
  }
  if (s.type === GOOGLE_SHEETS && s.mappingJson) {
    authFields = googleSheetsFields(JSON.parse(s.mappingJson) as Record<string, string>);
  }
  // REQ-1740: default_graph_uri round-trips through federation_hints, merged beside the auth
  // fields: it is orthogonal to which auth mode (none/bearer/basic) is selected.
  if (s.type === "sparql") {
    authFields = { ...authFields, ...sparqlGraphFields(hints) };
  }
  if (s.type === "sparql" && s.mappingJson) {
    authFields = { ...authFields, ...sparqlFieldsFromMapping(s.mappingJson) }; // REQ-2138
  }
  if (s.type === "sparql") {
    // REQ-2138: the credential it declares opens as it is held, in its auth mode's fields.
    const credential = sparqlCredentialFromSource(s.mappingJson, s.username, s.passwordRef);
    authType = credential.authType;
    authFields = { ...authFields, ...credential.fields };
  }

  const sharepoint =
    s.type === SHAREPOINT && s.mappingJson ? sharepointFromMapping(mapping) : EMPTY_SHAREPOINT;
  const splunkMapping =
    s.type === SPLUNK && s.mappingJson ? parsed<Record<string, unknown>>(s.mappingJson) : null;

  let govdataSubjects: string[] | null = [];
  if (s.type === "govdata" && s.database) {
    const stored = s.database
      .split(",")
      .map((x) => x.trim())
      .filter(Boolean);
    govdataSubjects = govdataCatalog ? subjectsOfSchemas(govdataCatalog, stored) : null;
  }

  return {
    form: {
      id: s.id,
      type: sourceBrand(hintsJson) ?? uiType(s.type),
      host: s.type === "graphql_remote" ? (s.path ?? "") : (s.host ?? ""),
      port: s.port ?? getDefaultPort(uiType(s.type)),
      database: s.database ?? "",
      username: s.username ?? "",
      // The field shows again what it holds: the value typed, or the reference typed.
      password: s.passwordRef ?? "",
      gqlNamingConvention: s.gqlNamingConvention ?? "",
      cacheTtl: s.cacheTtl != null ? String(s.cacheTtl) : "",
      cacheEnabled: s.cacheEnabled,
      replicate: s.replicate,
      region: s.region,
      loadProtected: s.loadProtected ?? false,
      offPeakWindow: s.offPeakWindow ?? "",
      offPeakTz: s.offPeakTz ?? "UTC",
      changeSignal: s.changeSignal || sourceChangeSignals(uiType(s.type))[0],
      sentinelPath: s.sentinelPath ?? "",
      downloadFrom: s.downloadFrom ?? "",
      freshnessGate: s.freshnessGate,
      maxLiveConcurrency: s.maxLiveConcurrency != null ? String(s.maxLiveConcurrency) : "",
      path: s.type === "files" ? parseFilesPath(s.path ?? "").path : (s.path ?? ""),
      allowedDomains: (s.allowedDomains ?? []).join(", "),
      description: s.description ?? "",
    },
    filesTransport: s.type === "files" ? parseFilesPath(s.path ?? "").transport : "file://",
    cdc: s.cdc
      ? {
          bootstrapServers: s.cdc.bootstrapServers ?? "",
          topicPrefix: s.cdc.topicPrefix ?? "",
          schemaRegistryUrl: s.cdc.schemaRegistryUrl ?? "",
          schemaRegistryAuth: s.cdc.schemaRegistryAuth ?? "none",
          schemaRegistryUsername: s.cdc.schemaRegistryUsername ?? "",
          schemaRegistryPassword: s.cdc.schemaRegistryPassword ?? "",
          schemaRegistryToken: s.cdc.schemaRegistryToken ?? "",
          schemaRegistryClientCert: s.cdc.schemaRegistryClientCert ?? "",
          schemaRegistryClientKey: s.cdc.schemaRegistryClientKey ?? "",
          schemaRegistryCa: s.cdc.schemaRegistryCa ?? "",
          consumerGroupId: s.cdc.consumerGroupId ?? "",
        }
      : { ...EMPTY_CDC },
    authType,
    authFields,
    sharepoint,
    splunk: splunkFromMapping(splunkMapping),
    websocket: websocketFromHints(s.type === WEBSOCKET ? hints : null),
    rss: rssFromHints(s.type === RSS ? hints : null),
    govdataSubjects,
  };
}

/** A registration's stored auth, as its list gives it. */
type RemoteAuth = Record<string, string> | null | undefined;

/** What a remote API source's registration adds to the form: kept apart from the source record. */
export type RemoteSeed =
  | {
      kind: "graphql";
      namespace: string;
      cacheTtl: string;
      auth: RemoteAuth;
      // REQ-1923: what a brand reached at a per-customer address was set up with.
      setup: Record<string, string>;
    }
  | {
      kind: "openapi";
      specMode: "path" | "inline";
      specPath: string;
      baseUrl: string;
      cacheTtl: string;
      auth: RemoteAuth;
      // REQ-2265: what an OpenAPI brand's setup was given (its base URL).
      setup: Record<string, string>;
    }
  | {
      kind: "grpc";
      protoPath: string;
      serverAddress: string;
      namespace: string;
      tls: boolean;
      importPaths: string;
      cacheTtl: string;
      auth: RemoteAuth;
    }
  | ({ kind: "dbt" } & import("./dbt").DbtSeed); // REQ-2228

const REMOTE_LISTS: Record<string, string> = {
  graphql_remote: "/admin/sources/graphql-remote",
  openapi: "/admin/openapi/list",
  grpc_remote: "/admin/grpc-remote/list",
  dbt: "/admin/dbt/list", // REQ-2228
};

/** Whether a source of this type keeps settings in a registration of its own. */
export function hasRemoteSeed(type: string): boolean {
  return type in REMOTE_LISTS;
}

/**
 * A remote API source's registration, read from its own list. A list that does not answer, or
 * that does not hold this source, is an error naming which: the form never shows invented values.
 */
export async function fetchRemoteSeed(s: Source): Promise<RemoteSeed> {
  const url = REMOTE_LISTS[s.type];
  const r = await fetch(url);
  if (!r.ok) throw new Error(requestFailed(url, r.status));
  const regs = (await r.json()) as ({ source_id: string } & Record<string, unknown>)[];
  const reg = regs.find((x) => x.source_id === s.id);
  if (!reg) {
    throw new Error(i18n.t("sourcesPage.registrationMissing", { list: url, source: s.id }));
  }
  if (s.type === "dbt") {
    const cloud = (reg.cloud as Record<string, string> | undefined) ?? {};
    return {
      kind: "dbt",
      spec: reg.spec as "location" | "upload" | "cloud",
      location: String(reg.location ?? ""),
      commandLine: String(reg.command_line ?? ""),
      host: cloud.host ?? "",
      accountId: cloud.account_id ?? "",
      jobId: cloud.job_id ?? "",
    };
  }
  const ttl = String(reg.cache_ttl);
  if (s.type === "graphql_remote") {
    return {
      kind: "graphql",
      namespace: String(reg.namespace),
      cacheTtl: ttl,
      auth: reg.auth as RemoteAuth,
      setup: (reg.setup as Record<string, string> | undefined) ?? {},
    };
  }
  if (s.type === "openapi") {
    const inline = reg.has_inline_spec === true;
    return {
      kind: "openapi",
      specMode: inline ? "inline" : "path",
      specPath: inline ? "" : String(reg.spec_path),
      baseUrl: String(reg.base_url),
      cacheTtl: ttl,
      auth: reg.auth_config as RemoteAuth,
      setup: reg.setup as Record<string, string>,
    };
  }
  return {
    kind: "grpc",
    protoPath: String(reg.proto_path),
    serverAddress: String(reg.server_address),
    namespace: String(reg.namespace),
    tls: reg.tls === true,
    importPaths: (reg.import_paths as string[]).join(", "),
    cacheTtl: ttl,
    auth: reg.auth_config as RemoteAuth,
  };
}

/** A registration's auth as the form holds it: its type, and its other keys as fields. */
export function remoteAuth(
  auth: RemoteAuth,
): { authType: string; authFields: Record<string, string> } | null {
  if (!auth?.type) return null;
  const { type, ...rest } = auth;
  return { authType: type, authFields: rest };
}
