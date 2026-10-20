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
import { cloudopsFieldsFromMapping } from "./cloudops";
import { GOOGLE_WORKSPACE, gwFieldsFromMapping } from "./googleWorkspace";
import { MICROSOFT_365, m365FieldsFromMapping } from "./microsoft365";
import { LOGS, logsFieldsFromMapping } from "./logs";
import { COSMOSDB, cosmosFieldsFromHints } from "./cosmosdb";
import { QDRANT, qdrantFieldsFromHints } from "./qdrant";
import { CODE_SOURCE_TYPES, codeFieldsFromMapping } from "./codeSource";
import { WIKIPEDIA, wikipediaFieldsFromHints } from "./wikipedia";
import { subjectsOfSchemas, type GovdataSubjectCatalog } from "./govdataSubjects";

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

export interface SharepointSeed {
  authType: string;
  certPath: string;
  certPassword: string;
  username: string;
  password: string;
}

export const EMPTY_SHAREPOINT: SharepointSeed = {
  authType: "CLIENT_CREDENTIALS",
  certPath: "",
  certPassword: "",
  username: "",
  password: "",
};

export interface SourceSeed {
  form: SourceFormState;
  filesTransport: string;
  cdc: CdcState;
  authType: string;
  authFields: Record<string, string>;
  sharepoint: SharepointSeed;
  splunk: { disableSsl: boolean; authMode: "token" | "userpass" };
  websocket: {
    useSsl: boolean;
    subscribePayload: string;
    eventPath: string;
    reconnectInterval: string;
  };
  rss: { feedUrl: string; pollInterval: string; useSsl: boolean };
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

/** What the form holds for a registered source, read from the source alone. */
export function seedSource(s: Source, govdataCatalog: GovdataSubjectCatalog | null): SourceSeed {
  const hintsJson = s.federationHintsJson ?? null;
  const hints = hintsJson ? parsed<Record<string, string>>(hintsJson) : null;
  const mapping = s.mappingJson ? parsed<Record<string, string>>(s.mappingJson) : null;

  let authType = "none";
  let authFields: Record<string, string> = {};
  if ((s.type === "hive" || s.type === "hive_s3") && s.mappingJson) {
    if (mapping) {
      const { storage, ...creds } = mapping;
      authType =
        storage === "s3"
          ? "aws"
          : storage === "adls"
            ? "azure"
            : storage === "gcs"
              ? "gcs"
              : "none";
      authFields = creds;
    }
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
  if (
    [
      "delta_lake",
      "iceberg",
      "snowflake",
      "databricks",
      "bigquery",
      "athena",
      "exasol",
      "sqlserver",
      "fabric",
      "synapse",
    ].includes(s.type) &&
    hintsJson
  ) {
    if (!hints) {
      authType = "none";
      authFields = {};
    } else {
      if (s.type === "delta_lake" || s.type === "iceberg") {
        authType = hints.access_key_id ? "aws" : "none";
      } else if (s.type === "bigquery") {
        authType = hints.credentials_path ? "service_account" : "application_default";
      } else if (s.type === "athena") {
        authType = "none"; // REQ-1981: its settings are its hints, shown again as saved
      } else if (s.type === "databricks") {
        authType = "token";
      } else if (s.type === "exasol") {
        authType = hints.tls_fingerprint ? "tls_fingerprint" : "none";
      } else if (s.type === "sqlserver") {
        authType = hints.trust_server_certificate ? "trust_server_certificate" : "none";
      } else if (s.type === "fabric" || s.type === "synapse") {
        authType = hints.tenant_id ? "service_principal" : "none";
      } else {
        authType = "password";
      }
      // The credential these two kinds keep in the source's password field is shown again as
      // that field holds it, like any other.
      const held: Record<string, string> = !s.passwordRef
        ? {}
        : s.type === "snowflake"
          ? { password: s.passwordRef }
          : s.type === "databricks"
            ? { access_token: s.passwordRef }
            : {};
      authFields = hints.credentials_path
        ? { credentials_json: hints.credentials_path }
        : { ...hints, ...held };
    }
  }
  // REQ-1731: auth_mechanism round-trips through federation_hints, so a non-default (GSSAPI/LDAP)
  // hiveserver2 source reopens as it was saved, not as PLAIN.
  if (s.type === "hiveserver2" && hintsJson) {
    authFields = hints?.auth_mechanism ? { auth_mechanism: hints.auth_mechanism } : {};
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
  if (s.type === QDRANT) {
    authFields = qdrantFieldsFromHints(hintsJson); // REQ-1983
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
  if (s.type === "google_sheets" && s.mappingJson) {
    const m = JSON.parse(s.mappingJson) as Record<string, string>;
    authFields = { credentials_json: m.credentials_json };
  }
  // REQ-1740: default_graph_uri round-trips through federation_hints, merged beside the auth
  // fields: it is orthogonal to which auth mode (none/bearer/basic) is selected.
  if (s.type === "sparql" && hints?.default_graph_uri) {
    authFields = { ...authFields, default_graph_uri: hints.default_graph_uri };
  }

  const sp =
    s.type === "sharepoint" && s.mappingJson
      ? mapping
        ? {
            authType: mapping.auth_type ?? "CLIENT_CREDENTIALS",
            certPath: mapping.certificate_path ?? "",
            certPassword: mapping.certificate_password ?? "",
            username: mapping.sp_username ?? "",
            password: mapping.sp_password ?? "",
          }
        : EMPTY_SHAREPOINT
      : EMPTY_SHAREPOINT;

  const splunkMapping =
    s.type === "splunk" && s.mappingJson ? parsed<Record<string, unknown>>(s.mappingJson) : null;
  const ws = s.type === "websocket" ? hints : null;
  const rss = s.type === "rss" ? hints : null;

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
    sharepoint: sp,
    splunk: {
      disableSsl: !!splunkMapping?.disable_ssl_validation,
      authMode: splunkMapping?.use_token === false ? "userpass" : "token",
    },
    websocket: {
      useSsl: (ws?.use_ssl ?? "false").toLowerCase() === "true",
      subscribePayload: ws?.subscribe_payload ?? "",
      eventPath: ws?.event_path ?? "",
      reconnectInterval: ws?.reconnect_interval ?? "",
    },
    rss: {
      feedUrl: rss?.feed_url ?? "",
      pollInterval: rss?.poll_interval ?? "",
      useSsl: (rss?.use_ssl ?? "true").toLowerCase() === "true",
    },
    govdataSubjects,
  };
}

/** A registration's stored auth, as its list gives it. */
type RemoteAuth = Record<string, string> | null | undefined;

/** What a remote API source's registration adds to the form: kept apart from the source record. */
export type RemoteSeed =
  | { kind: "graphql"; namespace: string; cacheTtl: string; auth: RemoteAuth }
  | {
      kind: "openapi";
      specMode: "path" | "inline";
      specPath: string;
      baseUrl: string;
      cacheTtl: string;
      auth: RemoteAuth;
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
    };

const REMOTE_LISTS: Record<string, string> = {
  graphql_remote: "/admin/sources/graphql-remote",
  openapi: "/admin/openapi/list",
  grpc_remote: "/admin/grpc-remote/list",
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
  const ttl = String(reg.cache_ttl);
  if (s.type === "graphql_remote") {
    return {
      kind: "graphql",
      namespace: String(reg.namespace),
      cacheTtl: ttl,
      auth: reg.auth as RemoteAuth,
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
