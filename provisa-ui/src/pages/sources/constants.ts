// Copyright (c) 2026 Kenneth Stott
// Canary: d5201669-e816-47d1-9ee2-df3f5241e0b9
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import generated from "../../generated/sourceTypes.json";

// REQ-929: human labels for the source change-signal options.
export const CHANGE_SIGNAL_LABELS: Record<string, string> = {
  ttl: "ttl (timer)",
  probe: "probe (freshness query)",
  ttl_probe: "probe + ttl",
  native: "native (source push)",
  debezium: "debezium",
  kafka: "kafka",
};

/** Source types that support schema discovery via adapter. */
// REQ-1767: kafka discovers a topic's columns from Confluent Schema Registry
// (provisa.kafka.schema_registry.discover_topic_columns) — same discover/edit/register flow as
// the other types here.
export const DISCOVERABLE_TYPES = new Set([
  "mongodb",
  "elasticsearch",
  "cassandra",
  "prometheus",
  "kafka",
]);

/** Source types that need a table mapping builder (NoSQL / non-relational). */
export const MAPPING_TYPES = new Set([
  "redis",
  "mongodb",
  "elasticsearch",
  "cassandra",
  "prometheus",
]);

/** One entry of the Sources type picker. */
export interface SourceTypeOffer {
  value: string;
  label: string;
  category: string;
  defaultPort: number;
  /** The hosted services that ARE this type, shown in the type dropdown only. */
  managed?: string;
}

// REQ-2114: the picker is the server's source-type descriptor (provisa/api/admin/source_types.py),
// written for the UI by scripts/gen_source_types.py; Polly's source_types tool reads the same one.
export const SOURCE_TYPES: SourceTypeOffer[] = generated.types.map((t) => ({
  value: t.name,
  label: t.label,
  category: t.category,
  defaultPort: t.default_port,
  ...(t.managed ? { managed: t.managed } : {}),
}));

export const API_AUTH_TYPES = [
  { value: "none", label: "No Auth" },
  { value: "bearer", label: "Bearer Token" },
  { value: "basic", label: "Basic Auth" },
  { value: "api_key", label: "API Key" },
  { value: "oauth2_client_credentials", label: "OAuth2 Client Credentials" },
  { value: "custom_headers", label: "Custom Headers" },
];

export const CATEGORIES = [...new Set(SOURCE_TYPES.map((s) => s.category))];

// File-based source types (path only, no host/port/auth)
export const FILE_SOURCES = new Set<string>(generated.families.fileSources);
export const DB_DESCRIPTION_TYPES = new Set(["postgresql", "mysql", "mariadb", "sqlserver"]);

// Which source types use simple host/port/db/user/pass
export const SIMPLE_RDBMS = new Set<string>(generated.families.simpleRdbms);

// Data lake types
export const DATA_LAKE = new Set<string>(generated.families.dataLake);

// Host+port only, no database/username/password — the connector's location string derives from
// host+port alone (e.g. airport's ATTACH location, connector_duckdb.py's DuckDBAirportConnector).
// REQ-1739: websocket/rss derive their URL from host+port when no federation_hints override is
// set (push_wiring.py / subscribe.py) — same shape as airport.
// REQ-1740: Trino's pinot connector reaches the cluster through the controller's REST endpoint
// (pinot.controller-urls, TrinoPinotConnector.details() in trino_connectors.py) — host:port only;
// database/username/password rendered under SIMPLE_RDBMS were dead inputs nothing ever read.
// REQ-1766: kafka was in none of SIMPLE_RDBMS/HOST_PORT_ONLY/NO_CONNECTION_TYPES — the Sources
// form rendered ZERO connection fields for it (the same class of bug REQ-1753 fixed for saphana),
// so a kafka registration could never be filled in. push_wiring.py's kafka branch builds
// bootstrap_servers from host+port (see that file), same shape as websocket's ws://host:port.
export const HOST_PORT_ONLY = new Set<string>(generated.families.hostPortOnly);

// Source types needing no connection fields at all beyond id/description (REQ-1739): ingest is a
// pure push receiver.
export const NO_CONNECTION_TYPES = new Set<string>(generated.families.noConnection);

// REQ-1923: branded source types → the UI type that carries them. A brand is registered and
// classified as its carrier; the source row records the brand in federation_hints.brand.
export const BRAND_CARRIER: Record<string, string> = generated.brandCarrier;

// REQ-1923/REQ-1960: the credential each brand asks for, stated by the brand. None is given
// by default: a brand asks for what its own service takes, and one read without a credential
// asks for nothing. "token" is a token the brand's API is called with; "client_credentials" is
// an OAuth client ID and secret exchanged for a token at the brand's token endpoint (REQ-2265,
// the OpenAPI source's oauth2_client_credentials auth).
export type BrandAuth = "token" | "client_credentials" | "none";
export const BRAND_AUTH = generated.brandAuth as Record<string, BrandAuth>;

// REQ-1923: what a brand reached at a per-customer address asks for besides its credential,
// sent as the registration's `setup` under each field's `name` (the brand's setup fields,
// provisa/graphql_remote/brands.py and provisa/openapi/brands.py). A field with a
// `defaultValue` starts filled with it.
export interface BrandSetupField {
  name: string;
  labelKey: string;
  hintKey: string;
  placeholder?: string;
  defaultValue?: string;
  required: boolean;
}
// The Admin API version a Shopify source is added with unless the steward names another: the
// newest stable one when the shipped schema was baked (SHOPIFY_API_VERSION in brands.py).
export const SHOPIFY_API_VERSION = "2026-10";
// REQ-2265: the setup field of an OpenAPI brand each customer runs at its own host: the base URL
// its calls go to, and its spec is read from when the brand reads it from the instance.
export const BRAND_BASE_URL_FIELD: BrandSetupField = {
  name: "base_url",
  labelKey: "sourceFormFieldsExtended.brandBaseUrl",
  hintKey: "sourceFormFieldsExtended.brandBaseUrlHint",
  placeholder: "https://catalog.example.com",
  required: true,
};

// REQ-2265: what a brand's credential is, where the generic "access token" says too little to
// find it (an i18n key for the token field's hint).
export const BRAND_TOKEN_HINT: Record<string, string> = {
  openmetadata: "sourceFormFieldsExtended.openmetadataTokenHint",
  datahub: "sourceFormFieldsExtended.datahubTokenHint",
};
export const BRAND_SETUP: Record<string, BrandSetupField[]> = {
  // REQ-2265: the host the instance runs at (provisa/openapi/brands.py).
  openmetadata: [
    {
      ...BRAND_BASE_URL_FIELD,
      hintKey: "sourceFormFieldsExtended.openmetadataBaseUrlHint",
      placeholder: "https://openmetadata.example.com",
    },
  ],
  datahub: [
    {
      ...BRAND_BASE_URL_FIELD,
      hintKey: "sourceFormFieldsExtended.datahubBaseUrlHint",
      placeholder: "https://datahub.example.com/gms",
    },
  ],
  shopify: [
    {
      name: "store",
      labelKey: "sourceFormFieldsExtended.shopifyStore",
      hintKey: "sourceFormFieldsExtended.shopifyStoreHint",
      placeholder: "my-store",
      required: true,
    },
    {
      name: "api_version",
      labelKey: "sourceFormFieldsExtended.shopifyApiVersion",
      hintKey: "sourceFormFieldsExtended.shopifyApiVersionHint",
      defaultValue: SHOPIFY_API_VERSION,
      required: true,
    },
  ],
};

/** The setup values a brand's form starts with: each field's default, or empty. */
export function brandSetupDefaults(brand: string): Record<string, string> {
  return Object.fromEntries((BRAND_SETUP[brand] ?? []).map((f) => [f.name, f.defaultValue ?? ""]));
}

// UI source-type values → backend SourceType vocabulary where the two differ (REQ-947).
export const TYPE_ALIAS: Record<string, string> = generated.typeAlias;

export const FILE_TRANSPORTS = [
  { value: "file://", label: "file:// (file mount / local disk)", needsAuth: false as const },
  { value: "ftp://", label: "ftp://", needsAuth: "userpass" as const },
  { value: "sftp://", label: "sftp://", needsAuth: "cert-or-userpass" as const },
  { value: "s3://", label: "s3://", needsAuth: "s3" as const },
  { value: "s3a://", label: "s3a://", needsAuth: "s3" as const },
  { value: "http://", label: "http://", needsAuth: "userpass" as const },
  { value: "https://", label: "https://", needsAuth: "userpass" as const },
  { value: "sharepoint://", label: "sharepoint://", needsAuth: "cert-or-userpass" as const },
];
