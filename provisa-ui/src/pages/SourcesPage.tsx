// Copyright (c) 2026 Kenneth Stott
// Canary: 55ecea78-4131-4bb4-8a93-71e1bdacea84
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import React, { useState, useEffect } from "react";
import { Trash2, Pencil, Save, X } from "lucide-react";
import { FilterInput } from "../components/admin/FilterInput";
import { fetchSources, deleteSource, createSource, updateSource, renameSource, updateSourceCache, updateSourceNaming, fetchSettings } from "../api/admin";
import type { PlatformSettings } from "../api/admin";
import { ConfirmDialog } from "../components/ConfirmDialog";
import { SchemaDiscovery } from "../components/SchemaDiscovery";
import { TableMappingBuilder } from "../components/TableMappingBuilder";
import type { TableMapping } from "../components/TableMappingBuilder";
import type { Source } from "../types/admin";

/** Source types that support schema discovery via adapter. */
const DISCOVERABLE_TYPES = new Set(["mongodb", "elasticsearch", "cassandra", "prometheus"]);

/** Source types that need a table mapping builder (NoSQL / non-relational). */
const MAPPING_TYPES = new Set(["redis", "mongodb", "elasticsearch", "cassandra", "prometheus", "accumulo"]);

const SOURCE_TYPES = [
  // RDBMS
  { value: "postgresql", label: "PostgreSQL", category: "RDBMS", defaultPort: 5432 },
  { value: "mysql", label: "MySQL", category: "RDBMS", defaultPort: 3306 },
  { value: "singlestore", label: "SingleStore", category: "RDBMS", defaultPort: 3306 },
  { value: "mariadb", label: "MariaDB", category: "RDBMS", defaultPort: 3306 },
  { value: "sqlserver", label: "SQL Server", category: "RDBMS", defaultPort: 1433 },
  { value: "oracle", label: "Oracle", category: "RDBMS", defaultPort: 1521 },
  { value: "duckdb", label: "DuckDB", category: "RDBMS", defaultPort: 0 },
  // Cloud DW
  { value: "snowflake", label: "Snowflake", category: "Cloud DW", defaultPort: 443 },
  { value: "bigquery", label: "BigQuery", category: "Cloud DW", defaultPort: 443 },
  { value: "databricks", label: "Databricks", category: "Cloud DW", defaultPort: 443 },
  { value: "redshift", label: "Redshift", category: "Cloud DW", defaultPort: 5439 },
  // Analytics / OLAP
  { value: "clickhouse", label: "ClickHouse", category: "Analytics", defaultPort: 8123 },
  { value: "elasticsearch", label: "Elasticsearch", category: "Analytics", defaultPort: 9200 },
  { value: "pinot", label: "Apache Pinot", category: "Analytics", defaultPort: 8099 },
  { value: "druid", label: "Apache Druid", category: "Analytics", defaultPort: 8082 },
  // Data Lake
  { value: "delta_lake", label: "Delta Lake", category: "Data Lake", defaultPort: 0 },
  { value: "iceberg", label: "Apache Iceberg", category: "Data Lake", defaultPort: 0 },
  { value: "hive", label: "Hive / S3 / ADLS", category: "Data Lake", defaultPort: 9083 },
  // NoSQL
  { value: "mongodb", label: "MongoDB", category: "NoSQL", defaultPort: 27017 },
  { value: "cassandra", label: "Cassandra", category: "NoSQL", defaultPort: 9042 },
  { value: "redis", label: "Redis", category: "NoSQL", defaultPort: 6379 },
  { value: "kudu", label: "Apache Kudu", category: "NoSQL", defaultPort: 7051 },
  { value: "accumulo", label: "Accumulo", category: "NoSQL", defaultPort: 9995 },
  // File
  { value: "sqlite", label: "SQLite", category: "File", defaultPort: 0 },
  { value: "csv", label: "CSV File", category: "File", defaultPort: 0 },
  { value: "parquet", label: "Parquet File", category: "File", defaultPort: 0 },
  // Other
  { value: "google_sheets", label: "Google Sheets", category: "Other", defaultPort: 0 },
  { value: "prometheus", label: "Prometheus", category: "Other", defaultPort: 9090 },
  // API
  { value: "openapi", label: "REST API (OpenAPI)", category: "API", defaultPort: 443 },
  { value: "graphql", label: "GraphQL", category: "API", defaultPort: 443 },
  { value: "grpc", label: "gRPC", category: "API", defaultPort: 50051 },
  // Streaming
  { value: "kafka", label: "Kafka", category: "Streaming", defaultPort: 9092 },
];

const API_AUTH_TYPES = [
  { value: "none", label: "No Auth" },
  { value: "bearer", label: "Bearer Token" },
  { value: "basic", label: "Basic Auth" },
  { value: "api_key", label: "API Key" },
  { value: "oauth2_client_credentials", label: "OAuth2 Client Credentials" },
  { value: "custom_headers", label: "Custom Headers" },
];

const KAFKA_AUTH_TYPES = [
  { value: "none", label: "No Auth" },
  { value: "sasl_plain", label: "SASL/PLAIN" },
  { value: "sasl_scram_256", label: "SASL/SCRAM-SHA-256" },
  { value: "sasl_scram_512", label: "SASL/SCRAM-SHA-512" },
];

const NAMING_CONVENTIONS = [
  { value: "", label: "Inherit (global)" },
  { value: "none", label: "none" },
  { value: "snake_case", label: "snake_case" },
  { value: "camelCase", label: "camelCase" },
  { value: "PascalCase", label: "PascalCase" },
];

const CATEGORIES = [...new Set(SOURCE_TYPES.map((s) => s.category))];

function getCategory(type: string) {
  return SOURCE_TYPES.find((s) => s.value === type)?.category ?? "RDBMS";
}
function getDefaultPort(type: string) {
  return SOURCE_TYPES.find((s) => s.value === type)?.defaultPort ?? 5432;
}

// File-based source types (path only, no host/port/auth)
const FILE_SOURCES = new Set(["sqlite", "csv", "parquet"]);

// Which source types use simple host/port/db/user/pass
const SIMPLE_RDBMS = new Set([
  "postgresql", "mysql", "singlestore", "mariadb", "sqlserver", "oracle",
  "clickhouse", "pinot", "druid", "mongodb", "cassandra", "redis", "kudu", "accumulo",
]);

// Data lake types
const DATA_LAKE = new Set(["delta_lake", "iceberg", "hive"]);

/** Reusable auth fields for username/password */
function AuthUserPass({ authFields, setAuthFields }: {
  authFields: Record<string, string>;
  setAuthFields: (f: Record<string, string>) => void;
}) {
  return (
    <>
      <label>Username <input required value={authFields.username ?? ""} onChange={(e) => setAuthFields({ ...authFields, username: e.target.value })} /></label>
      <label>Password <input type="password" required value={authFields.password ?? ""} onChange={(e) => setAuthFields({ ...authFields, password: e.target.value })} /></label>
    </>
  );
}

export function SourcesPage() {
  const [sources, setSources] = useState<Source[]>([]);
  const [loading, setLoading] = useState(true);
  const [showForm, setShowForm] = useState(false);
  const [editingSourceId, setEditingSourceId] = useState<string | null>(null);
  const [expanded, setExpanded] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [sourceSearch, setSourceSearch] = useState("");
  const [form, setForm] = useState({
    id: "", type: "postgresql", host: "", port: 5432,
    database: "", username: "", password: "",
    namingConvention: "", cacheTtl: "", cacheEnabled: true,
    path: "" as string,
  });
  const [authType, setAuthType] = useState("none");
  const [authFields, setAuthFields] = useState<Record<string, string>>({});
  const [settings, setSettings] = useState<PlatformSettings | null>(null);
  const [discoverSourceId, setDiscoverSourceId] = useState<string | null>(null);
  const [discoverSourceType, setDiscoverSourceType] = useState<string | null>(null);
  const [mappingSourceId, setMappingSourceId] = useState<string | null>(null);
  const [mappingSourceType, setMappingSourceType] = useState<string | null>(null);

  const load = () => {
    setLoading(true);
    setError(null);
    Promise.all([fetchSources(), fetchSettings()])
      .then(([s, st]) => { setSources(s); setSettings(st); })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  };
  useEffect(load, []);

  const getEffectiveTtl = (source: Source): string => {
    if (source.cacheTtl != null) return `${source.cacheTtl}s (custom)`;
    if (settings) return `${settings.cache.default_ttl}s (global)`;
    return "default";
  };

  const handleTypeChange = (type: string) => {
    setForm({ ...form, type, port: getDefaultPort(type) });
    setAuthType("none");
    setAuthFields({});
  };

  const handleEdit = (s: Source) => {
    setForm({
      id: s.id,
      type: s.type,
      host: s.host ?? "",
      port: s.port ?? getDefaultPort(s.type),
      database: s.database ?? "",
      username: s.username ?? "",
      password: "",
      namingConvention: s.namingConvention ?? "",
      cacheTtl: s.cacheTtl != null ? String(s.cacheTtl) : "",
      cacheEnabled: s.cacheEnabled,
      path: s.path ?? "",
    });
    setAuthType("none");
    setAuthFields({});
    setEditingSourceId(s.id);
    setExpanded(s.id);
    setShowForm(false);
  };

  const handleCancelForm = () => {
    setShowForm(false);
    setEditingSourceId(null);
    setForm({ id: "", type: "postgresql", host: "", port: 5432, database: "", username: "", password: "", namingConvention: "", cacheTtl: "", cacheEnabled: true, path: "" });
    setAuthType("none");
    setAuthFields({});
  };

  const handleCreate = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);
    try {
      const { namingConvention: _nc, cacheTtl: _ct, cacheEnabled: _ce, ...coreForm } = form;
      const sourcePayload = {
        ...coreForm,
        path: FILE_SOURCES.has(form.type) ? form.path || null : null,
      };
      if (editingSourceId) {
        const effectiveId = form.id.trim() || editingSourceId;
        if (effectiveId !== editingSourceId) {
          const renameResult = await renameSource(editingSourceId, effectiveId);
          if (!renameResult.success) throw new Error(renameResult.message);
        }
        const result = await updateSource({ ...sourcePayload, id: effectiveId });
        if (!result.success) throw new Error(result.message);
        const ttlValue = form.cacheTtl.trim() === "" ? null : parseInt(form.cacheTtl, 10);
        if (ttlValue !== null && isNaN(ttlValue)) throw new Error("TTL must be a number");
        const cacheResult = await updateSourceCache(effectiveId, form.cacheEnabled, ttlValue);
        if (!cacheResult.success) throw new Error(cacheResult.message);
        const namingResult = await updateSourceNaming(effectiveId, form.namingConvention === "" ? null : form.namingConvention);
        if (!namingResult.success) throw new Error(namingResult.message);
      } else {
        await createSource(sourcePayload);
      }
      handleCancelForm();
      load();
    } catch (err: any) {
      setError(err.message);
    }
  };

  const [gqlNamespace, setGqlNamespace] = useState("");
  const [gqlCacheTtl, setGqlCacheTtl] = useState("300");
  const [refreshingSourceId, setRefreshingSourceId] = useState<string | null>(null);
  const [refreshError, setRefreshError] = useState<string | null>(null);

  // OpenAPI-specific state
  const [openapiSpecPath, setOpenapiSpecPath] = useState("");
  const [openapiSpecInline, setOpenapiSpecInline] = useState("");
  const [openapiSpecMode, setOpenapiSpecMode] = useState<"path" | "inline">("path");
  const [openapiBaseUrl, setOpenapiBaseUrl] = useState("");
  const [openapiCacheTtl, setOpenapiCacheTtl] = useState("300");
  const [openapiPreview, setOpenapiPreview] = useState<{ queries: any[]; mutations: any[] } | null>(null);
  const [openapiPreviewing, setOpenapiPreviewing] = useState(false);
  const [openapiPreviewError, setOpenapiPreviewError] = useState<string | null>(null);

  // gRPC Remote-specific state
  const [grpcProtoPath, setGrpcProtoPath] = useState("");
  const [grpcServerAddress, setGrpcServerAddress] = useState("");
  const [grpcNamespace, setGrpcNamespace] = useState("");
  const [grpcTls, setGrpcTls] = useState(false);
  const [grpcImportPaths, setGrpcImportPaths] = useState("");
  const [grpcCacheTtl, setGrpcCacheTtl] = useState("300");

  const handleRefreshSchema = async (sourceId: string, sourceType?: string) => {
    setRefreshingSourceId(sourceId);
    setRefreshError(null);
    try {
      const url = sourceType === "openapi"
        ? `/admin/openapi/refresh/${sourceId}`
        : sourceType === "grpc"
          ? `/admin/grpc-remote/refresh/${sourceId}`
          : `/admin/sources/graphql-remote/${sourceId}/refresh`;
      const resp = await fetch(url, { method: "POST" });
      if (!resp.ok) {
        const body = await resp.json().catch(() => ({ detail: resp.statusText }));
        throw new Error(body.detail ?? resp.statusText);
      }
    } catch (err: any) {
      setRefreshError(err.message);
    } finally {
      setRefreshingSourceId(null);
    }
  };

  const handleOpenapiPreview = async () => {
    setOpenapiPreviewing(true);
    setOpenapiPreviewError(null);
    setOpenapiPreview(null);
    try {
      const resp = await fetch("/admin/openapi/preview", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(
          openapiSpecMode === "inline"
            ? { spec_content: openapiSpecInline }
            : { spec_path: openapiSpecPath }
        ),
      });
      if (!resp.ok) {
        const body = await resp.json().catch(() => ({ detail: resp.statusText }));
        throw new Error(body.detail ?? resp.statusText);
      }
      setOpenapiPreview(await resp.json());
    } catch (err: any) {
      setOpenapiPreviewError(err.message);
    } finally {
      setOpenapiPreviewing(false);
    }
  };

  const handleOpenapiRegister = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);
    try {
      const resp = await fetch("/admin/openapi/register", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          ...(openapiSpecMode === "inline"
            ? { spec_content: openapiSpecInline }
            : { spec_path: openapiSpecPath }),
          base_url: openapiBaseUrl || undefined,
          source_id: form.id,
          domain_id: "",
          auth_config: authType !== "none" ? { type: authType, ...authFields } : null,
          cache_ttl: parseInt(openapiCacheTtl, 10) || 300,
        }),
      });
      if (!resp.ok) {
        const body = await resp.json().catch(() => ({ detail: resp.statusText }));
        throw new Error(body.detail ?? resp.statusText);
      }
      handleCancelForm();
      load();
    } catch (err: any) {
      setError(err.message);
    }
  };

  const handleGrpcRegister = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);
    try {
      const resp = await fetch("/admin/grpc-remote/register", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          source_id: form.id,
          proto_path: grpcProtoPath,
          server_address: grpcServerAddress,
          namespace: grpcNamespace,
          domain_id: "",
          import_paths: grpcImportPaths ? grpcImportPaths.split(",").map((s) => s.trim()).filter(Boolean) : [],
          tls: grpcTls,
          auth_config: authType !== "none" ? { type: authType, ...authFields } : null,
          cache_ttl: parseInt(grpcCacheTtl, 10) || 300,
        }),
      });
      if (!resp.ok) {
        const body = await resp.json().catch(() => ({ detail: resp.statusText }));
        throw new Error(body.detail ?? resp.statusText);
      }
      await createSource({ id: form.id, type: form.type, host: form.host, port: form.port, database: form.database, username: form.username, password: form.password, path: null });
      handleCancelForm();
      load();
    } catch (err: any) {
      setError(err.message);
    }
  };

  const category = getCategory(form.type);
  const isApi = false; // All API types have dedicated form sections
  const isKafka = category === "Streaming";
  const isFile = FILE_SOURCES.has(form.type);
  const isSimpleRdbms = SIMPLE_RDBMS.has(form.type);
  const isDataLake = DATA_LAKE.has(form.type);

  const renderFormFields = () => (
    <>
      {isSimpleRdbms && (
        <>
          <label>Host <input required value={form.host} onChange={(e) => setForm({ ...form, host: e.target.value })} placeholder="localhost" /></label>
          <label>Port <input type="number" required value={form.port} onChange={(e) => setForm({ ...form, port: +e.target.value })} /></label>
          <label>Database <input required value={form.database} onChange={(e) => setForm({ ...form, database: e.target.value })} /></label>
          <label>Username <input value={form.username} onChange={(e) => setForm({ ...form, username: e.target.value })} /></label>
          <label>Password <input type="password" value={form.password} onChange={(e) => setForm({ ...form, password: e.target.value })} /></label>
        </>
      )}
      {form.type === "duckdb" && (
        <label style={{ gridColumn: "1 / -1" }}>File Path <input required value={form.database} onChange={(e) => setForm({ ...form, database: e.target.value })} placeholder="/path/to/db.duckdb" /></label>
      )}
      {isFile && (
        <label style={{ gridColumn: "1 / -1" }}>
          {form.type === "sqlite" ? "SQLite File Path" : form.type === "csv" ? "CSV File Path or URL" : "Parquet File Path or URL"}
          <input required value={form.path} onChange={(e) => setForm({ ...form, path: e.target.value })}
            placeholder={form.type === "sqlite" ? "./demo/files/orders.sqlite" : form.type === "csv" ? "./demo/files/customers.csv" : "./demo/files/products.parquet"} />
        </label>
      )}
      {form.type === "snowflake" && (
        <>
          <label>Account URL <input required value={form.host} onChange={(e) => setForm({ ...form, host: e.target.value })} placeholder="org-account.snowflakecomputing.com" /></label>
          <label>Warehouse / Database <input required value={form.database} onChange={(e) => setForm({ ...form, database: e.target.value })} placeholder="COMPUTE_WH/MY_DB" /></label>
          <label style={{ gridColumn: "1 / -1" }}>Authentication
            <select value={authType} onChange={(e) => { setAuthType(e.target.value); setAuthFields({}); }}>
              <option value="password">Username / Password</option>
              <option value="key_pair">Key Pair</option>
              <option value="oauth">OAuth Token</option>
            </select>
          </label>
          {authType === "password" && <AuthUserPass authFields={authFields} setAuthFields={setAuthFields} />}
          {authType === "key_pair" && (
            <>
              <label>Username <input required value={authFields.username ?? ""} onChange={(e) => setAuthFields({ ...authFields, username: e.target.value })} /></label>
              <label>Private Key Path <input required value={authFields.private_key_path ?? ""} onChange={(e) => setAuthFields({ ...authFields, private_key_path: e.target.value })} placeholder="/path/to/rsa_key.p8" /></label>
              <label>Passphrase <input type="password" value={authFields.passphrase ?? ""} onChange={(e) => setAuthFields({ ...authFields, passphrase: e.target.value })} placeholder="optional" /></label>
            </>
          )}
          {authType === "oauth" && (
            <label style={{ gridColumn: "1 / -1" }}>Token <input required value={authFields.token ?? ""} onChange={(e) => setAuthFields({ ...authFields, token: e.target.value })} placeholder="${env:SNOWFLAKE_TOKEN}" /></label>
          )}
        </>
      )}
      {form.type === "bigquery" && (
        <>
          <label style={{ gridColumn: "1 / -1" }}>Project ID <input required value={form.database} onChange={(e) => setForm({ ...form, database: e.target.value })} placeholder="my-gcp-project" /></label>
          <label style={{ gridColumn: "1 / -1" }}>Authentication
            <select value={authType} onChange={(e) => { setAuthType(e.target.value); setAuthFields({}); }}>
              <option value="service_account">Service Account Key</option>
              <option value="application_default">Application Default Credentials</option>
            </select>
          </label>
          {authType === "service_account" && (
            <label style={{ gridColumn: "1 / -1" }}>Credentials JSON Path <input required value={authFields.credentials_json ?? ""} onChange={(e) => setAuthFields({ ...authFields, credentials_json: e.target.value })} placeholder="/path/to/service-account.json" /></label>
          )}
        </>
      )}
      {form.type === "databricks" && (
        <>
          <label>Workspace URL <input required value={form.host} onChange={(e) => setForm({ ...form, host: e.target.value })} placeholder="https://dbc-xxxxx.cloud.databricks.com" /></label>
          <label>Catalog <input required value={form.database} onChange={(e) => setForm({ ...form, database: e.target.value })} placeholder="main" /></label>
          <label style={{ gridColumn: "1 / -1" }}>Authentication
            <select value={authType} onChange={(e) => { setAuthType(e.target.value); setAuthFields({}); }}>
              <option value="token">Personal Access Token</option>
              <option value="oauth">OAuth2 (M2M)</option>
            </select>
          </label>
          {authType === "token" && (
            <label style={{ gridColumn: "1 / -1" }}>Access Token <input required value={authFields.access_token ?? ""} onChange={(e) => setAuthFields({ ...authFields, access_token: e.target.value })} placeholder="${env:DATABRICKS_TOKEN}" /></label>
          )}
          {authType === "oauth" && (
            <>
              <label>Client ID <input required value={authFields.client_id ?? ""} onChange={(e) => setAuthFields({ ...authFields, client_id: e.target.value })} /></label>
              <label>Client Secret <input type="password" required value={authFields.client_secret ?? ""} onChange={(e) => setAuthFields({ ...authFields, client_secret: e.target.value })} /></label>
              <label style={{ gridColumn: "1 / -1" }}>Token URL <input required value={authFields.token_url ?? ""} onChange={(e) => setAuthFields({ ...authFields, token_url: e.target.value })} /></label>
            </>
          )}
        </>
      )}
      {form.type === "redshift" && (
        <>
          <label>Host <input required value={form.host} onChange={(e) => setForm({ ...form, host: e.target.value })} placeholder="cluster.xxxxx.region.redshift.amazonaws.com" /></label>
          <label>Port <input type="number" required value={form.port} onChange={(e) => setForm({ ...form, port: +e.target.value })} /></label>
          <label>Database <input required value={form.database} onChange={(e) => setForm({ ...form, database: e.target.value })} placeholder="dev" /></label>
          <label style={{ gridColumn: "1 / -1" }}>Authentication
            <select value={authType} onChange={(e) => { setAuthType(e.target.value); setAuthFields({}); }}>
              <option value="password">Username / Password</option>
              <option value="iam">IAM Credentials</option>
            </select>
          </label>
          {authType === "password" && <AuthUserPass authFields={authFields} setAuthFields={setAuthFields} />}
          {authType === "iam" && (
            <>
              <label>Access Key ID <input required value={authFields.access_key_id ?? ""} onChange={(e) => setAuthFields({ ...authFields, access_key_id: e.target.value })} placeholder="${env:AWS_ACCESS_KEY_ID}" /></label>
              <label>Secret Access Key <input type="password" required value={authFields.secret_access_key ?? ""} onChange={(e) => setAuthFields({ ...authFields, secret_access_key: e.target.value })} placeholder="${env:AWS_SECRET_ACCESS_KEY}" /></label>
              <label>Region <input value={authFields.region ?? "us-east-1"} onChange={(e) => setAuthFields({ ...authFields, region: e.target.value })} /></label>
            </>
          )}
        </>
      )}
      {form.type === "elasticsearch" && (
        <>
          <label>Host <input required value={form.host} onChange={(e) => setForm({ ...form, host: e.target.value })} placeholder="https://localhost:9200" /></label>
          <label>Port <input type="number" required value={form.port} onChange={(e) => setForm({ ...form, port: +e.target.value })} /></label>
          <label style={{ gridColumn: "1 / -1" }}>Authentication
            <select value={authType} onChange={(e) => { setAuthType(e.target.value); setAuthFields({}); }}>
              <option value="none">No Auth</option>
              <option value="basic">Basic Auth</option>
              <option value="api_key">API Key</option>
              <option value="bearer">Bearer Token</option>
            </select>
          </label>
          {authType === "basic" && <AuthUserPass authFields={authFields} setAuthFields={setAuthFields} />}
          {authType === "api_key" && (
            <label style={{ gridColumn: "1 / -1" }}>API Key (base64 id:key) <input required value={authFields.api_key ?? ""} onChange={(e) => setAuthFields({ ...authFields, api_key: e.target.value })} placeholder="${env:ES_API_KEY}" /></label>
          )}
          {authType === "bearer" && (
            <label style={{ gridColumn: "1 / -1" }}>Token <input required value={authFields.token ?? ""} onChange={(e) => setAuthFields({ ...authFields, token: e.target.value })} /></label>
          )}
        </>
      )}
      {isDataLake && (
        <>
          {form.type === "hive" && (
            <>
              <label>Metastore URI <input required value={form.host} onChange={(e) => setForm({ ...form, host: e.target.value })} placeholder="thrift://hive-metastore:9083" /></label>
              <label>Warehouse Path <input required value={form.database} onChange={(e) => setForm({ ...form, database: e.target.value })} placeholder="s3://bucket/warehouse" /></label>
            </>
          )}
          {(form.type === "delta_lake" || form.type === "iceberg") && (
            <>
              <label>Metastore URI <input value={form.host} onChange={(e) => setForm({ ...form, host: e.target.value })} placeholder="thrift://hive-metastore:9083 (optional)" /></label>
              <label>Warehouse Path <input required value={form.database} onChange={(e) => setForm({ ...form, database: e.target.value })} placeholder="s3://bucket/warehouse" /></label>
            </>
          )}
          <label style={{ gridColumn: "1 / -1" }}>Storage Authentication
            <select value={authType} onChange={(e) => { setAuthType(e.target.value); setAuthFields({}); }}>
              <option value="none">None (instance role / local)</option>
              <option value="aws">AWS S3 (Access Key)</option>
              <option value="azure">Azure ADLS</option>
              <option value="gcs">Google Cloud Storage</option>
            </select>
          </label>
          {authType === "aws" && (
            <>
              <label>Access Key ID <input required value={authFields.access_key_id ?? ""} onChange={(e) => setAuthFields({ ...authFields, access_key_id: e.target.value })} placeholder="${env:AWS_ACCESS_KEY_ID}" /></label>
              <label>Secret Access Key <input type="password" required value={authFields.secret_access_key ?? ""} onChange={(e) => setAuthFields({ ...authFields, secret_access_key: e.target.value })} placeholder="${env:AWS_SECRET_ACCESS_KEY}" /></label>
              <label>Region <input value={authFields.region ?? "us-east-1"} onChange={(e) => setAuthFields({ ...authFields, region: e.target.value })} /></label>
              <label>S3 Endpoint (MinIO) <input value={authFields.endpoint ?? ""} onChange={(e) => setAuthFields({ ...authFields, endpoint: e.target.value })} placeholder="optional — for S3-compatible" /></label>
            </>
          )}
          {authType === "azure" && (
            <>
              <label>Storage Account <input required value={authFields.storage_account ?? ""} onChange={(e) => setAuthFields({ ...authFields, storage_account: e.target.value })} /></label>
              <label>Access Key <input type="password" value={authFields.access_key ?? ""} onChange={(e) => setAuthFields({ ...authFields, access_key: e.target.value })} placeholder="shared key (or use SAS)" /></label>
              <label>SAS Token <input value={authFields.sas_token ?? ""} onChange={(e) => setAuthFields({ ...authFields, sas_token: e.target.value })} placeholder="alternative to access key" /></label>
            </>
          )}
          {authType === "gcs" && (
            <label style={{ gridColumn: "1 / -1" }}>Credentials JSON Path <input required value={authFields.credentials_json ?? ""} onChange={(e) => setAuthFields({ ...authFields, credentials_json: e.target.value })} placeholder="/path/to/service-account.json" /></label>
          )}
        </>
      )}
      {form.type === "prometheus" && (
        <>
          <label style={{ gridColumn: "1 / -1" }}>URL <input required value={form.host} onChange={(e) => setForm({ ...form, host: e.target.value })} placeholder="http://prometheus:9090" /></label>
          <label style={{ gridColumn: "1 / -1" }}>Authentication
            <select value={authType} onChange={(e) => { setAuthType(e.target.value); setAuthFields({}); }}>
              <option value="none">No Auth</option>
              <option value="basic">Basic Auth</option>
              <option value="bearer">Bearer Token</option>
            </select>
          </label>
          {authType === "basic" && <AuthUserPass authFields={authFields} setAuthFields={setAuthFields} />}
          {authType === "bearer" && (
            <label style={{ gridColumn: "1 / -1" }}>Token <input required value={authFields.token ?? ""} onChange={(e) => setAuthFields({ ...authFields, token: e.target.value })} /></label>
          )}
        </>
      )}
      {form.type === "google_sheets" && (
        <>
          <label style={{ gridColumn: "1 / -1" }}>Credentials JSON Path <input required value={authFields.credentials_json ?? ""} onChange={(e) => setAuthFields({ ...authFields, credentials_json: e.target.value })} placeholder="/path/to/service-account.json" /></label>
          <label style={{ gridColumn: "1 / -1" }}>Metadata Sheet ID <input value={form.database} onChange={(e) => setForm({ ...form, database: e.target.value })} placeholder="Sheet ID for table definitions" /></label>
        </>
      )}
      {form.type === "openapi" && (
        <>
          <div style={{ gridColumn: "1 / -1", display: "flex", gap: "1rem" }}>
            <label style={{ flexDirection: "row", alignItems: "center", gap: "0.4rem", whiteSpace: "nowrap" }}>
              <input type="radio" name="openapiSpecMode" checked={openapiSpecMode === "path"} onChange={() => setOpenapiSpecMode("path")} style={{ width: "auto" }} />
              Spec path / URL
            </label>
            <label style={{ flexDirection: "row", alignItems: "center", gap: "0.4rem", whiteSpace: "nowrap" }}>
              <input type="radio" name="openapiSpecMode" checked={openapiSpecMode === "inline"} onChange={() => setOpenapiSpecMode("inline")} style={{ width: "auto" }} />
              Write spec inline
            </label>
          </div>
          {openapiSpecMode === "path" ? (
            <label style={{ gridColumn: "1 / -1" }}>Spec Path or URL
              <input required value={openapiSpecPath} onChange={(e) => setOpenapiSpecPath(e.target.value)} placeholder="https://api.example.com/openapi.json or ./spec.yaml" />
            </label>
          ) : (
            <label style={{ gridColumn: "1 / -1" }}>OpenAPI Spec (YAML or JSON)
              <textarea
                required
                value={openapiSpecInline}
                onChange={(e) => setOpenapiSpecInline(e.target.value)}
                placeholder={"openapi: '3.0.0'\ninfo:\n  title: My API\n  version: '1.0'\npaths:\n  /items:\n    get:\n      operationId: listItems\n      responses:\n        '200':\n          description: OK"}
                style={{ fontFamily: "monospace", fontSize: "0.8rem", minHeight: "200px", resize: "vertical" }}
              />
            </label>
          )}
          <label style={{ gridColumn: "1 / -1" }}>Base URL <span style={{ color: "var(--text-muted)", fontSize: "0.8rem" }}>(leave blank to use servers[0].url from spec)</span>
            <input value={openapiBaseUrl} onChange={(e) => setOpenapiBaseUrl(e.target.value)} placeholder="https://api.example.com (optional override)" />
          </label>
          <label>Cache TTL (seconds)
            <input type="number" min={0} value={openapiCacheTtl} onChange={(e) => setOpenapiCacheTtl(e.target.value)} placeholder="300" />
          </label>
          <label style={{ gridColumn: "1 / -1" }}>Authentication
            <select value={authType} onChange={(e) => { setAuthType(e.target.value); setAuthFields({}); }}>
              {API_AUTH_TYPES.map((a) => <option key={a.value} value={a.value}>{a.label}</option>)}
            </select>
          </label>
          {authType === "bearer" && (
            <label style={{ gridColumn: "1 / -1" }}>Token <input required value={authFields.token ?? ""} onChange={(e) => setAuthFields({ ...authFields, token: e.target.value })} placeholder="${env:API_TOKEN}" /></label>
          )}
          {authType === "basic" && <AuthUserPass authFields={authFields} setAuthFields={setAuthFields} />}
          {authType === "api_key" && (
            <>
              <label>Header Name <input required value={authFields.header_name ?? "X-API-Key"} onChange={(e) => setAuthFields({ ...authFields, header_name: e.target.value })} placeholder="X-API-Key" /></label>
              <label>API Key <input required value={authFields.api_key ?? ""} onChange={(e) => setAuthFields({ ...authFields, api_key: e.target.value })} placeholder="${env:API_KEY}" /></label>
            </>
          )}
          <div style={{ gridColumn: "1 / -1", display: "flex", gap: "0.5rem", alignItems: "center" }}>
            <button type="button" onClick={handleOpenapiPreview}
              disabled={openapiPreviewing || (openapiSpecMode === "path" ? !openapiSpecPath : !openapiSpecInline)}>
              {openapiPreviewing ? "Loading..." : "Preview"}
            </button>
            {openapiPreviewError && <span className="error" style={{ fontSize: "0.85rem" }}>{openapiPreviewError}</span>}
          </div>
          {openapiPreview && (
            <div style={{ gridColumn: "1 / -1", fontSize: "0.85rem", color: "var(--text-muted)" }}>
              <strong>{openapiPreview.queries.length} queries</strong>: {openapiPreview.queries.map((q) => q.operation_id).join(", ") || "none"}
              <br />
              <strong>{openapiPreview.mutations.length} mutations</strong>: {openapiPreview.mutations.map((m) => m.operation_id).join(", ") || "none"}
            </div>
          )}
        </>
      )}
      {form.type === "graphql" && (
        <>
          <label style={{ gridColumn: "1 / -1" }}>Endpoint URL <input required value={form.host} onChange={(e) => setForm({ ...form, host: e.target.value })} placeholder="https://api.example.com/graphql" /></label>
          <label>Namespace <input required value={gqlNamespace} onChange={(e) => setGqlNamespace(e.target.value)} placeholder="myapi" /></label>
          <label>Cache TTL (seconds) <input type="number" min={0} value={gqlCacheTtl} onChange={(e) => setGqlCacheTtl(e.target.value)} placeholder="300" /></label>
          <label style={{ gridColumn: "1 / -1" }}>Authentication
            <select value={authType} onChange={(e) => { setAuthType(e.target.value); setAuthFields({}); }}>
              <option value="none">No Auth</option>
              <option value="bearer">Bearer Token</option>
              <option value="basic">Basic Auth</option>
            </select>
          </label>
          {authType === "bearer" && (
            <label style={{ gridColumn: "1 / -1" }}>Token <input required value={authFields.token ?? ""} onChange={(e) => setAuthFields({ ...authFields, token: e.target.value })} placeholder="${env:GQL_TOKEN}" /></label>
          )}
          {authType === "basic" && <AuthUserPass authFields={authFields} setAuthFields={setAuthFields} />}
        </>
      )}
      {form.type === "grpc" && (
        <>
          <label style={{ gridColumn: "1 / -1" }}>Proto Path or URL
            <input required value={grpcProtoPath} onChange={(e) => setGrpcProtoPath(e.target.value)} placeholder="https://api.example.com/service.proto or ./service.proto" />
          </label>
          <label style={{ gridColumn: "1 / -1" }}>Server Address
            <input required value={grpcServerAddress} onChange={(e) => setGrpcServerAddress(e.target.value)} placeholder="api.example.com:50051" />
          </label>
          <label>Namespace
            <input value={grpcNamespace} onChange={(e) => setGrpcNamespace(e.target.value)} placeholder="mygrpc" />
          </label>
          <label>Cache TTL (seconds)
            <input type="number" min={0} value={grpcCacheTtl} onChange={(e) => setGrpcCacheTtl(e.target.value)} placeholder="300" />
          </label>
          <label style={{ flexDirection: "row", alignItems: "center", gap: "0.5rem", whiteSpace: "nowrap" }}>
            <input type="checkbox" checked={grpcTls} onChange={(e) => setGrpcTls(e.target.checked)} style={{ width: "auto" }} />
            TLS
          </label>
          <label style={{ gridColumn: "1 / -1" }}>Import Paths <span style={{ color: "var(--text-muted)", fontSize: "0.8rem" }}>(comma-separated, optional)</span>
            <input value={grpcImportPaths} onChange={(e) => setGrpcImportPaths(e.target.value)} placeholder="/path/to/protos,/another/path" />
          </label>
          <label style={{ gridColumn: "1 / -1" }}>Authentication
            <select value={authType} onChange={(e) => { setAuthType(e.target.value); setAuthFields({}); }}>
              {API_AUTH_TYPES.map((a) => <option key={a.value} value={a.value}>{a.label}</option>)}
            </select>
          </label>
          {authType === "bearer" && (
            <label style={{ gridColumn: "1 / -1" }}>Token <input required value={authFields.token ?? ""} onChange={(e) => setAuthFields({ ...authFields, token: e.target.value })} placeholder="${env:GRPC_TOKEN}" /></label>
          )}
          {authType === "basic" && <AuthUserPass authFields={authFields} setAuthFields={setAuthFields} />}
          {authType === "api_key" && (
            <>
              <label>Header Name <input required value={authFields.header_name ?? "X-API-Key"} onChange={(e) => setAuthFields({ ...authFields, header_name: e.target.value })} placeholder="X-API-Key" /></label>
              <label>API Key <input required value={authFields.api_key ?? ""} onChange={(e) => setAuthFields({ ...authFields, api_key: e.target.value })} placeholder="${env:API_KEY}" /></label>
            </>
          )}
        </>
      )}
      {isKafka && (
        <>
          <label>Bootstrap Servers <input required value={form.host} onChange={(e) => setForm({ ...form, host: e.target.value })} placeholder="kafka:9092" /></label>
          <label>Schema Registry URL <input value={form.database} onChange={(e) => setForm({ ...form, database: e.target.value })} placeholder="http://schema-registry:8081" /></label>
          <label style={{ gridColumn: "1 / -1" }}>Authentication
            <select value={authType} onChange={(e) => { setAuthType(e.target.value); setAuthFields({}); }}>
              {KAFKA_AUTH_TYPES.map((a) => <option key={a.value} value={a.value}>{a.label}</option>)}
            </select>
          </label>
          {authType !== "none" && <AuthUserPass authFields={authFields} setAuthFields={setAuthFields} />}
        </>
      )}
      {editingSourceId && (
        <>
          <label>Naming Convention
            <select value={form.namingConvention} onChange={(e) => setForm({ ...form, namingConvention: e.target.value })}>
              {NAMING_CONVENTIONS.map((nc) => <option key={nc.value} value={nc.value}>{nc.label}</option>)}
            </select>
          </label>
          <label style={{ flexDirection: "row", alignItems: "center", gap: "0.5rem", whiteSpace: "nowrap" }}>
            <input type="checkbox" checked={form.cacheEnabled} onChange={(e) => setForm({ ...form, cacheEnabled: e.target.checked })} style={{ width: "auto" }} />
            Cache Enabled
          </label>
          <label>Cache TTL (seconds)
            <input type="number" min={0} value={form.cacheTtl} onChange={(e) => setForm({ ...form, cacheTtl: e.target.value })} placeholder="inherit global" />
          </label>
        </>
      )}
    </>
  );

  if (loading) return <div className="page">Loading sources...</div>;

  return (
    <div className="page">
      <div className="page-header">
        <h2>Data Sources</h2>
        <FilterInput value={sourceSearch} onChange={setSourceSearch} placeholder="Filter by source ID or type…" />
        {!editingSourceId && (
          <button onClick={() => { if (showForm) { handleCancelForm(); } else { setShowForm(true); } }}>
            {showForm ? "Cancel" : "+ Source"}
          </button>
        )}
      </div>

      {error && <div className="error">{error}</div>}
      {refreshError && <div className="error">Schema refresh failed: {refreshError}</div>}

      {showForm && !editingSourceId && (
        <form className="form-card" onSubmit={form.type === "openapi" ? handleOpenapiRegister : form.type === "grpc" ? handleGrpcRegister : handleCreate}>
          <label>ID <input required value={form.id} onChange={(e) => setForm({ ...form, id: e.target.value })} placeholder="e.g. sales-pg" /></label>
          <label>Type
            <select value={form.type} onChange={(e) => handleTypeChange(e.target.value)}>
              {CATEGORIES.map((cat) => (
                <optgroup key={cat} label={cat}>
                  {SOURCE_TYPES.filter((s) => s.category === cat).map((s) => (
                    <option key={s.value} value={s.value}>{s.label}</option>
                  ))}
                </optgroup>
              ))}
            </select>
          </label>
          {renderFormFields()}
          <button type="submit">Create</button>
        </form>
      )}

      <table className="data-table">
        <thead>
          <tr><th>ID</th><th>Type</th><th>Host</th><th>Port</th><th>Database</th><th>Naming</th><th>Cache</th><th>Effective TTL</th><th></th></tr>
        </thead>
        <tbody>
          {sources.filter((s) => {
            if (!sourceSearch.trim()) return true;
            const q = sourceSearch.toLowerCase();
            return s.id.toLowerCase().includes(q) || s.type.toLowerCase().includes(q);
          }).map((s) => {
            const isExpanded = expanded === s.id;
            const isEditing = editingSourceId === s.id;
            return (
              <React.Fragment key={s.id}>
                <tr
                  onClick={() => {
                    setExpanded(isExpanded ? null : s.id);
                    if (isEditing && isExpanded) { setEditingSourceId(null); handleCancelForm(); }
                  }}
                  style={{ cursor: "pointer", background: isExpanded ? "var(--surface)" : undefined }}
                >
                  <td>{s.id}</td>
                  <td>{SOURCE_TYPES.find((t) => t.value === s.type)?.label ?? s.type}</td>
                  <td>{s.host}</td>
                  <td>{s.port || "—"}</td>
                  <td>{s.database || "—"}</td>
                  <td style={{ color: "var(--text-muted)", fontSize: "0.85rem" }}>{s.namingConvention || "inherit"}</td>
                  <td style={{ color: "var(--text-muted)", fontSize: "0.85rem" }}>{s.cacheEnabled ? "on" : "off"}</td>
                  <td style={{ color: "var(--text-muted)", fontSize: "0.85rem" }}>{getEffectiveTtl(s)}</td>
                  <td style={{ display: "flex", gap: "0.25rem", flexWrap: "wrap" }} onClick={(e) => e.stopPropagation()}>
                    {DISCOVERABLE_TYPES.has(s.type) && (
                      <button onClick={() => { setDiscoverSourceId(s.id); setDiscoverSourceType(s.type); setMappingSourceId(null); }} style={{ padding: "0.25rem 0.5rem", fontSize: "0.75rem" }}>Discover</button>
                    )}
                    {MAPPING_TYPES.has(s.type) && (
                      <button onClick={() => { setMappingSourceId(s.id); setMappingSourceType(s.type); setDiscoverSourceId(null); }} style={{ padding: "0.25rem 0.5rem", fontSize: "0.75rem" }}>Map Table</button>
                    )}
                    {(s.type === "graphql" || s.type === "openapi" || s.type === "grpc") && (
                      <button
                        onClick={() => handleRefreshSchema(s.id, s.type)}
                        disabled={refreshingSourceId === s.id}
                        style={{ padding: "0.25rem 0.5rem", fontSize: "0.75rem" }}
                      >
                        {refreshingSourceId === s.id ? "Refreshing..." : "Refresh Schema"}
                      </button>
                    )}
                  </td>
                </tr>
                {isExpanded && (
                  <tr key={`${s.id}-detail`}>
                    <td colSpan={9} style={{ padding: "0.75rem 1rem", background: "var(--bg)", borderTop: "1px solid var(--border)" }}>
                      {isEditing ? (
                        <form className="form-card" onSubmit={handleCreate} style={{ margin: 0 }}>
                          <label>ID <input required value={form.id} onChange={(e) => setForm({ ...form, id: e.target.value })} /></label>
                          <label>Type
                            <select value={form.type} onChange={(e) => handleTypeChange(e.target.value)}>
                              {CATEGORIES.map((cat) => (
                                <optgroup key={cat} label={cat}>
                                  {SOURCE_TYPES.filter((st) => st.category === cat).map((st) => (
                                    <option key={st.value} value={st.value}>{st.label}</option>
                                  ))}
                                </optgroup>
                              ))}
                            </select>
                          </label>
                          {renderFormFields()}
                          <div style={{ display: "flex", gap: "0.5rem", justifyContent: "flex-end", alignItems: "flex-start", alignSelf: "end" }}>
                            <button type="button" className="btn-icon" title="Cancel" onClick={handleCancelForm}><X size={14} /></button>
                            <button type="submit" className="btn-icon-primary" title="Save"><Save size={14} /></button>
                          </div>
                        </form>
                      ) : (
                        <div style={{ display: "flex", flexDirection: "column", gap: "0.5rem" }}>
                          <dl style={{ display: "grid", gridTemplateColumns: "max-content 1fr", gap: "0.25rem 1rem", margin: 0, color: "var(--text)" }}>
                            {([
                              ["Type", SOURCE_TYPES.find((t) => t.value === s.type)?.label ?? s.type],
                              ["Host", s.host || "—"],
                              ["Port", s.port || "—"],
                              ["Database", s.database || "—"],
                              ["Username", s.username || "—"],
                              ["Naming", s.namingConvention || "inherit (global)"],
                              ["Cache", s.cacheEnabled ? "enabled" : "disabled"],
                              ["Cache TTL", s.cacheTtl != null ? `${s.cacheTtl}s` : "inherit"],
                              ["Effective TTL", getEffectiveTtl(s)],
                            ] as [string, string | number][]).map(([k, v]) => (
                              <React.Fragment key={k}>
                                <dt style={{ color: "var(--text-muted)", fontWeight: 500, fontSize: "0.875rem" }}>{k}</dt>
                                <dd style={{ color: "var(--text)", margin: 0, fontSize: "0.875rem" }}>{v}</dd>
                              </React.Fragment>
                            ))}
                          </dl>
                          <div style={{ display: "flex", gap: "0.5rem", marginTop: "0.25rem" }}>
                            <button className="btn-icon" title="Edit" onClick={(e) => { e.stopPropagation(); handleEdit(s); }}><Pencil size={14} /></button>
                            <ConfirmDialog
                              title={`Delete source "${s.id}"?`}
                              consequence={`This will remove the data source "${s.id}" and may break tables that reference it.`}
                              onConfirm={async () => { await deleteSource(s.id); if (expanded === s.id) setExpanded(null); load(); }}
                            >
                              {(open) => <button className="btn-icon-danger" title="Delete" onClick={(e) => { e.stopPropagation(); open(); }}><Trash2 size={14} /></button>}
                            </ConfirmDialog>
                          </div>
                        </div>
                      )}
                    </td>
                  </tr>
                )}
              </React.Fragment>
            );
          })}
        </tbody>
      </table>

      {discoverSourceId && discoverSourceType && (
        <SchemaDiscovery
          sourceId={discoverSourceId}
          sourceType={discoverSourceType}
          onClose={() => { setDiscoverSourceId(null); setDiscoverSourceType(null); }}
          onRegistered={load}
        />
      )}

      {mappingSourceId && mappingSourceType && (
        <TableMappingBuilder
          sourceType={mappingSourceType}
          onCancel={() => { setMappingSourceId(null); setMappingSourceType(null); }}
          onSave={(_mapping: TableMapping) => {
            setMappingSourceId(null);
            setMappingSourceType(null);
            load();
          }}
        />
      )}
    </div>
  );
}
