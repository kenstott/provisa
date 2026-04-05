// Copyright (c) 2026 Kenneth Stott
// Canary: 55ecea78-4131-4bb4-8a93-71e1bdacea84
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState, useEffect } from "react";
import { fetchSources, deleteSource, createSource, updateSource, updateSourceCache, updateSourceNaming, fetchSettings } from "../api/admin";
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
  // Other
  { value: "google_sheets", label: "Google Sheets", category: "Other", defaultPort: 0 },
  { value: "prometheus", label: "Prometheus", category: "Other", defaultPort: 9090 },
  // API
  { value: "openapi", label: "REST API (OpenAPI)", category: "API", defaultPort: 443 },
  { value: "graphql_api", label: "GraphQL API", category: "API", defaultPort: 443 },
  { value: "grpc_api", label: "gRPC API", category: "API", defaultPort: 443 },
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

interface CacheEdit {
  cacheEnabled: boolean;
  cacheTtl: string;
  dirty: boolean;
  saving: boolean;
}

export function SourcesPage() {
  const [sources, setSources] = useState<Source[]>([]);
  const [loading, setLoading] = useState(true);
  const [showForm, setShowForm] = useState(false);
  const [editingSourceId, setEditingSourceId] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [form, setForm] = useState({
    id: "", type: "postgresql", host: "", port: 5432,
    database: "", username: "", password: "",
  });
  const [authType, setAuthType] = useState("none");
  const [authFields, setAuthFields] = useState<Record<string, string>>({});
  const [settings, setSettings] = useState<PlatformSettings | null>(null);
  const [cacheEdits, setCacheEdits] = useState<Record<string, CacheEdit>>({});
  const [discoverSourceId, setDiscoverSourceId] = useState<string | null>(null);
  const [discoverSourceType, setDiscoverSourceType] = useState<string | null>(null);
  const [mappingSourceId, setMappingSourceId] = useState<string | null>(null);
  const [mappingSourceType, setMappingSourceType] = useState<string | null>(null);

  const load = () => {
    setLoading(true);
    setError(null);
    Promise.all([fetchSources(), fetchSettings()])
      .then(([s, st]) => {
        setSources(s);
        setSettings(st);
        const edits: Record<string, CacheEdit> = {};
        for (const src of s) {
          edits[src.id] = {
            cacheEnabled: src.cacheEnabled,
            cacheTtl: src.cacheTtl != null ? String(src.cacheTtl) : "",
            dirty: false,
            saving: false,
          };
        }
        setCacheEdits(edits);
      })
      .catch((e) => setError(e.message))
      .finally(() => setLoading(false));
  };
  useEffect(load, []);

  const updateCacheEdit = (sourceId: string, patch: Partial<CacheEdit>) => {
    setCacheEdits((prev) => ({
      ...prev,
      [sourceId]: { ...prev[sourceId], ...patch, dirty: true },
    }));
  };

  const handleSaveCache = async (sourceId: string) => {
    const edit = cacheEdits[sourceId];
    setCacheEdits((prev) => ({ ...prev, [sourceId]: { ...prev[sourceId], saving: true } }));
    setError(null);
    try {
      const ttlValue = edit.cacheTtl.trim() === "" ? null : parseInt(edit.cacheTtl, 10);
      if (ttlValue !== null && isNaN(ttlValue)) throw new Error("TTL must be a number");
      const result = await updateSourceCache(sourceId, edit.cacheEnabled, ttlValue);
      if (!result.success) throw new Error(result.message);
      setCacheEdits((prev) => ({ ...prev, [sourceId]: { ...prev[sourceId], dirty: false, saving: false } }));
      load();
    } catch (e: any) {
      setError(e.message);
      setCacheEdits((prev) => ({ ...prev, [sourceId]: { ...prev[sourceId], saving: false } }));
    }
  };

  const handleNamingChange = async (sourceId: string, value: string) => {
    setError(null);
    try {
      const result = await updateSourceNaming(sourceId, value === "" ? null : value);
      if (!result.success) throw new Error(result.message);
      load();
    } catch (e: any) {
      setError(e.message);
    }
  };

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
    });
    setAuthType("none");
    setAuthFields({});
    setEditingSourceId(s.id);
    setShowForm(true);
  };

  const handleCancelForm = () => {
    setShowForm(false);
    setEditingSourceId(null);
    setForm({ id: "", type: "postgresql", host: "", port: 5432, database: "", username: "", password: "" });
    setAuthType("none");
    setAuthFields({});
  };

  const handleCreate = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);
    try {
      if (editingSourceId) {
        const result = await updateSource(form);
        if (!result.success) throw new Error(result.message);
      } else {
        await createSource(form);
      }
      handleCancelForm();
      load();
    } catch (err: any) {
      setError(err.message);
    }
  };

  const category = getCategory(form.type);
  const isApi = category === "API";
  const isKafka = category === "Streaming";
  const isSimpleRdbms = SIMPLE_RDBMS.has(form.type);
  const isDataLake = DATA_LAKE.has(form.type);

  if (loading) return <div className="page">Loading sources...</div>;

  return (
    <div className="page">
      <div className="page-header">
        <h2>Data Sources</h2>
        <button onClick={() => { if (showForm) { handleCancelForm(); } else { setShowForm(true); } }}>{showForm ? "Cancel" : "Add Source"}</button>
      </div>

      {error && <div className="error">{error}</div>}

      {showForm && (
        <form className="form-card" onSubmit={handleCreate}>
          <label>ID <input required value={form.id} onChange={(e) => setForm({ ...form, id: e.target.value })} placeholder="e.g. sales-pg" disabled={!!editingSourceId} /></label>
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

          {/* ── Simple RDBMS / NoSQL / Analytics ── */}
          {isSimpleRdbms && (
            <>
              <label>Host <input required value={form.host} onChange={(e) => setForm({ ...form, host: e.target.value })} placeholder="localhost" /></label>
              <label>Port <input type="number" required value={form.port} onChange={(e) => setForm({ ...form, port: +e.target.value })} /></label>
              <label>Database <input required value={form.database} onChange={(e) => setForm({ ...form, database: e.target.value })} /></label>
              <label>Username <input value={form.username} onChange={(e) => setForm({ ...form, username: e.target.value })} /></label>
              <label>Password <input type="password" value={form.password} onChange={(e) => setForm({ ...form, password: e.target.value })} /></label>
            </>
          )}

          {/* ── DuckDB ── */}
          {form.type === "duckdb" && (
            <label style={{ gridColumn: "1 / -1" }}>File Path <input required value={form.database} onChange={(e) => setForm({ ...form, database: e.target.value })} placeholder="/path/to/db.duckdb" /></label>
          )}

          {/* ── Snowflake ── */}
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

          {/* ── BigQuery ── */}
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

          {/* ── Databricks ── */}
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

          {/* ── Redshift ── */}
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

          {/* ── Elasticsearch ── */}
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

          {/* ── Data Lake (Delta Lake, Iceberg, Hive) ── */}
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

          {/* ── Prometheus ── */}
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

          {/* ── Google Sheets ── */}
          {form.type === "google_sheets" && (
            <>
              <label style={{ gridColumn: "1 / -1" }}>Credentials JSON Path <input required value={authFields.credentials_json ?? ""} onChange={(e) => setAuthFields({ ...authFields, credentials_json: e.target.value })} placeholder="/path/to/service-account.json" /></label>
              <label style={{ gridColumn: "1 / -1" }}>Metadata Sheet ID <input value={form.database} onChange={(e) => setForm({ ...form, database: e.target.value })} placeholder="Sheet ID for table definitions" /></label>
            </>
          )}

          {/* ── API Sources ── */}
          {isApi && (
            <>
              <label>Base URL <input required value={form.host} onChange={(e) => setForm({ ...form, host: e.target.value })} placeholder="https://api.example.com" /></label>
              <label>Spec URL <input value={form.database} onChange={(e) => setForm({ ...form, database: e.target.value })} placeholder="https://api.example.com/openapi.json" /></label>
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
                  <label>API Key <input required value={authFields.key ?? ""} onChange={(e) => setAuthFields({ ...authFields, key: e.target.value })} placeholder="${env:API_KEY}" /></label>
                  <label>Header/Param Name <input required value={authFields.name ?? ""} onChange={(e) => setAuthFields({ ...authFields, name: e.target.value })} placeholder="X-API-Key" /></label>
                  <label>Location
                    <select value={authFields.location ?? "header"} onChange={(e) => setAuthFields({ ...authFields, location: e.target.value })}>
                      <option value="header">Header</option>
                      <option value="query">Query Parameter</option>
                    </select>
                  </label>
                </>
              )}
              {authType === "oauth2_client_credentials" && (
                <>
                  <label>Client ID <input required value={authFields.client_id ?? ""} onChange={(e) => setAuthFields({ ...authFields, client_id: e.target.value })} /></label>
                  <label>Client Secret <input type="password" required value={authFields.client_secret ?? ""} onChange={(e) => setAuthFields({ ...authFields, client_secret: e.target.value })} /></label>
                  <label>Token URL <input required value={authFields.token_url ?? ""} onChange={(e) => setAuthFields({ ...authFields, token_url: e.target.value })} placeholder="https://auth.example.com/oauth/token" /></label>
                  <label>Scope <input value={authFields.scope ?? ""} onChange={(e) => setAuthFields({ ...authFields, scope: e.target.value })} placeholder="optional" /></label>
                </>
              )}
              {authType === "custom_headers" && (
                <label style={{ gridColumn: "1 / -1" }}>Headers (JSON) <input required value={authFields.headers_json ?? ""} onChange={(e) => setAuthFields({ ...authFields, headers_json: e.target.value })} placeholder='{"X-Custom": "${env:TOKEN}"}' /></label>
              )}
            </>
          )}

          {/* ── Kafka ── */}
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

          <button type="submit">{editingSourceId ? "Save" : "Create"}</button>
          {editingSourceId && <button type="button" onClick={handleCancelForm}>Cancel</button>}
        </form>
      )}

      <table className="data-table">
        <thead>
          <tr><th>ID</th><th>Type</th><th>Host</th><th>Port</th><th>Database</th><th>Naming</th><th>Cache Enabled</th><th>Cache TTL</th><th>Effective TTL</th><th>Actions</th></tr>
        </thead>
        <tbody>
          {sources.map((s) => {
            const edit = cacheEdits[s.id];
            return (
              <tr key={s.id}>
                <td>{s.id}</td>
                <td>{SOURCE_TYPES.find((t) => t.value === s.type)?.label ?? s.type}</td>
                <td>{s.host}</td>
                <td>{s.port || "—"}</td>
                <td>{s.database || "—"}</td>
                <td>
                  <select
                    value={s.namingConvention ?? ""}
                    onChange={(e) => handleNamingChange(s.id, e.target.value)}
                    style={{ fontSize: "0.85rem" }}
                  >
                    {NAMING_CONVENTIONS.map((nc) => (
                      <option key={nc.value} value={nc.value}>{nc.label}</option>
                    ))}
                  </select>
                </td>
                <td>
                  <input
                    type="checkbox"
                    checked={edit?.cacheEnabled ?? s.cacheEnabled}
                    onChange={(e) => updateCacheEdit(s.id, { cacheEnabled: e.target.checked })}
                  />
                </td>
                <td>
                  <input
                    type="number"
                    min={0}
                    value={edit?.cacheTtl ?? (s.cacheTtl != null ? String(s.cacheTtl) : "")}
                    onChange={(e) => updateCacheEdit(s.id, { cacheTtl: e.target.value })}
                    placeholder="inherit"
                    style={{ width: "5rem" }}
                  />
                </td>
                <td style={{ color: "var(--text-muted)", fontSize: "0.85rem" }}>{getEffectiveTtl(s)}</td>
                <td style={{ display: "flex", gap: "0.25rem", flexWrap: "wrap" }}>
                  {edit?.dirty && (
                    <button
                      onClick={() => handleSaveCache(s.id)}
                      disabled={edit.saving}
                      style={{ padding: "0.25rem 0.5rem", fontSize: "0.75rem" }}
                    >
                      {edit.saving ? "Saving..." : "Save Cache"}
                    </button>
                  )}
                  {DISCOVERABLE_TYPES.has(s.type) && (
                    <button
                      onClick={() => { setDiscoverSourceId(s.id); setDiscoverSourceType(s.type); setMappingSourceId(null); }}
                      style={{ padding: "0.25rem 0.5rem", fontSize: "0.75rem" }}
                    >
                      Discover
                    </button>
                  )}
                  {MAPPING_TYPES.has(s.type) && (
                    <button
                      onClick={() => { setMappingSourceId(s.id); setMappingSourceType(s.type); setDiscoverSourceId(null); }}
                      style={{ padding: "0.25rem 0.5rem", fontSize: "0.75rem" }}
                    >
                      Map Table
                    </button>
                  )}
                  <button
                    onClick={() => handleEdit(s)}
                    style={{ padding: "0.25rem 0.5rem", fontSize: "0.75rem" }}
                  >
                    Edit
                  </button>
                  <ConfirmDialog
                    title={`Delete source "${s.id}"?`}
                    consequence={`This will remove the data source "${s.id}" and may break tables that reference it.`}
                    onConfirm={async () => { await deleteSource(s.id); load(); }}
                  >
                    {(open) => <button className="destructive" onClick={open}>Delete</button>}
                  </ConfirmDialog>
                </td>
              </tr>
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
            // TableMapping is saved via the mapping builder's onSave callback.
            // In a full implementation this would call an API endpoint to persist
            // the mapping configuration. For now, close the panel.
            setMappingSourceId(null);
            setMappingSourceType(null);
            load();
          }}
        />
      )}
    </div>
  );
}
