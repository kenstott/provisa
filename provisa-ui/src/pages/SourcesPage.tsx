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
import { useNavigate, useSearchParams } from "react-router-dom";
import { Trash2, Pencil, Save, X, ArrowRight } from "lucide-react";
import { FilterInput } from "../components/admin/FilterInput";
import { fetchSettings } from "../api/admin";
import type { PlatformSettings } from "../api/admin";
import {
  useSources,
  useCreateSource,
  useUpdateSource,
  useRenameSource,
  useDeleteSource,
  useUpdateSourceCache,
  useUpdateSourceNaming,
  useUpdateSourceAllowedDomains,
} from "../hooks/useAdminQueries";
import { ConfirmDialog } from "../components/ConfirmDialog";
import { SchemaDiscovery } from "../components/SchemaDiscovery";
import { TableMappingBuilder } from "../components/TableMappingBuilder";
import type { TableMapping } from "../components/TableMappingBuilder";
import type { Source } from "../types/admin";

/** Source types that support schema discovery via adapter. */
const DISCOVERABLE_TYPES = new Set(["mongodb", "elasticsearch", "cassandra", "prometheus"]);

/** Source types that need a table mapping builder (NoSQL / non-relational). */
const MAPPING_TYPES = new Set([
  "redis",
  "mongodb",
  "elasticsearch",
  "cassandra",
  "prometheus",
  "accumulo",
]);

const SOURCE_TYPES = [
  // Subscriptions
  {
    value: "govdata",
    label: "AskAmerica (US Government Data)",
    category: "Subscriptions",
    defaultPort: 0,
  },
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
  // Graph
  { value: "neo4j", label: "Neo4j", category: "Graph", defaultPort: 7474 },
  { value: "sparql", label: "SPARQL", category: "Graph", defaultPort: 443 },
  // File
  { value: "sqlite", label: "SQLite", category: "File", defaultPort: 0 },
  { value: "csv", label: "CSV File", category: "File", defaultPort: 0 },
  { value: "parquet", label: "Parquet File", category: "File", defaultPort: 0 },
  { value: "files", label: "File Directory (CSV/Parquet/XLSX/JSON)", category: "File", defaultPort: 0 },
  // Other
  { value: "google_sheets", label: "Google Sheets", category: "Other", defaultPort: 0 },
  { value: "prometheus", label: "Prometheus", category: "Other", defaultPort: 9090 },
  // API
  { value: "openapi", label: "REST API (OpenAPI)", category: "API", defaultPort: 443 },
  { value: "graphql", label: "GraphQL", category: "API", defaultPort: 443 },
  { value: "grpc", label: "gRPC", category: "API", defaultPort: 50051 },
  // Streaming
  { value: "kafka", label: "Kafka", category: "Streaming", defaultPort: 9092 },
  // Enterprise SaaS
  { value: "sharepoint", label: "SharePoint", category: "Enterprise", defaultPort: 0 },
  { value: "splunk", label: "Splunk", category: "Enterprise", defaultPort: 8089 },
  // Public Data
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
const DB_DESCRIPTION_TYPES = new Set(["postgresql", "mysql", "mariadb", "sqlserver"]);

// Which source types use simple host/port/db/user/pass
const SIMPLE_RDBMS = new Set([
  "postgresql",
  "mysql",
  "singlestore",
  "mariadb",
  "sqlserver",
  "oracle",
  "clickhouse",
  "pinot",
  "druid",
  "mongodb",
  "cassandra",
  "redis",
  "kudu",
  "accumulo",
]);

// Data lake types
const DATA_LAKE = new Set(["delta_lake", "iceberg", "hive"]);

/** Reusable auth fields for username/password */
function AuthUserPass({
  authFields,
  setAuthFields,
}: {
  authFields: Record<string, string>;
  setAuthFields: (f: Record<string, string>) => void;
}) {
  return (
    <>
      <label>
        Username{" "}
        <input
          required
          value={authFields.username ?? ""}
          onChange={(e) => setAuthFields({ ...authFields, username: e.target.value })}
        />
      </label>
      <label>
        Password{" "}
        <input
          type="password"
          required
          value={authFields.password ?? ""}
          onChange={(e) => setAuthFields({ ...authFields, password: e.target.value })}
        />
      </label>
    </>
  );
}

export function SourcesPage() {
  const navigate = useNavigate();
  const [searchParams, setSearchParams] = useSearchParams();
  const { sources, loading: sourcesLoading, refetch: refetchSources } = useSources();
  const { createSource } = useCreateSource();
  const { updateSource } = useUpdateSource();
  const { renameSource } = useRenameSource();
  const { deleteSource } = useDeleteSource();
  const { updateSourceCache } = useUpdateSourceCache();
  const { updateSourceNaming } = useUpdateSourceNaming();
  const { updateSourceAllowedDomains } = useUpdateSourceAllowedDomains();
  const [settingsLoading, setSettingsLoading] = useState(true);
  const [showForm, setShowForm] = useState(false);
  const [editingSourceId, setEditingSourceId] = useState<string | null>(null);
  const [expanded, setExpanded] = useState<string | null>(() => searchParams.get("expanded"));
  const [error, setError] = useState<string | null>(null);
  const [sourceSearch, setSourceSearch] = useState(() => searchParams.get("search") ?? "");
  const [page, setPage] = useState(0);
  const PAGE_SIZE = 50;
  const [form, setForm] = useState({
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
    path: "" as string,
    allowedDomains: "" as string,
    description: "" as string,
  });
  const [authType, setAuthType] = useState("none");
  const [authFields, setAuthFields] = useState<Record<string, string>>({});
  const [settings, setSettings] = useState<PlatformSettings | null>(null);
  const domainsEnabled = settings?.naming.use_domains !== false;
  const [discoverSourceId, setDiscoverSourceId] = useState<string | null>(null);
  const [discoverSourceType, setDiscoverSourceType] = useState<string | null>(null);
  const [mappingSourceId, setMappingSourceId] = useState<string | null>(null);
  const [mappingSourceType, setMappingSourceType] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [spAuthType, setSpAuthType] = useState("CLIENT_CREDENTIALS");
  const [spCertPath, setSpCertPath] = useState("");
  const [spCertPassword, setSpCertPassword] = useState("");
  const [spUsername, setSpUsername] = useState("");
  const [spPassword, setSpPassword] = useState("");
  const [splunkDisableSsl, setSplunkDisableSsl] = useState(false);

  const updateSearch = (v: string) => {
    setSourceSearch(v);
    setPage(0);
    setSearchParams(
      (p) => {
        const n = new URLSearchParams(p);
        if (v) n.set("search", v);
        else n.delete("search");
        return n;
      },
      { replace: true },
    );
  };
  const updateExpanded = (v: string | null) => {
    setExpanded(v);
    setSearchParams(
      (p) => {
        const n = new URLSearchParams(p);
        if (v) n.set("expanded", v);
        else n.delete("expanded");
        return n;
      },
      { replace: true },
    );
  };

  useEffect(() => {
    if (editingSourceId) return;
    /* eslint-disable-next-line react-hooks/set-state-in-effect --
       autofills a default description from the chosen type/host/database
       when the user hasn't typed one; cannot be pure-derived because the
       field stays user-editable after the default is applied */
    setForm((prev) => {
      if (prev.description) return prev;
      const typeLabel = SOURCE_TYPES.find((s) => s.value === prev.type)?.label ?? prev.type;
      const parts = [typeLabel];
      if (prev.host) parts.push(`on ${prev.host}`);
      if (prev.database) parts.push(`/ ${prev.database}`);
      return { ...prev, description: parts.join(" ") };
    });
  }, [form.type, form.host, form.database, editingSourceId]);

  useEffect(() => {
    if (editingSourceId) return;
    if (!DB_DESCRIPTION_TYPES.has(form.type)) return;
    if (!form.host || !form.database || !form.username || !form.password) return;
    fetch("/admin/source-meta/db-description", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        type: form.type,
        host: form.host,
        port: form.port,
        database: form.database,
        username: form.username,
        password: form.password,
      }),
    })
      .then((r) => (r.ok ? r.json() : null))
      .then((data) => {
        if (data?.description) setForm((prev) => ({ ...prev, description: data.description }));
      })
      .catch(() => {});
  }, [
    form.type,
    form.host,
    form.port,
    form.database,
    form.username,
    form.password,
    editingSourceId,
  ]);

  const load = () => {
    setSettingsLoading(true);
    setError(null);
    Promise.all([refetchSources(), fetchSettings()])
      .then(([, st]) => {
        setSettings(st);
      })
      .catch((e) => setError(e.message))
      .finally(() => setSettingsLoading(false));
  };
  useEffect(() => {
    fetchSettings()
      .then(setSettings)
      .catch((e) => setError(e.message))
      .finally(() => setSettingsLoading(false));
  }, []);

  const loading = sourcesLoading || settingsLoading;

  const getEffectiveTtl = (source: Source): string => {
    if (source.cacheTtl != null) return `${source.cacheTtl}s (custom)`;
    if (settings) return `${settings.cache.default_ttl}s (global)`;
    return "default";
  };

  const resetSpFields = () => {
    setSpAuthType("CLIENT_CREDENTIALS");
    setSpCertPath("");
    setSpCertPassword("");
    setSpUsername("");
    setSpPassword("");
  };

  const handleTypeChange = (type: string) => {
    setForm({ ...form, type, port: getDefaultPort(type), description: "" });
    setAuthType("none");
    setAuthFields({});
    resetSpFields();
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
      gqlNamingConvention: s.gqlNamingConvention ?? "",
      cacheTtl: s.cacheTtl != null ? String(s.cacheTtl) : "",
      cacheEnabled: s.cacheEnabled,
      path: s.path ?? "",
      allowedDomains: (s.allowedDomains ?? []).join(", "),
      description: s.description ?? "",
    });
    setAuthType("none");
    setAuthFields({});
    if (s.type === "sharepoint" && s.mappingJson) {
      try {
        const m = JSON.parse(s.mappingJson) as Record<string, string>;
        setSpAuthType(m.auth_type ?? "CLIENT_CREDENTIALS");
        setSpCertPath(m.certificate_path ?? "");
        setSpCertPassword(m.certificate_password ?? "");
        setSpUsername(m.sp_username ?? "");
        setSpPassword(m.sp_password ?? "");
      } catch {
        setSpAuthType("CLIENT_CREDENTIALS");
        setSpCertPath("");
        setSpCertPassword("");
        setSpUsername("");
        setSpPassword("");
      }
    } else {
      setSpAuthType("CLIENT_CREDENTIALS");
      setSpCertPath("");
      setSpCertPassword("");
      setSpUsername("");
      setSpPassword("");
    }
    if (s.type === "splunk" && s.mappingJson) {
      try {
        const m = JSON.parse(s.mappingJson) as Record<string, unknown>;
        setSplunkDisableSsl(!!m.disable_ssl_validation);
      } catch {
        setSplunkDisableSsl(false);
      }
    } else {
      setSplunkDisableSsl(false);
    }
    if (s.type === "govdata" && s.database) {
      const storedSchemas = s.database
        .split(",")
        .map((x: string) => x.trim())
        .filter(Boolean);
      setGovdataSubjects(
        GOVDATA_SUBJECTS.filter((subj) =>
          subj.schemas.some((schema) => storedSchemas.includes(schema)),
        ).map((subj) => subj.value),
      );
    } else {
      setGovdataSubjects([]);
    }
    setEditingSourceId(s.id);
    updateExpanded(s.id);
    setShowForm(false);
  };

  const handleCancelForm = () => {
    setShowForm(false);
    setEditingSourceId(null);
    setForm({
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
      path: "",
      allowedDomains: "",
      description: "",
    });
    setAuthType("none");
    setAuthFields({});
    resetSpFields();
    setGovdataSubjects([]);
  };

  const handleCreate = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);
    setSubmitting(true);
    try {
      const { gqlNamingConvention: _nc, cacheTtl: _ct, cacheEnabled: _ce, ...coreForm } = form;
      const spMappingJson =
        form.type === "sharepoint"
          ? JSON.stringify({
              auth_type: spAuthType,
              ...(spAuthType === "CERTIFICATE"
                ? { certificate_path: spCertPath, certificate_password: spCertPassword }
                : {}),
              ...(spAuthType === "USERNAME_PASSWORD"
                ? { sp_username: spUsername, sp_password: spPassword }
                : {}),
            })
          : form.type === "splunk" && splunkDisableSsl
            ? JSON.stringify({ disable_ssl_validation: true })
            : undefined;
      const sourcePayload = {
        ...coreForm,
        path: FILE_SOURCES.has(form.type) || form.type === "files" ? form.path || null : null,
        database:
          form.type === "govdata"
            ? Array.from(
                new Set([
                  ...govdataSubjects.flatMap(
                    (sv) => GOVDATA_SUBJECTS.find((s) => s.value === sv)?.schemas ?? [],
                  ),
                  "ref",
                  "geo",
                ]),
              ).join(",")
            : coreForm.database,
        ...(spMappingJson !== undefined ? { mappingJson: spMappingJson } : {}),
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
        const namingResult = await updateSourceNaming(
          effectiveId,
          form.gqlNamingConvention === "" ? null : form.gqlNamingConvention,
        );
        if (!namingResult.success) throw new Error(namingResult.message);
        const parsedDomains = form.allowedDomains
          .split(",")
          .map((d) => d.trim())
          .filter(Boolean);
        const domainsResult = await updateSourceAllowedDomains(effectiveId, parsedDomains);
        if (!domainsResult.success) throw new Error(domainsResult.message);
      } else {
        const { allowedDomains: _ad, ...createPayload } = sourcePayload as typeof sourcePayload & {
          allowedDomains?: unknown;
        };
        void _ad;
        const createResult = await createSource(
          createPayload as Parameters<typeof createSource>[0],
        );
        if (!createResult.success) throw new Error(createResult.message);
        const parsedDomainsCreate = form.allowedDomains
          .split(",")
          .map((d) => d.trim())
          .filter(Boolean);
        if (parsedDomainsCreate.length > 0) {
          const domainsResult = await updateSourceAllowedDomains(form.id, parsedDomainsCreate);
          if (!domainsResult.success) throw new Error(domainsResult.message);
        }
      }
      handleCancelForm();
      load();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setSubmitting(false);
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
  const [openapiPreview, setOpenapiPreview] = useState<{
    queries: { operation_id: string }[];
    mutations: { operation_id: string }[];
    spec_description?: string;
  } | null>(null);
  const [openapiPreviewing, setOpenapiPreviewing] = useState(false);
  const [openapiPreviewError, setOpenapiPreviewError] = useState<string | null>(null);

  // GovData-specific state — subjects map to schema groups; "ref" is always included silently
  const [govdataSubjects, setGovdataSubjects] = useState<string[]>([]);
  const GOVDATA_SUBJECTS: { value: string; label: string; schemas: string[] }[] = [
    { value: "COMMERCE", label: "Commerce", schemas: ["sec", "patents"] },
    { value: "ECONOMY", label: "Economy", schemas: ["econ"] },
    { value: "EDUCATION", label: "Education", schemas: ["census", "edu"] },
    { value: "HEALTH", label: "Health", schemas: ["health"] },
    { value: "CYBER", label: "Cyber", schemas: ["cyber_threat", "cyber_vuln"] },
    { value: "PUBLIC_SAFETY", label: "Public Safety", schemas: ["crime"] },
    { value: "ENVIRONMENT", label: "Environment", schemas: ["lands"] },
    { value: "WEATHER", label: "Weather", schemas: ["weather"] },
    { value: "GOVERNMENT", label: "Government", schemas: ["fedregister", "fec"] },
  ];

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
      const url =
        sourceType === "openapi"
          ? `/admin/openapi/refresh/${sourceId}`
          : sourceType === "grpc"
            ? `/admin/grpc-remote/refresh/${sourceId}`
            : `/admin/sources/graphql-remote/${sourceId}/refresh`;
      const resp = await fetch(url, { method: "POST" });
      if (!resp.ok) {
        const body = await resp.json().catch(() => ({ detail: resp.statusText }));
        throw new Error(body.detail ?? resp.statusText);
      }
    } catch (err) {
      setRefreshError(err instanceof Error ? err.message : String(err));
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
            : { spec_path: openapiSpecPath },
        ),
      });
      if (!resp.ok) {
        const body = await resp.json().catch(() => ({ detail: resp.statusText }));
        throw new Error(body.detail ?? resp.statusText);
      }
      const data = await resp.json();
      setOpenapiPreview(data);
      if (data.spec_description) {
        setForm((prev) => ({ ...prev, description: prev.description || data.spec_description }));
      }
    } catch (err) {
      setOpenapiPreviewError(err instanceof Error ? err.message : String(err));
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
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
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
          import_paths: grpcImportPaths
            ? grpcImportPaths
                .split(",")
                .map((s) => s.trim())
                .filter(Boolean)
            : [],
          tls: grpcTls,
          auth_config: authType !== "none" ? { type: authType, ...authFields } : null,
          cache_ttl: parseInt(grpcCacheTtl, 10) || 300,
        }),
      });
      if (!resp.ok) {
        const body = await resp.json().catch(() => ({ detail: resp.statusText }));
        throw new Error(body.detail ?? resp.statusText);
      }
      await createSource({
        id: form.id,
        type: form.type,
        host: form.host,
        port: form.port,
        database: form.database,
        username: form.username,
        password: form.password,
        path: null,
      });
      handleCancelForm();
      load();
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  };

  const category = getCategory(form.type);
  const isKafka = category === "Streaming";
  const isFile = FILE_SOURCES.has(form.type);
  const isSimpleRdbms = SIMPLE_RDBMS.has(form.type);
  const isDataLake = DATA_LAKE.has(form.type);

  const renderFormFields = () => (
    <>
      <label style={{ gridColumn: "1 / -1" }}>
        Description
        <input
          value={form.description}
          onChange={(e) => setForm({ ...form, description: e.target.value })}
          placeholder="Brief description of this data source"
        />
      </label>
      {isSimpleRdbms && (
        <>
          <label>
            Host{" "}
            <input
              required
              value={form.host}
              onChange={(e) => setForm({ ...form, host: e.target.value })}
              placeholder="localhost"
            />
          </label>
          <label>
            Port{" "}
            <input
              type="number"
              required
              value={form.port}
              onChange={(e) => setForm({ ...form, port: +e.target.value })}
            />
          </label>
          <label>
            Database{" "}
            <input
              required
              value={form.database}
              onChange={(e) => setForm({ ...form, database: e.target.value })}
            />
          </label>
          <label>
            Username{" "}
            <input
              value={form.username}
              onChange={(e) => setForm({ ...form, username: e.target.value })}
            />
          </label>
          <label>
            Password{" "}
            <input
              type="password"
              value={form.password}
              onChange={(e) => setForm({ ...form, password: e.target.value })}
            />
          </label>
        </>
      )}
      {form.type === "duckdb" && (
        <label style={{ gridColumn: "1 / -1" }}>
          File Path{" "}
          <input
            required
            value={form.database}
            onChange={(e) => setForm({ ...form, database: e.target.value })}
            placeholder="/path/to/db.duckdb"
          />
        </label>
      )}
      {isFile && (
        <label style={{ gridColumn: "1 / -1" }}>
          {form.type === "sqlite"
            ? "SQLite File Path"
            : form.type === "csv"
              ? "CSV File Path or URL"
              : "Parquet File Path or URL"}
          <input
            required
            value={form.path}
            onChange={(e) => setForm({ ...form, path: e.target.value })}
            placeholder={
              form.type === "sqlite"
                ? "./demo/files/orders.sqlite"
                : form.type === "csv"
                  ? "./demo/files/customers.csv"
                  : "./demo/files/products.parquet"
            }
          />
        </label>
      )}
      {form.type === "snowflake" && (
        <>
          <label>
            Account URL{" "}
            <input
              required
              value={form.host}
              onChange={(e) => setForm({ ...form, host: e.target.value })}
              placeholder="org-account.snowflakecomputing.com"
            />
          </label>
          <label>
            Warehouse / Database{" "}
            <input
              required
              value={form.database}
              onChange={(e) => setForm({ ...form, database: e.target.value })}
              placeholder="COMPUTE_WH/MY_DB"
            />
          </label>
          <label style={{ gridColumn: "1 / -1" }}>
            Authentication
            <select
              value={authType}
              onChange={(e) => {
                setAuthType(e.target.value);
                setAuthFields({});
              }}
            >
              <option value="password">Username / Password</option>
              <option value="key_pair">Key Pair</option>
              <option value="oauth">OAuth Token</option>
            </select>
          </label>
          {authType === "password" && (
            <AuthUserPass authFields={authFields} setAuthFields={setAuthFields} />
          )}
          {authType === "key_pair" && (
            <>
              <label>
                Username{" "}
                <input
                  required
                  value={authFields.username ?? ""}
                  onChange={(e) => setAuthFields({ ...authFields, username: e.target.value })}
                />
              </label>
              <label>
                Private Key Path{" "}
                <input
                  required
                  value={authFields.private_key_path ?? ""}
                  onChange={(e) =>
                    setAuthFields({ ...authFields, private_key_path: e.target.value })
                  }
                  placeholder="/path/to/rsa_key.p8"
                />
              </label>
              <label>
                Passphrase{" "}
                <input
                  type="password"
                  value={authFields.passphrase ?? ""}
                  onChange={(e) => setAuthFields({ ...authFields, passphrase: e.target.value })}
                  placeholder="optional"
                />
              </label>
            </>
          )}
          {authType === "oauth" && (
            <label style={{ gridColumn: "1 / -1" }}>
              Token{" "}
              <input
                required
                value={authFields.token ?? ""}
                onChange={(e) => setAuthFields({ ...authFields, token: e.target.value })}
                placeholder="${env:SNOWFLAKE_TOKEN}"
              />
            </label>
          )}
        </>
      )}
      {form.type === "bigquery" && (
        <>
          <label style={{ gridColumn: "1 / -1" }}>
            Project ID{" "}
            <input
              required
              value={form.database}
              onChange={(e) => setForm({ ...form, database: e.target.value })}
              placeholder="my-gcp-project"
            />
          </label>
          <label style={{ gridColumn: "1 / -1" }}>
            Authentication
            <select
              value={authType}
              onChange={(e) => {
                setAuthType(e.target.value);
                setAuthFields({});
              }}
            >
              <option value="service_account">Service Account Key</option>
              <option value="application_default">Application Default Credentials</option>
            </select>
          </label>
          {authType === "service_account" && (
            <label style={{ gridColumn: "1 / -1" }}>
              Credentials JSON Path{" "}
              <input
                required
                value={authFields.credentials_json ?? ""}
                onChange={(e) => setAuthFields({ ...authFields, credentials_json: e.target.value })}
                placeholder="/path/to/service-account.json"
              />
            </label>
          )}
        </>
      )}
      {form.type === "databricks" && (
        <>
          <label>
            Workspace URL{" "}
            <input
              required
              value={form.host}
              onChange={(e) => setForm({ ...form, host: e.target.value })}
              placeholder="https://dbc-xxxxx.cloud.databricks.com"
            />
          </label>
          <label>
            Catalog{" "}
            <input
              required
              value={form.database}
              onChange={(e) => setForm({ ...form, database: e.target.value })}
              placeholder="main"
            />
          </label>
          <label style={{ gridColumn: "1 / -1" }}>
            Authentication
            <select
              value={authType}
              onChange={(e) => {
                setAuthType(e.target.value);
                setAuthFields({});
              }}
            >
              <option value="token">Personal Access Token</option>
              <option value="oauth">OAuth2 (M2M)</option>
            </select>
          </label>
          {authType === "token" && (
            <label style={{ gridColumn: "1 / -1" }}>
              Access Token{" "}
              <input
                required
                value={authFields.access_token ?? ""}
                onChange={(e) => setAuthFields({ ...authFields, access_token: e.target.value })}
                placeholder="${env:DATABRICKS_TOKEN}"
              />
            </label>
          )}
          {authType === "oauth" && (
            <>
              <label>
                Client ID{" "}
                <input
                  required
                  value={authFields.client_id ?? ""}
                  onChange={(e) => setAuthFields({ ...authFields, client_id: e.target.value })}
                />
              </label>
              <label>
                Client Secret{" "}
                <input
                  type="password"
                  required
                  value={authFields.client_secret ?? ""}
                  onChange={(e) => setAuthFields({ ...authFields, client_secret: e.target.value })}
                />
              </label>
              <label style={{ gridColumn: "1 / -1" }}>
                Token URL{" "}
                <input
                  required
                  value={authFields.token_url ?? ""}
                  onChange={(e) => setAuthFields({ ...authFields, token_url: e.target.value })}
                />
              </label>
            </>
          )}
        </>
      )}
      {form.type === "redshift" && (
        <>
          <label>
            Host{" "}
            <input
              required
              value={form.host}
              onChange={(e) => setForm({ ...form, host: e.target.value })}
              placeholder="cluster.xxxxx.region.redshift.amazonaws.com"
            />
          </label>
          <label>
            Port{" "}
            <input
              type="number"
              required
              value={form.port}
              onChange={(e) => setForm({ ...form, port: +e.target.value })}
            />
          </label>
          <label>
            Database{" "}
            <input
              required
              value={form.database}
              onChange={(e) => setForm({ ...form, database: e.target.value })}
              placeholder="dev"
            />
          </label>
          <label style={{ gridColumn: "1 / -1" }}>
            Authentication
            <select
              value={authType}
              onChange={(e) => {
                setAuthType(e.target.value);
                setAuthFields({});
              }}
            >
              <option value="password">Username / Password</option>
              <option value="iam">IAM Credentials</option>
            </select>
          </label>
          {authType === "password" && (
            <AuthUserPass authFields={authFields} setAuthFields={setAuthFields} />
          )}
          {authType === "iam" && (
            <>
              <label>
                Access Key ID{" "}
                <input
                  required
                  value={authFields.access_key_id ?? ""}
                  onChange={(e) => setAuthFields({ ...authFields, access_key_id: e.target.value })}
                  placeholder="${env:AWS_ACCESS_KEY_ID}"
                />
              </label>
              <label>
                Secret Access Key{" "}
                <input
                  type="password"
                  required
                  value={authFields.secret_access_key ?? ""}
                  onChange={(e) =>
                    setAuthFields({ ...authFields, secret_access_key: e.target.value })
                  }
                  placeholder="${env:AWS_SECRET_ACCESS_KEY}"
                />
              </label>
              <label>
                Region{" "}
                <input
                  value={authFields.region ?? "us-east-1"}
                  onChange={(e) => setAuthFields({ ...authFields, region: e.target.value })}
                />
              </label>
            </>
          )}
        </>
      )}
      {form.type === "elasticsearch" && (
        <>
          <label>
            Host{" "}
            <input
              required
              value={form.host}
              onChange={(e) => setForm({ ...form, host: e.target.value })}
              placeholder="https://localhost:9200"
            />
          </label>
          <label>
            Port{" "}
            <input
              type="number"
              required
              value={form.port}
              onChange={(e) => setForm({ ...form, port: +e.target.value })}
            />
          </label>
          <label style={{ gridColumn: "1 / -1" }}>
            Authentication
            <select
              value={authType}
              onChange={(e) => {
                setAuthType(e.target.value);
                setAuthFields({});
              }}
            >
              <option value="none">No Auth</option>
              <option value="basic">Basic Auth</option>
              <option value="api_key">API Key</option>
              <option value="bearer">Bearer Token</option>
            </select>
          </label>
          {authType === "basic" && (
            <AuthUserPass authFields={authFields} setAuthFields={setAuthFields} />
          )}
          {authType === "api_key" && (
            <label style={{ gridColumn: "1 / -1" }}>
              API Key (base64 id:key){" "}
              <input
                required
                value={authFields.api_key ?? ""}
                onChange={(e) => setAuthFields({ ...authFields, api_key: e.target.value })}
                placeholder="${env:ES_API_KEY}"
              />
            </label>
          )}
          {authType === "bearer" && (
            <label style={{ gridColumn: "1 / -1" }}>
              Token{" "}
              <input
                required
                value={authFields.token ?? ""}
                onChange={(e) => setAuthFields({ ...authFields, token: e.target.value })}
              />
            </label>
          )}
        </>
      )}
      {isDataLake && (
        <>
          {form.type === "hive" && (
            <>
              <label>
                Metastore URI{" "}
                <input
                  required
                  value={form.host}
                  onChange={(e) => setForm({ ...form, host: e.target.value })}
                  placeholder="thrift://hive-metastore:9083"
                />
              </label>
              <label>
                Warehouse Path{" "}
                <input
                  required
                  value={form.database}
                  onChange={(e) => setForm({ ...form, database: e.target.value })}
                  placeholder="s3://bucket/warehouse"
                />
              </label>
            </>
          )}
          {(form.type === "delta_lake" || form.type === "iceberg") && (
            <>
              <label>
                Metastore URI{" "}
                <input
                  value={form.host}
                  onChange={(e) => setForm({ ...form, host: e.target.value })}
                  placeholder="thrift://hive-metastore:9083 (optional)"
                />
              </label>
              <label>
                Warehouse Path{" "}
                <input
                  required
                  value={form.database}
                  onChange={(e) => setForm({ ...form, database: e.target.value })}
                  placeholder="s3://bucket/warehouse"
                />
              </label>
            </>
          )}
          <label style={{ gridColumn: "1 / -1" }}>
            Storage Authentication
            <select
              value={authType}
              onChange={(e) => {
                setAuthType(e.target.value);
                setAuthFields({});
              }}
            >
              <option value="none">None (instance role / local)</option>
              <option value="aws">AWS S3 (Access Key)</option>
              <option value="azure">Azure ADLS</option>
              <option value="gcs">Google Cloud Storage</option>
            </select>
          </label>
          {authType === "aws" && (
            <>
              <label>
                Access Key ID{" "}
                <input
                  required
                  value={authFields.access_key_id ?? ""}
                  onChange={(e) => setAuthFields({ ...authFields, access_key_id: e.target.value })}
                  placeholder="${env:AWS_ACCESS_KEY_ID}"
                />
              </label>
              <label>
                Secret Access Key{" "}
                <input
                  type="password"
                  required
                  value={authFields.secret_access_key ?? ""}
                  onChange={(e) =>
                    setAuthFields({ ...authFields, secret_access_key: e.target.value })
                  }
                  placeholder="${env:AWS_SECRET_ACCESS_KEY}"
                />
              </label>
              <label>
                Region{" "}
                <input
                  value={authFields.region ?? "us-east-1"}
                  onChange={(e) => setAuthFields({ ...authFields, region: e.target.value })}
                />
              </label>
              <label>
                S3 Endpoint (MinIO){" "}
                <input
                  value={authFields.endpoint ?? ""}
                  onChange={(e) => setAuthFields({ ...authFields, endpoint: e.target.value })}
                  placeholder="optional — for S3-compatible"
                />
              </label>
            </>
          )}
          {authType === "azure" && (
            <>
              <label>
                Storage Account{" "}
                <input
                  required
                  value={authFields.storage_account ?? ""}
                  onChange={(e) =>
                    setAuthFields({ ...authFields, storage_account: e.target.value })
                  }
                />
              </label>
              <label>
                Access Key{" "}
                <input
                  type="password"
                  value={authFields.access_key ?? ""}
                  onChange={(e) => setAuthFields({ ...authFields, access_key: e.target.value })}
                  placeholder="shared key (or use SAS)"
                />
              </label>
              <label>
                SAS Token{" "}
                <input
                  value={authFields.sas_token ?? ""}
                  onChange={(e) => setAuthFields({ ...authFields, sas_token: e.target.value })}
                  placeholder="alternative to access key"
                />
              </label>
            </>
          )}
          {authType === "gcs" && (
            <label style={{ gridColumn: "1 / -1" }}>
              Credentials JSON Path{" "}
              <input
                required
                value={authFields.credentials_json ?? ""}
                onChange={(e) => setAuthFields({ ...authFields, credentials_json: e.target.value })}
                placeholder="/path/to/service-account.json"
              />
            </label>
          )}
        </>
      )}
      {form.type === "prometheus" && (
        <>
          <label style={{ gridColumn: "1 / -1" }}>
            URL{" "}
            <input
              required
              value={form.host}
              onChange={(e) => setForm({ ...form, host: e.target.value })}
              placeholder="http://prometheus:9090"
            />
          </label>
          <label style={{ gridColumn: "1 / -1" }}>
            Authentication
            <select
              value={authType}
              onChange={(e) => {
                setAuthType(e.target.value);
                setAuthFields({});
              }}
            >
              <option value="none">No Auth</option>
              <option value="basic">Basic Auth</option>
              <option value="bearer">Bearer Token</option>
            </select>
          </label>
          {authType === "basic" && (
            <AuthUserPass authFields={authFields} setAuthFields={setAuthFields} />
          )}
          {authType === "bearer" && (
            <label style={{ gridColumn: "1 / -1" }}>
              Token{" "}
              <input
                required
                value={authFields.token ?? ""}
                onChange={(e) => setAuthFields({ ...authFields, token: e.target.value })}
              />
            </label>
          )}
        </>
      )}
      {form.type === "google_sheets" && (
        <>
          <label style={{ gridColumn: "1 / -1" }}>
            Credentials JSON Path{" "}
            <input
              required
              value={authFields.credentials_json ?? ""}
              onChange={(e) => setAuthFields({ ...authFields, credentials_json: e.target.value })}
              placeholder="/path/to/service-account.json"
            />
          </label>
          <label style={{ gridColumn: "1 / -1" }}>
            Metadata Sheet ID{" "}
            <input
              value={form.database}
              onChange={(e) => setForm({ ...form, database: e.target.value })}
              placeholder="Sheet ID for table definitions"
            />
          </label>
        </>
      )}
      {form.type === "openapi" && (
        <>
          <div style={{ gridColumn: "1 / -1", display: "flex", gap: "1rem" }}>
            <label
              style={{
                flexDirection: "row",
                alignItems: "center",
                gap: "0.4rem",
                whiteSpace: "nowrap",
              }}
            >
              <input
                type="radio"
                name="openapiSpecMode"
                checked={openapiSpecMode === "path"}
                onChange={() => setOpenapiSpecMode("path")}
                style={{ width: "auto" }}
              />
              Spec path / URL
            </label>
            <label
              style={{
                flexDirection: "row",
                alignItems: "center",
                gap: "0.4rem",
                whiteSpace: "nowrap",
              }}
            >
              <input
                type="radio"
                name="openapiSpecMode"
                checked={openapiSpecMode === "inline"}
                onChange={() => setOpenapiSpecMode("inline")}
                style={{ width: "auto" }}
              />
              Write spec inline
            </label>
          </div>
          {openapiSpecMode === "path" ? (
            <label style={{ gridColumn: "1 / -1" }}>
              Spec Path or URL
              <input
                required
                value={openapiSpecPath}
                onChange={(e) => setOpenapiSpecPath(e.target.value)}
                placeholder="https://api.example.com/openapi.json or ./spec.yaml"
              />
            </label>
          ) : (
            <label style={{ gridColumn: "1 / -1" }}>
              OpenAPI Spec (YAML or JSON)
              <textarea
                required
                value={openapiSpecInline}
                onChange={(e) => setOpenapiSpecInline(e.target.value)}
                placeholder={
                  "openapi: '3.0.0'\ninfo:\n  title: My API\n  version: '1.0'\npaths:\n  /items:\n    get:\n      operationId: listItems\n      responses:\n        '200':\n          description: OK"
                }
                style={{
                  fontFamily: "monospace",
                  fontSize: "0.8rem",
                  minHeight: "200px",
                  resize: "vertical",
                }}
              />
            </label>
          )}
          <label style={{ gridColumn: "1 / -1" }}>
            Base URL{" "}
            <span style={{ color: "var(--text-muted)", fontSize: "0.8rem" }}>
              (leave blank to use servers[0].url from spec)
            </span>
            <input
              value={openapiBaseUrl}
              onChange={(e) => setOpenapiBaseUrl(e.target.value)}
              placeholder="https://api.example.com (optional override)"
            />
          </label>
          <label>
            Cache TTL (seconds)
            <input
              type="number"
              min={0}
              value={openapiCacheTtl}
              onChange={(e) => setOpenapiCacheTtl(e.target.value)}
              placeholder="300"
            />
          </label>
          <label style={{ gridColumn: "1 / -1" }}>
            Authentication
            <select
              value={authType}
              onChange={(e) => {
                setAuthType(e.target.value);
                setAuthFields({});
              }}
            >
              {API_AUTH_TYPES.map((a) => (
                <option key={a.value} value={a.value}>
                  {a.label}
                </option>
              ))}
            </select>
          </label>
          {authType === "bearer" && (
            <label style={{ gridColumn: "1 / -1" }}>
              Token{" "}
              <input
                required
                value={authFields.token ?? ""}
                onChange={(e) => setAuthFields({ ...authFields, token: e.target.value })}
                placeholder="${env:API_TOKEN}"
              />
            </label>
          )}
          {authType === "basic" && (
            <AuthUserPass authFields={authFields} setAuthFields={setAuthFields} />
          )}
          {authType === "api_key" && (
            <>
              <label>
                Header Name{" "}
                <input
                  required
                  value={authFields.header_name ?? "X-API-Key"}
                  onChange={(e) => setAuthFields({ ...authFields, header_name: e.target.value })}
                  placeholder="X-API-Key"
                />
              </label>
              <label>
                API Key{" "}
                <input
                  required
                  value={authFields.api_key ?? ""}
                  onChange={(e) => setAuthFields({ ...authFields, api_key: e.target.value })}
                  placeholder="${env:API_KEY}"
                />
              </label>
            </>
          )}
          <div
            style={{ gridColumn: "1 / -1", display: "flex", gap: "0.5rem", alignItems: "center" }}
          >
            <button
              type="button"
              onClick={handleOpenapiPreview}
              disabled={
                openapiPreviewing ||
                (openapiSpecMode === "path" ? !openapiSpecPath : !openapiSpecInline)
              }
            >
              {openapiPreviewing ? "Loading..." : "Preview"}
            </button>
            {openapiPreviewError && (
              <span className="error" style={{ fontSize: "0.85rem" }}>
                {openapiPreviewError}
              </span>
            )}
          </div>
          {openapiPreview && (
            <div style={{ gridColumn: "1 / -1", fontSize: "0.85rem", color: "var(--text-muted)" }}>
              <strong>{openapiPreview.queries.length} queries</strong>:{" "}
              {openapiPreview.queries.map((q) => q.operation_id).join(", ") || "none"}
              <br />
              <strong>{openapiPreview.mutations.length} mutations</strong>:{" "}
              {openapiPreview.mutations.map((m) => m.operation_id).join(", ") || "none"}
            </div>
          )}
        </>
      )}
      {form.type === "graphql" && (
        <>
          <label style={{ gridColumn: "1 / -1" }}>
            Endpoint URL{" "}
            <input
              required
              value={form.host}
              onChange={(e) => setForm({ ...form, host: e.target.value })}
              placeholder="https://api.example.com/graphql"
            />
          </label>
          <label>
            Namespace{" "}
            <input
              required
              value={gqlNamespace}
              onChange={(e) => setGqlNamespace(e.target.value)}
              placeholder="myapi"
            />
          </label>
          <label>
            Cache TTL (seconds){" "}
            <input
              type="number"
              min={0}
              value={gqlCacheTtl}
              onChange={(e) => setGqlCacheTtl(e.target.value)}
              placeholder="300"
            />
          </label>
          <label style={{ gridColumn: "1 / -1" }}>
            Authentication
            <select
              value={authType}
              onChange={(e) => {
                setAuthType(e.target.value);
                setAuthFields({});
              }}
            >
              <option value="none">No Auth</option>
              <option value="bearer">Bearer Token</option>
              <option value="basic">Basic Auth</option>
            </select>
          </label>
          {authType === "bearer" && (
            <label style={{ gridColumn: "1 / -1" }}>
              Token{" "}
              <input
                required
                value={authFields.token ?? ""}
                onChange={(e) => setAuthFields({ ...authFields, token: e.target.value })}
                placeholder="${env:GQL_TOKEN}"
              />
            </label>
          )}
          {authType === "basic" && (
            <AuthUserPass authFields={authFields} setAuthFields={setAuthFields} />
          )}
        </>
      )}
      {form.type === "grpc" && (
        <>
          <label style={{ gridColumn: "1 / -1" }}>
            Proto Path or URL
            <input
              required
              value={grpcProtoPath}
              onChange={(e) => setGrpcProtoPath(e.target.value)}
              placeholder="https://api.example.com/service.proto or ./service.proto"
            />
          </label>
          <label style={{ gridColumn: "1 / -1" }}>
            Server Address
            <input
              required
              value={grpcServerAddress}
              onChange={(e) => setGrpcServerAddress(e.target.value)}
              placeholder="api.example.com:50051"
            />
          </label>
          <label>
            Namespace
            <input
              value={grpcNamespace}
              onChange={(e) => setGrpcNamespace(e.target.value)}
              placeholder="mygrpc"
            />
          </label>
          <label>
            Cache TTL (seconds)
            <input
              type="number"
              min={0}
              value={grpcCacheTtl}
              onChange={(e) => setGrpcCacheTtl(e.target.value)}
              placeholder="300"
            />
          </label>
          <label
            style={{
              flexDirection: "row",
              alignItems: "center",
              gap: "0.5rem",
              whiteSpace: "nowrap",
            }}
          >
            <input
              type="checkbox"
              checked={grpcTls}
              onChange={(e) => setGrpcTls(e.target.checked)}
              style={{ width: "auto" }}
            />
            TLS
          </label>
          <label style={{ gridColumn: "1 / -1" }}>
            Import Paths{" "}
            <span style={{ color: "var(--text-muted)", fontSize: "0.8rem" }}>
              (comma-separated, optional)
            </span>
            <input
              value={grpcImportPaths}
              onChange={(e) => setGrpcImportPaths(e.target.value)}
              placeholder="/path/to/protos,/another/path"
            />
          </label>
          <label style={{ gridColumn: "1 / -1" }}>
            Authentication
            <select
              value={authType}
              onChange={(e) => {
                setAuthType(e.target.value);
                setAuthFields({});
              }}
            >
              {API_AUTH_TYPES.map((a) => (
                <option key={a.value} value={a.value}>
                  {a.label}
                </option>
              ))}
            </select>
          </label>
          {authType === "bearer" && (
            <label style={{ gridColumn: "1 / -1" }}>
              Token{" "}
              <input
                required
                value={authFields.token ?? ""}
                onChange={(e) => setAuthFields({ ...authFields, token: e.target.value })}
                placeholder="${env:GRPC_TOKEN}"
              />
            </label>
          )}
          {authType === "basic" && (
            <AuthUserPass authFields={authFields} setAuthFields={setAuthFields} />
          )}
          {authType === "api_key" && (
            <>
              <label>
                Header Name{" "}
                <input
                  required
                  value={authFields.header_name ?? "X-API-Key"}
                  onChange={(e) => setAuthFields({ ...authFields, header_name: e.target.value })}
                  placeholder="X-API-Key"
                />
              </label>
              <label>
                API Key{" "}
                <input
                  required
                  value={authFields.api_key ?? ""}
                  onChange={(e) => setAuthFields({ ...authFields, api_key: e.target.value })}
                  placeholder="${env:API_KEY}"
                />
              </label>
            </>
          )}
        </>
      )}
      {form.type === "neo4j" && (
        <>
          <label>
            Host{" "}
            <input
              required
              value={form.host}
              onChange={(e) => setForm({ ...form, host: e.target.value })}
              placeholder="localhost"
            />
          </label>
          <label>
            Port{" "}
            <input
              type="number"
              required
              value={form.port}
              onChange={(e) => setForm({ ...form, port: +e.target.value })}
            />
          </label>
          <label>
            Database{" "}
            <input
              value={form.database}
              onChange={(e) => setForm({ ...form, database: e.target.value })}
              placeholder="neo4j"
            />
          </label>
          <label
            style={{
              flexDirection: "row",
              alignItems: "center",
              gap: "0.5rem",
              whiteSpace: "nowrap",
            }}
          >
            <input
              type="checkbox"
              checked={authFields.use_https === "true"}
              onChange={(e) =>
                setAuthFields({ ...authFields, use_https: e.target.checked ? "true" : "false" })
              }
              style={{ width: "auto" }}
            />
            HTTPS
          </label>
          <label>
            Username{" "}
            <input
              value={form.username}
              onChange={(e) => setForm({ ...form, username: e.target.value })}
              placeholder="neo4j"
            />
          </label>
          <label>
            Password{" "}
            <input
              type="password"
              value={form.password}
              onChange={(e) => setForm({ ...form, password: e.target.value })}
            />
          </label>
        </>
      )}
      {form.type === "sparql" && (
        <>
          <label style={{ gridColumn: "1 / -1" }}>
            Endpoint URL{" "}
            <input
              required
              value={form.host}
              onChange={(e) => setForm({ ...form, host: e.target.value })}
              placeholder="https://dbpedia.org/sparql"
            />
          </label>
          <label style={{ gridColumn: "1 / -1" }}>
            Authentication
            <select
              value={authType}
              onChange={(e) => {
                setAuthType(e.target.value);
                setAuthFields({});
              }}
            >
              <option value="none">No Auth</option>
              <option value="bearer">Bearer Token</option>
              <option value="basic">Basic Auth</option>
            </select>
          </label>
          {authType === "bearer" && (
            <label style={{ gridColumn: "1 / -1" }}>
              Token{" "}
              <input
                required
                value={authFields.token ?? ""}
                onChange={(e) => setAuthFields({ ...authFields, token: e.target.value })}
                placeholder="${env:SPARQL_TOKEN}"
              />
            </label>
          )}
          {authType === "basic" && (
            <AuthUserPass authFields={authFields} setAuthFields={setAuthFields} />
          )}
        </>
      )}
      {form.type === "govdata" && (
        <>
          <div style={{ gridColumn: "1 / -1" }}>
            <label style={{ marginBottom: "0.5rem" }}>Data Subjects</label>
            <div style={{ display: "flex", flexWrap: "wrap", gap: "0.5rem", marginTop: "0.25rem" }}>
              {GOVDATA_SUBJECTS.map((subj) => (
                <label
                  key={subj.value}
                  style={{
                    flexDirection: "row",
                    alignItems: "center",
                    gap: "0.4rem",
                    whiteSpace: "nowrap",
                    fontWeight: "normal",
                  }}
                >
                  <input
                    type="checkbox"
                    style={{ width: "auto" }}
                    checked={govdataSubjects.includes(subj.value)}
                    onChange={(e) =>
                      setGovdataSubjects(
                        e.target.checked
                          ? [...govdataSubjects, subj.value]
                          : govdataSubjects.filter((x) => x !== subj.value),
                      )
                    }
                  />
                  {subj.label}
                </label>
              ))}
            </div>
          </div>
          <div
            style={{ gridColumn: "1 / -1", display: "flex", gap: "0.5rem", alignItems: "flex-end" }}
          >
            <label style={{ flex: 1 }}>
              AskAmerica API Key
              <input
                value={form.username}
                onChange={(e) => setForm({ ...form, username: e.target.value })}
                placeholder="${ASKAMERICA_API_KEY}"
              />
            </label>
            <button type="button" onClick={() => window.open("https://askamerica.ai", "_blank")}>
              Get API Key →
            </button>
          </div>
          {submitting && !editingSourceId && (
            <div style={{ gridColumn: "1 / -1", color: "var(--text-muted)", fontSize: "0.85rem" }}>
              Validating API key and pre-loading metadata… this may take up to 30 seconds on first
              use.
            </div>
          )}
        </>
      )}
      {isKafka && (
        <>
          <label>
            Bootstrap Servers{" "}
            <input
              required
              value={form.host}
              onChange={(e) => setForm({ ...form, host: e.target.value })}
              placeholder="kafka:9092"
            />
          </label>
          <label>
            Schema Registry URL{" "}
            <input
              value={form.database}
              onChange={(e) => setForm({ ...form, database: e.target.value })}
              placeholder="http://schema-registry:8081"
            />
          </label>
          <label style={{ gridColumn: "1 / -1" }}>
            Authentication
            <select
              value={authType}
              onChange={(e) => {
                setAuthType(e.target.value);
                setAuthFields({});
              }}
            >
              {KAFKA_AUTH_TYPES.map((a) => (
                <option key={a.value} value={a.value}>
                  {a.label}
                </option>
              ))}
            </select>
          </label>
          {authType !== "none" && (
            <AuthUserPass authFields={authFields} setAuthFields={setAuthFields} />
          )}
        </>
      )}
      {form.type === "sharepoint" && (
        <>
          <label style={{ gridColumn: "1 / -1" }}>
            Site URL{" "}
            <input
              required
              value={form.host}
              onChange={(e) => setForm({ ...form, host: e.target.value })}
              placeholder="https://contoso.sharepoint.com/sites/mysite"
            />
          </label>
          <label>
            Tenant ID{" "}
            <input
              required
              value={form.database}
              onChange={(e) => setForm({ ...form, database: e.target.value })}
              placeholder="Azure AD tenant ID"
            />
          </label>
          <label>
            Auth Type{" "}
            <select
              value={spAuthType}
              onChange={(e) => setSpAuthType(e.target.value)}
            >
              <option value="CLIENT_CREDENTIALS">Client Credentials</option>
              <option value="USERNAME_PASSWORD">Username / Password</option>
              <option value="CERTIFICATE">Certificate</option>
            </select>
          </label>
          <label>
            Client ID{" "}
            <input
              required
              value={form.username}
              onChange={(e) => setForm({ ...form, username: e.target.value })}
              placeholder="Azure AD app client ID"
            />
          </label>
          {spAuthType === "CLIENT_CREDENTIALS" && (
            <label>
              Client Secret{" "}
              <input
                type="password"
                value={form.password}
                onChange={(e) => setForm({ ...form, password: e.target.value })}
                placeholder="Client secret"
              />
            </label>
          )}
          {spAuthType === "USERNAME_PASSWORD" && (
            <>
              <label>
                SP Username{" "}
                <input
                  required
                  value={spUsername}
                  onChange={(e) => setSpUsername(e.target.value)}
                  placeholder="user@contoso.com"
                />
              </label>
              <label>
                SP Password{" "}
                <input
                  type="password"
                  required
                  value={spPassword}
                  onChange={(e) => setSpPassword(e.target.value)}
                />
              </label>
            </>
          )}
          {spAuthType === "CERTIFICATE" && (
            <>
              <label>
                Client Secret{" "}
                <input
                  type="password"
                  value={form.password}
                  onChange={(e) => setForm({ ...form, password: e.target.value })}
                  placeholder="Client secret (if required alongside cert)"
                />
              </label>
              <label style={{ gridColumn: "1 / -1" }}>
                Certificate Path{" "}
                <input
                  required
                  value={spCertPath}
                  onChange={(e) => setSpCertPath(e.target.value)}
                  placeholder="/certs/sharepoint.pfx"
                />
              </label>
              <label>
                Certificate Password{" "}
                <input
                  type="password"
                  value={spCertPassword}
                  onChange={(e) => setSpCertPassword(e.target.value)}
                />
              </label>
            </>
          )}
        </>
      )}
      {form.type === "splunk" && (
        <>
          <label>
            Host{" "}
            <input
              required
              value={form.host}
              onChange={(e) => setForm({ ...form, host: e.target.value })}
              placeholder="splunk.example.com"
            />
          </label>
          <label>
            Port{" "}
            <input
              type="number"
              value={form.port}
              onChange={(e) => setForm({ ...form, port: +e.target.value })}
            />
          </label>
          <label style={{ gridColumn: "1 / -1" }}>
            Auth Token{" "}
            <input
              type="password"
              required
              value={form.password}
              onChange={(e) => setForm({ ...form, password: e.target.value })}
              placeholder="Splunk authentication token"
            />
          </label>
          <label style={{ gridColumn: "1 / -1" }}>
            App (optional){" "}
            <input
              value={form.database}
              onChange={(e) => setForm({ ...form, database: e.target.value })}
              placeholder="Splunk_SA_CIM"
            />
          </label>
          <label style={{ flexDirection: "row", alignItems: "center", gap: "0.5rem" }}>
            <input
              type="checkbox"
              checked={splunkDisableSsl}
              onChange={(e) => setSplunkDisableSsl(e.target.checked)}
            />
            Disable SSL Validation
          </label>
        </>
      )}
      {form.type === "files" && (
        <label style={{ gridColumn: "1 / -1" }}>
          Directory Glob{" "}
          <input
            value={form.path}
            onChange={(e) => setForm({ ...form, path: e.target.value })}
            placeholder="/data/files/**"
          />
          <small style={{ color: "var(--text-muted, #888)" }}>
            Glob pattern passed to the Trino file connector. Leave blank to use the catalog default.
          </small>
        </label>
      )}
      {editingSourceId && (
        <>
          <label>
            Naming Convention
            <select
              value={form.gqlNamingConvention}
              onChange={(e) => setForm({ ...form, gqlNamingConvention: e.target.value })}
            >
              {NAMING_CONVENTIONS.map((nc) => (
                <option key={nc.value} value={nc.value}>
                  {nc.label}
                </option>
              ))}
            </select>
          </label>
          <label
            style={{
              flexDirection: "row",
              alignItems: "center",
              gap: "0.5rem",
              whiteSpace: "nowrap",
            }}
          >
            <input
              type="checkbox"
              checked={form.cacheEnabled}
              onChange={(e) => setForm({ ...form, cacheEnabled: e.target.checked })}
              style={{ width: "auto" }}
            />
            Cache Enabled
          </label>
          <label>
            Cache TTL (seconds)
            <input
              type="number"
              min={0}
              value={form.cacheTtl}
              onChange={(e) => setForm({ ...form, cacheTtl: e.target.value })}
              placeholder="inherit global"
            />
          </label>
          {domainsEnabled && (
            <label>
              Allowed Domains
              <input
                value={form.allowedDomains}
                onChange={(e) => setForm({ ...form, allowedDomains: e.target.value })}
                placeholder="comma-separated domain IDs, blank = unrestricted"
              />
            </label>
          )}
        </>
      )}
    </>
  );

  if (loading) return <div className="page">Loading sources...</div>;

  return (
    <div className="page">
      <div className="page-header">
        <h2>Data Sources</h2>
        <FilterInput
          value={sourceSearch}
          onChange={updateSearch}
          placeholder="Filter by source ID or type…"
        />
        <div className="page-actions">
          {!editingSourceId && (
            <button
              onClick={() => {
                if (showForm) {
                  handleCancelForm();
                } else {
                  setShowForm(true);
                }
              }}
            >
              {showForm ? "Cancel" : "+ Source"}
            </button>
          )}
        </div>
      </div>

      {error && <div className="error">{error}</div>}
      {refreshError && <div className="error">Schema refresh failed: {refreshError}</div>}

      {showForm && !editingSourceId && (
        <form
          className="form-card"
          onSubmit={
            form.type === "openapi"
              ? handleOpenapiRegister
              : form.type === "grpc"
                ? handleGrpcRegister
                : handleCreate
          }
        >
          <label>
            ID{" "}
            <input
              required
              value={form.id}
              onChange={(e) => setForm({ ...form, id: e.target.value })}
              placeholder="e.g. sales-pg"
            />
          </label>
          <label>
            Type
            <select value={form.type} onChange={(e) => handleTypeChange(e.target.value)}>
              {CATEGORIES.map((cat) => (
                <optgroup key={cat} label={cat}>
                  {SOURCE_TYPES.filter((s) => s.category === cat).map((s) => (
                    <option key={s.value} value={s.value}>
                      {s.label}
                    </option>
                  ))}
                </optgroup>
              ))}
            </select>
          </label>
          {renderFormFields()}
          <button type="submit" disabled={submitting}>
            {submitting && <span className="btn-spinner" />}
            {submitting ? "Creating…" : "Create"}
          </button>
        </form>
      )}

      <table className="data-table">
        <thead>
          <tr>
            <th>ID</th>
            <th>Type</th>
            <th>Host</th>
            <th>Port</th>
            <th>Database</th>
            <th>Naming</th>
            <th>Cache</th>
            <th>Effective TTL</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          {(() => {
            const filtered = sources.filter((s) => {
              if (["__provisa__", "provisa-admin", "provisa-otel"].includes(s.id)) return false;
              if (!sourceSearch.trim()) return true;
              const q = sourceSearch.toLowerCase();
              return (
                s.id.toLowerCase().includes(q) ||
                s.type.toLowerCase().includes(q) ||
                (s.description ?? "").toLowerCase().includes(q)
              );
            });
            const paged = filtered.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);
            return paged.map((s) => {
              const isExpanded = expanded === s.id;
              const isEditing = editingSourceId === s.id;
              return (
                <React.Fragment key={s.id}>
                  <tr
                    onClick={() => {
                      updateExpanded(isExpanded ? null : s.id);
                      if (isEditing && isExpanded) {
                        setEditingSourceId(null);
                        handleCancelForm();
                      }
                    }}
                    style={{
                      cursor: "pointer",
                      background: isExpanded ? "var(--surface)" : undefined,
                    }}
                  >
                    <td>{s.id}</td>
                    <td>{SOURCE_TYPES.find((t) => t.value === s.type)?.label ?? s.type}</td>
                    <td>{s.host}</td>
                    <td>{s.port || "—"}</td>
                    <td>{s.database || "—"}</td>
                    <td style={{ color: "var(--text-muted)", fontSize: "0.85rem" }}>
                      {s.gqlNamingConvention || "inherit"}
                    </td>
                    <td style={{ color: "var(--text-muted)", fontSize: "0.85rem" }}>
                      {s.cacheEnabled ? "on" : "off"}
                    </td>
                    <td style={{ color: "var(--text-muted)", fontSize: "0.85rem" }}>
                      {getEffectiveTtl(s)}
                    </td>
                    <td onClick={(e) => e.stopPropagation()}>
                      <div style={{ display: "flex", gap: "0.25rem", flexWrap: "wrap" }}>
                        {DISCOVERABLE_TYPES.has(s.type) && (
                          <button
                            onClick={() => {
                              setDiscoverSourceId(s.id);
                              setDiscoverSourceType(s.type);
                              setMappingSourceId(null);
                            }}
                            style={{ padding: "0.25rem 0.5rem", fontSize: "0.75rem" }}
                          >
                            Discover
                          </button>
                        )}
                        {MAPPING_TYPES.has(s.type) && (
                          <button
                            onClick={() => {
                              setMappingSourceId(s.id);
                              setMappingSourceType(s.type);
                              setDiscoverSourceId(null);
                            }}
                            style={{ padding: "0.25rem 0.5rem", fontSize: "0.75rem" }}
                          >
                            Map Table
                          </button>
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
                      </div>
                    </td>
                  </tr>
                  {isExpanded && (
                    <tr key={`${s.id}-detail`}>
                      <td
                        colSpan={9}
                        style={{
                          padding: "0.75rem 1rem",
                          background: "var(--bg)",
                          borderTop: "1px solid var(--border)",
                        }}
                      >
                        {isEditing ? (
                          <form className="form-card" onSubmit={handleCreate} style={{ margin: 0 }}>
                            <label>
                              ID{" "}
                              <input
                                required
                                value={form.id}
                                onChange={(e) => setForm({ ...form, id: e.target.value })}
                              />
                            </label>
                            <label>
                              Type
                              <select
                                value={form.type}
                                onChange={(e) => handleTypeChange(e.target.value)}
                              >
                                {CATEGORIES.map((cat) => (
                                  <optgroup key={cat} label={cat}>
                                    {SOURCE_TYPES.filter((st) => st.category === cat).map((st) => (
                                      <option key={st.value} value={st.value}>
                                        {st.label}
                                      </option>
                                    ))}
                                  </optgroup>
                                ))}
                              </select>
                            </label>
                            {renderFormFields()}
                            <div
                              style={{
                                display: "flex",
                                gap: "0.5rem",
                                justifyContent: "flex-end",
                                alignItems: "flex-start",
                                alignSelf: "end",
                              }}
                            >
                              <button
                                type="button"
                                className="btn-icon"
                                title="Cancel"
                                onClick={handleCancelForm}
                              >
                                <X size={14} />
                              </button>
                              <button type="submit" className="btn-icon-primary" title="Save">
                                <Save size={14} />
                              </button>
                            </div>
                          </form>
                        ) : (
                          <div style={{ display: "flex", flexDirection: "column", gap: "0.5rem" }}>
                            <dl
                              style={{
                                display: "grid",
                                gridTemplateColumns: "max-content 1fr",
                                gap: "0.25rem 1rem",
                                margin: 0,
                                color: "var(--text)",
                              }}
                            >
                              {(
                                [
                                  ["Description", s.description || "—"],
                                  [
                                    "Type",
                                    SOURCE_TYPES.find((t) => t.value === s.type)?.label ?? s.type,
                                  ],
                                  ["Host", s.host || "—"],
                                  ["Port", s.port || "—"],
                                  ["Database", s.database || "—"],
                                  ["Username", s.username || "—"],
                                  ["Naming", s.gqlNamingConvention || "inherit (global)"],
                                  ["Cache", s.cacheEnabled ? "enabled" : "disabled"],
                                  ["Cache TTL", s.cacheTtl != null ? `${s.cacheTtl}s` : "inherit"],
                                  ["Effective TTL", getEffectiveTtl(s)],
                                  [
                                    "Allowed Domains",
                                    (s.allowedDomains ?? []).length
                                      ? (s.allowedDomains ?? []).join(", ")
                                      : "unrestricted",
                                  ],
                                ] as [string, string | number][]
                              )
                                .filter(([k]) => domainsEnabled || k !== "Allowed Domains")
                                .map(([k, v]) => (
                                <React.Fragment key={k}>
                                  <dt
                                    style={{
                                      color: "var(--text-muted)",
                                      fontWeight: 500,
                                      fontSize: "0.875rem",
                                    }}
                                  >
                                    {k}
                                  </dt>
                                  <dd
                                    style={{
                                      color: "var(--text)",
                                      margin: 0,
                                      fontSize: "0.875rem",
                                    }}
                                  >
                                    {v}
                                  </dd>
                                </React.Fragment>
                              ))}
                            </dl>
                            <div style={{ display: "flex", gap: "0.5rem", marginTop: "0.25rem" }}>
                              <button
                                className="btn-icon"
                                title="Edit"
                                onClick={(e) => {
                                  e.stopPropagation();
                                  handleEdit(s);
                                }}
                              >
                                <Pencil size={14} />
                              </button>
                              {s.id !== "provisa-otel" && (
                                <button
                                  className="btn-icon"
                                  title="View registered tables"
                                  onClick={(e) => {
                                    e.stopPropagation();
                                    navigate(`/tables?source=${encodeURIComponent(s.id)}`);
                                  }}
                                >
                                  <ArrowRight size={14} />
                                </button>
                              )}
                              <ConfirmDialog
                                title={`Delete source "${s.id}"?`}
                                consequence={`This will remove the data source "${s.id}" and may break tables that reference it.`}
                                onConfirm={async () => {
                                  await deleteSource(s.id);
                                  if (expanded === s.id) updateExpanded(null);
                                  load();
                                }}
                              >
                                {(open) => (
                                  <button
                                    className="btn-icon-danger"
                                    title="Delete"
                                    onClick={(e) => {
                                      e.stopPropagation();
                                      open();
                                    }}
                                  >
                                    <Trash2 size={14} />
                                  </button>
                                )}
                              </ConfirmDialog>
                            </div>
                          </div>
                        )}
                      </td>
                    </tr>
                  )}
                </React.Fragment>
              );
            });
          })()}
        </tbody>
      </table>

      {(() => {
        const filtered = sources.filter((s) => {
          if (["__provisa__", "provisa-admin", "provisa-otel"].includes(s.id)) return false;
          if (!sourceSearch.trim()) return true;
          const q = sourceSearch.toLowerCase();
          return (
            s.id.toLowerCase().includes(q) ||
            s.type.toLowerCase().includes(q) ||
            (s.description ?? "").toLowerCase().includes(q)
          );
        });
        const totalPages = Math.max(1, Math.ceil(filtered.length / PAGE_SIZE));
        if (totalPages === 1) return null;
        return (
          <div
            style={{
              display: "flex",
              gap: "0.5rem",
              alignItems: "center",
              justifyContent: "flex-end",
              padding: "0.5rem 0",
            }}
          >
            <button onClick={() => setPage(0)} disabled={page === 0}>
              «
            </button>
            <button onClick={() => setPage((p) => p - 1)} disabled={page === 0}>
              ‹
            </button>
            <span>
              Page {page + 1} / {totalPages}
            </span>
            <button onClick={() => setPage((p) => p + 1)} disabled={page >= totalPages - 1}>
              ›
            </button>
            <button onClick={() => setPage(totalPages - 1)} disabled={page >= totalPages - 1}>
              »
            </button>
          </div>
        );
      })()}

      {discoverSourceId && discoverSourceType && (
        <SchemaDiscovery
          sourceId={discoverSourceId}
          sourceType={discoverSourceType}
          onClose={() => {
            setDiscoverSourceId(null);
            setDiscoverSourceType(null);
          }}
          onRegistered={load}
        />
      )}

      {mappingSourceId && mappingSourceType && (
        <TableMappingBuilder
          sourceType={mappingSourceType}
          onCancel={() => {
            setMappingSourceId(null);
            setMappingSourceType(null);
          }}
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
