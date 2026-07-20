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
import { useTranslation } from "react-i18next";
import { Check, X } from "lucide-react";
import {
  ActionIcon,
  Alert,
  Button,
  Group,
  Select,
  Table,
  Text,
  TextInput,
  Title,
} from "@mantine/core";
import { FilterInput } from "../components/admin/FilterInput";
import { fetchSettings, fetchFederationEngine } from "../api/admin";
import type { PlatformSettings, FederationEngineState } from "../api/admin";
import {
  useSources,
  useCreateSource,
  useUpdateSource,
  useRenameSource,
  useDeleteSource,
  useUpdateSourceCache,
  useUpdateSourcePreferMaterialized,
  useUpdateSourceLoadProtection,
  useUpdateSourceNaming,
  useUpdateSourceAllowedDomains,
  useDomains,
} from "../hooks/useAdminQueries";
import { SchemaDiscovery } from "../components/SchemaDiscovery";
import { TableMappingBuilder } from "../components/TableMappingBuilder";
import type { TableMapping } from "../components/TableMappingBuilder";
import type { Source } from "../types/admin";
import { cdcTransportApplicable, sourceChangeSignals } from "../liveCapability";
import {
  CATEGORIES,
  DATA_LAKE,
  DB_DESCRIPTION_TYPES,
  DISCOVERABLE_TYPES,
  FILE_SOURCES,
  GOVDATA_SUBJECTS,
  MAPPING_TYPES,
  SOURCE_TYPES,
} from "./sources/constants";
import {
  getDefaultPort,
  parseFilesPath,
  reachInfoFor,
  reachSuffix,
} from "./sources/sourceHelpers";
import type { CdcState, SourceFormFieldsProps, SourceFormState } from "./sources/SourceFormFields";
import { SourceFormFields } from "./sources/SourceFormFields";
import { SourceDetailPanel } from "./sources/SourceDetailPanel";

export function SourcesPage() {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const [searchParams, setSearchParams] = useSearchParams();
  const { sources, loading: sourcesLoading, refetch: refetchSources } = useSources();
  const { domains } = useDomains();
  const { createSource } = useCreateSource();
  const { updateSource } = useUpdateSource();
  const { renameSource } = useRenameSource();
  const { deleteSource } = useDeleteSource();
  const { updateSourceCache } = useUpdateSourceCache();
  const { updateSourcePreferMaterialized } = useUpdateSourcePreferMaterialized();
  const { updateSourceLoadProtection } = useUpdateSourceLoadProtection();
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
  const [form, setForm] = useState<SourceFormState>({
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
    preferMaterialized: false,
    loadProtected: false,
    offPeakWindow: "",
    offPeakTz: "UTC",
    changeSignal: "ttl",
    path: "" as string,
    allowedDomains: "" as string,
    description: "" as string,
  });
  // REQ-824: source-level CDC transport (Debezium), entered once per source
  const emptyCdc: CdcState = {
    bootstrapServers: "",
    topicPrefix: "",
    schemaRegistryUrl: "",
    consumerGroupId: "",
  };
  const [cdc, setCdc] = useState<CdcState>({ ...emptyCdc });
  const [authType, setAuthType] = useState("none");
  const [authFields, setAuthFields] = useState<Record<string, string>>({});
  const [settings, setSettings] = useState<PlatformSettings | null>(null);
  const [engineState, setEngineState] = useState<FederationEngineState | null>(null);
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
  const [filesTransport, setFilesTransport] = useState("file://");
  const [filesAuthMode, setFilesAuthMode] = useState<"userpass" | "certificate">("userpass");
  const [filesCertPath, setFilesCertPath] = useState("");
  const [filesCertPassword, setFilesCertPassword] = useState("");

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

  // Reach faces of the configured federation engine — gates + annotates the type dropdown (REQ-947).
  useEffect(() => {
    fetchFederationEngine()
      .then(setEngineState)
      .catch((e) => setError(String(e)));
  }, []);

  const loading = sourcesLoading || settingsLoading;

  const getEffectiveTtl = (source: Source): string => {
    if (source.cacheTtl != null) return `${source.cacheTtl}s (custom)`;
    if (settings) return `${settings.cache.default_ttl}s (global)`;
    return "default";
  };

  // Type dropdown annotated + gated by the current engine's reach (REQ-947): each option shows
  // LIVE / REPLICA, and a type the engine cannot reach is disabled with the engine(s) that can.
  const typeSelectData = () =>
    CATEGORIES.map((cat) => ({
      group: cat,
      items: SOURCE_TYPES.filter((s) => s.category === cat).map((s) => {
        const info = reachInfoFor(s.value, engineState);
        return {
          value: s.value,
          label: `${s.label}${reachSuffix(info)}`,
          disabled: !info.selectable,
        };
      }),
    }));

  const resetSpFields = () => {
    setSpAuthType("CLIENT_CREDENTIALS");
    setSpCertPath("");
    setSpCertPassword("");
    setSpUsername("");
    setSpPassword("");
  };

  const handleTypeChange = (type: string) => {
    // REQ-929: keep change_signal valid for the new type's capabilities.
    const signals = sourceChangeSignals(type);
    const changeSignal = signals.includes(form.changeSignal) ? form.changeSignal : signals[0];
    setForm({ ...form, type, port: getDefaultPort(type), description: "", changeSignal });
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
      preferMaterialized: s.preferMaterialized ?? false,
      loadProtected: s.loadProtected ?? false,
      offPeakWindow: s.offPeakWindow ?? "",
      offPeakTz: s.offPeakTz ?? "UTC",
      changeSignal: s.changeSignal || "ttl",
      path: s.type === "files" ? parseFilesPath(s.path ?? "").path : (s.path ?? ""),
      allowedDomains: (s.allowedDomains ?? []).join(", "),
      description: s.description ?? "",
    });
    setFilesTransport(s.type === "files" ? parseFilesPath(s.path ?? "").transport : "file://");
    setCdc(
      s.cdc
        ? {
            bootstrapServers: s.cdc.bootstrapServers ?? "",
            topicPrefix: s.cdc.topicPrefix ?? "",
            schemaRegistryUrl: s.cdc.schemaRegistryUrl ?? "",
            consumerGroupId: s.cdc.consumerGroupId ?? "",
          }
        : { ...emptyCdc },
    );
    setAuthType("none");
    setAuthFields({});
    if (DATA_LAKE.has(s.type) && s.mappingJson) {
      try {
        const { storage, ...creds } = JSON.parse(s.mappingJson) as Record<string, string>;
        setAuthType(
          storage === "s3" ? "aws" : storage === "adls" ? "azure" : storage === "gcs" ? "gcs" : "none",
        );
        setAuthFields(creds);
      } catch {
        setAuthType("none");
        setAuthFields({});
      }
    }
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
      preferMaterialized: false,
      loadProtected: false,
      offPeakWindow: "",
      offPeakTz: "UTC",
      changeSignal: "ttl",
      path: "",
      allowedDomains: "",
      description: "",
    });
    setFilesTransport("file://");
    setFilesAuthMode("userpass");
    setFilesCertPath("");
    setFilesCertPassword("");
    setCdc({ ...emptyCdc });
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
      const {
        gqlNamingConvention: _nc,
        cacheTtl: _ct,
        cacheEnabled: _ce,
        preferMaterialized: _pm,
        ...coreForm
      } = form;
      // Data-lake storage is a config choice, not a separate source type: the object store its tables
      // live on (Hadoop/local, S3, ADLS, GCS) + its credentials land in source.mapping, discriminated
      // by mapping.storage. Derived from the Storage Authentication select (none→hadoop, aws→s3,
      // azure→adls, gcs→gcs) so the connector wires the matching native filesystem.
      const lakeStorage =
        authType === "aws"
          ? "s3"
          : authType === "azure"
            ? "adls"
            : authType === "gcs"
              ? "gcs"
              : "hadoop";
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
            : DATA_LAKE.has(form.type)
              ? JSON.stringify({ storage: lakeStorage, ...authFields })
              : undefined;
      const sourcePayload = {
        ...coreForm,
        path: FILE_SOURCES.has(form.type) || form.type === "files"
          ? form.type === "files" && form.path
            ? filesTransport === "file://" ? form.path : filesTransport + form.path
            : form.path || null
          : null,
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
        // REQ-824: attach source-level CDC transport only for Debezium-captured RDBMS
        cdc:
          cdcTransportApplicable(form.type) && cdc.bootstrapServers && cdc.topicPrefix
            ? {
                bootstrapServers: cdc.bootstrapServers,
                topicPrefix: cdc.topicPrefix,
                schemaRegistryUrl: cdc.schemaRegistryUrl || null,
                consumerGroupId: cdc.consumerGroupId.trim() || null,
              }
            : null,
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
        const preferResult = await updateSourcePreferMaterialized(
          effectiveId,
          form.preferMaterialized,
        );
        if (!preferResult.success) throw new Error(preferResult.message);
        const loadProtResult = await updateSourceLoadProtection(
          effectiveId,
          form.loadProtected,
          form.offPeakWindow.trim() || null,
          form.offPeakTz.trim() || "UTC",
        );
        if (!loadProtResult.success) throw new Error(loadProtResult.message);
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
        if (form.loadProtected) {
          const lp = await updateSourceLoadProtection(
            form.id,
            true,
            form.offPeakWindow.trim() || null,
            form.offPeakTz.trim() || "UTC",
          );
          if (!lp.success) throw new Error(lp.message);
        }
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

  // Shared props object passed to SourceFormFields at both call sites.
  const sourceFormFieldsProps: SourceFormFieldsProps = {
    form, setForm,
    authType, setAuthType,
    authFields, setAuthFields,
    editingSourceId,
    cdc, setCdc,
    domainsEnabled,
    domains,
    spAuthType, setSpAuthType,
    spCertPath, setSpCertPath,
    spCertPassword, setSpCertPassword,
    spUsername, setSpUsername,
    spPassword, setSpPassword,
    splunkDisableSsl, setSplunkDisableSsl,
    filesTransport, setFilesTransport,
    filesAuthMode, setFilesAuthMode,
    filesCertPath, setFilesCertPath,
    filesCertPassword, setFilesCertPassword,
    govdataSubjects, setGovdataSubjects,
    submitting,
    openapiSpecPath, setOpenapiSpecPath,
    openapiSpecInline, setOpenapiSpecInline,
    openapiSpecMode, setOpenapiSpecMode,
    openapiBaseUrl, setOpenapiBaseUrl,
    openapiCacheTtl, setOpenapiCacheTtl,
    openapiPreview, openapiPreviewing, openapiPreviewError,
    onOpenapiPreview: handleOpenapiPreview,
    gqlNamespace, setGqlNamespace,
    gqlCacheTtl, setGqlCacheTtl,
    grpcProtoPath, setGrpcProtoPath,
    grpcServerAddress, setGrpcServerAddress,
    grpcNamespace, setGrpcNamespace,
    grpcTls, setGrpcTls,
    grpcImportPaths, setGrpcImportPaths,
    grpcCacheTtl, setGrpcCacheTtl,
  };

  if (loading) return <div className="page">{t("sourcesPage.loading")}</div>;

  return (
    <div className="page">
      <div className="page-header">
        <Title order={2}>{t("sourcesPage.title")}</Title>
        <FilterInput
          value={sourceSearch}
          onChange={updateSearch}
          placeholder={t("sourcesPage.filterPlaceholder")}
        />
        <div className="page-actions">
          {!editingSourceId && (
            <Button
              data-tour="sources-add"
              data-testid="sources-add-toggle"
              variant={showForm ? "outline" : "filled"}
              aria-label={showForm ? t("sourcesPage.closeForm") : t("sourcesPage.addSource")}
              onClick={() => {
                if (showForm) {
                  handleCancelForm();
                } else {
                  setShowForm(true);
                }
              }}
            >
              {showForm ? <X size={14} /> : t("sourcesPage.addSource")}
            </Button>
          )}
        </div>
      </div>

      {error && (
        <Alert color="red" mb="md" data-testid="sources-error">
          {error}
        </Alert>
      )}
      {refreshError && (
        <Alert color="red" mb="md" data-testid="sources-refresh-error">
          {t("sourcesPage.schemaRefreshFailed", { message: refreshError })}
        </Alert>
      )}

      {showForm && !editingSourceId && (
        <form
          data-tour="sources-form"
          className="form-card"
          onSubmit={
            form.type === "openapi"
              ? handleOpenapiRegister
              : form.type === "grpc"
                ? handleGrpcRegister
                : handleCreate
          }
        >
          <TextInput
            label={t("sourcesPage.idLabel")}
            required
            value={form.id}
            onChange={(e) => setForm({ ...form, id: e.target.value })}
            placeholder={t("sourcesPage.idPlaceholder")}
            data-testid="sources-id-input"
          />
          <Select
            label={t("sourcesPage.typeLabel")}
            data-tour="sources-type"
            data-testid="sources-type-select"
            value={form.type}
            onChange={(v) => v && handleTypeChange(v)}
            data={typeSelectData()}
            allowDeselect={false}
            searchable
          />
          <SourceFormFields {...sourceFormFieldsProps} />
          <Button type="submit" loading={submitting} data-testid="sources-submit">
            {submitting ? t("sourcesPage.creating") : t("sourcesPage.create")}
          </Button>
        </form>
      )}

      <Table.ScrollContainer minWidth={860}>
        <Table striped highlightOnHover withTableBorder verticalSpacing="xs" className="data-table">
          <Table.Thead>
            <Table.Tr>
              <Table.Th>{t("sourcesPage.colId")}</Table.Th>
              <Table.Th>{t("sourcesPage.colType")}</Table.Th>
              <Table.Th>{t("sourcesPage.colHost")}</Table.Th>
              <Table.Th>{t("sourcesPage.colPort")}</Table.Th>
              <Table.Th>{t("sourcesPage.colDatabase")}</Table.Th>
              <Table.Th>{t("sourcesPage.colNaming")}</Table.Th>
              <Table.Th>{t("sourcesPage.colCache")}</Table.Th>
              <Table.Th>{t("sourcesPage.colEffectiveTtl")}</Table.Th>
              <Table.Th>
                <Text span visibleFrom="xs" fz="sm" fw={600}>
                  {t("sourcesPage.colActions")}
                </Text>
              </Table.Th>
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
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
              if (filtered.length === 0) {
                return (
                  <Table.Tr>
                    <Table.Td colSpan={9} ta="center" c="dimmed">
                      {t("sourcesPage.empty")}
                    </Table.Td>
                  </Table.Tr>
                );
              }
              const paged = filtered.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);
              return paged.map((s) => {
                const isExpanded = expanded === s.id;
                const isEditing = editingSourceId === s.id;
                return (
                  <React.Fragment key={s.id}>
                    <Table.Tr
                      data-testid={`sources-row-${s.id}`}
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
                      <Table.Td>{s.id}</Table.Td>
                      <Table.Td>{SOURCE_TYPES.find((st) => st.value === s.type)?.label ?? s.type}</Table.Td>
                      <Table.Td>{s.host}</Table.Td>
                      <Table.Td>{s.port || "—"}</Table.Td>
                      <Table.Td>{s.database || "—"}</Table.Td>
                      <Table.Td c="dimmed" fz="0.85rem">
                        {s.gqlNamingConvention || t("sourcesPage.naOrInherit")}
                      </Table.Td>
                      <Table.Td c="dimmed" fz="0.85rem">
                        {s.cacheEnabled ? t("sourcesPage.cacheOn") : t("sourcesPage.cacheOff")}
                      </Table.Td>
                      <Table.Td c="dimmed" fz="0.85rem">
                        {getEffectiveTtl(s)}
                      </Table.Td>
                      <Table.Td onClick={(e) => e.stopPropagation()}>
                        <Group gap="xs" wrap="wrap">
                          {DISCOVERABLE_TYPES.has(s.type) && (
                            <Button
                              size="compact-xs"
                              variant="default"
                              data-testid={`sources-discover-${s.id}`}
                              onClick={() => {
                                setDiscoverSourceId(s.id);
                                setDiscoverSourceType(s.type);
                                setMappingSourceId(null);
                              }}
                            >
                              {t("sourcesPage.discover")}
                            </Button>
                          )}
                          {MAPPING_TYPES.has(s.type) && (
                            <Button
                              size="compact-xs"
                              variant="default"
                              data-testid={`sources-map-table-${s.id}`}
                              onClick={() => {
                                setMappingSourceId(s.id);
                                setMappingSourceType(s.type);
                                setDiscoverSourceId(null);
                              }}
                            >
                              {t("sourcesPage.mapTable")}
                            </Button>
                          )}
                          {(s.type === "graphql" || s.type === "openapi" || s.type === "grpc") && (
                            <Button
                              size="compact-xs"
                              variant="default"
                              data-testid={`sources-refresh-schema-${s.id}`}
                              onClick={() => handleRefreshSchema(s.id, s.type)}
                              disabled={refreshingSourceId === s.id}
                            >
                              {refreshingSourceId === s.id
                                ? t("sourcesPage.refreshing")
                                : t("sourcesPage.refreshSchema")}
                            </Button>
                          )}
                        </Group>
                      </Table.Td>
                    </Table.Tr>
                    {isExpanded && (
                      <Table.Tr key={`${s.id}-detail`}>
                        <Table.Td
                          colSpan={9}
                          style={{
                            padding: "0.75rem 1rem",
                            background: "var(--bg)",
                            borderTop: "1px solid var(--border)",
                          }}
                        >
                          {isEditing ? (
                            <form className="form-card" onSubmit={handleCreate} style={{ margin: 0 }}>
                              <TextInput
                                label={t("sourcesPage.idLabel")}
                                required
                                value={form.id}
                                onChange={(e) => setForm({ ...form, id: e.target.value })}
                              />
                              <Select
                                label={t("sourcesPage.typeLabel")}
                                value={form.type}
                                onChange={(v) => v && handleTypeChange(v)}
                                data={typeSelectData()}
                                allowDeselect={false}
                                searchable
                              />
                              <SourceFormFields {...sourceFormFieldsProps} />
                              <Group justify="flex-end" align="flex-start" gap="sm" style={{ alignSelf: "end" }}>
                                <ActionIcon
                                  variant="subtle"
                                  type="button"
                                  aria-label={t("sourcesPage.cancelEdit")}
                                  title={t("sourcesPage.cancelEdit")}
                                  onClick={handleCancelForm}
                                >
                                  <X size={14} />
                                </ActionIcon>
                                <ActionIcon
                                  variant="filled"
                                  type="submit"
                                  aria-label={t("sourcesPage.saveEdit")}
                                  title={t("sourcesPage.saveEdit")}
                                >
                                  <Check size={14} />
                                </ActionIcon>
                              </Group>
                            </form>
                          ) : (
                            <SourceDetailPanel
                              s={s}
                              domainsEnabled={domainsEnabled}
                              getEffectiveTtl={getEffectiveTtl}
                              onEdit={() => handleEdit(s)}
                              onNavigate={() => navigate(`/tables?source=${encodeURIComponent(s.id)}`)}
                              onDelete={async () => {
                                await deleteSource(s.id);
                                if (expanded === s.id) updateExpanded(null);
                                load();
                              }}
                            />
                          )}
                        </Table.Td>
                      </Table.Tr>
                    )}
                  </React.Fragment>
                );
              });
            })()}
          </Table.Tbody>
        </Table>
      </Table.ScrollContainer>

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
          <Group justify="flex-end" gap="xs" align="center" py="xs">
            <ActionIcon
              variant="default"
              aria-label={t("sourcesPage.firstPage")}
              onClick={() => setPage(0)}
              disabled={page === 0}
            >
              «
            </ActionIcon>
            <ActionIcon
              variant="default"
              aria-label={t("sourcesPage.previousPage")}
              onClick={() => setPage((p) => p - 1)}
              disabled={page === 0}
            >
              ‹
            </ActionIcon>
            <Text fz="sm">{t("sourcesPage.pageStatus", { page: page + 1, totalPages })}</Text>
            <ActionIcon
              variant="default"
              aria-label={t("sourcesPage.nextPage")}
              onClick={() => setPage((p) => p + 1)}
              disabled={page >= totalPages - 1}
            >
              ›
            </ActionIcon>
            <ActionIcon
              variant="default"
              aria-label={t("sourcesPage.lastPage")}
              onClick={() => setPage(totalPages - 1)}
              disabled={page >= totalPages - 1}
            >
              »
            </ActionIcon>
          </Group>
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
