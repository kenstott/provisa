// Copyright (c) 2026 Kenneth Stott
// Canary: 55ecea78-4131-4bb4-8a93-71e1bdacea84
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { ttlText } from "../components/form/cacheTtl";
import { useSetSourceRegion, useRegionChoices } from "../hooks/useRegionQueries";
import { useRegionSelection } from "../hooks/useRegionSelection";
import { filterByRegion } from "../hooks/regionFilter";
import { RegionSelector } from "../components/RegionSelector";
import {
  FormField,
  FormFooter,
  FormIdentifiers,
  FormLayout,
  FormRead,
  FormSections,
} from "../components/form/FormLayout";
import React, { useState, useEffect, useMemo, type ReactNode } from "react";
import { useNavigate, useSearchParams } from "react-router-dom";
import { useTranslation } from "react-i18next";
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
import { HelpBubble } from "../components/HelpBubble";
import { TipSurface } from "../components/Tip";
import { RefreshCw } from "lucide-react";
import { fetchSettings, fetchSourceReach } from "../api/admin";
import type { PlatformSettings, SourceReach } from "../api/admin";
import {
  useSources,
  useCreateSource,
  useUpdateSource,
  useRenameSource,
  useDeleteSource,
  useUpdateSourceCache,
  useUpdateSourceReplicate,
  useUpdateSourceLoadProtection,
  useUpdateSourceNaming,
  useUpdateSourceAllowedDomains,
  useDomains,
  useTables,
} from "../hooks/useAdminQueries";
import { useGovdataSubjects } from "../hooks/useGovdataSubjects";
import { schemasForSubjects } from "./sources/govdataSubjects";
import { useAuth } from "../context/AuthContext";
import { SchemaDiscovery } from "../components/SchemaDiscovery";
import { TagControl } from "../components/TagControl";
import { refusalError, requestFailed, serverMessage } from "../i18n/serverMessage";
import { TableMappingBuilder } from "../components/TableMappingBuilder";
import type { TableMapping } from "../components/TableMappingBuilder";
import type { Source } from "../types/admin";
import { DERIVED_SOURCE_ID } from "../types/admin";
import { cdcTransportApplicable, sourceChangeSignals } from "../liveCapability";
import {
  BRAND_CARRIER,
  CATEGORIES,
  DB_DESCRIPTION_TYPES,
  DISCOVERABLE_TYPES,
  FILE_SOURCES,
  MAPPING_TYPES,
  SOURCE_TYPES,
} from "./sources/constants";
import {
  backendType,
  getDefaultPort,
  pickerTypeLabel,
  reachInfoFor,
  reachSuffix,
  carrierOf,
  sourceTypeLabel,
} from "./sources/sourceHelpers";
import { schemaRegistryAuthPayload } from "./sources/schemaRegistryAuth";
import type { CdcState, SourceFormFieldsProps, SourceFormState } from "./sources/SourceFormFields";
import { profilerMappingJson } from "./sources/profilerMapping";
import { salesforceMappingJson } from "./sources/salesforce";
import { cloudopsMappingJson } from "./sources/cloudops";
import { GOOGLE_WORKSPACE, gwMapping } from "./sources/googleWorkspace";
import { MICROSOFT_365, m365Mapping } from "./sources/microsoft365";
import { LOGS, logsMapping } from "./sources/logs";
import { COSMOSDB, cosmosHints } from "./sources/cosmosdb";
import { QDRANT, qdrantHints } from "./sources/qdrant";
import { CODE_SOURCE_TYPES, codeMappingJson } from "./sources/codeSource";
import { NEW_SOURCE, keepFolderAs, resendHeldFolder } from "./sources/folderZip";
import { uploadCodeArchive } from "../api/codeUploads";
import { WIKIPEDIA, wikipediaHints } from "./sources/wikipedia";
import { sourceFormCacheTtl, sourceLoadFieldsValid } from "./sources/loadManagement";
import { useCacheTtlRequired } from "./sources/sourceKind";
import { SourceFormFields } from "./sources/SourceFormFields";
import { icebergCatalogHints } from "./sources/icebergCatalog";
import { CLOUD_FILE_TYPES, objectStoreHints, objectStoreOf } from "./sources/objectStoreHints";
import { draftTable, fileLocationOf } from "./sources/sourceUpload";
import { SourceDetailPanel } from "./sources/SourceDetailPanel";
import { SourceSectionTabs } from "./sources/SourceSectionTabs";
import { SourceTablesList } from "./sources/SourceTablesList";
import { SourceOperations } from "./sources/SourceOperations";
import { shownSourceSections, type SourceSection } from "./sources/sourceSections";
import {
  EMPTY_CDC,
  fetchRemoteSeed,
  hasRemoteSeed,
  remoteAuth,
  seedSource,
  type RemoteSeed,
} from "./sources/sourceSeed";
import { PanelWorking } from "./sources/PanelWorking";
import { PanelActions, SplitPane } from "../components/list/SplitPane";
import { PageLoading } from "../components/PageLoading";
import { useDependentsDialog } from "../hooks/useDependentsDialog";
import { ListTable, ListHead, ListRow, ListEmpty, ListItems } from "../components/list/ListTable";
import { useListSortGroup, pageItems, type ListColumn } from "../components/list/useListSortGroup";
import { KeptTablesNotice } from "../components/KeptTablesNotice";
import { keptTablesOf } from "../lib/keptTables";
import type { KeptTable } from "../lib/keptTables";
import { SourceTypePicker } from "../components/SourceTypePicker";
import { athenaHints } from "./sources/athena";

export function SourcesPage() {
  // REQ-1918: a delete is refused while anything depends on the object; this lists them.
  const refusal = useDependentsDialog();
  const { t } = useTranslation();
  // REQ-1739: `billing` is true only where the commercial plugin is mounted (REQ-1469) — the same
  // hosted-vs-self-hosted signal provisa/api/admin/schema_mutation.py's
  // `_refuse_soda_on_hosted_plane` gates on server-side (commerce.enabled()). soda is Elastic
  // License 2.0 and must never be offered as a hosted/managed service; hiding it here is
  // defense-in-depth UX, not the enforcement boundary — the server refuses it regardless.
  const { billing: hostedPlane, activeOrgId } = useAuth();
  const navigate = useNavigate();
  const [searchParams, setSearchParams] = useSearchParams();
  const { sources, loading: sourcesLoading, refetch: refetchSources } = useSources();
  // The tables registered from each source, for its panel's Tables section.
  const { tables: registeredTables } = useTables();
  // The section of the source's panel in view, the same in its read view and its edit form, and
  // the form's sections that have fields for the source's kind (reported by the form as drawn).
  const [section, setSection] = useState<SourceSection>("connection");
  const [formSections, setFormSections] = useState<string[]>([]);
  const noteFormSections = (found: string[]) =>
    setFormSections((held) =>
      held.length === found.length && held.every((x) => found.includes(x)) ? held : found,
    );
  const { domains } = useDomains();
  const { createSource } = useCreateSource();
  const { updateSource } = useUpdateSource();
  const { renameSource } = useRenameSource();
  const { deleteSource } = useDeleteSource();
  const { updateSourceCache } = useUpdateSourceCache();
  const { updateSourceReplicate } = useUpdateSourceReplicate();
  const setSourceRegion = useSetSourceRegion();
  const { updateSourceLoadProtection } = useUpdateSourceLoadProtection();
  const { updateSourceNaming } = useUpdateSourceNaming();
  const { updateSourceAllowedDomains } = useUpdateSourceAllowedDomains();
  const [settingsLoading, setSettingsLoading] = useState(true);
  const [showForm, setShowForm] = useState(false);
  // The registered source whose settings the form's state holds for its read view.
  const [readSeeded, setReadSeeded] = useState<string | null>(null);
  const [editingSourceId, setEditingSourceId] = useState<string | null>(null);
  const [typePickerOpen, setTypePickerOpen] = useState(false);
  // "+ Source" opens the type picker first; the create form opens on a choice, with that type.
  const [addPickerOpen, setAddPickerOpen] = useState(false);
  // Only the type-specific branches below (snowflake/databricks/.../sparql) rebuild
  // federation_hints on submit -- any OTHER type's hints (e.g. kaggle_owner/kaggle_ref, stashed on
  // create by KaggleFormSection with no UI field of its own to reconstruct them from) fell through
  // to {} on every edit, silently wiping them. Stashed here on edit-open, merged back in on submit
  // only for a type with no dedicated branch -- never overrides a branch's own intentional {} (e.g.
  // hiveserver2 omitting the default PLAIN auth_mechanism on purpose).
  const [editingSourceHintsJson, setEditingSourceHintsJson] = useState<string | null>(null);
  const [expanded, setExpanded] = useState<string | null>(() => searchParams.get("expanded"));
  const [error, setError] = useState<string | null>(null);
  const [sourceSearch, setSourceSearch] = useState(() => searchParams.get("search") ?? "");
  const [page, setPage] = useState(0);
  const PAGE_SIZE = 50;
  const { regions, connected, error: regionError } = useRegionChoices();
  const [regionSel, setRegionSel] = useRegionSelection(regions, connected);
  const hasRegions = regions.length > 0;
  // REQ-1940: sort and group are the shared list mechanism.
  const filteredSources = useMemo(
    () =>
      sources.filter((s) => {
        if ([DERIVED_SOURCE_ID, "provisa-admin", "provisa-otel"].includes(s.id)) return false;
        if (!sourceSearch.trim()) return true;
        const q = sourceSearch.toLowerCase();
        return (
          s.id.toLowerCase().includes(q) ||
          s.type.toLowerCase().includes(q) ||
          (s.description ?? "").toLowerCase().includes(q)
        );
      }),
    [sources, sourceSearch],
  );
  const listColumns = useMemo<ListColumn<Source>[]>(
    () => [
      { key: "id", label: t("sourcesPage.colId"), sortValue: (s) => s.id },
      {
        key: "type",
        label: t("sourcesPage.colType"),
        sortValue: (s) => sourceTypeLabel(s.type, s.federationHintsJson),
        groupValue: (s) => sourceTypeLabel(s.type, s.federationHintsJson),
      },
      { key: "host", label: t("sourcesPage.colHost"), sortValue: (s) => s.host ?? "" },
      { key: "port", label: t("sourcesPage.colPort"), sortValue: (s) => s.port ?? 0 },
      { key: "database", label: t("sourcesPage.colDatabase"), sortValue: (s) => s.database ?? "" },
      ...(hasRegions
        ? [
            {
              key: "region",
              label: t("regionSelector.columnHeader"),
              sortValue: (s: Source) => s.region ?? "",
              groupValue: (s: Source) => s.region ?? t("regionSelector.noRegion"),
            },
          ]
        : []),
      {
        key: "naming",
        label: t("sourcesPage.colNaming"),
        sortValue: (s) => s.gqlNamingConvention || t("sourcesPage.naOrInherit"),
        groupValue: (s) => s.gqlNamingConvention || t("sourcesPage.naOrInherit"),
      },
      {
        key: "cache",
        label: t("sourcesPage.colCache"),
        sortValue: (s) => (s.cacheEnabled ? 1 : 0),
        groupValue: (s) => (s.cacheEnabled ? t("sourcesPage.cacheOn") : t("sourcesPage.cacheOff")),
      },
    ],
    [t, hasRegions],
  );
  // REQ-1922: the region selector filters the list; a name search crosses regions (region
  // filter off).
  const searchActive = !!sourceSearch.trim();
  const { visible: regionSources, hidden: regionHidden } = searchActive
    ? { visible: filteredSources, hidden: 0 }
    : filterByRegion(filteredSources, regionSel, connected, (s) => s.region ?? null);
  const sortGroup = useListSortGroup(regionSources, listColumns, "sources");
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
    replicate: null,
    region: null,
    loadProtected: false,
    offPeakWindow: "",
    offPeakTz: "UTC",
    changeSignal: "ttl",
    sentinelPath: "",
    freshnessGate: false,
    maxLiveConcurrency: "",
    path: "" as string,
    allowedDomains: "" as string,
    description: "" as string,
  });
  // Whether the server says a source of this kind states its Cache TTL itself (REQ-1907).
  const cacheTtlRequired = useCacheTtlRequired(form.type);
  // REQ-824: source-level CDC transport (Debezium), entered once per source
  const [cdc, setCdc] = useState<CdcState>({ ...EMPTY_CDC });
  const [authType, setAuthType] = useState("none");
  const [authFields, setAuthFields] = useState<Record<string, string>>({});
  const [settings, setSettings] = useState<PlatformSettings | null>(null);
  const [sourceReach, setSourceReach] = useState<SourceReach | null>(null);
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
  // Splunk authenticates with a bearer token (the connector's default, `use_token: true`) or a
  // username/password pair; both ride in the standard username/password fields, so a secret
  // reference (`${secret:...}`, `${env:...}`) works there like for every other source.
  const [splunkAuthMode, setSplunkAuthMode] = useState<"token" | "userpass">("token");
  const [filesTransport, setFilesTransport] = useState("file://");
  const [filesAuthMode, setFilesAuthMode] = useState<"userpass" | "certificate">("userpass");
  const [filesCertPath, setFilesCertPath] = useState("");
  const [filesCertPassword, setFilesCertPassword] = useState("");
  // REQ-1739: websocket/rss federation_hints extras (see federationHintsJson above).
  const [wsUseSsl, setWsUseSsl] = useState(false);
  const [wsSubscribePayload, setWsSubscribePayload] = useState("");
  const [wsEventPath, setWsEventPath] = useState("");
  const [wsReconnectInterval, setWsReconnectInterval] = useState("");
  const [rssFeedUrl, setRssFeedUrl] = useState("");
  const [rssPollInterval, setRssPollInterval] = useState("");
  const [rssUseSsl, setRssUseSsl] = useState(true);

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

  // A new source's description is filled from what it is until one is typed; a registered source
  // (read or edited) shows the description it has.
  useEffect(() => {
    if (!showForm || editingSourceId) return;
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
  }, [form.type, form.host, form.database, editingSourceId, showForm]);

  useEffect(() => {
    if (!showForm || editingSourceId) return;
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
    showForm,
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

  // Reach faces of the engine serving this org — gates + annotates the type dropdown (REQ-947).
  // Read through the source kinds, which whoever registers sources may read; the engine settings
  // are the platform's.
  useEffect(() => {
    fetchSourceReach()
      .then(setSourceReach)
      .catch((e) => setError(String(e)));
  }, []);

  // The page loader is for the first load only. A refetch (after a save, a registration, a
  // delete) keeps the page mounted, so an open Schema Discovery panel keeps what is typed and
  // discovered in it (REQ-150); the refetched rows replace the old ones in place.
  const loading = sourcesLoading || (settingsLoading && settings === null);

  const getEffectiveTtl = (source: Source): string => {
    if (source.cacheTtl != null) return `${ttlText(source.cacheTtl, t)} (custom)`;
    if (settings) return `${settings.cache.default_ttl}s (global)`;
    return "default";
  };

  // Type dropdown annotated + gated by the current engine's reach (REQ-947): each option shows
  // LIVE / REPLICA, and a type the engine cannot reach is disabled with the engine(s) that can.
  const typeSelectData = () =>
    CATEGORIES.map((cat) => ({
      group: cat,
      items: SOURCE_TYPES.filter((s) => s.category === cat)
        .filter((s) => s.value !== "soda" || !hostedPlane)
        .map((s) => {
          const info = reachInfoFor(s.value, sourceReach);
          return {
            value: s.value,
            label: `${pickerTypeLabel(s)}${reachSuffix(info)}`,
            // REQ-1938: the picker shows the plain name with a reach icon instead of the suffix.
            name: pickerTypeLabel(s),
            reach: info,
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
    // REQ-229: hive_s3 DECLARES S3 storage — the Storage Authentication select offers only "aws"
    // for this type (SourceFormFields.tsx), so default straight to it instead of "none".
    setAuthType(type === "hive_s3" ? "aws" : "none");
    setAuthFields({});
    resetSpFields();
  };

  // A source's stored settings into the form's state: the state its edit form opens with and its
  // read view shows. False when that state cannot be read whole (the error says why).
  const seedFrom = (s: Source): boolean => {
    const seed = seedSource(s, govdataCatalog);
    setForm(seed.form);
    setFilesTransport(seed.filesTransport);
    setCdc(seed.cdc);
    setAuthType(seed.authType);
    setAuthFields(seed.authFields);
    setSpAuthType(seed.sharepoint.authType);
    setSpCertPath(seed.sharepoint.certPath);
    setSpCertPassword(seed.sharepoint.certPassword);
    setSpUsername(seed.sharepoint.username);
    setSpPassword(seed.sharepoint.password);
    setSplunkDisableSsl(seed.splunk.disableSsl);
    setSplunkAuthMode(seed.splunk.authMode);
    setWsUseSsl(seed.websocket.useSsl);
    setWsSubscribePayload(seed.websocket.subscribePayload);
    setWsEventPath(seed.websocket.eventPath);
    setWsReconnectInterval(seed.websocket.reconnectInterval);
    setRssFeedUrl(seed.rss.feedUrl);
    setRssPollInterval(seed.rss.pollInterval);
    setRssUseSsl(seed.rss.useSsl);
    if (hasRemoteSeed(s.type)) {
      fetchRemoteSeed(s).then(applyRemoteSeed, (e: unknown) =>
        setError(e instanceof Error ? e.message : String(e)),
      );
    }
    if (seed.govdataSubjects === null) {
      setError(t("sourceFormFieldsExtended.govdataSubjectsUnavailable"));
      return false;
    }
    setGovdataSubjects(seed.govdataSubjects);
    return true;
  };

  const applyRemoteSeed = (r: RemoteSeed) => {
    if (r.kind === "graphql") {
      setGqlNamespace(r.namespace);
      setGqlCacheTtl(r.cacheTtl);
    } else if (r.kind === "openapi") {
      setOpenapiSpecMode(r.specMode);
      setOpenapiSpecPath(r.specPath);
      setOpenapiSpecInline("");
      setOpenapiBaseUrl(r.baseUrl);
      setOpenapiCacheTtl(r.cacheTtl);
    } else {
      setGrpcProtoPath(r.protoPath);
      setGrpcServerAddress(r.serverAddress);
      setGrpcNamespace(r.namespace);
      setGrpcTls(r.tls);
      setGrpcImportPaths(r.importPaths);
      setGrpcCacheTtl(r.cacheTtl);
    }
    const auth = remoteAuth(r.auth);
    if (auth) {
      setAuthType(auth.authType);
      setAuthFields(auth.authFields);
    }
  };

  const handleEdit = (s: Source) => {
    if (!seedFrom(s)) return;
    setEditingSourceId(s.id);
    setEditingSourceHintsJson(s.federationHintsJson ?? null);
    updateExpanded(s.id);
    setShowForm(false);
  };

  // The source the short create step just made is selected and its detail opens in edit, with
  // everything the step did not ask for at its default: read from the list as it is now.
  const openCreated = async (id: string) => {
    const fresh = await refetchSources();
    const made = fresh.data?.sources.find((x) => x.id === id);
    if (made) handleEdit(made);
  };

  const handleCancelForm = () => {
    setShowForm(false);
    setEditingSourceId(null);
    setEditingSourceHintsJson(null);
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
    });
    setFilesTransport("file://");
    setFilesAuthMode("userpass");
    setFilesCertPath("");
    setFilesCertPassword("");
    setCdc({ ...EMPTY_CDC });
    setAuthType("none");
    setAuthFields({});
    resetSpFields();
    setGovdataSubjects([]);
    setWsUseSsl(false);
    setWsSubscribePayload("");
    setWsEventPath("");
    setWsReconnectInterval("");
    setRssFeedUrl("");
    setRssPollInterval("");
    setRssUseSsl(true);
  };

  const handleCreate = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);
    // The Load Management and Timeliness panel's inputs show their own errors; an invalid one
    // blocks the save before any mutation runs.
    if (!sourceLoadFieldsValid(form, cacheTtlRequired)) {
      setError(t("sourceFormFieldsExtended.loadManagementFixErrors"));
      return;
    }
    setSubmitting(true);
    try {
      const {
        gqlNamingConvention: _nc,
        cacheTtl: _ct,
        cacheEnabled: _ce,
        replicate: _rep,
        region: _region, // REQ-1921: saved on its own (setSourceRegion)
        ...coreForm
      } = form;
      // Data-lake storage is a config choice, not a separate source type: the object store its tables
      // live on (Hadoop/local, S3, ADLS, GCS) + its credentials land in source.mapping, discriminated
      // by mapping.storage. Derived from the Storage Authentication select (none→hadoop, aws→s3,
      // azure→adls, gcs→gcs) so the connector wires the matching native filesystem.
      const lakeStorage =
        // REQ-229: hive_s3 DECLARES S3 storage — always "s3" regardless of the (single-option)
        // Storage Authentication select, never the hadoop/local default plain `hive` falls back to.
        form.type === "hive_s3"
          ? "s3"
          : authType === "aws"
            ? "s3"
            : authType === "azure"
              ? "adls"
              : authType === "gcs"
                ? "gcs"
                : "hadoop";
      // Warehouse-specific extras that host/port/database/username/password can't carry
      // (Source.federation_hints, provisa/core/models.py:232-235). authFields is per-type/per-auth-
      // mode scratch state that must be explicitly routed here — it is never in `coreForm`.
      const federationHints: Record<string, unknown> =
        form.type === COSMOSDB
          ? cosmosHints(authFields) // REQ-1982: how the Cosmos DB source signs in
          : form.type === QDRANT
            ? qdrantHints(authFields) // REQ-1983: whether the Qdrant source reads over gRPC
            : form.type === WIKIPEDIA
              ? wikipediaHints(authFields) // REQ-1960: the server makes these the files source's crawl
              : form.type === "athena"
                ? athenaHints(authFields) // REQ-1981
                : form.type === "snowflake"
                  ? Object.fromEntries(
                      (["warehouse", "schema", "role"] as const)
                        .filter((k) => authFields[k])
                        .map((k) => [k, authFields[k]]),
                    )
                  : form.type === "databricks" && authFields.http_path
                    ? {
                        http_path: authFields.http_path,
                        // REQ-1867: read in place only when the steward chose it; the default lands
                        ...(authFields.reach === "scan" ? { reach: "scan" } : {}),
                      }
                    : form.type === "bigquery" &&
                        authType === "service_account" &&
                        authFields.credentials_json
                      ? { credentials_path: authFields.credentials_json }
                      : // REQ-990: a CSV/Parquet file in an object store carries that store's credentials,
                        // the store named by its path (S3, GCS HMAC keys, an Azure account key).
                        CLOUD_FILE_TYPES.has(form.type) && objectStoreOf(fileLocationOf(form))
                        ? objectStoreHints(objectStoreOf(fileLocationOf(form))!, authFields)
                        : // delta_lake/iceberg: DuckDB's _s3_secret_ddl (connector_duckdb.py) reads S3 creds
                          // from federation_hints, never from mapping — unlike hive, which keeps its
                          // storage.aws creds in mapping (Trino's hive connector props). Only "aws" is wired
                          // here: DuckDB's delta_scan/iceberg_scan have no azure/gcs SECRET support yet. The
                          // region is saved with them (it was shown and then dropped). An iceberg source also
                          // saves its catalog (REQ-990), whatever its storage authentication.
                          form.type === "iceberg"
                          ? {
                              ...(authType === "aws" ? objectStoreHints("S3", authFields) : {}),
                              ...icebergCatalogHints(authFields),
                            }
                          : form.type === "delta_lake" && authType === "aws"
                            ? objectStoreHints("S3", authFields)
                            : // exasol: an optional TLS-fingerprint pin for a self-signed cert the truststore
                              // can't chain (models.py's Source.jdbc_url reads federation_hints["tls_fingerprint"]).
                              form.type === "exasol" &&
                                authType === "tls_fingerprint" &&
                                authFields.tls_fingerprint
                              ? { tls_fingerprint: authFields.tls_fingerprint }
                              : // sqlserver: an optional flag trusting a self-signed/internal-CA cert the
                                // truststore can't chain — common on internal/on-prem deployments (models.py's
                                // Source.jdbc_url reads federation_hints["trust_server_certificate"]).
                                form.type === "sqlserver" && authType === "trust_server_certificate"
                                ? { trust_server_certificate: "true" }
                                : // fabric/synapse: an optional Azure AD service-principal identity pinned per-source
                                  // (mssql_warehouse.py's MssqlWarehouseDriver._token reads tenant_id/client_id/
                                  // client_secret); absent, the driver falls back to the ambient credential.
                                  (form.type === "fabric" || form.type === "synapse") &&
                                    authType === "service_principal"
                                  ? Object.fromEntries(
                                      (["tenant_id", "client_id", "client_secret"] as const)
                                        .filter((k) => authFields[k])
                                        .map((k) => [k, authFields[k]]),
                                    )
                                  : // REQ-1731: HiveDriver.configure() reads `auth_mechanism` from federation_hints;
                                    // PLAIN is the driver's own default, so only submit it when the operator picked a
                                    // non-default mechanism (GSSAPI/LDAP) — no point round-tripping the default.
                                    form.type === "hiveserver2" &&
                                      authFields.auth_mechanism &&
                                      authFields.auth_mechanism !== "PLAIN"
                                    ? { auth_mechanism: authFields.auth_mechanism }
                                    : // REQ-1739: websocket/rss connection extras that don't fit host/port/path
                                      // (use_ssl/subscribe_payload/event_path/reconnect_interval for websocket;
                                      // feed_url/poll_interval/use_ssl for rss) travel the same channel.
                                      form.type === "websocket"
                                      ? Object.fromEntries(
                                          (
                                            [
                                              ["use_ssl", wsUseSsl ? "true" : ""],
                                              ["subscribe_payload", wsSubscribePayload.trim()],
                                              ["event_path", wsEventPath.trim()],
                                              ["reconnect_interval", wsReconnectInterval.trim()],
                                            ] as const
                                          ).filter(([, v]) => v),
                                        )
                                      : form.type === "rss"
                                        ? Object.fromEntries(
                                            (
                                              [
                                                ["feed_url", rssFeedUrl.trim()],
                                                ["poll_interval", rssPollInterval.trim()],
                                                ["use_ssl", rssUseSsl ? "true" : "false"],
                                              ] as const
                                            ).filter(([, v]) => v),
                                          )
                                        : // REQ-1740: sparql/source.py's SparqlSourceConfig.default_graph_uri —
                                          // optional, restricts queries to one named graph.
                                          form.type === "sparql" &&
                                            authFields.default_graph_uri?.trim()
                                          ? {
                                              default_graph_uri:
                                                authFields.default_graph_uri.trim(),
                                            }
                                          : // A type with no dedicated branch above has no UI field this form could
                                            // ever reconstruct its federation_hints from -- falling through to {}
                                            // unconditionally wiped them on every edit (confirmed: kaggle_owner/
                                            // kaggle_ref, stashed on create by KaggleFormSection, silently lost on
                                            // the next edit of that same csv/parquet source). Preserve verbatim
                                            // instead when editing such a type; a brand-new source has nothing yet.
                                            editingSourceId && editingSourceHintsJson
                                            ? (JSON.parse(editingSourceHintsJson) as Record<
                                                string,
                                                string
                                              >)
                                            : {};
      const federationHintsJson =
        Object.keys(federationHints).length > 0 ? JSON.stringify(federationHints) : undefined;
      // password auth (Snowflake) / personal-access-token auth (Databricks) collect into authFields,
      // never into `form.username`/`form.password` — SIMPLE_RDBMS is the only family that binds
      // those directly (SourceFormFields.tsx:136-172). key_pair/oauth auth modes have no backend
      // driver support yet (SnowflakeDriver/DatabricksDriver only implement plain password auth) —
      // deliberately left unwired rather than silently accepted and then failing at connect time.
      const authCredentials: { username?: string; password?: string } =
        form.type === "snowflake" && authType === "password"
          ? { username: authFields.username, password: authFields.password }
          : form.type === "databricks" && authType === "token"
            ? { password: authFields.access_token }
            : {};
      // REQ-2121: a folder the browser keeps for this source's code is zipped and sent again.
      const codeFields =
        CODE_SOURCE_TYPES.has(form.type) && activeOrgId
          ? await resendHeldFolder(editingSourceId ?? NEW_SOURCE, authFields, (file) =>
              uploadCodeArchive(activeOrgId, file),
            )
          : authFields;
      const spMappingJson =
        form.type === "data_profiler"
          ? profilerMappingJson(authFields) // REQ-1934
          : CODE_SOURCE_TYPES.has(form.type)
            ? codeMappingJson(form.type, codeFields) // REQ-1984, REQ-1993
            : form.type === "cloudops"
              ? cloudopsMappingJson(authFields) // REQ-1947
              : form.type === GOOGLE_WORKSPACE
                ? JSON.stringify(gwMapping(authFields)) // REQ-1923
                : form.type === MICROSOFT_365
                  ? JSON.stringify(m365Mapping(authFields)) // REQ-1923
                  : form.type === LOGS
                    ? JSON.stringify(logsMapping(authFields)) // REQ-1996
                    : form.type === "salesforce"
                      ? salesforceMappingJson(authFields) // REQ-1946
                      : form.type === "sharepoint"
                        ? JSON.stringify({
                            auth_type: spAuthType,
                            ...(spAuthType === "CERTIFICATE"
                              ? {
                                  certificate_path: spCertPath,
                                  certificate_password: spCertPassword,
                                }
                              : {}),
                            ...(spAuthType === "USERNAME_PASSWORD"
                              ? { sp_username: spUsername, sp_password: spPassword }
                              : {}),
                          })
                        : form.type === "splunk"
                          ? JSON.stringify({
                              use_token: splunkAuthMode === "token",
                              ...(splunkDisableSsl ? { disable_ssl_validation: true } : {}),
                            })
                          : // The Trino gsheets catalog needs the service-account key path alongside the metadata
                            // sheet id (which rides in `database`), so it travels in the mapping like the other
                            // connectors' extra options.
                            form.type === "google_sheets"
                            ? JSON.stringify({
                                credentials_json: authFields.credentials_json ?? "",
                              })
                            : // hive/hive_s3's storage backend creds (mapping.access_key_id/secret_access_key/
                              // region/endpoint) are a mapping-discriminated config choice read by
                              // trino_connectors.py's _hive_s3_props (provisa/core/models.py:91-92) — but
                              // delta_lake/iceberg have no such mapping.storage reader; their S3 creds route
                              // through federationHintsJson above instead (_s3_secret_ddl reads federation_hints).
                              form.type === "hive" || form.type === "hive_s3"
                              ? JSON.stringify({ storage: lakeStorage, ...authFields })
                              : undefined;
      const sourcePayload = {
        ...coreForm,
        ...authCredentials,
        type: backendType(form.type),
        offPeakWindow: coreForm.offPeakWindow?.trim() || null,
        // SourceInput is a full upsert: every load/timeliness field is sent with its current value.
        // The Cache TTL goes with it: the server judges the source's change signal against the TTL
        // in THIS input, so a source whose tables always land (a mail source) was refused at the
        // save although its TTL was filled in — it used to travel only in the call that follows.
        cacheTtl: sourceFormCacheTtl(_ct),
        sentinelPath: coreForm.sentinelPath.trim() || null,
        // REQ-2113: read in place, or downloaded into the organization's folder from here; a new
        // single-file source's table set up with it, as a draft.
        downloadFrom: coreForm.downloadFrom?.trim() || null,
        ...(editingSourceId
          ? { registerTableDraft: false, tableDomainId: null }
          : draftTable(form, domains)),
        maxLiveConcurrency:
          coreForm.maxLiveConcurrency.trim() === "" ? null : Number(coreForm.maxLiveConcurrency),
        path:
          // REQ-1736: DuckDBFirebirdConnector.details() (connector_duckdb.py) builds its DSN from
          // source.path, not source.database — but firebird is SIMPLE_RDBMS, whose single shared
          // "Database" input binds to form.database (the .fdb file path the user actually types
          // there). Route it into `path` on submit rather than adding a firebird-only form branch.
          // duckdb-as-a-source has the identical split: DuckDBDriver's source_pools connection
          // (introspect.py's native_schemas/native_tables "duckdb" branch) reads source.database,
          // but DuckDBDuckdbConnector.details() (the ATTACH the query-time federation engine
          // issues) reads source.path — SourceFormFields' "File Path" input for form.type ===
          // "duckdb" binds to form.database, same shared-field shape as firebird, so it needs the
          // same routing here or the engine ATTACHes `None` at query time.
          form.type === "firebird" || form.type === "duckdb"
            ? form.database || null
            : form.type === WIKIPEDIA
              ? // REQ-1960: where the crawl's files land; empty lets the server give the source a
                // directory of its own under the data directory.
                authFields.wp_directory?.trim() || null
              : FILE_SOURCES.has(form.type) ||
                  form.type === "files" ||
                  form.type === "delta_lake" ||
                  form.type === "iceberg" ||
                  // REQ-1739: websocket/rss's path is optional (default "/") — the push_wiring.py /
                  // subscribe.py URL derivation reads Source.path the same way a file source does.
                  form.type === "websocket" ||
                  form.type === "rss"
                ? form.type === "files" && form.path
                  ? filesTransport === "file://"
                    ? form.path
                    : filesTransport + form.path
                  : form.path || null
                : null,
        database:
          form.type === "govdata"
            ? schemasForSubjects(requireGovdataCatalog(), govdataSubjects).join(",")
            : coreForm.database,
        ...(spMappingJson !== undefined ? { mappingJson: spMappingJson } : {}),
        ...(federationHintsJson !== undefined ? { federationHintsJson } : {}),
        // REQ-824: attach source-level CDC transport only for Debezium-captured RDBMS
        cdc:
          cdcTransportApplicable(form.type) && cdc.bootstrapServers && cdc.topicPrefix
            ? {
                bootstrapServers: cdc.bootstrapServers,
                topicPrefix: cdc.topicPrefix,
                schemaRegistryUrl: cdc.schemaRegistryUrl || null,
                ...schemaRegistryAuthPayload(cdc),
                consumerGroupId: cdc.consumerGroupId.trim() || null,
              }
            : null,
      };
      if (editingSourceId) {
        const effectiveId = form.id.trim() || editingSourceId;
        if (effectiveId !== editingSourceId) {
          const renameResult = await renameSource(editingSourceId, effectiveId);
          if (!renameResult.success) throw refusalError(renameResult);
        }
        const result = await updateSource({ ...sourcePayload, id: effectiveId });
        if (!result.success) throw refusalError(result);
        const ttlValue = form.cacheTtl.trim() === "" ? null : parseInt(form.cacheTtl, 10);
        if (ttlValue !== null && isNaN(ttlValue)) throw new Error("TTL must be a number");
        const cacheResult = await updateSourceCache(effectiveId, form.cacheEnabled, ttlValue);
        if (!cacheResult.success) throw refusalError(cacheResult);
        // REQ-826: the server refuses load protection with Replicate = Never, judged against what
        // is stored at each save. Turning load protection off goes first, so a following Never is
        // accepted; turning it on goes last, after the Replicate value it is compatible with.
        const saveReplicate = async () => {
          const replicateResult = await updateSourceReplicate(effectiveId, form.replicate);
          if (!replicateResult.success) throw refusalError(replicateResult);
        };
        const saveLoadProtection = async () => {
          const loadProtResult = await updateSourceLoadProtection(
            effectiveId,
            form.loadProtected,
            form.offPeakWindow.trim() || null,
            form.offPeakTz.trim() || "UTC",
          );
          if (!loadProtResult.success) throw refusalError(loadProtResult);
        };
        if (form.loadProtected) {
          await saveReplicate();
          await saveLoadProtection();
        } else {
          await saveLoadProtection();
          await saveReplicate();
        }
        const namingResult = await updateSourceNaming(
          effectiveId,
          form.gqlNamingConvention === "" ? null : form.gqlNamingConvention,
        );
        if (!namingResult.success) throw refusalError(namingResult);
        // REQ-1921: the region its new tables start in, saved on its own when it changed.
        const savedRegion = sources.find((s) => s.id === effectiveId)?.region ?? null;
        if (form.region !== savedRegion) {
          const regionResult = await setSourceRegion(effectiveId, form.region);
          if (!regionResult.success) throw refusalError(regionResult);
        }
        const parsedDomains = form.allowedDomains
          .split(",")
          .map((d) => d.trim())
          .filter(Boolean);
        const domainsResult = await updateSourceAllowedDomains(effectiveId, parsedDomains);
        if (!domainsResult.success) throw refusalError(domainsResult);
      } else {
        const { allowedDomains: _ad, ...createPayload } = sourcePayload as typeof sourcePayload & {
          allowedDomains?: unknown;
        };
        void _ad;
        const createResult = await createSource(
          createPayload as Parameters<typeof createSource>[0],
        );
        if (!createResult.success) throw refusalError(createResult);
        // REQ-1907: the cache and Replicate settings offered at create are saved with it.
        const createTtl = form.cacheTtl.trim() === "" ? null : parseInt(form.cacheTtl, 10);
        if (createTtl !== null && isNaN(createTtl)) throw new Error("TTL must be a number");
        const createCache = await updateSourceCache(form.id, form.cacheEnabled, createTtl);
        if (!createCache.success) throw refusalError(createCache);
        const createReplicate = await updateSourceReplicate(form.id, form.replicate);
        if (!createReplicate.success) throw refusalError(createReplicate);
        if (form.region !== null) {
          const createRegion = await setSourceRegion(form.id, form.region); // REQ-1921
          if (!createRegion.success) throw refusalError(createRegion);
        }
        if (form.loadProtected) {
          const lp = await updateSourceLoadProtection(
            form.id,
            true,
            form.offPeakWindow.trim() || null,
            form.offPeakTz.trim() || "UTC",
          );
          if (!lp.success) throw refusalError(lp);
        }
        const parsedDomainsCreate = form.allowedDomains
          .split(",")
          .map((d) => d.trim())
          .filter(Boolean);
        if (parsedDomainsCreate.length > 0) {
          const domainsResult = await updateSourceAllowedDomains(form.id, parsedDomainsCreate);
          if (!domainsResult.success) throw refusalError(domainsResult);
        }
      }
      const created = editingSourceId ? null : form.id.trim();
      if (created) keepFolderAs(created); // REQ-2121
      handleCancelForm();
      load();
      if (created) await openCreated(created);
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
  const [keptTables, setKeptTables] = useState<KeptTable[]>([]);

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

  // REQ-540: the subjects ticked for an AskAmerica source. What each brings, and the linker
  // schemas every such source serves, are the server's (useGovdataSubjects).
  const [govdataSubjects, setGovdataSubjects] = useState<string[]>([]);
  const { catalog: govdataCatalog } = useGovdataSubjects();
  const requireGovdataCatalog = () => {
    if (!govdataCatalog) throw new Error(t("sourceFormFieldsExtended.govdataSubjectsUnavailable"));
    return govdataCatalog;
  };

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
        throw new Error(serverMessage(body, requestFailed("Schema refresh", resp.status)));
      }
      // REQ-1918: tables the remote no longer has that something still refers to were kept.
      setKeptTables(keptTablesOf(await resp.json().catch(() => null)));
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
        throw new Error(serverMessage(body, requestFailed("OpenAPI preview", resp.status)));
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
        body: JSON.stringify(
          form.type in BRAND_CARRIER
            ? // REQ-1923: a branded source sends its brand and a credential; the spec and the
              // address are the brand's, and no tables are registered by adding it.
              { source_id: form.id, brand: form.type, token: authFields.token ?? "" }
            : {
                ...(openapiSpecMode === "inline"
                  ? { spec_content: openapiSpecInline }
                  : { spec_path: openapiSpecPath }),
                base_url: openapiBaseUrl || undefined,
                source_id: form.id,
                domain_id: "",
                auth_config: authType !== "none" ? { type: authType, ...authFields } : null,
                cache_ttl: parseInt(openapiCacheTtl, 10) || 300,
              },
        ),
      });
      if (!resp.ok) {
        const body = await resp.json().catch(() => ({ detail: resp.statusText }));
        throw new Error(serverMessage(body, requestFailed("OpenAPI register", resp.status)));
      }
      // REQ-1907: the cache TTL offered at create, as the GraphQL and generic paths apply it.
      const ttlValue = form.cacheTtl.trim() === "" ? null : parseInt(form.cacheTtl, 10);
      if (ttlValue !== null && isNaN(ttlValue)) throw new Error("TTL must be a number");
      const cacheResult = await updateSourceCache(form.id, form.cacheEnabled, ttlValue);
      if (!cacheResult.success) throw refusalError(cacheResult);
      const created = editingSourceId ? null : form.id.trim();
      handleCancelForm();
      load();
      if (created) await openCreated(created);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  };

  const handleGrpcRegister = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);
    try {
      // Registration is entirely server-side (grpc_remote_router.py upserts the `sources` row
      // itself with the real SourceType, "grpc_remote" — the same pattern
      // register_graphql_remote_source uses), so there is no separate createSource call here,
      // matching handleGraphqlRegister just above.
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
        throw new Error(serverMessage(body, requestFailed("gRPC register", resp.status)));
      }
      // REQ-1907: the cache TTL offered at create, as the GraphQL and generic paths apply it.
      const ttlValue = form.cacheTtl.trim() === "" ? null : parseInt(form.cacheTtl, 10);
      if (ttlValue !== null && isNaN(ttlValue)) throw new Error("TTL must be a number");
      const cacheResult = await updateSourceCache(form.id, form.cacheEnabled, ttlValue);
      if (!cacheResult.success) throw refusalError(cacheResult);
      const created = editingSourceId ? null : form.id.trim();
      handleCancelForm();
      load();
      if (created) await openCreated(created);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    }
  };

  const handleGraphqlRegister = async (e: React.FormEvent) => {
    e.preventDefault();
    setError(null);
    setSubmitting(true);
    try {
      const resp = await fetch("/admin/sources/graphql-remote", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(
          form.type in BRAND_CARRIER
            ? {
                // REQ-1923: a branded source sends its brand and a credential; the endpoint
                // is the brand's, and no tables are registered by adding it.
                source_id: form.id,
                brand: form.type,
                namespace: gqlNamespace,
                domain_id: "",
                auth: { type: "bearer", token: authFields.token ?? "" },
                description: form.description,
              }
            : {
                source_id: form.id,
                url: form.host,
                namespace: gqlNamespace,
                domain_id: "",
                auth: authType !== "none" ? { type: authType, ...authFields } : null,
                cache_ttl: parseInt(gqlCacheTtl, 10) || 300,
                description: form.description,
              },
        ),
      });
      if (!resp.ok) {
        const body = await resp.json().catch(() => ({ detail: resp.statusText }));
        throw new Error(serverMessage(body, requestFailed("GraphQL register", resp.status)));
      }
      const ttlValue = form.cacheTtl.trim() === "" ? null : parseInt(form.cacheTtl, 10);
      if (ttlValue !== null && isNaN(ttlValue)) throw new Error("TTL must be a number");
      const cacheResult = await updateSourceCache(form.id, form.cacheEnabled, ttlValue);
      if (!cacheResult.success) throw refusalError(cacheResult);
      const loadProtResult = await updateSourceLoadProtection(
        form.id,
        form.loadProtected,
        form.offPeakWindow.trim() || null,
        form.offPeakTz.trim() || "UTC",
      );
      if (!loadProtResult.success) throw refusalError(loadProtResult);
      const parsedDomains = form.allowedDomains
        .split(",")
        .map((d) => d.trim())
        .filter(Boolean);
      const domainsResult = await updateSourceAllowedDomains(form.id, parsedDomains);
      if (!domainsResult.success) throw refusalError(domainsResult);
      const created = editingSourceId ? null : form.id.trim();
      handleCancelForm();
      load();
      if (created) await openCreated(created);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setSubmitting(false);
    }
  };

  // Shared props object passed to SourceFormFields at both call sites.
  const sourceFormFieldsProps: SourceFormFieldsProps = {
    form,
    setForm,
    authType,
    setAuthType,
    authFields,
    setAuthFields,
    // A source read is a registered source, as one being edited is.
    editingSourceId: editingSourceId ?? (showForm ? null : readSeeded),
    cdc,
    setCdc,
    domainsEnabled,
    domains,
    spAuthType,
    setSpAuthType,
    spCertPath,
    setSpCertPath,
    spCertPassword,
    setSpCertPassword,
    spUsername,
    setSpUsername,
    spPassword,
    setSpPassword,
    splunkDisableSsl,
    setSplunkDisableSsl,
    splunkAuthMode,
    setSplunkAuthMode,
    filesTransport,
    setFilesTransport,
    filesAuthMode,
    setFilesAuthMode,
    filesCertPath,
    setFilesCertPath,
    filesCertPassword,
    setFilesCertPassword,
    govdataSubjects,
    setGovdataSubjects,
    govdataSubjectOptions: govdataCatalog?.subjects ?? null,
    submitting,
    openapiSpecPath,
    setOpenapiSpecPath,
    openapiSpecInline,
    setOpenapiSpecInline,
    openapiSpecMode,
    setOpenapiSpecMode,
    openapiBaseUrl,
    setOpenapiBaseUrl,
    openapiCacheTtl,
    setOpenapiCacheTtl,
    openapiPreview,
    openapiPreviewing,
    openapiPreviewError,
    onOpenapiPreview: handleOpenapiPreview,
    gqlNamespace,
    setGqlNamespace,
    gqlCacheTtl,
    setGqlCacheTtl,
    grpcProtoPath,
    setGrpcProtoPath,
    grpcServerAddress,
    setGrpcServerAddress,
    grpcNamespace,
    setGrpcNamespace,
    grpcTls,
    setGrpcTls,
    grpcImportPaths,
    setGrpcImportPaths,
    grpcCacheTtl,
    setGrpcCacheTtl,
    wsUseSsl,
    setWsUseSsl,
    wsSubscribePayload,
    setWsSubscribePayload,
    wsEventPath,
    setWsEventPath,
    wsReconnectInterval,
    setWsReconnectInterval,
    rssFeedUrl,
    setRssFeedUrl,
    rssPollInterval,
    setRssPollInterval,
    rssUseSsl,
    setRssUseSsl,
    // REQ-1780/1783: kaggle has no createSource of its own to run on the outer form's submit (it
    // is not a real SourceType) — KaggleFormSection stages+registers its own csv/parquet
    // Sources/Tables and calls this when done, same close+refresh handleOpenapiRegister/
    // handleGrpcRegister already do for their own server-side-registration types.
    onKaggleSourcesRegistered: () => {
      handleCancelForm();
      load();
    },
  };

  const selectedSource = expanded ? (sources.find((x) => x.id === expanded) ?? null) : null;
  // The selected source's settings, in the form's state: its read view is its edit form, read.
  useEffect(() => {
    if (!selectedSource || editingSourceId !== null || showForm) return;
    // eslint-disable-next-line react-hooks/set-state-in-effect -- the form's state is read from the selected source whenever it changes, as Edit reads it
    if (seedFrom(selectedSource)) setReadSeeded(selectedSource.id);
    // eslint-disable-next-line react-hooks/exhaustive-deps -- seedFrom is a fresh closure each render; the source, edit and create states decide a reseed
  }, [selectedSource, editingSourceId, showForm]);
  if (loading) return <PageLoading message={t("sourcesPage.loading")} />;

  // What a source's row offered beside it: now in the head of its panel, with its name.
  const sourceActions = (s: Source) => (
    <PanelActions>
      <Group gap="xs" wrap="nowrap">
        {DISCOVERABLE_TYPES.has(s.type) && (
          <Button
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
          // Held open while the refresh runs: pressing the icon closes its hint, and a busy icon
          // is disabled, so pointing at it shows nothing -- the hint is what says it is running.
          <TipSurface
            content={
              refreshingSourceId === s.id
                ? t("sourcesPage.refreshing")
                : t("sourcesPage.refreshSchema")
            }
            opened={refreshingSourceId === s.id}
            named
          >
            <ActionIcon
              aria-label={t("sourcesPage.refreshSchema")}
              data-testid={`sources-refresh-schema-${s.id}`}
              loading={refreshingSourceId === s.id}
              onClick={() => handleRefreshSchema(s.id, s.type)}
            >
              <RefreshCw size={14} />
            </ActionIcon>
          </TipSurface>
        )}
      </Group>
    </PanelActions>
  );

  // What the right-hand panel shows: a working surface opened from a source (it has a way back
  // to the source), the short step that creates one, or the selected source -- being edited, or
  // read.
  // One form for a source: edited in the panel, or read there.
  const sourceForm = (
    <FormLayout
      component="form"
      id="sources-edit-form"
      className="form-card"
      onSubmit={
        carrierOf(form.type) === "openapi"
          ? handleOpenapiRegister
          : form.type === "grpc"
            ? handleGrpcRegister
            : carrierOf(form.type) === "graphql"
              ? handleGraphqlRegister
              : handleCreate
      }
      style={{ margin: 0 }}
    >
      <FormIdentifiers>
        <FormField size="medium">
          <TextInput
            label={t("sourcesPage.idLabel")}
            required
            value={form.id}
            onChange={(e) => setForm({ ...form, id: e.target.value })}
          />
        </FormField>
        <FormField size="long">
          <Select
            label={t("sourcesPage.typeLabel")}
            value={form.type}
            onChange={(v) => v && handleTypeChange(v)}
            data={typeSelectData()}
            allowDeselect={false}
            searchable
          />
        </FormField>
      </FormIdentifiers>
      <SourceFormFields {...sourceFormFieldsProps} />
      <FormFooter
        panel
        submit
        form="sources-edit-form"
        saving={submitting}
        onCancel={handleCancelForm}
        saveProps={{ "data-testid": "sources-edit-submit" }}
        cancelProps={{ "data-testid": "sources-edit-cancel" }}
      />
    </FormLayout>
  );
  const working =
    discoverSourceId && discoverSourceType ? (
      <PanelWorking
        back={t("sourcesPage.backToSource")}
        onBack={() => {
          setDiscoverSourceId(null);
          setDiscoverSourceType(null);
        }}
        testId="sources-working-back"
      >
        <SchemaDiscovery
          sourceId={discoverSourceId}
          sourceType={discoverSourceType}
          onClose={() => {
            setDiscoverSourceId(null);
            setDiscoverSourceType(null);
          }}
          onRegistered={load}
        />
      </PanelWorking>
    ) : mappingSourceId && mappingSourceType ? (
      <PanelWorking
        back={t("sourcesPage.backToSource")}
        onBack={() => {
          setMappingSourceId(null);
          setMappingSourceType(null);
        }}
        testId="sources-working-back"
      >
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
      </PanelWorking>
    ) : null;
  const panelBody: React.ReactNode =
    working ??
    (showForm && !editingSourceId ? (
      <FormLayout
        component="form"
        id="sources-new-form"
        data-tour="sources-form"
        className="form-card"
        onSubmit={
          carrierOf(form.type) === "openapi"
            ? handleOpenapiRegister
            : form.type === "grpc"
              ? handleGrpcRegister
              : carrierOf(form.type) === "graphql"
                ? handleGraphqlRegister
                : // REQ-1780/1783: kaggle registers per-file through KaggleFormSection's own
                  // "Add Dataset" button (each file needs its own createSource+registerTable
                  // call, not one createSource for a single "kaggle" type — which does not
                  // exist as a SourceType) — the outer submit has nothing of its own to do.
                  form.type === "kaggle"
                  ? (e: React.FormEvent) => e.preventDefault()
                  : handleCreate
        }
      >
        <FormIdentifiers>
          <FormField size="medium">
            <TextInput
              label={t("sourcesPage.idLabel")}
              required
              value={form.id}
              onChange={(e) => setForm({ ...form, id: e.currentTarget.value })}
              placeholder={t("sourcesPage.idPlaceholder")}
              data-testid="sources-id-input"
            />
          </FormField>
          <FormField size="long">
            <label>
              {t("sourcesPage.typeLabel")}
              <select
                className="form-select"
                data-tour="sources-type"
                data-testid="sources-type-select"
                value={form.type}
                onChange={(e) => handleTypeChange(e.target.value)}
                // REQ-1938: clicking the field, or opening it from the keyboard, shows the picker
                // dialog instead of the long native list. The select still holds the value, so
                // setting it programmatically (selectOption) works unchanged.
                onMouseDown={(e) => {
                  e.preventDefault();
                  setTypePickerOpen(true);
                }}
                // A click that was not preceded by a mouse press (the guided tour's) opens it too.
                onClick={() => setTypePickerOpen(true)}
                onKeyDown={(e) => {
                  if (e.key === "Enter" || e.key === " " || (e.altKey && e.key === "ArrowDown")) {
                    e.preventDefault();
                    setTypePickerOpen(true);
                  }
                }}
              >
                {typeSelectData().map((group) => (
                  <optgroup
                    key={group.group}
                    label={t(`sourceTypePicker.categories.${group.group}`)}
                  >
                    {group.items.map((item) => (
                      <option key={item.value} value={item.value} disabled={item.disabled}>
                        {item.label}
                      </option>
                    ))}
                  </optgroup>
                ))}
              </select>
            </label>
          </FormField>
        </FormIdentifiers>
        <SourceTypePicker
          opened={typePickerOpen}
          onClose={() => setTypePickerOpen(false)}
          groups={typeSelectData()}
          value={form.type}
          onPick={handleTypeChange}
        />
        {/* Only what a source needs in order to exist; the rest is set in the edit form the
            new source opens in. */}
        <SourceFormFields {...sourceFormFieldsProps} essential />
        {/* The same two actions, in the same place and order, as the edit form below: Save first,
            Cancel beside it, at the start of the row and as wide as their labels. A Kaggle source
            is created by its own section's button, so it gets Cancel alone. */}
        <FormFooter
          panel
          submit={form.type !== "kaggle"}
          form="sources-new-form"
          saving={submitting}
          onCancel={handleCancelForm}
          saveProps={{ "data-testid": "sources-submit" }}
          cancelProps={{ "data-testid": "sources-cancel" }}
        />
      </FormLayout>
    ) : selectedSource ? (
      ((s: Source) => {
        const shown = shownSourceSections(s.type, formSections);
        // A section the source's kind does not have is not in view; its first is.
        const inView: SourceSection = shown.includes(section) ? section : "connection";
        const sectioned = (node: ReactNode) => (
          <FormSections active={inView} fallback="connection" onSections={noteFormSections}>
            {node}
          </FormSections>
        );
        return (
          <>
            {/* While the form is open the panel's head is the form's: its Save and Cancel. */}
            {editingSourceId !== s.id && sourceActions(s)}
            <SourceSectionTabs value={inView} shown={shown} onChange={setSection} />
            {inView === "tables" && <SourceTablesList sourceId={s.id} tables={registeredTables} />}
            {inView === "operations" && <SourceOperations sourceId={s.id} type={s.type} />}
            {editingSourceId === s.id ? (
              sectioned(sourceForm)
            ) : (
              <SourceDetailPanel
                s={s}
                fields={
                  readSeeded === s.id ? (
                    <FormRead
                      testId="source-read"
                      derived={
                        inView === "load"
                          ? [
                              {
                                label: t("sourceDetailPanel.field.effectiveTtl"),
                                value: getEffectiveTtl(s),
                              },
                            ]
                          : []
                      }
                    >
                      {sectioned(sourceForm)}
                    </FormRead>
                  ) : null
                }
                onEdit={() => handleEdit(s)}
                onNavigate={() => navigate(`/tables?source=${encodeURIComponent(s.id)}`)}
                onDelete={async () => {
                  const result = await deleteSource(s.id);
                  if (refusal.refused(result, s.id)) return;
                  if (expanded === s.id) updateExpanded(null);
                  load();
                }}
              />
            )}
          </>
        );
      })(selectedSource)
    ) : null);
  const panelIsReadOnly = working === null && !showForm && editingSourceId === null;
  const panelTitle =
    showForm && !editingSourceId
      ? t("sourcesPage.panelNewSource")
      : (selectedSource?.id ?? discoverSourceId ?? mappingSourceId ?? "");
  const closePanel = () => {
    setDiscoverSourceId(null);
    setDiscoverSourceType(null);
    setMappingSourceId(null);
    setMappingSourceType(null);
    if (showForm || editingSourceId) handleCancelForm();
    updateExpanded(null);
  };

  return (
    <div className="page page-sticky-head">
      <div className="page-header">
        <Title order={2}>{t("sourcesPage.title")}</Title>
        <FilterInput
          value={sourceSearch}
          onChange={updateSearch}
          placeholder={t("sourcesPage.filterPlaceholder")}
        />
        <RegionSelector
          regions={regions}
          connected={connected}
          value={regionSel}
          onChange={setRegionSel}
          hidden={regionHidden}
          error={regionError}
        />
        <div className="page-actions">
          {/* Not shown while its form is open: the panel has its own close. */}
          {!editingSourceId && !showForm && (
            <Button
              data-tour="sources-add"
              data-testid="sources-add-toggle"
              onClick={() => setAddPickerOpen(true)}
            >
              {t("sourcesPage.addSource")}
            </Button>
          )}
          {/* Closed without a choice, it opens nothing. */}
          <SourceTypePicker
            opened={addPickerOpen}
            onClose={() => setAddPickerOpen(false)}
            groups={typeSelectData()}
            value={form.type}
            onPick={(type) => {
              handleTypeChange(type);
              setShowForm(true);
            }}
          />
          <HelpBubble
            title={t("sourcesPage.purposeTitle")}
            paragraphs={[t("sourcesPage.purposeBody"), t("sourcesPage.purposeAdd")]}
            ariaLabel={t("sourcesPage.purposeAria")}
            testId="sources-purpose-help"
          />
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
      <KeptTablesNotice kept={keptTables} onClose={() => setKeptTables([])} />

      <SplitPane
        testId="sources-split"
        storageKey="provisa.sources.detailWidth"
        title={panelTitle}
        onClose={closePanel}
        // Escape closes a source that is only being looked at; it never drops what is being typed.
        closeOnEscape={panelIsReadOnly}
        labels={{
          panel: t("sourcesPage.detailPanel"),
          resize: t("sourcesPage.resizeDetail"),
          expand: t("sourcesPage.expandDetail"),
          restore: t("sourcesPage.restoreDetail"),
          close: t("sourcesPage.closeDetail"),
          backToList: t("sourcesPage.backToList"),
        }}
        list={
          <>
            {/* REQ-1587: the page owns the vertical scroll, so the table's own scroller is the plain
                horizontal one — a Table.ScrollContainer would become the scrollport the header sticks to
                and, growing to its content, would never scroll vertically. */}
            <ListTable minWidth={860} testId="sources-list">
              <ListHead
                sortGroup={sortGroup}
                columns={[
                  { col: "id" },
                  { col: "type" },
                  { col: "host" },
                  { col: "port" },
                  { col: "database" },
                  ...(hasRegions ? [{ col: "region" }] : []),
                  { col: "naming" },
                  { col: "cache" },
                  t("sourcesPage.colEffectiveTtl"),
                ]}
              />
              <Table.Tbody>
                {(() => {
                  if (regionSources.length === 0) {
                    return (
                      <ListEmpty colSpan={hasRegions ? 9 : 8}>{t("sourcesPage.empty")}</ListEmpty>
                    );
                  }
                  return (
                    <ListItems
                      state={sortGroup}
                      items={pageItems(sortGroup, page, PAGE_SIZE)}
                      colSpan={hasRegions ? 9 : 8}
                      rowKey={(s) => s.id}
                      render={(s) => {
                        const isExpanded = expanded === s.id;
                        const isEditing = editingSourceId === s.id;
                        return (
                          <React.Fragment key={s.id}>
                            <ListRow
                              testId={`sources-row-${s.id}`}
                              // The row whose detail the right-hand panel is showing.
                              selected={isExpanded}
                              onClick={() => {
                                updateExpanded(isExpanded ? null : s.id);
                                if (isEditing && isExpanded) {
                                  setEditingSourceId(null);
                                  handleCancelForm();
                                }
                              }}
                            >
                              <Table.Td>
                                {/* As on Tables: the tag chips follow the identifier, the edit-tags control follows the chips. */}
                                <Group gap="0.35rem">
                                  {s.id}
                                  <TagControl objectType="source" sourceId={s.id} />
                                </Group>
                              </Table.Td>
                              <Table.Td>{sourceTypeLabel(s.type, s.federationHintsJson)}</Table.Td>
                              <Table.Td>{s.host}</Table.Td>
                              <Table.Td>{s.port || "—"}</Table.Td>
                              <Table.Td>{s.database || "—"}</Table.Td>
                              {hasRegions && (
                                <Table.Td c="dimmed" fz="0.85rem">
                                  {s.region ?? t("regionSelector.noRegion")}
                                </Table.Td>
                              )}
                              <Table.Td c="dimmed" fz="0.85rem">
                                {s.gqlNamingConvention || t("sourcesPage.naOrInherit")}
                              </Table.Td>
                              <Table.Td c="dimmed" fz="0.85rem">
                                {s.cacheEnabled
                                  ? t("sourcesPage.cacheOn")
                                  : t("sourcesPage.cacheOff")}
                              </Table.Td>
                              <Table.Td c="dimmed" fz="0.85rem">
                                {getEffectiveTtl(s)}
                              </Table.Td>
                            </ListRow>
                          </React.Fragment>
                        );
                      }}
                    />
                  );
                })()}
              </Table.Tbody>
            </ListTable>

            {(() => {
              const filtered = filteredSources;
              const totalPages = Math.max(1, Math.ceil(filtered.length / PAGE_SIZE));
              if (totalPages === 1 || sortGroup.groupBy.length > 0) return null;
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
          </>
        }
      >
        {panelBody}
      </SplitPane>
      {refusal.dialog}
    </div>
  );
}
