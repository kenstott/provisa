// Copyright (c) 2026 Kenneth Stott
// Canary: cd14a3dd-06e4-4cbc-9643-ca1214ad376e
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useCallback, useMemo } from "react";
import { useQuery, useLazyQuery, useMutation } from "@apollo/client/react";
import type { Role } from "../types/auth";
import type {
  Source,
  Domain,
  RegisteredTable,
  Metric,
  RefreshPolicySummary,
  DqContract,
  DqContractText,
  DqContractParseVars,
  DqContractBuildVars,
  DqCheckCatalog,
  DqCheckCatalogVars,
  DqCheckDefinition,
  DqCheckDefinitionVars,
  DqDryRun,
  DqDryRunVars,
  Relationship,
  RLSRule,
  MutationResult,
} from "../types/admin";
import type { CompileResult, TableMetadata, ColumnMetadata } from "../api/admin";
import {
  RolesQuery as ROLES_QUERY,
  SourcesQuery as SOURCES_QUERY,
  DomainsQuery as DOMAINS_QUERY,
  TablesQuery as TABLES_QUERY,
  Calendars as CALENDARS_QUERY,
  CreateCalendar as CREATE_CALENDAR_MUTATION,
  DeleteCalendar as DELETE_CALENDAR_MUTATION,
  RefreshPolicyPreview as REFRESH_POLICY_PREVIEW_QUERY,
  DqContractParse as DQ_CONTRACT_PARSE_QUERY,
  DqContractBuild as DQ_CONTRACT_BUILD_QUERY,
  DqCheckCatalog as DQ_CHECK_CATALOG_QUERY,
  DqCheckDefinition as DQ_CHECK_DEFINITION_QUERY,
  DryRunDqContract as DRY_RUN_DQ_CONTRACT_MUTATION,
  MetricsQuery as METRICS_QUERY,
  UpsertMetric as UPSERT_METRIC_MUTATION,
  DeleteMetric as DELETE_METRIC_MUTATION,
  RelationshipsQuery as RELATIONSHIPS_QUERY,
  AllRelationshipsQuery as ALL_RELATIONSHIPS_QUERY,
  RLSRulesQuery as RLS_RULES_QUERY,
  AvailableSchemas,
  AvailableTables,
  AvailableColumnsMetadata,
  AvailableFunctions,
  GenerateColumnDescription,
  GenerateTableDescription,
  CompileQuery,
  CreateDomain,
  DeleteDomain,
  RegisterTable,
  RegisterEntity,
  RegisterFact,
  UpdateTable,
  DeleteTable,
  DeployViewToDb,
  UpsertRelationship,
  DeleteRelationship,
  CreateSource,
  UpdateSource,
  DeleteSource,
  RenameSource,
  UpsertRlsRule,
  DeleteRlsRule,
  CreateRole,
  DeleteRole,
  PurgeCache,
  UpdateSourceCache,
  UpdateTableCache,
  ForceRegen,
  UpdateSourcePreferMaterialized,
  UpdateTablePreferMaterialized,
  UpdateSourceLoadProtection,
  UpdateTableLoadProtection,
  UpdateSourceNaming,
  UpdateTableNaming,
  UpdateSourceAllowedDomains,
  SuggestTableAlias,
} from "./admin.graphql";

/**
 * `loading` on the list hooks below means "nothing to show yet", not "a request is in flight".
 *
 * They all read `cache-and-network`, which reports `loading: true` on every mount that revalidates
 * a warm cache. Pages gate their whole render on that flag, so a cached page would blank itself on
 * re-entry — which is what made the guided tour (REQ-1362) land on empty screens even after its
 * prefetch had already fetched the data. An undefined `data` is the only signal that there is
 * genuinely nothing to paint; a cached empty list is an answer, not an absence.
 */
function firstLoad(loading: boolean, data: unknown): boolean {
  return loading && data === undefined;
}

/**
 * REQ-1362: warm the data the guided tour's destination pages read, not just their code.
 *
 * Preloading the route chunks (see pageChunks.ts) only removes the fetch-and-parse delay. The
 * pages still gate their render on their own queries, so the tour would arrive on /relationships,
 * /security/*, or /admin and find a "Loading…" screen with none of the anchors its steps point at
 * — the runner then sits in waitForElement, showing nothing, for as long as those queries take.
 *
 * These are every query mounted by a page the tour navigates to: /relationships reads
 * relationships, all relationships, tables and domains; /security/roles and /security/rls read
 * roles, RLS rules, tables and domains; /sources, /tables and /views read sources, tables and
 * domains; /admin reads all of them. Running them fills the Apollo cache the page hooks then read
 * from, so each page paints on arrival and revalidates behind the visible content.
 *
 * A rejection is swallowed per query for the same reason the chunk prefetch swallows its own: an
 * endpoint the visitor's role cannot read (roles and RLS rules need admin capabilities) must not
 * keep the tour from starting. That page then loads on arrival exactly as it does without this.
 */
export function useTourPrefetch(): () => Promise<void> {
  const opts = { fetchPolicy: "network-only" as const };
  const [loadSources] = useLazyQuery(SOURCES_QUERY, opts);
  const [loadDomains] = useLazyQuery(DOMAINS_QUERY, opts);
  const [loadTables] = useLazyQuery(TABLES_QUERY, opts);
  const [loadRelationships] = useLazyQuery(RELATIONSHIPS_QUERY, opts);
  const [loadAllRelationships] = useLazyQuery(ALL_RELATIONSHIPS_QUERY, opts);
  const [loadRlsRules] = useLazyQuery(RLS_RULES_QUERY, opts);
  const [loadRoles] = useLazyQuery(ROLES_QUERY, opts);
  return useCallback(
    () =>
      Promise.all(
        [
          loadSources,
          loadDomains,
          loadTables,
          loadRelationships,
          loadAllRelationships,
          loadRlsRules,
          loadRoles,
        ].map((run) => run().catch(() => undefined)),
      ).then(() => undefined),
    [
      loadSources,
      loadDomains,
      loadTables,
      loadRelationships,
      loadAllRelationships,
      loadRlsRules,
      loadRoles,
    ],
  );
}

const NO_SOURCES: Source[] = [];
const NO_DOMAINS: Domain[] = [];
const NO_TABLES: RegisteredTable[] = [];
const NO_RELATIONSHIPS: Relationship[] = [];
const NO_RLS_RULES: RLSRule[] = [];

export function useSources() {
  const { data, loading, error, refetch } = useQuery<{ sources: Source[] }>(SOURCES_QUERY, {
    fetchPolicy: "cache-and-network",
  });
  return {
    sources: data?.sources ?? NO_SOURCES,
    loading: firstLoad(loading, data),
    error,
    refetch,
  };
}

export function useDomains() {
  const { data, loading, error, refetch } = useQuery<{ domains: Domain[] }>(DOMAINS_QUERY, {
    fetchPolicy: "cache-and-network",
  });
  return {
    domains: data?.domains ?? NO_DOMAINS,
    loading: firstLoad(loading, data),
    error,
    refetch,
  };
}

export function useTables() {
  const { data, loading, error, refetch } = useQuery<{ tables: RegisteredTable[] }>(TABLES_QUERY, {
    fetchPolicy: "cache-and-network",
  });
  return {
    tables: data?.tables ?? NO_TABLES,
    loading: firstLoad(loading, data),
    error,
    refetch,
  };
}

// REQ-1317: registered semantic metrics.
const NO_METRICS: Metric[] = [];

export function useMetrics() {
  const { data, loading, error, refetch } = useQuery<{ metrics: Metric[] }>(METRICS_QUERY, {
    fetchPolicy: "cache-and-network",
  });
  return {
    metrics: data?.metrics ?? NO_METRICS,
    loading,
    error,
    refetch,
  };
}

export interface MetricInput {
  name: string;
  expression: string;
  datatype?: string | null;
  description?: string | null;
  aiContext?: string | null;
  visibleTo?: string[];
}

export function useUpsertMetric() {
  const [upsertMetric, { loading }] = useMutation<{ upsertMetric: MutationResult }>(
    UPSERT_METRIC_MUTATION,
    { refetchQueries: [{ query: METRICS_QUERY }] },
  );
  return {
    upsertMetric: async (input: MetricInput) => {
      const result = await upsertMetric({ variables: { input } });
      return (result.data?.upsertMetric ?? { success: false, message: "" }) as MutationResult;
    },
    loading,
  };
}

export function useDeleteMetric() {
  const [deleteMetric, { loading }] = useMutation<{ deleteMetric: MutationResult }>(
    DELETE_METRIC_MUTATION,
    { refetchQueries: [{ query: METRICS_QUERY }] },
  );
  return {
    deleteMetric: async (name: string) => {
      const result = await deleteMetric({ variables: { name } });
      return (result.data?.deleteMetric ?? { success: false, message: "" }) as MutationResult;
    },
    loading,
  };
}

// REQ-962: the registered snapshot-boundary calendars — the picker source for an MV snapshot schedule.
export interface CalendarSummary {
  name: string;
  version: string;
  baseSystem: string;
  tz: string;
  fiscalAnchorMonth: number;
  fiscalAnchorDay: number;
  retailAnchor: string | null;
  weekStart: number;
  holidays: string[];
  weekend: number[];
}
const NO_CALENDARS: CalendarSummary[] = [];

export function useCalendars() {
  const { data, loading, error, refetch } = useQuery<{ calendars: CalendarSummary[] }>(
    CALENDARS_QUERY,
    { fetchPolicy: "cache-and-network" },
  );
  return { calendars: data?.calendars ?? NO_CALENDARS, loading, error, refetch };
}

export function useCreateCalendar() {
  const [mutate, { loading, error }] = useMutation(CREATE_CALENDAR_MUTATION, {
    refetchQueries: [{ query: CALENDARS_QUERY }],
  });
  return { createCalendar: mutate, loading, error };
}

export function useDeleteCalendar() {
  const [mutate, { loading, error }] = useMutation(DELETE_CALENDAR_MUTATION, {
    refetchQueries: [{ query: CALENDARS_QUERY }],
  });
  return { deleteCalendar: mutate, loading, error };
}

// REQ-1143: preview the effective refresh/serving summary for draft editor knobs, server-derived
// (the decision tree is never re-derived client-side). Returns a lazy fetcher the editor debounces.
export interface RefreshPolicyPreviewVars {
  sourceId: string;
  domainId: string;
  schemaName: string;
  tableName: string;
  cacheTtl?: number | null;
  preferMaterialized?: boolean | null;
  loadProtected?: boolean | null;
  offPeakWindow?: string | null;
  offPeakTz?: string | null;
  changeSignal?: string | null;
}

export function useRefreshPolicyPreview() {
  const [fetch] = useLazyQuery<
    { refreshPolicyPreview: RefreshPolicySummary | null },
    RefreshPolicyPreviewVars
  >(REFRESH_POLICY_PREVIEW_QUERY, { fetchPolicy: "no-cache" });
  return async (vars: RefreshPolicyPreviewVars): Promise<RefreshPolicySummary | null> => {
    const res = await fetch({ variables: vars });
    return res.data?.refreshPolicyPreview ?? null;
  };
}

// REQ-1443: the contract builder's three server calls. The soda / Great Expectations dialects are
// parsed and serialized server-side, so the panel holds contract TEXT and check rows and never a
// dialect of its own — a hand edit in the raw editor can therefore never disagree with the builder.
export function useDqContract() {
  const [parse] = useLazyQuery<{ dqContractParse: DqContract }, DqContractParseVars>(
    DQ_CONTRACT_PARSE_QUERY,
    { fetchPolicy: "no-cache" },
  );
  const [build] = useLazyQuery<{ dqContractBuild: DqContractText }, DqContractBuildVars>(
    DQ_CONTRACT_BUILD_QUERY,
    { fetchPolicy: "no-cache" },
  );
  const [catalog] = useLazyQuery<{ dqCheckCatalog: DqCheckCatalog }, DqCheckCatalogVars>(
    DQ_CHECK_CATALOG_QUERY,
    { fetchPolicy: "no-cache" },
  );
  const [buildOne] = useLazyQuery<{ dqCheckDefinition: DqCheckDefinition }, DqCheckDefinitionVars>(
    DQ_CHECK_DEFINITION_QUERY,
    { fetchPolicy: "no-cache" },
  );
  const [dryRun] = useMutation<{ dryRunDqContract: DqDryRun }, DqDryRunVars>(
    DRY_RUN_DQ_CONTRACT_MUTATION,
  );
  return {
    checkCatalog: useCallback(
      async (vars: DqCheckCatalogVars): Promise<DqCheckCatalog | null> =>
        (await catalog({ variables: vars })).data?.dqCheckCatalog ?? null,
      [catalog],
    ),
    buildCheck: useCallback(
      async (vars: DqCheckDefinitionVars): Promise<DqCheckDefinition | null> =>
        (await buildOne({ variables: vars })).data?.dqCheckDefinition ?? null,
      [buildOne],
    ),
    parseContract: useCallback(
      async (vars: DqContractParseVars): Promise<DqContract | null> =>
        (await parse({ variables: vars })).data?.dqContractParse ?? null,
      [parse],
    ),
    buildContract: useCallback(
      async (vars: DqContractBuildVars): Promise<DqContractText | null> =>
        (await build({ variables: vars })).data?.dqContractBuild ?? null,
      [build],
    ),
    dryRunContract: useCallback(
      async (vars: DqDryRunVars): Promise<DqDryRun | null> =>
        (await dryRun({ variables: vars })).data?.dryRunDqContract ?? null,
      [dryRun],
    ),
  };
}

export function useRelationships() {
  const { data, loading, error, refetch } = useQuery<{ relationships: Relationship[] }>(
    RELATIONSHIPS_QUERY,
    { fetchPolicy: "cache-and-network" },
  );
  return {
    relationships: data?.relationships ?? NO_RELATIONSHIPS,
    loading: firstLoad(loading, data),
    error,
    refetch,
  };
}

export function useAllRelationships() {
  const { data, loading, error, refetch } = useQuery<{ allRelationships: Relationship[] }>(
    ALL_RELATIONSHIPS_QUERY,
    { fetchPolicy: "cache-and-network" },
  );
  return {
    relationships: data?.allRelationships ?? NO_RELATIONSHIPS,
    loading: firstLoad(loading, data),
    error,
    refetch,
  };
}

export function useRLSRules() {
  const { data, loading, error, refetch } = useQuery<{ rlsRules: RLSRule[] }>(RLS_RULES_QUERY, {
    fetchPolicy: "cache-and-network",
  });
  return {
    rlsRules: data?.rlsRules ?? NO_RLS_RULES,
    loading: firstLoad(loading, data),
    error,
    refetch,
  };
}

export function useCreateDomain() {
  const [createDomain, { loading }] = useMutation<{ createDomain: MutationResult }>(CreateDomain, {
    refetchQueries: [{ query: DOMAINS_QUERY }],
  });
  return {
    createDomain: async (id: string, description: string, graphqlAlias?: string | null) => {
      const result = await createDomain({
        variables: { id, description, graphqlAlias: graphqlAlias ?? null },
      });
      return (result.data?.createDomain ?? { success: false, message: "" }) as MutationResult;
    },
    loading,
  };
}

export function useDeleteDomain() {
  const [deleteDomain, { loading }] = useMutation<{ deleteDomain: MutationResult }>(DeleteDomain, {
    refetchQueries: [{ query: DOMAINS_QUERY }],
  });
  return {
    deleteDomain: async (id: string) => {
      const result = await deleteDomain({ variables: { id } });
      return (result.data?.deleteDomain ?? { success: false, message: "" }) as MutationResult;
    },
    loading,
  };
}

export function useRegisterTable() {
  const [registerTable, { loading }] = useMutation<{ registerTable: MutationResult }>(
    RegisterTable,
    {
      refetchQueries: [{ query: TABLES_QUERY }],
    },
  );
  return {
    registerTable: async (input: Record<string, unknown>) => {
      const result = await registerTable({ variables: { input } });
      return (result.data?.registerTable ?? { success: false, message: "" }) as MutationResult;
    },
    loading,
  };
}

export function useRegisterEntity() {
  const [registerEntity, { loading }] = useMutation<{ registerEntity: MutationResult }>(
    RegisterEntity,
    { refetchQueries: [{ query: TABLES_QUERY }] },
  );
  return {
    registerEntity: async (input: Record<string, unknown>) => {
      const result = await registerEntity({ variables: { input } });
      return (result.data?.registerEntity ?? { success: false, message: "" }) as MutationResult;
    },
    loading,
  };
}

export function useRegisterFact() {
  const [registerFact, { loading }] = useMutation<{ registerFact: MutationResult }>(RegisterFact, {
    refetchQueries: [{ query: TABLES_QUERY }],
  });
  return {
    registerFact: async (input: Record<string, unknown>) => {
      const result = await registerFact({ variables: { input } });
      return (result.data?.registerFact ?? { success: false, message: "" }) as MutationResult;
    },
    loading,
  };
}

export function useUpdateTable() {
  const [updateTable, { loading }] = useMutation<{ updateTable: MutationResult }>(UpdateTable, {
    refetchQueries: [{ query: TABLES_QUERY }],
  });
  return {
    updateTable: async (input: Record<string, unknown>) => {
      const result = await updateTable({ variables: { input } });
      return (result.data?.updateTable ?? { success: false, message: "" }) as MutationResult;
    },
    loading,
  };
}

export function useDeleteTable() {
  const [deleteTable, { loading }] = useMutation<{ deleteTable: MutationResult }>(DeleteTable, {
    refetchQueries: [{ query: TABLES_QUERY }],
  });
  return {
    deleteTable: async (id: number) => {
      const result = await deleteTable({ variables: { id } });
      return (result.data?.deleteTable ?? { success: false, message: "" }) as MutationResult;
    },
    loading,
  };
}

export function useUpsertRelationship() {
  const [upsertRelationship, { loading }] = useMutation<{ upsertRelationship: MutationResult }>(
    UpsertRelationship,
    {
      refetchQueries: [{ query: RELATIONSHIPS_QUERY }],
    },
  );
  return {
    upsertRelationship: async (input: Record<string, unknown>) => {
      const result = await upsertRelationship({ variables: { input } });
      return (result.data?.upsertRelationship ?? { success: false, message: "" }) as MutationResult;
    },
    loading,
  };
}

export function useDeleteRelationship() {
  const [deleteRelationship, { loading }] = useMutation<{ deleteRelationship: MutationResult }>(
    DeleteRelationship,
    {
      refetchQueries: [{ query: RELATIONSHIPS_QUERY }],
    },
  );
  return {
    deleteRelationship: async (id: string) => {
      const result = await deleteRelationship({ variables: { id } });
      return (result.data?.deleteRelationship ?? { success: false, message: "" }) as MutationResult;
    },
    loading,
  };
}

export function useCreateSource() {
  const [createSource, { loading }] = useMutation<{ createSource: MutationResult }>(CreateSource);
  return {
    createSource: async (input: Record<string, unknown>) => {
      const result = await createSource({ variables: { input } });
      return (result.data?.createSource ?? { success: false, message: "" }) as MutationResult;
    },
    loading,
  };
}

export function useUpdateSource() {
  const [updateSource, { loading }] = useMutation<{ updateSource: MutationResult }>(UpdateSource);
  return {
    updateSource: async (input: Record<string, unknown>) => {
      const result = await updateSource({ variables: { input } });
      return (result.data?.updateSource ?? { success: false, message: "" }) as MutationResult;
    },
    loading,
  };
}

export function useDeleteSource() {
  const [deleteSource, { loading }] = useMutation<{ deleteSource: MutationResult }>(DeleteSource);
  return {
    deleteSource: async (id: string) => {
      const result = await deleteSource({ variables: { id } });
      return (result.data?.deleteSource ?? { success: false, message: "" }) as MutationResult;
    },
    loading,
  };
}

export function usePurgeCache() {
  const [purgeCache, { loading }] = useMutation<{ purgeCache: MutationResult }>(PurgeCache);
  return {
    purgeCache: async () => {
      const result = await purgeCache();
      return (result.data?.purgeCache ?? { success: false, message: "" }) as MutationResult;
    },
    loading,
  };
}

export function useUpdateSourceCache() {
  const [updateSourceCache, { loading }] = useMutation<{ updateSourceCache: MutationResult }>(
    UpdateSourceCache,
  );
  return {
    updateSourceCache: async (sourceId: string, cacheEnabled: boolean, cacheTtl: number | null) => {
      const result = await updateSourceCache({ variables: { sourceId, cacheEnabled, cacheTtl } });
      return (result.data?.updateSourceCache ?? { success: false, message: "" }) as MutationResult;
    },
    loading,
  };
}

export function useUpdateTableCache() {
  const [updateTableCache, { loading }] = useMutation<{ updateTableCache: MutationResult }>(
    UpdateTableCache,
  );
  return {
    updateTableCache: async (tableId: number, cacheTtl: number | null) => {
      const result = await updateTableCache({ variables: { tableId, cacheTtl } });
      return (result.data?.updateTableCache ?? { success: false, message: "" }) as MutationResult;
    },
    loading,
  };
}

export function useForceRegen() {
  // REQ-968: the reason is the audit why-tag the server records on the forced event, so it is a
  // required argument here rather than an optional note the caller may omit.
  const [forceRegen, { loading }] = useMutation<{ forceRegen: MutationResult }>(ForceRegen);
  return {
    forceRegen: async (tableId: number, reason: string) => {
      const result = await forceRegen({ variables: { tableId, reason } });
      return (result.data?.forceRegen ?? { success: false, message: "" }) as MutationResult;
    },
    loading,
  };
}

export function useUpdateSourcePreferMaterialized() {
  const [updateSourcePreferMaterialized, { loading }] = useMutation<{
    updateSourcePreferMaterialized: MutationResult;
  }>(UpdateSourcePreferMaterialized);
  return {
    updateSourcePreferMaterialized: async (sourceId: string, preferMaterialized: boolean) => {
      const result = await updateSourcePreferMaterialized({
        variables: { sourceId, preferMaterialized },
      });
      return (result.data?.updateSourcePreferMaterialized ?? {
        success: false,
        message: "",
      }) as MutationResult;
    },
    loading,
  };
}

export function useUpdateTablePreferMaterialized() {
  const [updateTablePreferMaterialized, { loading }] = useMutation<{
    updateTablePreferMaterialized: MutationResult;
  }>(UpdateTablePreferMaterialized);
  return {
    updateTablePreferMaterialized: async (tableId: number, preferMaterialized: boolean | null) => {
      const result = await updateTablePreferMaterialized({
        variables: { tableId, preferMaterialized },
      });
      return (result.data?.updateTablePreferMaterialized ?? {
        success: false,
        message: "",
      }) as MutationResult;
    },
    loading,
  };
}

export function useUpdateSourceLoadProtection() {
  const [updateSourceLoadProtection, { loading }] = useMutation<{
    updateSourceLoadProtection: MutationResult;
  }>(UpdateSourceLoadProtection);
  return {
    updateSourceLoadProtection: async (
      sourceId: string,
      loadProtected: boolean,
      offPeakWindow: string | null,
      offPeakTz: string,
    ) => {
      const result = await updateSourceLoadProtection({
        variables: { sourceId, loadProtected, offPeakWindow, offPeakTz },
      });
      return (result.data?.updateSourceLoadProtection ?? {
        success: false,
        message: "",
      }) as MutationResult;
    },
    loading,
  };
}

export function useUpdateTableLoadProtection() {
  const [updateTableLoadProtection, { loading }] = useMutation<{
    updateTableLoadProtection: MutationResult;
  }>(UpdateTableLoadProtection);
  return {
    updateTableLoadProtection: async (
      tableId: number,
      loadProtected: boolean | null,
      offPeakWindow: string | null,
      offPeakTz: string | null,
    ) => {
      const result = await updateTableLoadProtection({
        variables: { tableId, loadProtected, offPeakWindow, offPeakTz },
      });
      return (result.data?.updateTableLoadProtection ?? {
        success: false,
        message: "",
      }) as MutationResult;
    },
    loading,
  };
}

export function useUpdateSourceNaming() {
  const [updateSourceNaming, { loading }] = useMutation<{ updateSourceNaming: MutationResult }>(
    UpdateSourceNaming,
  );
  return {
    updateSourceNaming: async (sourceId: string, gqlNamingConvention: string | null) => {
      const result = await updateSourceNaming({ variables: { sourceId, gqlNamingConvention } });
      return (result.data?.updateSourceNaming ?? { success: false, message: "" }) as MutationResult;
    },
    loading,
  };
}

export function useUpdateTableNaming() {
  const [updateTableNaming, { loading }] = useMutation<{ updateTableNaming: MutationResult }>(
    UpdateTableNaming,
  );
  return {
    updateTableNaming: async (tableId: number, gqlNamingConvention: string | null) => {
      const result = await updateTableNaming({ variables: { tableId, gqlNamingConvention } });
      return (result.data?.updateTableNaming ?? { success: false, message: "" }) as MutationResult;
    },
    loading,
  };
}

export function useUpdateSourceAllowedDomains() {
  const [updateSourceAllowedDomains, { loading }] = useMutation<{
    updateSourceAllowedDomains: MutationResult;
  }>(UpdateSourceAllowedDomains);
  return {
    updateSourceAllowedDomains: async (sourceId: string, allowedDomains: string[]) => {
      const result = await updateSourceAllowedDomains({ variables: { sourceId, allowedDomains } });
      return (result.data?.updateSourceAllowedDomains ?? {
        success: false,
        message: "",
      }) as MutationResult;
    },
    loading,
  };
}

// ── Query hooks ──

const NO_ROLES: Role[] = [];

export function useRoles() {
  const { data, loading, error, refetch } = useQuery<{ roles: Role[] }>(ROLES_QUERY, {
    fetchPolicy: "cache-and-network",
  });
  const rawRoles = data?.roles ?? NO_ROLES;
  const roles = useMemo(
    () =>
      rawRoles.map((r) => ({
        ...r,
        domain_access: (r as { domainAccess?: string[] }).domainAccess ?? r.domain_access,
      })),
    [rawRoles],
  );
  return { roles, loading: firstLoad(loading, data), error, refetch };
}

// ── Lazy (on-demand) query hooks: imperative trigger that still participates in the cache ──

// The lazy executors returned below are wrapped in useCallback so they have a
// STABLE identity across renders. Callers list them in effect deps; without this,
// every render produces a new function, re-running the effect → infinite loop.

export function useAvailableSchemas(sourceId: string | null) {
  const { data, loading } = useQuery<{ availableSchemas: string[] }>(AvailableSchemas, {
    variables: { sourceId },
    skip: !sourceId,
    fetchPolicy: "no-cache",
  });
  return { schemas: data?.availableSchemas ?? [], loading };
}

export function useAvailableTables(sourceId: string | null, schemaName: string | null) {
  const { data, loading } = useQuery<{ availableTables: TableMetadata[] }>(AvailableTables, {
    variables: { sourceId, schemaName },
    skip: !sourceId || !schemaName,
    fetchPolicy: "no-cache",
  });
  return { tables: data?.availableTables ?? [], loading };
}

export function useAvailableColumnsMetadataLazy() {
  const [run] = useLazyQuery<{ availableColumnsMetadata: ColumnMetadata[] }>(
    AvailableColumnsMetadata,
    { fetchPolicy: "cache-first" },
  );
  return useCallback(
    async (sourceId: string, schemaName: string, tableName: string): Promise<ColumnMetadata[]> => {
      const { data } = await run({ variables: { sourceId, schemaName, tableName } });
      return data?.availableColumnsMetadata ?? [];
    },
    [run],
  );
}

export function useAvailableFunctionsLazy() {
  const [run] = useLazyQuery<{ availableFunctions: TableMetadata[] }>(AvailableFunctions, {
    fetchPolicy: "cache-first",
  });
  return useCallback(
    async (sourceId: string, schemaName = "openapi"): Promise<TableMetadata[]> => {
      const { data } = await run({ variables: { sourceId, schemaName } });
      return data?.availableFunctions ?? [];
    },
    [run],
  );
}

export function useGenerateColumnDescription() {
  const [run, { loading }] = useLazyQuery<{ generateColumnDescription: string }>(
    GenerateColumnDescription,
    { fetchPolicy: "network-only" },
  );
  return {
    generateColumnDescription: async (tableId: number, columnName: string): Promise<string> => {
      const { data } = await run({ variables: { tableId: String(tableId), columnName } });
      return data?.generateColumnDescription ?? "";
    },
    loading,
  };
}

export function useGenerateTableDescription() {
  const [run, { loading }] = useLazyQuery<{ generateTableDescription: string }>(
    GenerateTableDescription,
    { fetchPolicy: "network-only" },
  );
  return {
    generateTableDescription: async (tableId: number): Promise<string> => {
      const { data } = await run({ variables: { tableId: String(tableId) } });
      return data?.generateTableDescription ?? "";
    },
    loading,
  };
}

export function useSuggestTableAlias() {
  const [run, { loading }] = useLazyQuery<{ suggestTableAlias: string }>(SuggestTableAlias, {
    fetchPolicy: "no-cache",
  });
  return {
    suggestTableAlias: async (
      tableName: string,
      domainId: string,
      sourceId: string,
    ): Promise<string> => {
      const { data } = await run({ variables: { tableName, domainId, sourceId } });
      return data?.suggestTableAlias ?? tableName;
    },
    loading,
  };
}

// ── Mutation hooks ──

export function useCompileQuery() {
  const [compile, { loading }] = useMutation<{ compileQuery: Record<string, unknown>[] }>(
    CompileQuery,
  );
  // Stable identity so callers can list it in effect deps without re-running.
  const compileQuery = useCallback(
    async (
      roleId: string,
      query: string,
      variables?: Record<string, unknown>,
      flatSql?: boolean,
      flatCypher?: boolean,
      nodeOnlyCypher?: boolean,
    ): Promise<CompileResult | { queries: CompileResult[] }> => {
      const result = await compile({
        variables: {
          input: {
            query,
            role: roleId,
            variables: variables ?? null,
            flatSql: flatSql ?? false,
            flatCypher: flatCypher ?? false,
            nodeOnlyCypher: nodeOnlyCypher ?? false,
          },
        },
      });
      const rows = (result.data?.compileQuery ?? []) as Record<string, unknown>[];
      const results = rows.map((r) => ({
        ...r,
        semantic_sql: r.semanticSql ?? r.semantic_sql,
        engine_sql: r.engineSql ?? r.engine_sql,
        direct_sql: r.directSql ?? r.direct_sql,
        route_reason: r.routeReason ?? r.route_reason,
        root_field: r.rootField ?? r.root_field,
        canonical_field: r.canonicalField ?? r.canonical_field,
        compiled_cypher: r.compiledCypher ?? r.compiled_cypher,
        cypher_error: r.cypherError ?? r.cypher_error,
        column_aliases: (
          (r.columnAliases ?? r.column_aliases ?? []) as Record<string, unknown>[]
        ).map((ca) => ({ field_name: ca.fieldName ?? ca.field_name, column: ca.column })),
      })) as CompileResult[];
      return results.length === 1 ? results[0] : { queries: results };
    },
    [compile],
  );
  return { compileQuery, loading };
}

export function useUpsertRlsRule() {
  const [upsertRlsRule, { loading }] = useMutation<{ upsertRlsRule: MutationResult }>(
    UpsertRlsRule,
    {
      refetchQueries: [{ query: RLS_RULES_QUERY }],
    },
  );
  return {
    upsertRlsRule: async (input: {
      tableId?: string | null;
      domainId?: string | null;
      roleId: string;
      filterExpr: string;
    }) => {
      const result = await upsertRlsRule({ variables: { input } });
      return (result.data?.upsertRlsRule ?? { success: false, message: "" }) as MutationResult;
    },
    loading,
  };
}

export function useDeleteRlsRule() {
  const [deleteRlsRule, { loading }] = useMutation<{ deleteRlsRule: MutationResult }>(
    DeleteRlsRule,
    {
      refetchQueries: [{ query: RLS_RULES_QUERY }],
    },
  );
  return {
    deleteRlsRule: async (roleId: string, tableId?: number | null, domainId?: string | null) => {
      const result = await deleteRlsRule({
        variables: { roleId, tableId: tableId ?? null, domainId: domainId ?? null },
      });
      return (result.data?.deleteRlsRule ?? { success: false, message: "" }) as MutationResult;
    },
    loading,
  };
}

export function useUpsertRole() {
  const [createRole, { loading }] = useMutation<{ createRole: MutationResult }>(CreateRole, {
    refetchQueries: [{ query: ROLES_QUERY }],
  });
  return {
    upsertRole: async (input: {
      id: string;
      capabilities: string[];
      domainAccess: string[];
      rateLimit?: {
        requestsPerSecond: number | null;
        maxQueryDepth: number | null;
        maxQueryNodes: number | null;
        maxQueryTimeMs: number | null;
      } | null;
    }) => {
      const result = await createRole({ variables: { input } });
      return (result.data?.createRole ?? { success: false, message: "" }) as MutationResult;
    },
    loading,
  };
}

export function useDeleteRole() {
  const [deleteRole, { loading }] = useMutation<{ deleteRole: MutationResult }>(DeleteRole, {
    refetchQueries: [{ query: ROLES_QUERY }],
  });
  return {
    deleteRole: async (id: string) => {
      const result = await deleteRole({ variables: { id } });
      return (result.data?.deleteRole ?? { success: false, message: "" }) as MutationResult;
    },
    loading,
  };
}

export function useRenameSource() {
  const [renameSource, { loading }] = useMutation<{ renameSource: MutationResult }>(RenameSource, {
    refetchQueries: [{ query: SOURCES_QUERY }],
  });
  return {
    renameSource: async (oldId: string, newId: string) => {
      const result = await renameSource({ variables: { oldId, newId } });
      return (result.data?.renameSource ?? { success: false, message: "" }) as MutationResult;
    },
    loading,
  };
}

export function useDeployViewToDb() {
  const [deployViewToDb, { loading }] = useMutation<{ deployViewToDb: MutationResult }>(
    DeployViewToDb,
    { refetchQueries: [{ query: TABLES_QUERY }] },
  );
  return {
    deployViewToDb: async (tableId: number) => {
      const result = await deployViewToDb({ variables: { tableId } });
      return (result.data?.deployViewToDb ?? { success: false, message: "" }) as MutationResult;
    },
    loading,
  };
}
