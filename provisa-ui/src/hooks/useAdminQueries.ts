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
  Relationship,
  RLSRule,
  MutationResult,
} from "../types/admin";
import type {
  MVInfo,
  CacheStats,
  SystemHealth,
  ScheduledTask,
  CompileResult,
  TableMetadata,
  ColumnMetadata,
} from "../api/admin";
import {
  RolesQuery as ROLES_QUERY,
  SourcesQuery as SOURCES_QUERY,
  DomainsQuery as DOMAINS_QUERY,
  TablesQuery as TABLES_QUERY,
  RelationshipsQuery as RELATIONSHIPS_QUERY,
  RLSRulesQuery as RLS_RULES_QUERY,
  MVList as MV_LIST_QUERY,
  CacheStats as CACHE_STATS_QUERY,
  SystemHealth as SYSTEM_HEALTH_QUERY,
  ScheduledTasks as SCHEDULED_TASKS_QUERY,
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
  RefreshMv,
  ToggleMv,
  ToggleScheduledTask,
  PurgeCacheByTable,
  InvalidateFileSource,
  PurgeCache,
  UpdateSourceCache,
  UpdateTableCache,
  UpdateSourceNaming,
  UpdateTableNaming,
  UpdateSourceAllowedDomains,
  SuggestTableAlias,
} from "./admin.graphql";

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
    loading,
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
    loading,
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
    loading,
    error,
    refetch,
  };
}

export function useRelationships() {
  const { data, loading, error, refetch } = useQuery<{ relationships: Relationship[] }>(
    RELATIONSHIPS_QUERY,
    { fetchPolicy: "cache-and-network" },
  );
  return {
    relationships: data?.relationships ?? NO_RELATIONSHIPS,
    loading,
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
    loading,
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
  return { roles, loading, error, refetch };
}

export function useMVList() {
  const { data, loading, error, refetch } = useQuery<{ mvList: MVInfo[] }>(MV_LIST_QUERY, {
    fetchPolicy: "cache-and-network",
  });
  return { mvList: data?.mvList ?? [], loading, error, refetch };
}

export function useCacheStats() {
  const { data, loading, error, refetch } = useQuery<{ cacheStats: CacheStats }>(CACHE_STATS_QUERY, {
    fetchPolicy: "cache-and-network",
  });
  return { cacheStats: data?.cacheStats ?? null, loading, error, refetch };
}

export function useSystemHealth() {
  const { data, loading, error, refetch } = useQuery<{ systemHealth: SystemHealth }>(
    SYSTEM_HEALTH_QUERY,
    { fetchPolicy: "cache-and-network" },
  );
  return { systemHealth: data?.systemHealth ?? null, loading, error, refetch };
}

export function useScheduledTasks() {
  const { data, loading, error, refetch } = useQuery<{ scheduledTasks: ScheduledTask[] }>(
    SCHEDULED_TASKS_QUERY,
    { fetchPolicy: "cache-and-network" },
  );
  return { scheduledTasks: data?.scheduledTasks ?? [], loading, error, refetch };
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
  const [run, { loading }] = useLazyQuery<{ suggestTableAlias: string }>(
    SuggestTableAlias,
    { fetchPolicy: "no-cache" },
  );
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
        trino_sql: r.trinoSql ?? r.trino_sql,
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
  const [upsertRlsRule, { loading }] = useMutation<{ upsertRlsRule: MutationResult }>(UpsertRlsRule, {
    refetchQueries: [{ query: RLS_RULES_QUERY }],
  });
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
  const [deleteRlsRule, { loading }] = useMutation<{ deleteRlsRule: MutationResult }>(DeleteRlsRule, {
    refetchQueries: [{ query: RLS_RULES_QUERY }],
  });
  return {
    deleteRlsRule: async (
      roleId: string,
      tableId?: number | null,
      domainId?: string | null,
    ) => {
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
    upsertRole: async (input: { id: string; capabilities: string[]; domainAccess: string[] }) => {
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

export function useRefreshMV() {
  const [refreshMv, { loading }] = useMutation<{ refreshMv: MutationResult }>(RefreshMv, {
    refetchQueries: [{ query: MV_LIST_QUERY }],
  });
  return {
    refreshMV: async (mvId: string) => {
      const result = await refreshMv({ variables: { mvId } });
      return (result.data?.refreshMv ?? { success: false, message: "" }) as MutationResult;
    },
    loading,
  };
}

export function useToggleMV() {
  const [toggleMv, { loading }] = useMutation<{ toggleMv: MutationResult }>(ToggleMv, {
    refetchQueries: [{ query: MV_LIST_QUERY }],
  });
  return {
    toggleMV: async (mvId: string, enabled: boolean) => {
      const result = await toggleMv({ variables: { mvId, enabled } });
      return (result.data?.toggleMv ?? { success: false, message: "" }) as MutationResult;
    },
    loading,
  };
}

export function useToggleScheduledTask() {
  const [toggleScheduledTask, { loading }] = useMutation<{ toggleScheduledTask: MutationResult }>(
    ToggleScheduledTask,
    { refetchQueries: [{ query: SCHEDULED_TASKS_QUERY }] },
  );
  return {
    toggleScheduledTask: async (taskId: string, enabled: boolean) => {
      const result = await toggleScheduledTask({ variables: { taskId, enabled } });
      return (result.data?.toggleScheduledTask ?? {
        success: false,
        message: "",
      }) as MutationResult;
    },
    loading,
  };
}

export function usePurgeCacheByTable() {
  const [purgeCacheByTable, { loading }] = useMutation<{ purgeCacheByTable: MutationResult }>(
    PurgeCacheByTable,
  );
  return {
    purgeCacheByTable: async (tableId: number) => {
      const result = await purgeCacheByTable({ variables: { tableId } });
      return (result.data?.purgeCacheByTable ?? { success: false, message: "" }) as MutationResult;
    },
    loading,
  };
}

export function useInvalidateFileSource() {
  const [invalidateFileSource, { loading }] = useMutation<{ invalidateFileSource: MutationResult }>(
    InvalidateFileSource,
  );
  return {
    invalidateFileSource: async (tableId: number) => {
      const result = await invalidateFileSource({ variables: { tableId } });
      return (result.data?.invalidateFileSource ?? {
        success: false,
        message: "",
      }) as MutationResult;
    },
    loading,
  };
}
