import { useQuery, useMutation } from "@apollo/client/react";
import type { Source, Domain, RegisteredTable, Relationship, RLSRule, MutationResult } from "../types/admin";
import { SourcesQuery as SOURCES_QUERY, DomainsQuery as DOMAINS_QUERY, TablesQuery as TABLES_QUERY, RelationshipsQuery as RELATIONSHIPS_QUERY, RLSRulesQuery as RLS_RULES_QUERY, CreateDomain, DeleteDomain, RegisterTable, UpdateTable, DeleteTable, UpsertRelationship, DeleteRelationship, CreateSource, UpdateSource, DeleteSource, PurgeCache, UpdateSourceCache, UpdateTableCache, UpdateSourceNaming, UpdateTableNaming, UpdateSourceAllowedDomains } from './admin.graphql';

export function useSources() {
  const { data, loading, error, refetch } = useQuery<{ sources: Source[] }>(
    SOURCES_QUERY,
    { fetchPolicy: "cache-and-network" }
  );
  return {
    sources: data?.sources ?? [],
    loading,
    error,
    refetch,
  };
}

export function useDomains() {
  const { data, loading, error, refetch } = useQuery<{ domains: Domain[] }>(
    DOMAINS_QUERY,
    { fetchPolicy: "cache-and-network" }
  );
  return {
    domains: data?.domains ?? [],
    loading,
    error,
    refetch,
  };
}

export function useTables() {
  const { data, loading, error, refetch } = useQuery<{ tables: RegisteredTable[] }>(
    TABLES_QUERY,
    { fetchPolicy: "cache-and-network" }
  );
  return {
    tables: data?.tables ?? [],
    loading,
    error,
    refetch,
  };
}

export function useRelationships() {
  const { data, loading, error, refetch } = useQuery<{ relationships: Relationship[] }>(
    RELATIONSHIPS_QUERY,
    { fetchPolicy: "cache-and-network" }
  );
  return {
    relationships: data?.relationships ?? [],
    loading,
    error,
    refetch,
  };
}

export function useRLSRules() {
  const { data, loading, error, refetch } = useQuery<{ rlsRules: RLSRule[] }>(
    RLS_RULES_QUERY,
    { fetchPolicy: "cache-and-network" }
  );
  return {
    rlsRules: data?.rlsRules ?? [],
    loading,
    error,
    refetch,
  };
}

export function useCreateDomain() {
  const [createDomain, { loading }] = useMutation<{ createDomain: MutationResult }>(CreateDomain);
  return {
    createDomain: async (id: string, description: string, graphqlAlias?: string | null) => {
      const result = await createDomain({ variables: { id, description, graphqlAlias: graphqlAlias ?? null } });
      return (result.data?.createDomain ?? { success: false, message: "" }) as MutationResult;
    },
    loading,
  };
}

export function useDeleteDomain() {
  const [deleteDomain, { loading }] = useMutation<{ deleteDomain: MutationResult }>(DeleteDomain);
  return {
    deleteDomain: async (id: string) => {
      const result = await deleteDomain({ variables: { id } });
      return (result.data?.deleteDomain ?? { success: false, message: "" }) as MutationResult;
    },
    loading,
  };
}

export function useRegisterTable() {
  const [registerTable, { loading }] = useMutation<{ registerTable: MutationResult }>(RegisterTable);
  return {
    registerTable: async (input: any) => {
      const result = await registerTable({ variables: { input } });
      return (result.data?.registerTable ?? { success: false, message: "" }) as MutationResult;
    },
    loading,
  };
}

export function useUpdateTable() {
  const [updateTable, { loading }] = useMutation<{ updateTable: MutationResult }>(UpdateTable);
  return {
    updateTable: async (input: any) => {
      const result = await updateTable({ variables: { input } });
      return (result.data?.updateTable ?? { success: false, message: "" }) as MutationResult;
    },
    loading,
  };
}

export function useDeleteTable() {
  const [deleteTable, { loading }] = useMutation<{ deleteTable: MutationResult }>(DeleteTable);
  return {
    deleteTable: async (id: number) => {
      const result = await deleteTable({ variables: { id } });
      return (result.data?.deleteTable ?? { success: false, message: "" }) as MutationResult;
    },
    loading,
  };
}

export function useUpsertRelationship() {
  const [upsertRelationship, { loading }] = useMutation<{ upsertRelationship: MutationResult }>(UpsertRelationship);
  return {
    upsertRelationship: async (input: any) => {
      const result = await upsertRelationship({ variables: { input } });
      return (result.data?.upsertRelationship ?? { success: false, message: "" }) as MutationResult;
    },
    loading,
  };
}

export function useDeleteRelationship() {
  const [deleteRelationship, { loading }] = useMutation<{ deleteRelationship: MutationResult }>(DeleteRelationship);
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
    createSource: async (input: any) => {
      const result = await createSource({ variables: { input } });
      return (result.data?.createSource ?? { success: false, message: "" }) as MutationResult;
    },
    loading,
  };
}

export function useUpdateSource() {
  const [updateSource, { loading }] = useMutation<{ updateSource: MutationResult }>(UpdateSource);
  return {
    updateSource: async (input: any) => {
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
  const [updateSourceCache, { loading }] = useMutation<{ updateSourceCache: MutationResult }>(UpdateSourceCache);
  return {
    updateSourceCache: async (sourceId: string, cacheEnabled: boolean, cacheTtl: number | null) => {
      const result = await updateSourceCache({ variables: { sourceId, cacheEnabled, cacheTtl } });
      return (result.data?.updateSourceCache ?? { success: false, message: "" }) as MutationResult;
    },
    loading,
  };
}

export function useUpdateTableCache() {
  const [updateTableCache, { loading }] = useMutation<{ updateTableCache: MutationResult }>(UpdateTableCache);
  return {
    updateTableCache: async (tableId: number, cacheTtl: number | null) => {
      const result = await updateTableCache({ variables: { tableId, cacheTtl } });
      return (result.data?.updateTableCache ?? { success: false, message: "" }) as MutationResult;
    },
    loading,
  };
}

export function useUpdateSourceNaming() {
  const [updateSourceNaming, { loading }] = useMutation<{ updateSourceNaming: MutationResult }>(UpdateSourceNaming);
  return {
    updateSourceNaming: async (sourceId: string, namingConvention: string | null) => {
      const result = await updateSourceNaming({ variables: { sourceId, namingConvention } });
      return (result.data?.updateSourceNaming ?? { success: false, message: "" }) as MutationResult;
    },
    loading,
  };
}

export function useUpdateTableNaming() {
  const [updateTableNaming, { loading }] = useMutation<{ updateTableNaming: MutationResult }>(UpdateTableNaming);
  return {
    updateTableNaming: async (tableId: number, namingConvention: string | null) => {
      const result = await updateTableNaming({ variables: { tableId, namingConvention } });
      return (result.data?.updateTableNaming ?? { success: false, message: "" }) as MutationResult;
    },
    loading,
  };
}

export function useUpdateSourceAllowedDomains() {
  const [updateSourceAllowedDomains, { loading }] = useMutation<{ updateSourceAllowedDomains: MutationResult }>(UpdateSourceAllowedDomains);
  return {
    updateSourceAllowedDomains: async (sourceId: string, allowedDomains: string[]) => {
      const result = await updateSourceAllowedDomains({ variables: { sourceId, allowedDomains } });
      return (result.data?.updateSourceAllowedDomains ?? { success: false, message: "" }) as MutationResult;
    },
    loading,
  };
}
