// Copyright (c) 2026 Kenneth Stott
// Canary: 20fc6da4-bcad-430a-a206-53b7247a63b5
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Operations-plane hooks: cache and materialization statistics, system health, materialized-view
// control and the scheduled-task table. Split out of useAdminQueries so neither file carries more
// than the max-lines cap allows.

import { useQuery, useMutation } from "@apollo/client/react";
import type { MutationResult } from "../types/admin";
import type {
  MVInfo,
  CacheStats,
  CacheTableStat,
  HotTableStat,
  MaterializeStoreInfo,
  SystemHealth,
  ScheduledTask,
} from "../api/admin";
import {
  MVList as MV_LIST_QUERY,
  CacheStats as CACHE_STATS_QUERY,
  CacheTableStats as CACHE_TABLE_STATS_QUERY,
  HotTables as HOT_TABLES_QUERY,
  MaterializeStoreInfo as MATERIALIZE_STORE_INFO_QUERY,
  SystemHealth as SYSTEM_HEALTH_QUERY,
  ScheduledTasks as SCHEDULED_TASKS_QUERY,
  RefreshMv,
  ToggleMv,
  ToggleScheduledTask,
  CreateScheduledTask,
  DeleteScheduledTask,
  PurgeCacheByTable,
  InvalidateFileSource,
} from "./admin.graphql";

export function useMVList() {
  const { data, loading, error, refetch } = useQuery<{ mvList: MVInfo[] }>(MV_LIST_QUERY, {
    fetchPolicy: "cache-and-network",
  });
  return { mvList: data?.mvList ?? [], loading, error, refetch };
}

export function useCacheStats() {
  const { data, loading, error, refetch } = useQuery<{ cacheStats: CacheStats }>(
    CACHE_STATS_QUERY,
    {
      fetchPolicy: "cache-and-network",
    },
  );
  return { cacheStats: data?.cacheStats ?? null, loading, error, refetch };
}

export function useCacheTableStats() {
  const { data, loading, error, refetch } = useQuery<{ cacheTableStats: CacheTableStat[] }>(
    CACHE_TABLE_STATS_QUERY,
    { fetchPolicy: "cache-and-network" },
  );
  return { cacheTableStats: data?.cacheTableStats ?? [], loading, error, refetch };
}

export function useHotTables() {
  const { data, loading, error, refetch } = useQuery<{ hotTables: HotTableStat[] }>(
    HOT_TABLES_QUERY,
    { fetchPolicy: "cache-and-network" },
  );
  return { hotTables: data?.hotTables ?? [], loading, error, refetch };
}

export function useMaterializeStoreInfo() {
  const { data, loading, error, refetch } = useQuery<{
    materializeStoreInfo: MaterializeStoreInfo;
  }>(MATERIALIZE_STORE_INFO_QUERY, { fetchPolicy: "cache-and-network" });
  return { materializeStoreInfo: data?.materializeStoreInfo ?? null, loading, error, refetch };
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

export interface CreateScheduledTaskInput {
  id: string;
  name: string;
  cron: string;
  kind: "webhook" | "sql";
  webhookName?: string;
  argsJson?: string;
  sql?: string;
}

export function useCreateScheduledTask() {
  const [createScheduledTask, { loading }] = useMutation<{ createScheduledTask: MutationResult }>(
    CreateScheduledTask,
    { refetchQueries: [{ query: SCHEDULED_TASKS_QUERY }] },
  );
  return {
    createScheduledTask: async (input: CreateScheduledTaskInput) => {
      const result = await createScheduledTask({
        variables: {
          id: input.id,
          name: input.name,
          cron: input.cron,
          kind: input.kind,
          webhookName: input.webhookName ?? null,
          argsJson: input.argsJson ?? null,
          sql: input.sql ?? null,
        },
      });
      return (result.data?.createScheduledTask ?? {
        success: false,
        message: "",
      }) as MutationResult;
    },
    loading,
  };
}

export function useDeleteScheduledTask() {
  const [deleteScheduledTask, { loading }] = useMutation<{ deleteScheduledTask: MutationResult }>(
    DeleteScheduledTask,
    { refetchQueries: [{ query: SCHEDULED_TASKS_QUERY }] },
  );
  return {
    deleteScheduledTask: async (taskId: string) => {
      const result = await deleteScheduledTask({ variables: { taskId } });
      return (result.data?.deleteScheduledTask ?? {
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
