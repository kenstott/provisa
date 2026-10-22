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

import { useEffect } from "react";
import { useQuery, useMutation } from "@apollo/client/react";
import type { MutationResult, Paging, RoleTtl } from "../types/admin";
import { pagingInput } from "../pages/tables/paging";
import type {
  MVInfo,
  CacheStats,
  CacheTableStat,
  HotTableStat,
  ReplicaBuilds,
  MaterializeStoreInfo,
  SystemHealth,
  ScheduledTask,
} from "../api/admin";
import {
  MVList as MV_LIST_QUERY,
  CacheStats as CACHE_STATS_QUERY,
  CacheTableStats as CACHE_TABLE_STATS_QUERY,
  HotTables as HOT_TABLES_QUERY,
  ReplicaBuilds as REPLICA_BUILDS_QUERY,
  MaterializeStoreInfo as MATERIALIZE_STORE_INFO_QUERY,
  SystemHealth as SYSTEM_HEALTH_QUERY,
  ScheduledTasks as SCHEDULED_TASKS_QUERY,
  RefreshMv,
  ToggleMv,
  UpdateVersionLog,
  ToggleScheduledTask,
  CreateScheduledTask,
  UpdateScheduledTask,
  DeleteScheduledTask,
  PurgeCacheByTable,
  UpdateTablePaging,
  UpdateTableRoleTtl,
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

/** How often replica builds are asked for while one is running or waiting, or a table's detail
 * is open; and how often otherwise, so a build that starts while a list sits open still shows. */
export const REPLICA_BUILDS_BUSY_MS = 5000;
export const REPLICA_BUILDS_QUIET_MS = 30000;

/** The interval for a caller that only needs to notice builds (`watching` false, none running or
 * waiting): the quiet one. */
export function replicaBuildsInterval(builds: ReplicaBuilds | null, watching: boolean): number {
  const busy = (builds?.builds ?? []).some(
    (b) => b.state === "building" || b.state === "requested",
  );
  return busy || watching ? REPLICA_BUILDS_BUSY_MS : REPLICA_BUILDS_QUIET_MS;
}

/** Every replica of the acting org and environment with its build (REQ-1915). Polled, so a
 * running build's progress moves while it is shown: often while a build is running or waiting
 * or `watching` (a table's detail is open), seldom otherwise. One caller, the table list,
 * whose result its details read too. */
export function useReplicaBuilds(list: { watching: boolean }) {
  const { data, loading, error, refetch, startPolling, stopPolling } = useQuery<{
    replicaBuilds: ReplicaBuilds;
  }>(REPLICA_BUILDS_QUERY, { fetchPolicy: "cache-and-network" });
  const replicaBuilds = data?.replicaBuilds ?? null;
  const every = replicaBuildsInterval(replicaBuilds, list.watching);
  useEffect(() => {
    startPolling(every);
    return stopPolling;
  }, [every, startPolling, stopPolling]);
  return { replicaBuilds, loading, error, refetch };
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
    /** ``full``: a table's version log reads the whole table, recording what it deleted. */
    refreshMV: async (mvId: string, full = false) => {
      const result = await refreshMv({ variables: { mvId, full } });
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

/** REQ-2109: how often a table's version log is captured, and how late a row it reads may be. */
export function useUpdateVersionLog() {
  const [updateVersionLog, { loading }] = useMutation<{ updateVersionLog: MutationResult }>(
    UpdateVersionLog,
    { refetchQueries: [{ query: MV_LIST_QUERY }] },
  );
  return {
    updateVersionLog: async (mvId: string, refreshInterval: number, allowedLateness: number) => {
      const result = await updateVersionLog({
        variables: { mvId, refreshInterval, allowedLateness },
      });
      return (result.data?.updateVersionLog ?? { success: false, message: "" }) as MutationResult;
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

/** A trigger's definition, as it is created or updated: a cron schedule, or a table and the event
 * of it that fires it; and a webhook, a SQL statement or a command to run. */
export interface CreateScheduledTaskInput {
  id: string;
  name: string;
  cron?: string;
  onTable?: string;
  onEvent?: string;
  kind: "webhook" | "sql" | "command";
  webhookName?: string;
  argsJson?: string;
  sql?: string;
  command?: string;
  role?: string;
}

function taskVariables(input: CreateScheduledTaskInput) {
  return {
    id: input.id,
    name: input.name,
    cron: input.cron ?? null,
    onTable: input.onTable ?? null,
    onEvent: input.onEvent ?? null,
    kind: input.kind,
    webhookName: input.webhookName ?? null,
    argsJson: input.argsJson ?? null,
    sql: input.sql ?? null,
    command: input.command ?? null,
    role: input.role ?? null,
  };
}

export function useCreateScheduledTask() {
  const [createScheduledTask, { loading }] = useMutation<{ createScheduledTask: MutationResult }>(
    CreateScheduledTask,
    { refetchQueries: [{ query: SCHEDULED_TASKS_QUERY }] },
  );
  return {
    createScheduledTask: async (input: CreateScheduledTaskInput) => {
      const result = await createScheduledTask({ variables: taskVariables(input) });
      return (result.data?.createScheduledTask ?? {
        success: false,
        message: "",
      }) as MutationResult;
    },
    loading,
  };
}

/** REQ-2054: give a trigger that exists a new definition; whether it is enabled stays. */
export function useUpdateScheduledTask() {
  const [updateScheduledTask, { loading }] = useMutation<{ updateScheduledTask: MutationResult }>(
    UpdateScheduledTask,
    { refetchQueries: [{ query: SCHEDULED_TASKS_QUERY }] },
  );
  return {
    updateScheduledTask: async (input: CreateScheduledTaskInput) => {
      const result = await updateScheduledTask({ variables: taskVariables(input) });
      return (result.data?.updateScheduledTask ?? {
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

// REQ-318: replace a table's paging (null clears it).
export function useUpdateTablePaging() {
  const [updateTablePaging, { loading }] = useMutation<{ updateTablePaging: MutationResult }>(
    UpdateTablePaging,
  );
  return {
    updateTablePaging: async (tableId: number, paging: Paging | null) => {
      const result = await updateTablePaging({
        variables: { tableId, paging: paging === null ? null : pagingInput(paging) },
      });
      return (result.data?.updateTablePaging ?? { success: false, message: "" }) as MutationResult;
    },
    loading,
  };
}

// REQ-1907: full replace of a table's role → TTL list.
export function useUpdateTableRoleTtl() {
  const [updateTableRoleTtl, { loading }] = useMutation<{ updateTableRoleTtl: MutationResult }>(
    UpdateTableRoleTtl,
  );
  return {
    updateTableRoleTtl: async (tableId: number, roleTtl: RoleTtl[]) => {
      // Rows read back from the TablesQuery carry Apollo's __typename; the input type rejects it.
      const input = roleTtl.map(({ role, ttl }) => ({ role, ttl }));
      const result = await updateTableRoleTtl({ variables: { tableId, roleTtl: input } });
      return (result.data?.updateTableRoleTtl ?? { success: false, message: "" }) as MutationResult;
    },
    loading,
  };
}
