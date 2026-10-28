// Copyright (c) 2026 Kenneth Stott
// Canary: 97a886d6-42b1-4eea-83f4-2f945bc75a9f
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useCallback } from "react";
import { useApolloClient, useLazyQuery, useMutation, useQuery } from "@apollo/client/react";
import type { ListedTable, MutationResult, RegisteredTable } from "../types/admin";
import {
  DeleteTable,
  RegisterEntity,
  RegisterFact,
  RegisterTable,
  UpdateTable,
  TableListQuery as TABLE_LIST_QUERY,
  TablesQuery as TABLES_QUERY,
} from "./admin.graphql";
import { firstLoad } from "./firstLoad";

const NO_TABLES: RegisteredTable[] = [];
const NO_LISTED_TABLES: ListedTable[] = [];

/** The registered tables as the pages that pick, name or count them read them (TableListQuery).
 *
 * `inPage`: the caller renders inside a page that reads the tables itself. It reads that page's
 * cache and sends no request of its own: the page's mount revalidates the list, and a table
 * mutation or a schema-version bump refetches it while it is shown. Without it, every tab and
 * sub-form mounting under the page re-read the whole catalog. */
export function useTables({ inPage = false }: { inPage?: boolean } = {}) {
  const { data, loading, error, refetch } = useQuery<{ tables: ListedTable[] }>(TABLE_LIST_QUERY, {
    fetchPolicy: inPage ? "cache-first" : "cache-and-network",
  });
  return {
    tables: data?.tables ?? NO_LISTED_TABLES,
    loading: firstLoad(loading, data),
    error,
    refetch,
  };
}

/** The registered tables with every field the table editor reads (TablesQuery). */
export function useTableDetails() {
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

const TABLE_QUERY_NAMES = ["TablesQuery", "TableListQuery"];

/** The table queries a view is showing, for a table write's refetchQueries: the write refetches
 * those and no other. A `{ query: TablesQuery }` entry fetched the editor's whole catalog on every
 * write whether or not anything showed it. */
export function useShownTableQueries(): () => string[] {
  const client = useApolloClient();
  return useCallback(() => {
    const shown = new Set([...client.getObservableQueries("active")].map((oq) => oq.queryName));
    return TABLE_QUERY_NAMES.filter((name) => shown.has(name));
  }, [client]);
}

/** A read of every table's full row, current: for a save that writes whole rows back (the update
 * mutation takes the row entire) from a page that lists only what it shows. */
export function useReadTableDetails(): () => Promise<RegisteredTable[]> {
  const [load] = useLazyQuery<{ tables: RegisteredTable[] }>(TABLES_QUERY, {
    fetchPolicy: "network-only",
  });
  return useCallback(async () => {
    const result = await load();
    if (!result.data) {
      throw result.error ?? new Error("The registered tables could not be read");
    }
    return result.data.tables;
  }, [load]);
}

export function useRegisterTable() {
  const refetchTables = useShownTableQueries();
  const [registerTable, { loading }] = useMutation<{ registerTable: MutationResult }>(
    RegisterTable,
    {
      refetchQueries: refetchTables,
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
  const refetchTables = useShownTableQueries();
  const [registerEntity, { loading }] = useMutation<{ registerEntity: MutationResult }>(
    RegisterEntity,
    { refetchQueries: refetchTables },
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
  const refetchTables = useShownTableQueries();
  const [registerFact, { loading }] = useMutation<{ registerFact: MutationResult }>(RegisterFact, {
    refetchQueries: refetchTables,
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
  const refetchTables = useShownTableQueries();
  const [updateTable, { loading }] = useMutation<{ updateTable: MutationResult }>(UpdateTable, {
    refetchQueries: refetchTables,
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
  const refetchTables = useShownTableQueries();
  const [deleteTable, { loading }] = useMutation<{ deleteTable: MutationResult }>(DeleteTable, {
    refetchQueries: refetchTables,
  });
  return {
    deleteTable: async (id: number) => {
      const result = await deleteTable({ variables: { id } });
      return (result.data?.deleteTable ?? { success: false, message: "" }) as MutationResult;
    },
    loading,
  };
}
