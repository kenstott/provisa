// Copyright (c) 2026 Kenneth Stott
// Canary: 8f2c6a1e-4d9b-4b7a-9e3c-1a5d8f0b2c74
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The Security page's hooks: RLS rules (REQ-041, REQ-402, REQ-1679) and role writes
// (REQ-042, REQ-1174, REQ-1677). Split from useAdminQueries at the file-length ratchet.

import { useQuery, useMutation } from "@apollo/client/react";
import type { MutationResult, RLSRule } from "../types/admin";
import {
  RolesQuery as ROLES_QUERY,
  RLSRulesQuery as RLS_RULES_QUERY,
  UpsertRlsRule,
  DeleteRlsRule,
  CreateRole,
  DeleteRole,
} from "./admin.graphql";
import { firstLoad } from "./useAdminQueries";

const NO_RLS_RULES: RLSRule[] = [];

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
      actionName?: string | null; // REQ-1679
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
    deleteRlsRule: async (
      roleId: string,
      tableId?: number | null,
      domainId?: string | null,
      actionName?: string | null, // REQ-1679
    ) => {
      const result = await deleteRlsRule({
        variables: {
          roleId,
          tableId: tableId ?? null,
          domainId: domainId ?? null,
          actionName: actionName ?? null,
        },
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
      parentRoleId?: string | null; // REQ-1677
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
