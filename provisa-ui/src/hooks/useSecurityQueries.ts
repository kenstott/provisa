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
  RevokeRoleFromTable,
  RevokeRoleFromObject,
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
        maxQueryComplexity: number | null;
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

/** The grant kinds a role is taken off one object at a time (REQ-1918). */
export type GrantKind = "METRIC" | "COMMAND" | "WEBHOOK";

/** Take a role off a table's column grants, or off a metric's, command's or webhook's assigned
 *  roles, so the role can be deleted (REQ-1918). */
export function useRevokeRoleGrants() {
  const [fromTable] = useMutation<{ revokeRoleFromTable: MutationResult }>(RevokeRoleFromTable);
  const [fromObject] = useMutation<{ revokeRoleFromObject: MutationResult }>(
    RevokeRoleFromObject,
  );
  return {
    revokeFromTable: async (roleId: string, tableId: number) => {
      const answer = (await fromTable({ variables: { roleId, tableId } })).data?.revokeRoleFromTable;
      if (!answer) throw new Error("revokeRoleFromTable returned no result");
      return answer;
    },
    revokeFromObject: async (roleId: string, kind: GrantKind, name: string) => {
      const answer = (await fromObject({ variables: { roleId, kind, name } })).data
        ?.revokeRoleFromObject;
      if (!answer) throw new Error("revokeRoleFromObject returned no result");
      return answer;
    },
  };
}
