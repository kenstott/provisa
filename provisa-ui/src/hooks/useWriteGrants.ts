// Copyright (c) 2026 Kenneth Stott
// Canary: 2a3387f6-f7c1-40e6-828a-108b93d3a749
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2074: a table's write grants, saved whole.

import { useMutation } from "@apollo/client/react";
import type { MutationResult, WriteGrant } from "../types/admin";
import { SetTableWriteGrants, TablesQuery } from "./admin.graphql";

export function useSetTableWriteGrants() {
  const [run, { loading }] = useMutation<{ setTableWriteGrants: MutationResult }>(
    SetTableWriteGrants,
    { refetchQueries: [{ query: TablesQuery }] },
  );
  return {
    setTableWriteGrants: async (tableId: number, grants: WriteGrant[]) => {
      const result = await run({
        variables: {
          tableId,
          grants: grants.map((g) => ({
            role: g.role,
            operation: g.operation,
            columns: g.operation === "delete" ? [] : g.columns,
            rows: g.operation === "insert" ? null : g.rows,
            check: g.operation === "delete" ? null : g.check,
            presets:
              g.operation === "delete"
                ? []
                : g.presets.map((p) => ({
                    column: p.column,
                    source: p.source,
                    name: p.name,
                    value: p.value,
                  })),
          })),
        },
      });
      return (result.data?.setTableWriteGrants ?? {
        success: false,
        message: "",
      }) as MutationResult;
    },
    loading,
  };
}
