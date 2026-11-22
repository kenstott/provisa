// Copyright (c) 2026 Kenneth Stott
// Canary: c8e41b7a-2f63-4d0e-a95c-7b1d3e6f9a42
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useLazyQuery } from "@apollo/client/react";
import {
  GenerateUnregisteredColumnDescription,
  GenerateUnregisteredTableDescription,
} from "./admin.graphql";

/** A table the Register table form is about to register: what its description is written from. */
export interface UnregisteredTable {
  sourceId: string;
  schemaName: string;
  tableName: string;
  columns: string[];
}

/** The AI describe queries for a table not yet registered: the table and each of its columns. */
export function useDescribeUnregistered() {
  const [runTable] = useLazyQuery<{ generateTableDescription: string }>(
    GenerateUnregisteredTableDescription,
    { fetchPolicy: "network-only" },
  );
  const [runColumn] = useLazyQuery<{ generateColumnDescription: string }>(
    GenerateUnregisteredColumnDescription,
    { fetchPolicy: "network-only" },
  );
  return {
    describeTable: async (table: UnregisteredTable): Promise<string> => {
      const { data, error } = await runTable({ variables: { ...table } });
      if (error) throw error;
      if (data === undefined) throw new Error("generateTableDescription answered nothing");
      return data.generateTableDescription;
    },
    describeColumn: async (table: UnregisteredTable, columnName: string): Promise<string> => {
      const { data, error } = await runColumn({ variables: { ...table, columnName } });
      if (error) throw error;
      if (data === undefined) throw new Error("generateColumnDescription answered nothing");
      return data.generateColumnDescription;
    },
  };
}
