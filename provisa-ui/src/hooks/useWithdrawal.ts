// Copyright (c) 2026 Kenneth Stott
// Canary: 8e4c1b07-2f9d-4a63-b5e8-0d7a3c96f215
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1921: Withdraw and Put back, for a table or view and for a source. Each resolves once the
// lists shown are read again, and rejects with the refusal, in the reader's language, when the
// server refused it. A relationship is withdrawn while an end of it is, and a table while its
// source is, so each also reads those lists again.

import { useMutation } from "@apollo/client/react";
import type { MutationResult } from "../types/admin";
import { PutBackSource, PutBackTable, WithdrawSource, WithdrawTable } from "./admin.graphql";
import { refusalError } from "../i18n/serverMessage";
import { useShownTableQueries } from "./useTableQueries";

const RELATIONSHIP_QUERY_NAMES = ["RelationshipsQuery", "AllRelationshipsQuery"];

function settled(name: string, data: Record<string, MutationResult> | undefined): void {
  if (!data) throw new Error(`${name} answered nothing`);
  if (!data[name].success) throw refusalError(data[name]);
}

export function useTableWithdrawal() {
  const refetchTables = useShownTableQueries();
  const refetchQueries = () => [...refetchTables(), ...RELATIONSHIP_QUERY_NAMES];
  const options = { refetchQueries, awaitRefetchQueries: true };
  const [withdraw] = useMutation<{ withdrawTable: MutationResult }>(WithdrawTable, options);
  const [putBack] = useMutation<{ putBackTable: MutationResult }>(PutBackTable, options);
  return {
    withdrawTable: async (tableId: number) =>
      settled("withdrawTable", (await withdraw({ variables: { tableId } })).data),
    putBackTable: async (tableId: number) =>
      settled("putBackTable", (await putBack({ variables: { tableId } })).data),
  };
}

export function useSourceWithdrawal() {
  const refetchTables = useShownTableQueries();
  const refetchQueries = () => ["SourcesQuery", ...refetchTables(), ...RELATIONSHIP_QUERY_NAMES];
  const options = { refetchQueries, awaitRefetchQueries: true };
  const [withdraw] = useMutation<{ withdrawSource: MutationResult }>(WithdrawSource, options);
  const [putBack] = useMutation<{ putBackSource: MutationResult }>(PutBackSource, options);
  return {
    withdrawSource: async (sourceId: string) =>
      settled("withdrawSource", (await withdraw({ variables: { sourceId } })).data),
    putBackSource: async (sourceId: string) =>
      settled("putBackSource", (await putBack({ variables: { sourceId } })).data),
  };
}
