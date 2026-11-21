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

import { useMemo } from "react";
import { useMutation } from "@apollo/client/react";
import type { MutationResult } from "../types/admin";
import { PutBackSource, PutBackTable, WithdrawSource, WithdrawTable } from "./admin.graphql";
import { refusalError } from "../i18n/serverMessage";
import { useShownTableQueries } from "./useTableQueries";
import { useRegionChoices } from "./useRegionQueries";
import { useAuth } from "../context/AuthContext";

/** The grant value standing for "no region" (provisa/security/residency.py NO_REGION). */
const NO_REGION = "no_region";

/** Where a withdrawn object may be put back: the org's regions the caller's data_residency grants
 * cover, and whether they cover "no region". Null when the platform declares no regions: put back
 * then names none. As the server checks it (`_residency_covered`): every region in dev mode, else
 * the residency values of the acting roles that hold data_residency. */
export interface PutBackRegions {
  regions: string[];
  noRegion: boolean;
}

export function usePutBackRegions(): PutBackRegions | null {
  const { regions } = useRegionChoices();
  const { devMode, selectedRoles } = useAuth();
  return useMemo(() => {
    if (regions.length === 0) return null;
    if (devMode) return { regions, noRegion: true };
    const covered = new Set(
      selectedRoles
        .filter((r) => r.effectiveCapabilities.includes("data_residency"))
        .flatMap((r) => r.effectiveResidencyValues ?? []),
    );
    return { regions: regions.filter((r) => covered.has(r)), noRegion: covered.has(NO_REGION) };
  }, [regions, devMode, selectedRoles]);
}

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
    putBackTable: async (tableId: number, region: string | null) =>
      settled("putBackTable", (await putBack({ variables: { tableId, region } })).data),
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
    putBackSource: async (sourceId: string, region: string | null) =>
      settled("putBackSource", (await putBack({ variables: { sourceId, region } })).data),
  };
}
