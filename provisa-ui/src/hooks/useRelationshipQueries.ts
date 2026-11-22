// Copyright (c) 2026 Kenneth Stott
// Canary: 5a2d8f63-1b7e-4c94-a0f5-6e3b9d21c7a8
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useMutation, useQuery } from "@apollo/client/react";
import type { MutationResult, Relationship } from "../types/admin";
import {
  AllRelationshipsQuery as ALL_RELATIONSHIPS_QUERY,
  JoinThroughQuery as JOIN_THROUGH_QUERY,
  JoinThrough as JOIN_THROUGH_MUTATION,
} from "./admin.graphql";
import { firstLoad } from "./firstLoad";

const NO_RELATIONSHIPS: Relationship[] = [];

/** `includeWithdrawn` (REQ-1921): the withdrawn relationships too, marked, where the caller may
 * preview their ends; a graph view's Show withdrawn. */
export function useAllRelationships(opts: { includeWithdrawn?: boolean } = {}) {
  const { data, loading, error, refetch } = useQuery<{ allRelationships: Relationship[] }>(
    ALL_RELATIONSHIPS_QUERY,
    {
      fetchPolicy: "cache-and-network",
      variables: { includeWithdrawn: opts.includeWithdrawn === true },
    },
  );
  return {
    relationships: data?.allRelationships ?? NO_RELATIONSHIPS,
    loading: firstLoad(loading, data),
    error,
    refetch,
  };
}

/** REQ-1586: what joining two relationships through the table they share makes. `shape`:
 *  "junction" (both refer from the middle table) or "shared_dimension" (both refer to it). */
export interface JoinThroughPlan {
  shape: "junction" | "shared_dimension";
  name: string;
  sourceTable: string;
  sourceColumn: string;
  targetTable: string;
  targetColumn: string;
  viaTable: string;
  viaSourceColumn: string;
  viaTargetColumn: string;
  attributes: string[];
}

/** The plan for exactly two ticked relationships, in the order ticked; null when they share no
 *  middle table or are already joined through it. Nothing is asked for any other count. */
export function useJoinThroughPlan(ids: readonly string[]) {
  const pair = ids.length === 2;
  const { data, loading } = useQuery<{ joinThrough: JoinThroughPlan | null }>(JOIN_THROUGH_QUERY, {
    variables: { firstId: ids[0] ?? "", secondId: ids[1] ?? "" },
    skip: !pair,
    fetchPolicy: "network-only",
  });
  return { plan: pair ? (data?.joinThrough ?? null) : null, loading: pair && loading };
}

/** Create the joined relationship under `id`; both relationship lists are read again. */
export function useJoinThrough() {
  const [join, { loading }] = useMutation<{ joinThrough: MutationResult }>(JOIN_THROUGH_MUTATION, {
    // By name: each list read again with its own variables, wherever it is shown.
    refetchQueries: ["RelationshipsQuery", "AllRelationshipsQuery"],
    awaitRefetchQueries: true,
  });
  return {
    joinThrough: async (firstId: string, secondId: string, id: string): Promise<MutationResult> => {
      const result = await join({ variables: { firstId, secondId, id } });
      if (!result.data) throw new Error("joinThrough returned no data");
      return result.data.joinThrough;
    },
    saving: loading,
  };
}
