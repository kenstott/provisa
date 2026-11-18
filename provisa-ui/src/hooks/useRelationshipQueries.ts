// Copyright (c) 2026 Kenneth Stott
// Canary: 5a2d8f63-1b7e-4c94-a0f5-6e3b9d21c7a8
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useQuery } from "@apollo/client/react";
import type { Relationship } from "../types/admin";
import { AllRelationshipsQuery as ALL_RELATIONSHIPS_QUERY } from "./admin.graphql";
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
