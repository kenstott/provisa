// Copyright (c) 2026 Kenneth Stott
// Canary: 7e2f9b14-c5a3-4d86-b0f7-1a6c8e3d5b29
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Requirements: REQ-2249

import { useQuery } from "@apollo/client/react";
import type { IncrementalOption } from "../types/admin";
import { CodeResolverIncrementalQuery } from "./admin.graphql";

/** Whether the set `id` over `resolver` may keep its matches incrementally, asked before the set
 * exists. Null until both are given and the server has answered. */
export function useCodeResolverIncremental(resolver: string, id: string): IncrementalOption | null {
  const { data } = useQuery<{ codeResolverIncremental: IncrementalOption }>(
    CodeResolverIncrementalQuery,
    { variables: { resolver, id }, skip: !resolver || !id },
  );
  if (!resolver || !id) return null;
  return data?.codeResolverIncremental ?? null;
}
