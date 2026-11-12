// Copyright (c) 2026 Kenneth Stott
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2240: rows of a list deleted in one call. Each kind's bulk mutation answers which rows went
// and each one refused; the lists its single delete reads again are read again here too.

import { useMutation } from "@apollo/client/react";
import type { DocumentNode } from "graphql";
import type { BulkDeleteResult } from "../components/list/listSelection";

type Refetch = NonNullable<Parameters<typeof useMutation>[1]>["refetchQueries"];

export function useBulkDelete<K extends string | number = string>(
  document: DocumentNode,
  /** The mutation's field, as it answers (`deleteMetrics`). */
  field: string,
  /** Its one argument, the keys of the rows (`names`, `ids`). */
  variable: string,
  refetchQueries?: Refetch,
): (keys: K[]) => Promise<BulkDeleteResult> {
  const [run] = useMutation<Record<string, BulkDeleteResult>>(document, {
    refetchQueries,
    awaitRefetchQueries: true,
  });
  return async (keys) => {
    const result = await run({ variables: { [variable]: keys } });
    // A GraphQL error is thrown by the mutation itself (errorPolicy "none"); an answer without
    // data is not one the server gives.
    if (!result.data) throw new Error(`${field} answered without data`);
    return result.data[field];
  };
}
