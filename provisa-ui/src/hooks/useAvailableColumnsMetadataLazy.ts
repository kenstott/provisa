// Copyright (c) 2026 Kenneth Stott
// Canary: c66b7716-2564-42a1-bed0-ef23932575fe
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useCallback } from "react";
import { useLazyQuery } from "@apollo/client/react";
import type { ColumnMetadata } from "../api/admin";
import { AvailableColumnsMetadata } from "./admin.graphql";
import { isStillStarting, useStartingPoll } from "./discoveryStartingPoll";

export function useAvailableColumnsMetadataLazy() {
  const [run, { error, refetch }] = useLazyQuery<{ availableColumnsMetadata: ColumnMetadata[] }>(
    AvailableColumnsMetadata,
    { fetchPolicy: "cache-first", errorPolicy: "all" },
  );
  // STARTING (REQ-1824) is polled for, as the table list is.
  useStartingPoll(error, refetch);
  return useCallback(
    async (sourceId: string, schemaName: string, tableName: string): Promise<ColumnMetadata[]> => {
      const { data, error: e } = await run({ variables: { sourceId, schemaName, tableName } });
      if (e && !isStillStarting(e)) throw e;
      return data?.availableColumnsMetadata ?? [];
    },
    [run],
  );
}
