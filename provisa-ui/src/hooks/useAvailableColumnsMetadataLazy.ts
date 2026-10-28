// Copyright (c) 2026 Kenneth Stott
// Canary: c66b7716-2564-42a1-bed0-ef23932575fe
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useCallback, useRef, useState } from "react";
import { useLazyQuery } from "@apollo/client/react";
import type { ColumnMetadata } from "../api/admin";
import { AvailableColumnsMetadata } from "./admin.graphql";
import { isStillStarting, useStartingPoll } from "./discoveryStartingPoll";

/** The columns a read answered after the source reported STARTING, with the table they belong to. */
export interface LateColumns {
  sourceId: string;
  schemaName: string;
  tableName: string;
  columns: ColumnMetadata[];
}

/** `fetchColumns` resolves null while the source reports STARTING (REQ-1824) -- no answer yet, not
 * an empty table. It is asked again every few seconds; the answer arrives as `late`, with the
 * table it belongs to. Any other error throws. */
export function useAvailableColumnsMetadataLazy() {
  const [run, { error, refetch }] = useLazyQuery<{ availableColumnsMetadata: ColumnMetadata[] }>(
    AvailableColumnsMetadata,
    { fetchPolicy: "cache-first", errorPolicy: "all" },
  );
  const asked = useRef<Omit<LateColumns, "columns"> | null>(null);
  const [late, setLate] = useState<LateColumns | null>(null);
  const poll = useCallback(async () => {
    const r = await refetch();
    if (asked.current && !r.error && r.data) {
      setLate({ ...asked.current, columns: r.data.availableColumnsMetadata });
    }
  }, [refetch]);
  const { starting, timedOut } = useStartingPoll(error, poll);
  const fetchColumns = useCallback(
    async (sourceId: string, schemaName: string, tableName: string) => {
      asked.current = { sourceId, schemaName, tableName };
      setLate(null);
      const { data, error: e } = await run({ variables: { sourceId, schemaName, tableName } });
      if (e && !isStillStarting(e)) throw e;
      return e ? null : (data?.availableColumnsMetadata ?? []);
    },
    [run],
  );
  return { fetchColumns, late, starting, startingTimedOut: timedOut };
}
