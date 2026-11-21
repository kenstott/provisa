// Copyright (c) 2026 Kenneth Stott
// Canary: 6d2a9f47-3c81-4e05-b7a6-1e9c4d8f2b53
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2262: relationship review after registration. The discover dialog opens set up for the
// tables just registered: their declared foreign keys, matching key names and types, and (where a
// model can run) semantic joins, against every table in the model, either way round, across
// sources. Nothing runs until Run; every candidate, declared keys included, waits in the run's
// result to be accepted or skipped, and an accepted one follows the normal approval rules.
// Closing the dialog, or its Done, ends the review with how many relationships were created.

import { useCallback, useState } from "react";
import { domainToSqlName } from "../../naming";
import { useTables } from "../../hooks/useAdminQueries";
import type { ListedTable } from "../../types/admin";
import { DiscoverDialog } from "./DiscoverDialog";
import { reviewStart } from "./reviewStart";
import { useDiscoveryRun } from "./useDiscoveryRun";

export function RelationshipReview({
  tableIds,
  onDone,
}: {
  /** The tables just registered. */
  tableIds: number[];
  onDone: (created: number) => void;
}) {
  const { tables, refetch } = useTables();
  // The review is scoped to the tables just registered: it opens once the list holds them all.
  const listed = new Set(tables.map((t) => t.id));
  if (!tableIds.every((id) => listed.has(id))) return null;
  return <Review tableIds={tableIds} tables={tables} refetch={refetch} onDone={onDone} />;
}

function Review({
  tableIds,
  tables,
  refetch,
  onDone,
}: {
  tableIds: number[];
  tables: ListedTable[];
  refetch: () => unknown;
  onDone: (created: number) => void;
}) {
  const nothingListed = useCallback(() => {}, []);
  const discovery = useDiscoveryRun(nothingListed);
  // Read once as the review opens: the form's sets are restored from it as it mounts.
  const [start] = useState(() => reviewStart(tableIds, tables, new Date()));
  const onApplied = useCallback(() => void refetch(), [refetch]);
  return (
    <DiscoverDialog
      opened
      onClose={() => undefined}
      discovery={discovery}
      tables={tables}
      defaultDomains={[]}
      start={start}
      onDone={onDone}
      suggestions={{
        tableDomainById: Object.fromEntries(tables.map((t) => [t.id, domainToSqlName(t.domainId)])),
        tableNameById: Object.fromEntries(tables.map((t) => [t.id, t.tableName])),
        onApplied,
      }}
    />
  );
}
