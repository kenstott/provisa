// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { RegisteredTable, Source } from "../../types/admin";
import { UniquesPanel } from "../../components/admin/UniquesPanel";
import { DataQualityPanel } from "./DataQualityPanel";
import { ProfilerPanel } from "./ProfilerPanel";

/** The Quality section of a table's form: its contract or profiler, and its unique keys. */
export function TableQualitySection({
  editingTable,
  setEditingTable,
  editSource,
  isCheckerTable,
  savedProfilerId,
  tableId,
}: {
  editingTable: RegisteredTable;
  setEditingTable: React.Dispatch<React.SetStateAction<RegisteredTable | null>>;
  editSource: Source | undefined;
  isCheckerTable: boolean;
  savedProfilerId: string | null;
  /** The saved table's id, which a check run needs; absent while the table is being created. */
  tableId?: number;
}) {
  return (
    <>
      {/* REQ-1443 clause 7: a checker source's table lands that checker's scans, so its contract is
        edited here. Only a checker source has one — every other table has no contract to build. */}
      {editSource != null && isCheckerTable && (
        <DataQualityPanel
          checker={(editSource.type ?? "").toLowerCase()}
          sourceId={editSource.id}
          tableId={tableId}
          contractText={editingTable.dqContract ?? ""}
          onChange={(text) => setEditingTable({ ...editingTable, dqContract: text || null })}
        />
      )}
      {/* REQ-1934: any table but a checker's results or a profiler's own may join a profiler. */}
      {editSource != null && !isCheckerTable && editSource.type !== "data_profiler" && (
        <ProfilerPanel {...{ editingTable, savedProfilerId, setEditingTable }} />
      )}
      {/* REQ-1093: table-level UNIQUE constraints editor */}
      <div>
        <UniquesPanel
          flat
          uniques={editingTable.uniqueConstraints ?? []}
          columns={editingTable.columns.map((c) => c.columnName)}
          onChange={(uniques) => setEditingTable({ ...editingTable, uniqueConstraints: uniques })}
        />
      </div>
    </>
  );
}
