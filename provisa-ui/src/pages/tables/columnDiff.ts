// Copyright (c) 2026 Kenneth Stott
// Canary: 0bb65b98-88d8-4d1e-a053-af2b104c88d3
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1484: which columns a pending table edit renames or drops. Both break other artifacts —
// a rename breaks everything authored against the exposed SQL name, a drop breaks that plus the
// artifacts that store the physical column_name — so the save asks the server what depends on them
// before it lands.

import type { TableColumn } from "../../types/admin";

export interface ColumnEditDiff {
  renamed: string[]; // physical column_name of columns whose alias changed
  removed: string[]; // physical column_name of columns no longer present
}

export function diffEditedColumns(
  stored: readonly TableColumn[],
  edited: readonly TableColumn[],
): ColumnEditDiff {
  const editedNames = new Set(edited.map((c) => c.columnName));
  const removed = stored.filter((c) => !editedNames.has(c.columnName)).map((c) => c.columnName);
  const renamed = edited
    .filter((c) => {
      const before = stored.find((s) => s.columnName === c.columnName);
      return before !== undefined && (c.alias || "") !== (before.alias || "");
    })
    .map((c) => c.columnName);
  return { renamed, removed };
}
