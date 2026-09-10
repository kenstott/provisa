// Copyright (c) 2026 Kenneth Stott
// Canary: 5d3f8a26-7e19-4c4b-9b0a-1f6e2c8d4a73
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The description behind a lineage field, looked up in the registered tables by the graph's own
// relation name (domain.table) and the column's physical name.

import { domainToSqlName } from "../../naming";
import type { RegisteredTable } from "../../types/admin";

export type DescribeColumn = (relation: string, column: string) => string | null;

export function columnDescriber(tables: readonly RegisteredTable[]): DescribeColumn {
  const byKey = new Map<string, string>();
  for (const tb of tables) {
    const relation = `${domainToSqlName(tb.domainId)}.${tb.tableName}`;
    for (const c of tb.columns) {
      if (c.description) byKey.set(`${relation} ${c.columnName}`, c.description);
    }
  }
  return (relation, column) => byKey.get(`${relation} ${column}`) ?? null;
}
