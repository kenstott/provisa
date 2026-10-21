// Copyright (c) 2026 Kenneth Stott
// Canary: 0b7e4f29-8c13-4a65-b2d0-5e9a1c6f3d84
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2122: the catalogs of a table's domain, as its editor uses them.

import { useCallback, useEffect, useState } from "react";
import { fetchTableCatalogs, type TableCatalog } from "../../api/catalogLinks";

export interface TableCatalogs {
  catalogs: TableCatalog[];
  problem: string;
  /** The catalog the table is locked to, whose descriptive fields it shows read-only. */
  locked: TableCatalog | null;
  reload: () => Promise<void>;
  report: (problem: string) => void;
}

/** The catalogs of the table's domain. An editor who may only change how columns are hidden
 *  (``enabled`` false) is not offered them: they are a table edit. */
export function useTableCatalogs(tableId: number, enabled: boolean): TableCatalogs {
  const [catalogs, setCatalogs] = useState<TableCatalog[]>([]);
  const [problem, setProblem] = useState("");
  const reload = useCallback(
    () =>
      fetchTableCatalogs(tableId)
        .then((found) => {
          setCatalogs(found);
          setProblem("");
        })
        .catch((e: unknown) => setProblem(String(e))),
    [tableId],
  );
  useEffect(() => {
    if (enabled) void reload();
  }, [enabled, reload]);
  return {
    catalogs,
    problem,
    locked: catalogs.find((c) => c.locked) ?? null,
    reload,
    report: setProblem,
  };
}
