// Copyright (c) 2026 Kenneth Stott
// Canary: 01aa2382-6173-4455-9afe-01fb162a368c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2199: what the modeling wizards' pickers offer, read from the registered tables.

import { useMemo } from "react";
import { useTranslation } from "react-i18next";
import { useTables } from "../../../hooks/useAdminQueries";
import type { ListedColumn, ListedTable } from "../../../types/admin";
import { relationName } from "../../../naming";
import { colLabel, isKeyable, isNumeric, isUserTable } from "./columns";

export interface ModelTables {
  /** The user's own registered tables, by the name a model reads them by ({domain}.{table}). */
  sourceOptions: { value: string; label: string }[];
  columnsOf: (source: string | null) => ListedColumn[];
  /** Registered dimension tables: what a star schema's conformed dimension is picked from. */
  dimensionOptions: { value: string; label: string }[];
}

export function useModelTables(): ModelTables {
  const { tables } = useTables({ inPage: true });
  const userTables = useMemo(() => tables.filter(isUserTable), [tables]);
  return useMemo(() => {
    const byName = new Map<string, ListedTable>(userTables.map((tbl) => [relationName(tbl), tbl]));
    return {
      sourceOptions: [...byName.keys()].map((n) => ({ value: n, label: n })),
      columnsOf: (source) => (source ? (byName.get(source)?.columns ?? []) : []),
      dimensionOptions: tables
        .filter((tbl) => tbl.modelingRole === "dimension")
        .map((tbl) => ({ value: tbl.tableName, label: relationName(tbl) })),
    };
  }, [tables, userTables]);
}

/** The column options of a source: all of them, only those that can be a key, or only numerics. */
export function useColumnOptions(columns: ListedColumn[]) {
  const { t } = useTranslation();
  return useMemo(
    () => ({
      all: columns.map((c) => ({ value: c.columnName, label: colLabel(c) })),
      keys: columns.map((c) => ({
        value: c.columnName,
        label: isKeyable(c) ? colLabel(c) : `${colLabel(c)} — ${t("modelingForm.notKeyable")}`,
        disabled: !isKeyable(c),
      })),
      numeric: columns.filter(isNumeric).map((c) => ({ value: c.columnName, label: colLabel(c) })),
    }),
    [columns, t],
  );
}
