// Copyright (c) 2026 Kenneth Stott
// Canary: be989707-6e5e-47e9-962c-84304017a67b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1986/REQ-1991: the JSON:API Explorer's parameter values for the selected table, and the
// `params` object a read sends (by parameter name). Split out of JsonApiPage.tsx to keep that file
// under the max-lines gate.

import { useCallback, useMemo, useState, type SetStateAction } from "react";
import type { ListedTable, ListedColumn } from "../../types/admin";
import {
  paramColumns,
  requestParams,
  requiredParamsSatisfied,
} from "../../components/nativeParams";

type Values = Record<string, string>;

/** A table's data columns: every column but its parameters, which a read gives in `params`. */
export function dataColumnsOf(table: ListedTable | undefined): ListedColumn[] {
  return (table?.columns ?? []).filter((c) => !c.nativeFilterType);
}
const NONE: Values = {};

export function useJsonApiParams(table: ListedTable | undefined) {
  // Values by column name, held for the table they were typed for: another table starts empty.
  const key = table ? `${table.domainId}/${table.tableName}` : "";
  const [held, setHeld] = useState<{ key: string; values: Values }>({ key: "", values: NONE });
  const paramValues = held.key === key ? held.values : NONE;
  const setParamValues = useCallback(
    (next: SetStateAction<Values>) =>
      setHeld((prev) => {
        const current = prev.key === key ? prev.values : NONE;
        return { key, values: typeof next === "function" ? next(current) : next };
      }),
    [key],
  );
  const tableParams = useMemo(() => (table ? paramColumns(table) : []), [table]);
  const paramsParam = useMemo(() => {
    if (!table) return "";
    const params = requestParams(table, paramValues);
    return Object.keys(params).length > 0 ? JSON.stringify(params) : "";
  }, [table, paramValues]);
  const paramsReady = !table || requiredParamsSatisfied(table, paramValues);
  return { tableParams, paramValues, setParamValues, paramsParam, paramsReady };
}
