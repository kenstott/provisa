// Copyright (c) 2026 Kenneth Stott
// Canary: f639d986-4b37-4e0a-8b3c-7db034a48834
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2109: whether a form shows Time Travel on, and whether versions of a saved table or view
// exist (so what they were recorded under is no longer offered).

import { useState } from "react";
import { useQuery } from "@apollo/client/react";
import type { RegisteredTable } from "../../types/admin";
import type { MVInfo } from "../../api/admin";
import { MVList } from "../../hooks/admin.graphql";
import type { HistoryDeclaration } from "./BitemporalFields";

export const NO_HISTORY: HistoryDeclaration = {
  businessKey: [],
  validTime: null,
  recordedTime: null,
};

/** Whether the form shows Bitemporal, and how it is turned on or off. */
export interface HistoryChoice {
  on: boolean;
  onToggle: (on: boolean) => void;
}

/** Bitemporal is shown while anything is declared, or once it is picked; Off clears all three. */
export function useHistoryChoice(
  value: HistoryDeclaration,
  onChange: (next: HistoryDeclaration) => void,
): HistoryChoice {
  const declared = Boolean(value.businessKey.length || value.validTime || value.recordedTime);
  const [picked, setPicked] = useState(declared);
  return {
    on: picked || declared,
    onToggle: (on) => {
      setPicked(on);
      if (!on) onChange(NO_HISTORY);
    },
  };
}

const sameColumns = (a: string[], b: string[]) =>
  a.length === b.length && [...a].sort().join("\u0000") === [...b].sort().join("\u0000");

/**
 * REQ-2109: whether versions of a saved table or view exist, so its business key and valid time
 * can no longer change. A view's versions are its materialized rows: they exist once it has been
 * built. A table whose business key is its primary key is updated in place, and its versions are a
 * log Provisa captures; a table whose business key is not its primary key holds its versions as
 * its own rows, so mapping them anew invalidates nothing recorded.
 */
export function useHistoryLocked(saved: RegisteredTable | null): boolean {
  // A table or view being created (no saved table) has no versions yet.
  const declared = saved !== null && saved.businessKey.length > 0;
  const isView = saved !== null && saved.viewSql != null;
  const { data, error } = useQuery<{ mvList: MVInfo[] }>(MVList, {
    skip: !(declared && isView && saved.materialize),
    fetchPolicy: "cache-and-network",
  });
  if (saved === null || !declared) return false;
  if (!isView) {
    // Nothing reports whether the captured log has entries: settled to refuse (REQ-2109).
    const primaryKey = saved.columns.filter((c) => c.isPrimaryKey).map((c) => c.columnName);
    return sameColumns(primaryKey, saved.businessKey);
  }
  // Settled (REQ-2109): where the build cannot be told -- not yet read, or not readable -- the
  // change is refused, as for a view that has been built.
  if (error || !data) return true;
  const mv = data.mvList.find((m) => m.id === `view-${saved.tableName}`);
  return mv !== undefined && mv.lastRefreshAt !== null;
}
