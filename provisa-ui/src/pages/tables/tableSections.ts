// Copyright (c) 2026 Kenneth Stott
// Canary: 647bc57b-1bf1-4938-a4ab-5a7128845479
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState } from "react";

/**
 * A registered table's details, in eight sections: the same tabs in its read view and its edit
 * form, so what is read in one is found in the same place in the other.
 */
export type TableSection =
  | "identity"
  | "associations"
  | "load"
  | "query"
  | "quality"
  | "columns"
  | "writes" // REQ-2074: what each role may write
  | "access"; // REQ-2162: who can read or write each column
export const TABLE_SECTIONS: TableSection[] = [
  "identity",
  "associations",
  "load",
  "query",
  "quality",
  "columns",
  "writes",
  "access",
];

/**
 * The section in view. A page that keeps it across the read view and the edit form passes it in
 * (`value` and `onChange`): opening the editor keeps the tab that was being read. Without them
 * the component keeps its own, starting on Identity.
 */
export function useTableSection(value?: TableSection, onChange?: (section: TableSection) => void) {
  const [own, setOwn] = useState<TableSection>("identity");
  return value !== undefined && onChange !== undefined
    ? ([value, onChange] as const)
    : ([own, setOwn] as const);
}
