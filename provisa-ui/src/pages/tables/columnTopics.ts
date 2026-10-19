// Copyright (c) 2026 Kenneth Stott
// Canary: cc5b0822-d027-4ba3-8b43-c2ca8544f0e1
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Which topic of a column the column editors show: see ColumnTabs.

import { useState } from "react";

export type ColumnTab = "naming" | "type" | "access" | "testing";
/** A column's own attributes: what a column list shows before the table is registered. */
export const METADATA_TABS: ColumnTab[] = ["naming", "type", "access"];
/** Every topic of a registered table's columns; Testing is each column's test data. */
export const COLUMN_TABS: ColumnTab[] = [...METADATA_TABS, "testing"];

/** The tab in view; a form that hides a topic (no right to see grants) passes the ones it shows.
 * ``initial`` is the tab it opens on (a link to the table's Testing tab); else the first shown. */
export function useColumnTab(shown: ColumnTab[] = COLUMN_TABS, initial?: ColumnTab) {
  const [tab, setTab] = useState<ColumnTab>(initial ?? shown[0]);
  return [shown.includes(tab) ? tab : shown[0], setTab] as const;
}
