// Copyright (c) 2026 Kenneth Stott
// Canary: cb9fcd11-efd4-486f-af84-0ed635548fa2
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { reportStorageFailure } from "../lib/browserStorage";
import { StorageAPI } from "@graphiql/toolkit";

/** What GraphiQL starts its editors on: one saved tab's query, variables and headers. */
export interface SavedTab {
  query: string;
  variables: string;
  headers: string;
}

interface SavedTabs {
  activeTabIndex: number;
  tabs: { query: string | null; variables: string | null; headers: string | null }[];
}

/**
 * The tab GraphiQL last had open, from its saved tabs in `storage`; null when none are saved.
 *
 * GraphiQL reopens its saved tabs on the tab whose contents equal its starting query, variables
 * and headers, and when none does it adds a new empty "<untitled>" tab and opens that. It keeps
 * the starting query under a key of its own that it writes on an edit, not when tabs are seeded
 * (a demo's example queries) or switched, so the explorer opened again on an empty tab instead
 * of the one left open. This tab is the starting point that reopens it.
 */
export function savedActiveTab(
  storage: Pick<Storage, "getItem" | "setItem" | "removeItem" | "clear" | "length">,
): SavedTab | null {
  const raw = new StorageAPI(storage as Storage).get("tabState");
  if (raw === null) return null;
  let saved: SavedTabs;
  try {
    saved = JSON.parse(raw) as SavedTabs;
  } catch (e) {
    reportStorageFailure("parse", "tabState", e);
    return null;
  }
  const tab = saved.tabs[saved.activeTabIndex];
  if (tab === undefined) return null; // no tab saved: GraphiQL opens its default tabs
  return { query: tab.query ?? "", variables: tab.variables ?? "", headers: tab.headers ?? "" };
}
