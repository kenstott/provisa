// Copyright (c) 2026 Kenneth Stott
// Canary: 7d3c1e58-0b4a-4f92-8e67-c5a9f2d1b04e
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2259: each data product's sync status on the Data Products list, from the record of every
// publish: its latest outcome on each data catalog that exports. It links to Context Sync filtered
// to the product, drawn as a link only for a viewer holding that page's right.

import { useEffect, useState } from "react";
import type { TFunction } from "i18next";
import { fetchProductSyncStatus, type ProductSyncStatus } from "../../api/syncing";

/** Context Sync narrowed to the product `id`. */
export const syncHref = (id: string) => `/context/sync?product=${encodeURIComponent(id)}`;

export type SyncState = "synced" | "failed" | "none";

/** A product's state across its entries: failed when any entry's latest publish failed it,
 *  synced when every one published it, none when no publish carried it. */
export function syncState(statuses: ProductSyncStatus[]): SyncState {
  if (statuses.length === 0) return "none";
  return statuses.some((s) => s.outcome === "failed") ? "failed" : "synced";
}

/** What a product's sync cell shows: its state, with the count of entries it names. */
export function syncLabel(statuses: ProductSyncStatus[], t: TFunction): string {
  const state = syncState(statuses);
  if (state === "none") return t("dataProductsTab.syncNone");
  return state === "failed"
    ? t("dataProductsTab.syncFailed", {
        count: statuses.filter((s) => s.outcome === "failed").length,
      })
    : t("dataProductsTab.syncSynced", { count: statuses.length });
}

/** The order the list sorts the column in: failures first, then synced, then not synced. */
export const SYNC_ORDER: Record<SyncState, number> = { failed: 0, synced: 1, none: 2 };

/** Every product's statuses by id, read once with the list; `error` names a read that failed. A
 *  product no publish carried has no key: its status is "not synced". */
export function useProductSyncStatus(): {
  statuses: Record<string, ProductSyncStatus[]>;
  error: string | null;
} {
  const [statuses, setStatuses] = useState<Record<string, ProductSyncStatus[]>>({});
  const [error, setError] = useState<string | null>(null);
  useEffect(() => {
    let live = true;
    fetchProductSyncStatus()
      .then((s) => {
        if (live) setStatuses(s);
      })
      .catch((e: unknown) => {
        if (live) setError(e instanceof Error ? e.message : String(e));
      });
    return () => {
      live = false;
    };
  }, []);
  return { statuses, error };
}
