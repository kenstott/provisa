// Copyright (c) 2026 Kenneth Stott
// Canary: 3c6b8e51-2d94-4f07-a8c3-5e1d9b4f7a20
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useRef, useState } from "react";
import type { RefreshPolicySummary, RegisteredTable } from "../../types/admin";
import { useRefreshPolicyPreview } from "../../hooks/useAdminQueries";

// REQ-1143: keep the top-of-form refresh-policy summary in sync with the draft knobs. The tree is
// never re-derived client-side — a debounced preview query re-runs describe_refresh_policy server-
// side with the in-flight values, seeded from the persisted summary so it renders before the first
// fetch resolves. `stagedTtl` is the Cache TTL input's staged edit for this table, if any.
export function useLivePolicyPreview(
  editingTable: RegisteredTable,
  stagedTtl: string | undefined,
): RefreshPolicySummary | null {
  const previewPolicy = useRefreshPolicyPreview();
  const [livePolicy, setLivePolicy] = useState<RefreshPolicySummary | null>(
    editingTable.refreshPolicySummary,
  );
  // Effective cache_ttl mirrors the Cache TTL input's resolution: staged edit wins, then the row.
  const effCacheTtl =
    stagedTtl != null && stagedTtl !== ""
      ? Number(stagedTtl)
      : stagedTtl === ""
        ? null
        : editingTable.cacheTtl;
  const {
    id: tableId,
    sourceId,
    domainId,
    schemaName,
    tableName,
    preferMaterialized,
    loadProtected,
    offPeakWindow,
    offPeakTz,
    changeSignal,
    refreshPolicySummary,
  } = editingTable;
  // Keep the latest preview callback in a ref (written in an effect, never during render) so the
  // debounce effect below can call it without listing it as a dependency.
  const previewRef = useRef(previewPolicy);
  useEffect(() => {
    previewRef.current = previewPolicy;
  });
  useEffect(() => {
    let cancelled = false;
    const handle = setTimeout(() => {
      previewRef
        .current({
          sourceId,
          domainId,
          schemaName,
          tableName,
          cacheTtl: effCacheTtl,
          preferMaterialized,
          loadProtected,
          offPeakWindow,
          offPeakTz,
          changeSignal,
        })
        .then((summary) => {
          // A null preview means the engine is not yet connected (startup) — keep the persisted
          // summary rather than blanking the banner.
          if (!cancelled && summary) setLivePolicy(summary);
        })
        // The debounced preview outlives the form: closing the editor (or a test unmounting it)
        // leaves this query in flight, and a rejection with no handler becomes an unhandled
        // rejection that kills the surrounding context. The banner keeps the persisted summary,
        // which is the same state a null preview produces.
        .catch((err: unknown) => {
          // Apollo fires InvariantViolation("Store reset while query was in flight") whenever
          // client.resetStore() (triggered by a schema-version bump on any mutation response)
          // cancels this in-flight query.  That is expected behaviour — the next effect cycle
          // will re-issue the preview against the refreshed store.  Logging it to console.error
          // would surface as a spurious uncaught-browser-error in e2e coverage checks.
          const msg = err instanceof Error ? err.message : String(err);
          if (msg.includes("Store reset while query was in flight")) return;
          // Apollo aborts the underlying fetch when the last observer of a one-off query goes
          // away, which is exactly what closing the editor or navigating off /tables does to a
          // preview still in flight.  The AbortError is our own teardown coming back to us, so
          // reporting it is reporting an event we caused deliberately; `cancelled` says the same
          // thing for the case where the cleanup ran before the rejection landed.
          if (cancelled || (err instanceof Error && err.name === "AbortError")) return;
          // A full-document navigation (page.goto in a test helper, a real link click)
          // destroys the page without unmounting React, so no cleanup sets `cancelled`;
          // Chromium severs the in-flight fetch with TypeError: Failed to fetch rather
          // than an AbortError. Same teardown-of-our-own-request case as above.
          if (msg.includes("Failed to fetch")) return;
          console.error("refreshPolicyPreview failed:", err);
        });
    }, 300);
    return () => {
      cancelled = true;
      clearTimeout(handle);
    };
  }, [
    tableId,
    sourceId,
    domainId,
    schemaName,
    tableName,
    effCacheTtl,
    preferMaterialized,
    loadProtected,
    offPeakWindow,
    offPeakTz,
    changeSignal,
    refreshPolicySummary,
  ]);
  return livePolicy ?? refreshPolicySummary;
}
