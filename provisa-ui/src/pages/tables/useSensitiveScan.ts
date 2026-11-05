// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2147 (CONTAINED VALUES ARE HINTS): a person asks for a scan of a table's text columns for
// sensitive kinds held inside their values -- a social security number in a note -- from the
// column access settings, while the table is being registered or afterwards. The scan runs in the
// background; its hints show beside each column's grant and masks. A hint is never a
// classification and is never applied: what to grant, mask or fake stays the person's choice.

import { useCallback, useEffect, useState } from "react";
import {
  fetchSensitiveScan,
  startSensitiveScan,
  type SensitiveHint,
  type SensitiveScanState,
} from "../../api/profiler";

/** How often a running scan is looked at again, in milliseconds. */
export const SCAN_POLL_MS = 2000;

/** The table's latest scan and hints, a way to start one, and a running scan followed to its
 * end. ``tableId`` undefined: the table is not saved yet, and nothing can be scanned. */
export function useSensitiveScan(tableId: number | undefined) {
  const [state, setState] = useState<SensitiveScanState | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [starting, setStarting] = useState(false);

  const load = useCallback(() => {
    if (tableId === undefined) return;
    fetchSensitiveScan(tableId)
      .then((s) => {
        setState(s);
        setError(null);
      })
      .catch((e: Error) => setError(e.message));
  }, [tableId]);

  useEffect(load, [load]);

  const running = state?.scan?.status === "running";
  useEffect(() => {
    if (!running) return;
    const timer = setTimeout(load, SCAN_POLL_MS);
    return () => clearTimeout(timer);
  }, [running, state, load]);

  const start = () => {
    if (tableId === undefined) return;
    setStarting(true);
    startSensitiveScan(tableId)
      .then((scan) => {
        setState((held) => ({ scan, hints: held?.hints ?? [] }));
        setError(null);
      })
      .catch((e: Error) => setError(e.message))
      .finally(() => setStarting(false));
  };

  const hintsOf = (column: string): SensitiveHint[] =>
    (state?.hints ?? []).filter((h) => h.column === column);

  return { state, error, starting, running, start, hintsOf, saved: tableId !== undefined };
}

export type SensitiveScanHandle = ReturnType<typeof useSensitiveScan>;
