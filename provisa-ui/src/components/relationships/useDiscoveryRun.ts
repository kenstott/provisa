// Copyright (c) 2026 Kenneth Stott
// Canary: 6b1f4d92-3a7e-4c58-9e20-d41a8c5f7b36
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-413: a discovery run is the server's. The page reads the latest run when it mounts and when
// the dialog opens, so a run still going is shown, and its button animates, after the dialog is
// closed and opened again or the page is left and come back to. While it runs it is read again
// about once a second; the suggestions are read again whenever the run has stored more.

import { useCallback, useEffect, useRef, useState } from "react";
import {
  DiscoveryError,
  fetchDiscoveryRun,
  fetchDiscoveryRuns,
  startDiscoveryRun,
  stopDiscoveryRun,
  type DiscoveryRun,
  type DiscoveryScope,
} from "../../api/admin";

/** How long after one reading of a running run the next is made. */
export const RUN_POLL_MS = 1000;

export interface DiscoveryRunState {
  /** The run the dialog shows: one still going, or the one last watched, until a new one is set
   *  up. Null: the dialog sets up a run. */
  run: DiscoveryRun | null;
  running: boolean;
  /** Reading the run failed. */
  error: string;
  /** Read the latest run from the server: one still going is shown. */
  refresh: () => Promise<void>;
  /** Start a run; a refusal is thrown with the server's words. */
  start: (scope: DiscoveryScope) => Promise<void>;
  /** Stop after the request in flight; what was found is kept. */
  stop: () => Promise<void>;
  /** Leave the ended run's result and set up a new run. */
  dismiss: () => void;
  /** The fields a new run starts from: those of the run just left, else of the latest run the
   *  signed-in person started; null where there is none (the defaults). */
  previous: DiscoveryScope | null;
}

const errorText = (e: unknown) => (e instanceof Error ? e.message : String(e));

export function useDiscoveryRun(onStored: () => void): DiscoveryRunState {
  const [run, setRun] = useState<DiscoveryRun | null>(null);
  const [error, setError] = useState("");
  const [previous, setPrevious] = useState<DiscoveryScope | null>(null);
  const stored = useRef(onStored);
  useEffect(() => {
    stored.current = onStored;
  }, [onStored]);

  const refresh = useCallback(async () => {
    try {
      const [[latest], [own]] = await Promise.all([
        fetchDiscoveryRuns(1),
        fetchDiscoveryRuns(1, true),
      ]);
      setError("");
      if (own !== undefined) setPrevious(own.scope);
      // An ended run is shown only while it is the one being watched; a run still going always.
      // The server lists no run: what is shown stays (nothing, before any run).
      setRun((shown) =>
        latest === undefined
          ? shown
          : latest.status === "running" || shown?.id === latest.id
            ? latest
            : shown,
      );
    } catch (e) {
      setError(errorText(e));
    }
  }, []);

  useEffect(() => {
    // eslint-disable-next-line react-hooks/set-state-in-effect -- mount read of server state
    void refresh();
  }, [refresh]);

  const runId = run?.id;
  const running = run?.status === "running";
  useEffect(() => {
    if (runId === undefined || !running) return;
    let live = true;
    const seen = { current: -1 };
    let timer: ReturnType<typeof setTimeout>;
    const next = () => {
      timer = setTimeout(async () => {
        try {
          const read = await fetchDiscoveryRun(runId);
          if (!live) return;
          setError("");
          setRun(read);
          if (seen.current !== read.suggestions || read.status !== "running") stored.current();
          seen.current = read.suggestions;
          if (read.status === "running") next();
        } catch (e) {
          if (!live) return;
          setError(errorText(e));
          next();
        }
      }, RUN_POLL_MS);
    };
    next();
    return () => {
      live = false;
      clearTimeout(timer);
    };
  }, [runId, running]);

  const start = useCallback(async (scope: DiscoveryScope) => {
    const started = await startDiscoveryRun(scope);
    setError("");
    setPrevious(started.scope);
    setRun(started);
  }, []);

  const stop = useCallback(async () => {
    if (runId === undefined) return;
    try {
      setRun(await stopDiscoveryRun(runId));
    } catch (e) {
      // It ended on its own meanwhile: show how it ended.
      if (e instanceof DiscoveryError && e.code === "discovery.run_ended") {
        setRun(await fetchDiscoveryRun(runId));
        return;
      }
      setError(errorText(e));
    }
  }, [runId]);

  const dismiss = useCallback(() => {
    setRun((shown) => (shown?.status === "running" ? shown : null));
  }, []);

  return { run, running, error, refresh, start, stop, dismiss, previous };
}
