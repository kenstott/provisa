// Copyright (c) 2026 Kenneth Stott
// Canary: 6b1f4d92-3a7e-4c58-9e20-d41a8c5f7b36
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-413: a discovery run is the server's. The page reads the latest runs when it mounts and
// when the dialog opens, so a run still going is shown, and its button animates, after the dialog
// is closed or sent to the background, or the page is left and come back to. Each run going is
// read again about once a second; the suggestions are read again whenever a run has stored more.
// The latest run is the one the page's line is about: going, with its progress, or ended, with
// how many of its suggestions wait for a decision.

import { useCallback, useEffect, useRef, useState } from "react";
import {
  DiscoveryError,
  fetchDiscoveryRun,
  fetchDiscoveryRuns,
  fetchRunProposals,
  startDiscoveryRun,
  stopDiscoveryRun,
  type DiscoveryRun,
  type DiscoveryScope,
  type RunProposal,
} from "../../api/admin";

/** How long after one reading of the runs going the next is made. */
export const RUN_POLL_MS = 1000;

/** How many of the latest runs are read for the runs going. A run is given up on when the server
 *  it ran on is restarted, so the runs going are among the latest. */
export const RUNS_READ = 20;

/** The suggestions of a run waiting for a decision: its undecided proposals that are its
 *  suggestions (none of its rules left them out, or one waits from an earlier run). */
export function waitingCount(proposals: readonly RunProposal[]): number {
  return proposals.filter((p) => p.decidable && (p.reason === null || p.reason === "pending"))
    .length;
}

export interface DiscoveryRunState {
  /** The run the dialog shows: one still going, or the one last watched or opened, until a new
   *  one is set up. Null: the dialog sets up a run. */
  run: DiscoveryRun | null;
  /** Every run going, the newest first. */
  going: DiscoveryRun[];
  /** The latest run: the one the page's line is about. Null before any. */
  latest: DiscoveryRun | null;
  /** How many of the ended latest run's suggestions wait for a decision; null while it is going
   *  or not yet counted. */
  waiting: number | null;
  running: boolean;
  /** Reading the run failed. */
  error: string;
  /** Read the latest runs from the server: one still going is shown. */
  refresh: () => Promise<void>;
  /** Start a run; a refusal is thrown with the server's words. */
  start: (scope: DiscoveryScope) => Promise<void>;
  /** Stop the run after the request in flight; what was found is kept. */
  stop: (runId: number) => Promise<void>;
  /** Show the latest run in the dialog. */
  openLatest: () => void;
  /** Leave the ended run's result and set up a new run. */
  dismiss: () => void;
}

const errorText = (e: unknown) => (e instanceof Error ? e.message : String(e));

/** ``runs`` with ``read`` in its place: added where it is going, dropped once it has ended. */
function withGoing(runs: DiscoveryRun[], read: DiscoveryRun): DiscoveryRun[] {
  const others = runs.filter((r) => r.id !== read.id);
  return read.status === "running" ? [...others, read].sort((a, b) => b.id - a.id) : others;
}

export function useDiscoveryRun(onStored: () => void): DiscoveryRunState {
  const [run, setRun] = useState<DiscoveryRun | null>(null);
  const [going, setGoing] = useState<DiscoveryRun[]>([]);
  const [latest, setLatest] = useState<DiscoveryRun | null>(null);
  const [waiting, setWaiting] = useState<{ runId: number; count: number } | null>(null);
  const [error, setError] = useState("");
  // How many times the runs were read from the server: each reading counts the waiting again, so
  // a decision made in the dialog shows on the page's line.
  const [reads, setReads] = useState(0);
  const stored = useRef(onStored);
  useEffect(() => {
    stored.current = onStored;
  }, [onStored]);

  /** A reading of one run: wherever it is shown, it is shown as read. */
  const seen = useCallback((read: DiscoveryRun) => {
    setGoing((runs) => withGoing(runs, read));
    setLatest((shown) => (shown === null || read.id >= shown.id ? read : shown));
    setRun((shown) => (shown?.id === read.id ? read : shown));
  }, []);

  const count = useCallback(async (runId: number) => {
    try {
      const read = await fetchRunProposals(runId);
      setWaiting({ runId, count: waitingCount(read.proposals) });
    } catch (e) {
      setError(errorText(e));
    }
  }, []);

  const refresh = useCallback(async () => {
    try {
      const listed = await fetchDiscoveryRuns(RUNS_READ);
      setError("");
      const live = listed.filter((r) => r.status === "running");
      const [newest] = listed;
      // A run started after the listing was made is not in it: it stays among the runs going.
      setGoing((known) => [
        ...known.filter((r) => newest === undefined || r.id > newest.id),
        ...live,
      ]);
      // The server lists no run: what is shown stays (nothing, before any run).
      if (newest === undefined) return;
      setLatest(newest);
      // A run still going is shown; an ended run only while it is the one being watched.
      setRun((shown) => {
        const again = shown === null ? undefined : listed.find((r) => r.id === shown.id);
        if (again !== undefined && (again.status === "running" || live.length === 0)) return again;
        return live[0] ?? shown;
      });
      setReads((n) => n + 1);
    } catch (e) {
      setError(errorText(e));
    }
  }, []);

  useEffect(() => {
    // eslint-disable-next-line react-hooks/set-state-in-effect -- mount read of server state
    void refresh();
  }, [refresh]);

  // Each run going is read again; one that has ended is read once more and leaves the list.
  const goingIds = going.map((r) => r.id).join(",");
  useEffect(() => {
    if (goingIds === "") return;
    const ids = goingIds.split(",").map(Number);
    let live = true;
    const seenSuggestions = new Map<number, number>();
    let timer: ReturnType<typeof setTimeout>;
    const next = () => {
      timer = setTimeout(async () => {
        const reads = await Promise.allSettled(ids.map((id) => fetchDiscoveryRun(id)));
        if (!live) return;
        let still = false;
        const failed = reads.find((r) => r.status === "rejected");
        setError(failed ? errorText(failed.reason) : "");
        for (const [i, r] of reads.entries()) {
          if (r.status === "rejected") {
            still = true;
            continue;
          }
          const read = r.value;
          seen(read);
          if (seenSuggestions.get(ids[i]) !== read.suggestions || read.status !== "running") {
            stored.current();
          }
          seenSuggestions.set(ids[i], read.suggestions);
          if (read.status === "running") still = true;
        }
        if (still) next();
      }, RUN_POLL_MS);
    };
    next();
    return () => {
      live = false;
      clearTimeout(timer);
    };
  }, [goingIds, seen]);

  // The latest run has ended: how many of its suggestions wait is counted.
  const endedLatest = latest !== null && latest.status !== "running" ? latest : null;
  const endedId = endedLatest?.id;
  useEffect(() => {
    if (endedId === undefined) return;
    // eslint-disable-next-line react-hooks/set-state-in-effect -- read of server state, set after the response
    void count(endedId);
  }, [endedId, reads, count]);

  const start = useCallback(async (scope: DiscoveryScope) => {
    const started = await startDiscoveryRun(scope);
    setError("");
    setRun(started);
    setGoing((runs) => withGoing(runs, started));
    setLatest(started);
  }, []);

  const stop = useCallback(
    async (runId: number) => {
      try {
        seen(await stopDiscoveryRun(runId));
      } catch (e) {
        // It ended on its own meanwhile: show how it ended.
        if (e instanceof DiscoveryError && e.code === "discovery.run_ended") {
          seen(await fetchDiscoveryRun(runId));
          return;
        }
        setError(errorText(e));
      }
    },
    [seen],
  );

  const openLatest = useCallback(() => {
    setRun(latest);
  }, [latest]);

  const dismiss = useCallback(() => {
    setRun((shown) => (shown?.status === "running" ? shown : null));
  }, []);

  return {
    run,
    going,
    latest,
    waiting: endedLatest !== null && waiting?.runId === endedLatest.id ? waiting.count : null,
    running: going.length > 0,
    error,
    refresh,
    start,
    stop,
    openLatest,
    dismiss,
  };
}
