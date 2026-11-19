// Copyright (c) 2026 Kenneth Stott
// Canary: 1f8b3d76-9a2c-4e50-b4d1-0c7e5a9f3b68
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-413: every proposal an ended discovery run read, and what the reviewer has chosen of them.
// A row is ticked to accept, ticked not to be recommended again, or neither: left alone it stays
// undecided and nothing is recorded against it. The two ticks exclude each other. Rows whose
// score is at the threshold or above start ticked to accept; Apply sends both lists. The
// threshold is the signed-in person's own: set here, it is kept for them and re-ticks the rows.
// The rows are filtered on their Score, Score >= 10% until the person sets another filter or
// clears it, which is kept for them as the threshold is; Accept and the select-all act only on the
// rows the filter shows (REQ-413, amended 2026-11-18).

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  applyRunDecisions,
  fetchRunProposals,
  saveDiscoveryScoreFilter,
  saveDiscoveryThreshold,
  type ScoreFilter,
  type DecisionResult,
  type DiscoveryRun,
  type RunProposal,
} from "../../api/admin";
import { matchesFilter } from "../../pages/sql/columnFilter";

/** A row's Score as its cell shows it, in whole percent: what the Score filter is against. */
export const scorePercent = (p: RunProposal): number | null =>
  p.score === null ? null : Number((p.score * 100).toFixed(0));

/** Whether the Score filter shows a row; every row with none. */
export function scoreShown(p: RunProposal, filter: ScoreFilter | null): boolean {
  return filter === null || matchesFilter("number", filter, scorePercent(p));
}

/** Whether a proposal starts ticked to accept: it can be accepted, the run's rules removed it
 *  for nothing but its score or already waiting from an earlier run, and its score (the
 *  measure's where its values were compared, else the model's) is at the person's threshold.
 *  Jev's score is shown beside it and does not decide (REQ-1789). */
export function startsAccepted(p: RunProposal, threshold: number): boolean {
  return (
    p.acceptable &&
    (p.reason === null || p.reason === "below_threshold") &&
    p.score !== null &&
    p.score >= threshold
  );
}

const ticked = (proposals: RunProposal[], threshold: number) =>
  new Set(proposals.filter((p) => startsAccepted(p, threshold)).map((p) => p.id));

export interface RunProposalsState {
  /** The shown run's undecided proposals, the highest score first; null until read. */
  proposals: RunProposal[] | null;
  /** Those of them the Score filter shows. */
  shown: RunProposal[];
  /** The person's filter on the Score column; null when cleared. */
  scoreFilter: ScoreFilter | null;
  /** Set the Score filter (null clears it): kept for the person's next runs. */
  setScoreFilter: (filter: ScoreFilter | null) => void;
  threshold: number;
  /** The score the run kept a suggestion at, which a row's below_threshold reason is against. */
  keptAt: number;
  /** Set the person's threshold: kept for them, and the rows re-ticked by it. */
  setThreshold: (threshold: number) => void;
  accept: ReadonlySet<number>;
  notAgain: ReadonlySet<number>;
  toggleAccept: (id: number) => void;
  toggleNotAgain: (id: number) => void;
  /** Every listed row that can be accepted ticked, or none where all already are. The list
   *  names its rows: those the Score filter and its other column filters show. */
  toggleAcceptAll: (listed: readonly number[]) => void;
  /** Send what is ticked. No accepted proposal carries a name: nobody types one in the run's
   *  result, so the server names each and tells apart a name another relationship holds
   *  (REQ-415, amended 2026-11-16). */
  apply: (listed: ReadonlySet<number>) => Promise<void>;
  applying: boolean;
  /** What the last Apply could not do, by proposal. */
  failures: { proposal: RunProposal; error: string }[];
  /** How many the last Apply accepted and marked; null before any. */
  applied: { accepted: number; notAgain: number } | null;
  error: string;
}

/** How long the threshold rests before it is kept: typing 65 keeps 65, not 6 and then 65. */
export const KEEP_THRESHOLD_AFTER_MS = 400;

const errorText = (e: unknown) => (e instanceof Error ? e.message : String(e));

export function useRunProposals(
  run: DiscoveryRun | null,
  /** Told after an Apply that decided something: relationships and suggestions have changed. */
  onApplied: () => void,
): RunProposalsState {
  const [read, setRead] = useState<{
    runId: number;
    proposals: RunProposal[];
    threshold: number;
    keptAt: number;
    scoreFilter: ScoreFilter | null;
  } | null>(null);
  const [accept, setAccept] = useState<ReadonlySet<number>>(new Set());
  const [notAgain, setNotAgain] = useState<ReadonlySet<number>>(new Set());
  const [applying, setApplying] = useState(false);
  const [failures, setFailures] = useState<RunProposalsState["failures"]>([]);
  const [applied, setApplied] = useState<RunProposalsState["applied"]>(null);
  const [error, setError] = useState("");
  const keepTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const runId = run?.id;
  const ended = run !== null && run.status !== "running";

  const load = useCallback(async (id: number) => {
    const { proposals, threshold, kept_at: keptAt, score_filter } = await fetchRunProposals(id);
    setRead({ runId: id, proposals, threshold, keptAt, scoreFilter: score_filter });
    setAccept(ticked(proposals, threshold));
    setNotAgain(new Set());
  }, []);

  useEffect(() => {
    if (runId === undefined || !ended) return;
    let live = true;
    // eslint-disable-next-line react-hooks/set-state-in-effect -- a different run's result
    setFailures([]);
    setApplied(null);
    load(runId)
      .then(() => live && setError(""))
      .catch((e) => live && setError(errorText(e)));
    return () => {
      live = false;
    };
  }, [runId, ended, load]);

  const shown = read !== null && read.runId === runId && ended ? read : null;
  const visible = useMemo(
    () => (shown ? shown.proposals.filter((p) => scoreShown(p, shown.scoreFilter)) : []),
    [shown],
  );

  const setScoreFilter = useCallback(
    (scoreFilter: ScoreFilter | null) => {
      if (!shown) return;
      setRead({ ...shown, scoreFilter });
      saveDiscoveryScoreFilter(scoreFilter).catch((e) => setError(errorText(e)));
    },
    [shown],
  );

  const setThreshold = useCallback(
    (threshold: number) => {
      if (!shown) return;
      setRead({ ...shown, threshold });
      // The rows start again from the new threshold; a row marked not to be recommended again
      // keeps its mark and is not ticked to accept.
      setAccept(new Set([...ticked(shown.proposals, threshold)].filter((id) => !notAgain.has(id))));
      if (keepTimer.current !== null) clearTimeout(keepTimer.current);
      keepTimer.current = setTimeout(() => {
        keepTimer.current = null;
        saveDiscoveryThreshold(threshold).catch((e) => setError(errorText(e)));
      }, KEEP_THRESHOLD_AFTER_MS);
    },
    [shown, notAgain],
  );

  const flip = (id: number, into: typeof setAccept, outOf: typeof setAccept) => {
    into((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
    outOf((prev) => {
      if (!prev.has(id)) return prev;
      const next = new Set(prev);
      next.delete(id);
      return next;
    });
  };
  const toggleAccept = useCallback((id: number) => flip(id, setAccept, setNotAgain), []);
  const toggleNotAgain = useCallback((id: number) => flip(id, setNotAgain, setAccept), []);
  const toggleAcceptAll = useCallback(
    (acceptable: readonly number[]) => {
      const all = acceptable.length > 0 && acceptable.every((id) => accept.has(id));
      setAccept(all ? new Set() : new Set(acceptable));
      if (!all) setNotAgain((prev) => new Set([...prev].filter((id) => !acceptable.includes(id))));
    },
    [accept],
  );

  const apply = useCallback(
    async (listed: ReadonlySet<number>) => {
    if (!shown) return;
    const byId = new Map(shown.proposals.map((p) => [p.id, p]));
    const chosen = (ids: ReadonlySet<number>) =>
      shown.proposals.filter((p) => p.decidable && ids.has(p.id));
    setApplying(true);
    setError("");
    try {
      const results: DecisionResult[] = await applyRunDecisions(shown.runId, {
        accept: chosen(accept)
          .filter((p) => p.acceptable && listed.has(p.id))
          .map((p) => ({ id: p.id })),
        not_again: chosen(notAgain).map((p) => p.id),
      });
      setFailures(
        results.flatMap((r) => {
          const proposal = byId.get(r.id);
          return !r.ok && proposal ? [{ proposal, error: r.error ?? "" }] : [];
        }),
      );
      const ok = (action: DecisionResult["action"]) =>
        results.filter((r) => r.ok && r.action === action).length;
      setApplied({ accepted: ok("accept"), notAgain: ok("not_again") });
      await load(shown.runId);
      if (results.some((r) => r.ok)) onApplied();
    } catch (e) {
      setError(errorText(e));
    } finally {
      setApplying(false);
    }
  }, [shown, accept, notAgain, load, onApplied]);

  return {
    proposals: shown ? shown.proposals : null,
    shown: visible,
    scoreFilter: shown ? shown.scoreFilter : null,
    setScoreFilter,
    threshold: shown ? shown.threshold : 0,
    keptAt: shown ? shown.keptAt : 0,
    setThreshold,
    accept,
    notAgain,
    toggleAccept,
    toggleNotAgain,
    toggleAcceptAll,
    apply,
    applying,
    failures,
    applied,
    error,
  };
}
