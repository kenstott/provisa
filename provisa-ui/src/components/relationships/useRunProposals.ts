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
// score is at the threshold or above start ticked to accept; Apply sends both lists.

import { useCallback, useEffect, useMemo, useState } from "react";
import {
  applyRunDecisions,
  fetchRunProposals,
  type DecisionResult,
  type DiscoveryRun,
  type RunDecisions,
  type RunProposal,
  type RunProposals,
} from "../../api/admin";

/** Whether a proposal starts ticked to accept: it can be decided, the run's rules removed it
 *  for nothing but already waiting from an earlier run, and the model's score is at the
 *  threshold. Jev's score is shown beside it and does not decide (REQ-1789). */
export function startsAccepted(p: RunProposal, threshold: number): boolean {
  return (
    p.decidable &&
    (p.reason === null || p.reason === "pending") &&
    p.confidence !== null &&
    p.confidence >= threshold
  );
}

export interface RunProposalsState {
  /** The undecided proposals shown, the highest score first; null until read. */
  proposals: RunProposal[] | null;
  threshold: number;
  accept: ReadonlySet<number>;
  notAgain: ReadonlySet<number>;
  toggleAccept: (id: number) => void;
  toggleNotAgain: (id: number) => void;
  /** Every row that can be decided ticked to accept, or none where all already are. */
  toggleAcceptAll: () => void;
  /** Send what is ticked, each accepted proposal under the name ``nameOf`` gives it. */
  apply: (nameOf: (p: RunProposal) => string) => Promise<void>;
  applying: boolean;
  /** What the last Apply could not do, by proposal. */
  failures: { proposal: RunProposal; error: string }[];
  /** How many the last Apply accepted and marked; null before any. */
  applied: { accepted: number; notAgain: number } | null;
  error: string;
  /** Read the proposals again: what was decided elsewhere leaves, what was stored joins. */
  reload: () => Promise<void>;
}

/** Where a decision table's proposals come from and where its decisions go. ``key`` names
 *  what is shown: a different key is a different table, read afresh. */
export interface ProposalSource {
  key: string;
  read: () => Promise<RunProposals>;
  send: (decisions: RunDecisions) => Promise<DecisionResult[]>;
}

const errorText = (e: unknown) => (e instanceof Error ? e.message : String(e));

/** An ended run's result: its undecided proposals, decided through the run. */
export function useRunProposals(
  run: DiscoveryRun | null,
  /** Told after an Apply that decided something: relationships and suggestions have changed. */
  onApplied: () => void,
): RunProposalsState {
  const runId = run?.id;
  const ended = run !== null && run.status !== "running";
  const source = useMemo<ProposalSource | null>(
    () =>
      runId === undefined || !ended
        ? null
        : {
            key: `run:${runId}`,
            read: () => fetchRunProposals(runId),
            send: (decisions) => applyRunDecisions(runId, decisions),
          },
    [runId, ended],
  );
  return useProposalDecisions(source, onApplied);
}

/** A decision table over ``source``'s proposals (REQ-413): the dialog's result of a run, and the
 *  Relationships page's waiting suggestions, decided the one way. Null: nothing is shown. */
export function useProposalDecisions(
  source: ProposalSource | null,
  /** Told after an Apply that decided something: relationships and suggestions have changed. */
  onApplied: () => void,
): RunProposalsState {
  const [read, setRead] = useState<{
    key: string;
    proposals: RunProposal[];
    threshold: number;
  } | null>(null);
  const [accept, setAccept] = useState<ReadonlySet<number>>(new Set());
  const [notAgain, setNotAgain] = useState<ReadonlySet<number>>(new Set());
  const [applying, setApplying] = useState(false);
  const [failures, setFailures] = useState<RunProposalsState["failures"]>([]);
  const [applied, setApplied] = useState<RunProposalsState["applied"]>(null);
  const [error, setError] = useState("");

  const load = useCallback(async (from: ProposalSource) => {
    const { proposals, threshold } = await from.read();
    setRead({ key: from.key, proposals, threshold });
    setAccept(new Set(proposals.filter((p) => startsAccepted(p, threshold)).map((p) => p.id)));
    setNotAgain(new Set());
  }, []);

  useEffect(() => {
    if (source === null) return;
    let live = true;
    // eslint-disable-next-line react-hooks/set-state-in-effect -- a different table's result
    setFailures([]);
    setApplied(null);
    load(source)
      .then(() => live && setError(""))
      .catch((e) => live && setError(errorText(e)));
    return () => {
      live = false;
    };
  }, [source, load]);

  const reload = useCallback(async () => {
    if (source === null) return;
    try {
      await load(source);
      setError("");
    } catch (e) {
      setError(errorText(e));
    }
  }, [source, load]);

  const shown = read !== null && source !== null && read.key === source.key ? read : null;
  const decidable = useMemo(
    () => (shown ? shown.proposals.filter((p) => p.decidable).map((p) => p.id) : []),
    [shown],
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
  const toggleAcceptAll = useCallback(() => {
    const all = decidable.length > 0 && decidable.every((id) => accept.has(id));
    setAccept(all ? new Set() : new Set(decidable));
    if (!all) setNotAgain(new Set());
  }, [decidable, accept]);

  const apply = useCallback(
    async (nameOf: (p: RunProposal) => string) => {
      if (!shown || source === null) return;
      const byId = new Map(shown.proposals.map((p) => [p.id, p]));
      const chosen = (ids: ReadonlySet<number>) =>
        shown.proposals.filter((p) => p.decidable && ids.has(p.id));
      setApplying(true);
      setError("");
      try {
        const results: DecisionResult[] = await source.send({
          accept: chosen(accept).map((p) => ({ id: p.id, name: nameOf(p) })),
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
        await load(source);
        if (results.some((r) => r.ok)) onApplied();
      } catch (e) {
        setError(errorText(e));
      } finally {
        setApplying(false);
      }
    },
    [shown, source, accept, notAgain, load, onApplied],
  );

  return {
    proposals: shown ? shown.proposals : null,
    threshold: shown ? shown.threshold : 0,
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
    reload,
  };
}
