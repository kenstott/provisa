// Copyright (c) 2026 Kenneth Stott
// Canary: 3f8b2c61-7d04-4e9a-a1c5-6e2d9b0f4a87
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-413: Accept all and Reject all. One request handles the whole list, each suggestion as its
// row's own action would; what could not be done is named, suggestion by suggestion.

import { useCallback, useState } from "react";
import { useTranslation } from "react-i18next";
import { acceptCandidates, rejectCandidates, type CandidateOutcome } from "../../api/admin";
import { candidateName, REJECT_REASON, type Candidate } from "./relationship-types";

/** The suggestions an Accept all or Reject all could not handle, each by name with its reason. */
export interface BulkFailures {
  heading: string;
  lines: string[];
}

interface Surroundings {
  candidates: Candidate[];
  tableNameById: Record<string, string>;
  /** A new action starts: what the page said of the last one is cleared. */
  onStart: () => void;
  onMessage: (message: string) => void;
  onError: (message: string) => void;
  /** Some were accepted: the registered relationships and the suggestions are read again. */
  onAccepted: () => Promise<void>;
  /** These were rejected: they leave the list. */
  onRejected: (ids: Set<number>) => void;
}

type Failed = Extract<CandidateOutcome, { ok: false }>;

export function useCandidatesAtOnce(page: Surroundings) {
  const { t } = useTranslation();
  const [failures, setFailures] = useState<BulkFailures | null>(null);

  const report = (outcomes: CandidateOutcome[], done: string, failed: string) => {
    const missed = outcomes.filter((o): o is Failed => !o.ok);
    const nameById = new Map(
      page.candidates.map((c) => [c.id, candidateName(c, page.tableNameById)]),
    );
    page.onMessage(t(done, { count: outcomes.length - missed.length }));
    setFailures(
      missed.length === 0
        ? null
        : {
            heading: t(failed, { count: missed.length }),
            lines: missed.map((f) =>
              t("candidatesTable.failedOne", {
                name: nameById.get(f.id) ?? String(f.id),
                error: f.error,
              }),
            ),
          },
    );
  };

  const run = async (act: () => Promise<void>) => {
    page.onStart();
    setFailures(null);
    try {
      await act();
    } catch (e) {
      page.onError(e instanceof Error ? e.message : String(e));
    }
  };

  const acceptAll = (items: { id: number; name: string }[]) =>
    run(async () => {
      const outcomes = await acceptCandidates(items);
      report(outcomes, "candidatesTable.acceptedAll", "candidatesTable.acceptFailed");
      await page.onAccepted();
    });

  const rejectAll = (ids: number[]) =>
    run(async () => {
      const outcomes = await rejectCandidates(ids, REJECT_REASON);
      report(outcomes, "candidatesTable.rejectedAll", "candidatesTable.rejectFailed");
      page.onRejected(new Set(outcomes.filter((o) => o.ok).map((o) => o.id)));
    });

  /** Cleared when something else is reported in its place. */
  const clear = useCallback(() => setFailures(null), []);

  return { failures, acceptAll, rejectAll, clear };
}
