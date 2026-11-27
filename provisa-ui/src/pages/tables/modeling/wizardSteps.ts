// Copyright (c) 2026 Kenneth Stott
// Canary: 0a0a27b8-15f0-4635-b0db-d1b6cdda9099
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2199 (amended 2026-11-27) / REQ-2277: where a modeling wizard is. The step on screen is in
// the URL (useStepParam); the progress is the furthest step reached, so going back to an earlier
// step -- with Back or from the strip -- keeps the strip's progress, and a step reached before
// opens again from the strip. Next checks the step's answers; Skip step, offered only on a step no
// later step reads, drops that step's answers and moves on.

import { useState } from "react";
import { reachableStep, useStepParam } from "../../../hooks/useStepParam";

export interface WizardSteps {
  /** The step on screen. */
  step: number;
  /** The furthest step reached: the strip's current step. */
  progress: number;
  /** Open a step: one before the progress, or the progress itself. */
  open: (index: number) => void;
  /** The step's answers are complete: on to the next step. Otherwise the gap, to show. */
  next: () => string | null;
  back: () => void;
  /** Skip step: the caller has dropped the step's answers; on to the next step. */
  skip: () => void;
}

export function useWizardSteps<Id extends string>(
  ids: readonly Id[],
  gapOf: (id: Id) => string | null,
): WizardSteps {
  const reachable = reachableStep(ids, (id) => gapOf(id as Id));
  const [step, setStep] = useStepParam(ids.length, reachable);
  const [reached, setReached] = useState(step);
  // An answer changed on an earlier step can leave a later one unanswered: the progress is never
  // past the last step whose earlier steps are answered.
  const progress = Math.min(Math.max(reached, step), reachable);
  const go = (index: number) => {
    setReached((r) => Math.max(r, index));
    setStep(index);
  };
  return {
    step,
    progress,
    open: (index) => {
      if (index !== step && index <= progress) go(index);
    },
    next: () => {
      const gap = gapOf(ids[step]);
      if (gap === null) go(step + 1);
      return gap;
    },
    back: () => setStep(step - 1),
    skip: () => go(step + 1),
  };
}
