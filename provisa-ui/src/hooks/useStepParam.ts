// Copyright (c) 2026 Kenneth Stott
// Canary: 5d1c8e37-b0a4-4f92-8c6e-27a9f3d41e85
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2277: a wizard step in the URL.

import { useSearchParams } from "react-router-dom";

/**
 * The step in the URL (REQ-2277): `?step=n`, 1-based, one history entry per step so Back and a
 * reload land on the same step. `reachable` is the last step whose earlier steps are answered, so a
 * link past it opens at it.
 */
export function useStepParam(count: number, reachable: number): [number, (step: number) => void] {
  const [params, setParams] = useSearchParams();
  const raw = Number(params.get("step"));
  const requested = Number.isInteger(raw) && raw >= 1 ? raw - 1 : 0;
  const step = Math.min(requested, reachable, count - 1);
  const setStep = (index: number) =>
    setParams(
      (prev) => {
        const next = new URLSearchParams(prev);
        next.set("step", String(index + 1));
        return next;
      },
      { replace: false },
    );
  return [step, setStep];
}

/** The last step whose earlier steps are all answered: where a link to a later step opens. */
export function reachableStep(
  ids: readonly string[],
  gapOf: (id: string) => string | null,
): number {
  const open = ids.findIndex((id, i) => i < ids.length - 1 && gapOf(id) !== null);
  return open === -1 ? ids.length - 1 : open;
}
