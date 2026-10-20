// Copyright (c) 2026 Kenneth Stott
// Canary: 4fad6d50-f022-41b2-b6fc-9998fecd2f1e
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A tour step run against its page as the tour's runner (useTour) runs it: each ensureOpen click
// made unless what it opens is there (then what it opens awaited), its clickBefore made, its page
// ready (its readySelector), then its anchor found. A step whose anchor a page does not paint in one of its states waits out
// the runner's whole window on a visitor's screen; run here, it fails by the step's key.

import { fireEvent, waitFor } from "@testing-library/react";
import { TOUR_STEPS, type TourStep } from "../tour/tourSteps";

export function tourStep(key: string): TourStep {
  const step = TOUR_STEPS.find((s) => s.key === key);
  if (step === undefined) throw new Error(`no tour step ${key}`);
  return step;
}

async function found(selector: string, what: string): Promise<HTMLElement> {
  return waitFor(() => {
    const el = document.querySelector<HTMLElement>(selector);
    if (!el) throw new Error(`${what}: ${selector} is not on the page`);
    return el;
  });
}

/** Run step ``key`` on the page rendered now; its anchor element. */
export async function walkTourStep(key: string): Promise<HTMLElement> {
  const step = tourStep(key);
  for (const opener of step.ensureOpen ?? []) {
    if (document.querySelector(opener.unlessPresent)) continue;
    fireEvent.click(await found(opener.click, `${key} ensureOpen`));
    await found(opener.unlessPresent, `${key} ensureOpen opened`);
  }
  if (step.clickBefore) fireEvent.click(await found(step.clickBefore, `${key} clickBefore`));
  if (step.readySelector) await found(step.readySelector, `${key} readySelector`);
  return found(step.element, `${key} anchor`);
}
