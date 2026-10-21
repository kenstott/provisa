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
import { TOUR_SCOPES, TOUR_STEPS, type TourStep } from "../tour/tourSteps";
import { runUiActions } from "../mcp/uiActions";

export function tourStep(key: string): TourStep {
  const step = TOUR_STEPS.find((s) => s.key === key);
  if (step === undefined) throw new Error(`no tour step ${key}`);
  return step;
}

/** The page step ``key`` runs on: its own route, or, routeless, the one the steps before it in its
 * scope left the tour on. */
export function stepRoute(key: string): string {
  const keys = Object.values(TOUR_SCOPES).find((scope) => scope.includes(key));
  if (keys === undefined) throw new Error(`tour step ${key} is in no scope`);
  for (let i = keys.indexOf(key); i >= 0; i--) {
    const route = tourStep(keys[i]).route;
    if (route !== undefined) return route;
  }
  throw new Error(`tour step ${key} runs on no route`);
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
  // REQ-2115: a step that acts first does so unless what it builds is on screen already.
  if (step.actions && !document.querySelector(step.element)) {
    const acted = await runUiActions(step.actions, {
      confirm: async () => true,
      paceMs: 0,
      waitMs: 3000,
    });
    if (!acted.success) throw new Error(`${key} actions: ${acted.steps.join(" ")}`);
  }
  if (step.readySelector) await found(step.readySelector, `${key} readySelector`);
  return found(step.element, `${key} anchor`);
}

/** Step ``key``'s page ready on the page rendered now: its readySelector, for a step whose anchor is
 * the app's shell (tourAnchors.nav) and whose page gates it. */
export async function stepPageReady(key: string): Promise<HTMLElement> {
  const step = tourStep(key);
  if (!step.readySelector) throw new Error(`tour step ${key} has no readySelector`);
  return found(step.readySelector, `${key} readySelector`);
}

/** Steps ``keys`` in order on the page rendered now, each left by its Next (clickAfterNext) as the
 * runner leaves it, for a step that starts from where the one before it left the page. */
export async function walkTourSteps(keys: readonly string[]): Promise<void> {
  for (const key of keys) {
    await walkTourStep(key);
    const after = tourStep(key).clickAfterNext;
    if (after) fireEvent.click(await found(after, `${key} clickAfterNext`));
  }
}
