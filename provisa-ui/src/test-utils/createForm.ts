// Copyright (c) 2026 Kenneth Stott
// Canary: c8d9b2b4-b437-4fb9-8a8c-e2657de106c6
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The behavioural half of "+ Object opens empty" (the guard is
// components/list/__tests__/panelDuplicateGuard.test.ts): a page's create form, opened after an
// object was selected or edited, holds none of that object's values. A page's test selects an
// object (and edits it and cancels, in a second case), presses "+ Object", and calls
// `expectHoldsNoneOf(form, valuesOf(object))` on the form it opened. The values passed are the
// object's own, distinctive values (its id, host, names), not defaults any new object has.

import { expect } from "vitest";

/** What each field of `root` holds, by its label (or its test id, or its name). */
export function formValues(root: HTMLElement): Record<string, string> {
  const held: Record<string, string> = {};
  const fields = root.querySelectorAll<HTMLInputElement | HTMLSelectElement | HTMLTextAreaElement>(
    "input, select, textarea",
  );
  fields.forEach((field, i) => {
    if (field instanceof HTMLInputElement && ["checkbox", "radio", "hidden"].includes(field.type))
      return;
    const label =
      (field.labels && field.labels[0]?.textContent?.trim()) ||
      field.getAttribute("aria-label") ||
      field.getAttribute("data-testid") ||
      field.getAttribute("name") ||
      `field ${i + 1}`;
    held[label] = field.value;
  });
  return held;
}

/** Fails naming each field of `root` that holds one of `objectValues`. */
export function expectHoldsNoneOf(root: HTMLElement, objectValues: readonly string[]): void {
  const carried = Object.entries(formValues(root)).filter(
    ([, value]) => value !== "" && objectValues.includes(value),
  );
  expect(
    carried,
    "the create form holds values of the object selected or edited before it opened",
  ).toEqual([]);
}
