// Copyright (c) 2026 Kenneth Stott
// Canary: c0c6b0cc-f9ba-4c43-a361-b831d3088363
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import { test as base, expect, type Page } from "playwright/test";
import AxeBuilder from "@axe-core/playwright";

export { expect } from "playwright/test";

// WCAG 2.1 Level AA gate (REQ-1013, REQ-1014). `expectNoA11yViolations` runs
// axe-core against the current page state, scoped to the tags that constitute
// AA conformance, and fails the test with a readable rule/target breakdown.
export async function expectNoA11yViolations(page: Page, context?: string) {
  const results = await new AxeBuilder({ page })
    .withTags(["wcag2a", "wcag2aa", "wcag21a", "wcag21aa"])
    .analyze();
  const summary = results.violations
    .map(
      (v) =>
        `${v.id} (${v.impact}): ${v.help} [${v.nodes
          .map((n) => n.target.join(" "))
          .join(", ")}]`,
    )
    .join("\n");
  expect(
    results.violations,
    `${context ? context + " — " : ""}axe found ${results.violations.length} accessibility violation(s):\n${summary}`,
  ).toEqual([]);
}

export const test = base.extend<{ expectNoA11yViolations: typeof expectNoA11yViolations }>({
  page: async ({ page }, use) => {
    const errors: string[] = [];
    page.on("pageerror", (err) => errors.push(err.message));
    page.on("console", (msg) => {
      if (msg.type() === "error") errors.push(msg.text());
    });
    // eslint-disable-next-line react-hooks/rules-of-hooks -- `use` here is the Playwright fixture callback, not a React hook
    await use(page);
    expect(errors, `Uncaught browser errors: ${errors.join("; ")}`).toHaveLength(0);
  },
  // Exposed as a fixture so specs can call `await expectNoA11yViolations(page)`
  // without importing it separately; the standalone export remains available.
  expectNoA11yViolations: async ({}, use) => {
    // eslint-disable-next-line react-hooks/rules-of-hooks -- Playwright fixture callback, not a React hook
    await use(expectNoA11yViolations);
  },
});
