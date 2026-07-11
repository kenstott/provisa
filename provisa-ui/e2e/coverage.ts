// Copyright (c) 2026 Kenneth Stott
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import { test as base, expect } from "playwright/test";

export { expect } from "playwright/test";

export const test = base.extend({
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
});
