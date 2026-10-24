// Copyright (c) 2026 Kenneth Stott
// Canary: bec103db-ea14-4320-bb6d-89dc03cbce55
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import { test, expect } from "./coverage";

// Explore → GraphQL: the page itself never scrolls; only GraphiQL's own panes do. GraphiQL lets a
// Monaco editor's layers (its margin runs the full content height) out of the editor's clip, so the
// container must clip them. 800 tall keeps the default query taller than its editor at both widths.
for (const width of [1280, 1900]) {
  test(`the GraphQL page does not scroll past GraphiQL at ${width}`, async ({ page }) => {
    await page.setViewportSize({ width, height: 800 });
    await page.goto("/query");
    // The default query has more lines than an 800-tall editor shows, so its margin outruns it.
    const editor = page.locator(".graphiql-query-editor .monaco-editor").first();
    await expect
      .poll(() => editor.evaluate((el) => (el.querySelector(".margin") as HTMLElement).offsetHeight - (el as HTMLElement).offsetHeight), { timeout: 90000 })
      .toBeGreaterThan(0);
    const { scrollHeight, clientHeight } = await page.evaluate(() => {
      const doc = document.scrollingElement!;
      return { scrollHeight: doc.scrollHeight, clientHeight: doc.clientHeight };
    });
    expect(scrollHeight).toBe(clientHeight);
  });
}
