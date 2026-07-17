// Copyright (c) 2026 Kenneth Stott
// Canary: 87cee5b7-4115-4005-863b-3601641663b2
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import { test, expect } from "./coverage";
import type { Page } from "@playwright/test";

/**
 * Validates the single-domain ("no domains") configuration: when the backend reports
 * naming.use_domains === false, all domain UI must disappear. We stub GET /admin/settings
 * so the test is independent of the live backend's actual config.
 */

async function stubUseDomains(page: Page, useDomains: boolean | null) {
  await page.route("**/admin/settings", async (route) => {
    if (route.request().method() !== "GET") return route.continue();
    const resp = await route.fetch();
    const json = await resp.json();
    json.naming = { ...json.naming, use_domains: useDomains, default_domain: "global" };
    await route.fulfill({ response: resp, json });
  });
}

test("single-domain mode hides the NavBar domain filter and the Domain column", async ({ page }) => {
  await stubUseDomains(page, false);
  await page.goto("/tables");
  await page.waitForSelector(".data-table", { timeout: 10000 });

  // NavBar domain filter panel is gone.
  await expect(page.getByText(/Domains \(\d+\/\d+\)/)).toHaveCount(0);

  // The "Domain" table column header is gone.
  await expect(page.locator("th").filter({ hasText: "Domain" })).toHaveCount(0);

  // Sibling columns remain.
  await expect(page.locator("th").filter({ hasText: "Source" })).toHaveCount(1);
  await expect(page.locator("th").filter({ hasText: "Table" })).toHaveCount(1);
});

test("legacy mode (use_domains null) still shows the Domain column", async ({ page }) => {
  await stubUseDomains(page, null);
  await page.goto("/tables");
  await page.waitForSelector(".data-table", { timeout: 10000 });

  await expect(page.locator("th").filter({ hasText: "Domain" })).toHaveCount(1);
  await expect(page.getByText(/Domains \(\d+\/\d+\)/)).toHaveCount(1);
});
