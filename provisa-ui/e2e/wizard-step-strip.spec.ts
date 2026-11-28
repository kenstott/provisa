// Copyright (c) 2026 Kenneth Stott
// Canary: 2f8a5c63-d91e-4b07-a3c4-7e1b0d9f6a58
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2277: New Data Vault shows every step in a strip, at 1280 and 1900 wide, inside the panel
// and not overflowing it, with the current step highlighted.

import { test, expect } from "./coverage";

test.describe.configure({ timeout: 120_000 });

const STEPS = ["hubs", "links", "satellites", "rules", "review"];

for (const width of [1280, 1900]) {
  test(`New Data Vault shows its step strip at ${width}`, async ({ page }) => {
    await page.setViewportSize({ width, height: 900 });
    await page.goto("/tables");
    await page.waitForSelector(".page-header", { timeout: 60000 });
    await page.getByTestId("tables-model-menu").click();
    await page.getByTestId("tables-model-vault").click();

    const strip = page.getByTestId("wizard-steps");
    await expect(strip).toBeVisible();
    for (const key of STEPS) await expect(page.getByTestId(`wizard-steps-${key}`)).toBeVisible();
    await expect(strip).toHaveAttribute("data-active", "0");
    await expect(page.getByTestId("wizard-step-count")).toHaveCount(0);

    const box = await strip.boundingBox();
    const panel = await page.getByTestId("wizard-step-hubs").boundingBox();
    expect(box).not.toBeNull();
    expect(panel).not.toBeNull();
    expect(box!.x + box!.width).toBeLessThanOrEqual(panel!.x + panel!.width + 1);
  });
}
