// Copyright (c) 2026 Kenneth Stott
// Canary: 8c41f7a2-5d3e-4b96-a1f0-2e7b9d6c3a58
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import { test, expect } from "./coverage";
import { TOUR_STEPS } from "../src/tour/tourSteps";

const POLLY_STEP = TOUR_STEPS.findIndex((s) => s.key === "stepPolly");

test.describe.configure({ timeout: 120_000 });

// REQ-1945: the tour introduces Polly right after the welcome step with the Polly panel open, and Next
// closes it again so the next step's anchors are not covered.
test("tour introduces Polly with the Polly panel open", async ({ page }) => {
  await page.addInitScript((step: number) => {
    localStorage.setItem("provisa_tour_seen", "true");
    localStorage.setItem("provisa_tour_progress", String(step));
  }, POLLY_STEP);
  await page.goto("/sources");
  await page.locator(".navbar-tour-btn").click();

  const title = page.locator(".driver-popover-title");
  await expect(title).toHaveText("Meet Polly, your setup assistant", { timeout: 60000 });
  await expect(page.locator('[data-testid="chat-panel"]')).toBeVisible();

  await page.locator(".driver-popover-next-btn").click();
  await expect(page.locator('[data-testid="chat-panel"]')).toHaveCount(0);
  await expect(title).toHaveText("Start with Sources", { timeout: 30000 });
});
