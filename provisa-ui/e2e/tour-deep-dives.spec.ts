// Copyright (c) 2026 Kenneth Stott
// Canary: 91c7e5d2-3a68-4f0b-8e14-b6d09f2a7c35
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import { test, expect } from "./coverage";
import { TOUR_STEPS, TOUR_SCOPES } from "../src/tour/tourSteps";

test.describe.configure({ timeout: 180_000 });

const CORE_LAST_STEP = TOUR_STEPS.findIndex(
  (s) => s.key === TOUR_SCOPES.core[TOUR_SCOPES.core.length - 1],
);

// REQ-1945: the core tour starts for a new user, every core step carries a Deep Dives button that
// leaves for the menu at once, and the menu is a card per topic.
test("the core tour's Deep Dives button leaves for the menu", async ({ page }) => {
  await page.goto("/sources?tour=1");

  const title = page.locator(".driver-popover-title");
  await expect(title).toHaveText("Welcome, let's take the tour", { timeout: 60000 });

  await page.locator(".driver-popover-next-btn").click();
  await expect(title).toHaveText("Meet Polly, your setup assistant", { timeout: 30000 });

  // The button is on every core step, here the second.
  await page.locator(".driver-popover-deepdives-btn").click();
  await expect(page.locator('[data-testid="tour-menu"]')).toBeVisible();
  await expect(page.locator('[data-testid^="tour-topic-"]:not([data-testid*="done"])')).toHaveCount(
    8,
  );
});

// Done on the core tour's last step lands on the menu; a topic is its own tour, and Done on its
// last step returns to the menu with the topic marked completed.
test("Done on the core tour and on a topic both return to the menu", async ({ page }) => {
  await page.addInitScript((step: number) => {
    localStorage.setItem("provisa_tour_seen", "true");
    localStorage.setItem("provisa_tour_progress", String(step));
  }, CORE_LAST_STEP);
  await page.goto("/sources");
  await page.locator(".navbar-tour-btn").click(); // resumes the core tour at its last step

  const title = page.locator(".driver-popover-title");
  await expect(title).toHaveText("That's the five-minute core", { timeout: 60000 });
  await page.locator(".driver-popover-next-btn").click(); // Done
  const menu = page.locator('[data-testid="tour-menu"]');
  await expect(menu).toBeVisible();
  await expect(page.locator('[data-testid^="tour-topic-done-"]')).toHaveCount(0);

  // Govern it: step through every card the viewer may open, then Done on the last.
  await page.locator('[data-testid="tour-topic-govern"]').click();
  await expect(title).toHaveText("Access control (RBAC)", { timeout: 60000 });
  const next = page.locator(".driver-popover-next-btn");
  while (!/^Done/.test((await next.textContent()) ?? "")) {
    const before = await title.textContent();
    await next.click();
    await expect(title).not.toHaveText(before ?? "", { timeout: 30000 });
  }
  await next.click(); // Done
  await expect(menu).toBeVisible();
  await expect(page.locator('[data-testid="tour-topic-done-govern"]')).toBeVisible();

  // The mark outlives a reload: the tour button, with the core tour seen, reopens the menu.
  await page.reload();
  await page.locator(".navbar-tour-btn").click();
  await expect(page.locator('[data-testid="tour-topic-done-govern"]')).toBeVisible();
});

// REQ-1945: a topic's step also carries Deep Dives, which lands on the catalog, and the catalog's
// control starts the core tour again at its first step.
test("a topic's Deep Dives button reaches the catalog, which restarts the core tour", async ({ page }) => {
  await page.addInitScript(() => localStorage.setItem("provisa_tour_seen", "true"));
  await page.goto("/sources");
  await page.locator(".navbar-tour-btn").click();
  await page.locator('[data-testid="tour-topic-govern"]').click();

  const title = page.locator(".driver-popover-title");
  await expect(title).toHaveText("Access control (RBAC)", { timeout: 60000 });
  await page.locator(".driver-popover-deepdives-btn").click();
  const menu = page.locator('[data-testid="tour-menu"]');
  await expect(menu).toBeVisible();

  await page.locator('[data-testid="tour-menu-core-tour"]').click();
  await expect(title).toHaveText("Welcome, let's take the tour", { timeout: 60000 });
});
