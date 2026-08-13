// Copyright (c) 2026 Kenneth Stott
// Canary: 6f5c2b8a-6dc0-4a2e-9c4c-cbe2c2fa5a2e
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import { test, expect } from "./coverage";

// The guided tour hops between surfaces faster than a step's own destination can pay its
// first-visit chunk fetch/data load, which used to show a step's popover over a page still
// stuck on its own "Loading…" state (observed on both /tables and /relationships). ?tour=1
// starts the tour fresh regardless of the "seen" flag (App.tsx TourAutoStart).
test("tour does not show a step's popover over a page still loading its data", async ({ page }) => {
  // Delay the TablesPage data query so its loading window is long enough to observe — without
  // this, the admin GraphQL resolver and route chunk are both already warm in a dev/test run
  // (see app_startup.py warmup), so the race this test targets resolves too fast to catch even
  // on the pre-fix code.
  await page.route("**/admin/graphql", async (route) => {
    const body = route.request().postDataJSON() as { operationName?: string } | null;
    if (body?.operationName === "TablesQuery") {
      await new Promise((resolve) => setTimeout(resolve, 1000));
    }
    await route.continue();
  });

  await page.goto("/tables?tour=1");

  const nextBtn = page.locator(".driver-popover-next-btn");

  // Steps 0-3 live on /sources; advance to step4, which navigates to /tables and highlights
  // the persistent nav-tables anchor — the element that used to resolve before TablesPage's
  // own data had loaded. Synchronize on each popover's own "Next (n/30)" label (driver.js
  // renders it from TOUR_STEPS' 1-based position, matching the step reached — see
  // tour.nav.next in tour.json) rather than firing clicks on a fixed clock: driver.js only
  // swaps the button label once the *new* step's async waitForElement chain has resolved and
  // `highlight()` has actually run, so waiting on the label (not just "a popover is visible")
  // guarantees each click lands on the step it's meant to.
  for (let n = 1; n <= 4; n++) {
    // The first label has a different budget from the rest: reaching it means the app bundle has
    // booted, TourAutoStart has fired, and the tour has navigated /tables -> /sources and paid
    // that route's first-visit chunk fetch — all on a runner where five other workers are
    // compiling and querying at the same time. The default 5s expect timeout covers none of that
    // reliably. Once step0's popover is up, every later step is an in-app transition and 5s is
    // ample, so the extra budget is spent only where the work actually is.
    await expect(nextBtn).toHaveText(`Next (${n}/30)`, { timeout: n === 1 ? 60000 : 5000 });
    if (n < 4) await nextBtn.click();
  }
  await nextBtn.click(); // enters step4: navigates to /tables, highlights nav-tables

  // Poll in-browser (a single evaluate, not round-tripping two separate Playwright assertions)
  // for the *first instant* step4's own popover is showing (identified by its "Next (5/30)"
  // label — the driver.js popover container persists across step transitions, so merely
  // checking "a popover exists" can still match the *previous* step's lingering popover), and
  // capture in that same tick whether "Loading tables…" is still on screen. Two independent
  // `expect()` polls each carry their own IPC round-trip and would let the 1s query delay
  // elapse between them, hiding the very race this test exists to catch.
  const state = await page.waitForFunction(
    () => {
      const nextBtnEl = document.querySelector(".driver-popover-next-btn");
      if (nextBtnEl?.textContent?.trim() !== "Next (5/30)") return null;
      const loading = Array.from(document.querySelectorAll(".page")).some(
        (el) => el.textContent?.trim() === "Loading tables...",
      );
      return { loading };
    },
    { timeout: 5000 },
  );
  const { loading } = await state.jsonValue();
  expect(loading, "tour popover appeared while TablesPage was still on its loading state").toBe(false);

  await expect(page).toHaveURL(/\/tables/);
  await expect(page.locator('[data-tour="tables-add"]')).toBeVisible();
});

test("tour does not show the relationships step popover over a page still loading its data", async ({
  page,
}) => {
  // Resume straight into step15 (the first Relationships step, TOUR_STEPS[15] in tourSteps.ts)
  // instead of clicking through the spine/divider steps ahead of it — those involve executing
  // real demo queries across several surfaces, which is unrelated to what this test verifies.
  // Mark the tour "seen" so TourAutoStart's fresh-profile path (App.tsx) doesn't force a
  // restart-from-0 ahead of us; the navbar tour button's plain startTour() honors the saved step.
  await page.addInitScript(() => {
    localStorage.setItem("provisa_tour_seen", "true");
    localStorage.setItem("provisa_tour_progress", "15");
  });
  await page.goto("/sources");
  await page.locator(".navbar-tour-btn").click();

  const popover = page.locator(".driver-popover");
  await expect(popover).toBeVisible();
  await expect(page).toHaveURL(/\/relationships/);
  await expect(page.getByText("Loading relationships...")).toHaveCount(0);
  await expect(page.locator('[data-tour="rels-add"]')).toBeVisible();
});
