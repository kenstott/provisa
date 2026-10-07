// Copyright (c) 2026 Kenneth Stott
// Canary: 6f5c2b8a-6dc0-4a2e-9c4c-cbe2c2fa5a2e
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import { test, expect } from "./coverage";
import { TOUR_STEPS, TOUR_SCOPES } from "../src/tour/tourSteps";

// driver.js renders "Next (n/N)" from the 1-based position within the running tour (REQ-1945: the
// core tour or one Deep Dives topic), derived here so inserting a step re-points the test instead
// of silently walking it onto the wrong surface.
const CONNECT_TOTAL = TOUR_SCOPES.connect.length;
// 1-based position, within the Connect topic, of the first /tables step.
const TABLES_POSITION =
  TOUR_SCOPES.connect.indexOf(TOUR_STEPS.find((s) => s.route === "/tables")!.key) + 1;

// The waits below budget 60 s for the first popover alone — the app bundle boot, the identity
// bootstrap and a first-visit route chunk, all against five other workers. Under the 30 s default
// per-test timeout that budget could never be spent: the test died on the test clock while its own
// expect was still inside its allowance.
test.describe.configure({ timeout: 180_000 });

// The guided tour hops between surfaces faster than a step's own destination can pay its
// first-visit chunk fetch/data load, which used to show a step's popover over a page still
// stuck on its own "Loading…" state (observed on both /tables and /relationships). The core tour
// has been seen, so the navbar button opens the Deep Dives menu and a card runs its topic.
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

  await page.addInitScript(() => localStorage.setItem("provisa_tour_seen", "true"));
  await page.goto("/sources");
  await page.locator(".navbar-tour-btn").click();
  await page.locator('[data-testid="tour-topic-connect"]').click();

  const nextBtn = page.locator(".driver-popover-next-btn");

  // Synchronize on each popover's own "Next (n/N)" label rather than firing clicks on a fixed
  // clock: driver.js only swaps the button label once the *new* step's async waitForElement chain
  // has resolved and `highlight()` has actually run, so waiting on the label (not just "a popover
  // is visible") guarantees each click lands on the step it's meant to. The first label has the
  // larger budget: reaching it means the topic's prefetch and first route chunk have been paid.
  for (let n = 1; n < TABLES_POSITION; n++) {
    await expect(nextBtn).toHaveText(`Next (${n}/${CONNECT_TOTAL})`, {
      timeout: n === 1 ? 60000 : 5000,
    });
    await nextBtn.click(); // the last click enters the /tables step
  }

  // Poll in-browser (a single evaluate, not round-tripping two separate Playwright assertions)
  // for the *first instant* the /tables step's own popover is showing (identified by its
  // "Next (n/N)" label — the driver.js popover container persists across step transitions, so
  // merely checking "a popover exists" can still match the *previous* step's lingering popover),
  // and capture in that same tick whether "Loading tables…" is still on screen. Two independent
  // `expect()` polls each carry their own IPC round-trip and would let the 1s query delay elapse
  // between them, hiding the very race this test exists to catch.
  const state = await page.waitForFunction(
    ([position, total]: [number, number]) => {
      const nextBtnEl = document.querySelector(".driver-popover-next-btn");
      if (nextBtnEl?.textContent?.trim() !== `Next (${position}/${total})`) return null;
      const loading = Array.from(document.querySelectorAll(".page")).some(
        (el) => el.textContent?.trim() === "Loading tables...",
      );
      return { loading };
    },
    [TABLES_POSITION, CONNECT_TOTAL],
    // Entering the step navigates to /tables and pays that route's first-visit chunk fetch, so the
    // wait for its popover is a page transition, not an in-place DOM update. The budget does not
    // weaken the assertion: what is asserted is the loading state captured in the same tick the
    // popover first appears, whenever that is.
    { timeout: 60000 },
  );
  const { loading } = await state.jsonValue();
  expect(loading, "tour popover appeared while TablesPage was still on its loading state").toBe(false);

  await expect(page).toHaveURL(/\/tables/);
  await expect(page.locator('[data-tour="tables-add"]')).toBeVisible();
});

test("tour does not show the relationships step popover over a page still loading its data", async ({
  page,
}) => {
  // Open the Relationships topic from the Deep Dives menu rather than clicking through the core
  // tour ahead of it — those steps execute real demo queries across several surfaces, which is
  // unrelated to what this test verifies. "Seen" makes the navbar button open the menu.
  await page.addInitScript(() => localStorage.setItem("provisa_tour_seen", "true"));
  await page.goto("/sources");
  await page.locator(".navbar-tour-btn").click();
  await page.locator('[data-testid="tour-topic-relationships"]').click();

  const popover = page.locator(".driver-popover");
  // The topic's first step navigates and pays that route's first-visit chunk fetch before
  // driver.js highlights anything — more than the 5 s default expect timeout covers on a loaded
  // runner.
  await expect(popover).toBeVisible({ timeout: 60000 });
  await expect(page).toHaveURL(/\/relationships/);
  await expect(page.getByText("Loading relationships...")).toHaveCount(0);
  await expect(page.locator('[data-tour="rels-add"]')).toBeVisible();
});
