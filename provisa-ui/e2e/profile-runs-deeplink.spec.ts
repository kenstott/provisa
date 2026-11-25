// Copyright (c) 2026 Kenneth Stott
// Canary: 418986b2-c353-448d-bbe2-62ca005c0ee9
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

// REQ-2277: the profile runs dialog is a deep link. /tables?table=<id>&section=quality&profile=<run>
// &profileView=<view> opens in a fresh tab on that run and that result, and survives a reload.

import { test, expect } from "./coverage";

test.describe.configure({ timeout: 120_000 });

const runOf = (run_id: string, run_time: string) => ({
  run_id,
  run_time,
  region: null,
  profiled_table: "pets",
  row_count: 10,
  sampled: false,
  sample_method: "whole",
  target_fraction: null,
  sample_fraction: null,
  sample_attempts: null,
  profiled_rows: 10,
  previous_run_id: null,
  duplicate_rows: 0,
  duplicate_share: 0,
  key_duplicates: null,
  duration_ms: 5,
  status: "succeeded",
  error: null,
});

test("a profile runs link opens the table on the run and the result it names", async ({
  page,
}) => {
  // The demo's tables join no profiler and have no runs: the table's membership and its runs are
  // handed to the page, so the dialog's own code is what is under test.
  await page.route("**/admin/graphql", async (route) => {
    const response = await route.fetch();
    const body = JSON.stringify(await response.json(), (key, value) =>
      key === "profilerSourceId" ? "e2e-profiler" : value,
    );
    await route.fulfill({ response, body });
  });
  await page.route(/\/admin\/tables\/\d+\/profile-runs(\/[^/?]+)?(\?.*)?$/, (route) => {
    const url = new URL(route.request().url());
    const named = url.pathname.split("/profile-runs/")[1];
    if (named === undefined) {
      return route.fulfill({
        json: [runOf("r1", "2026-10-20T10:00:00Z"), runOf("r2", "2026-10-21T10:00:00Z")],
      });
    }
    return route.fulfill({
      json: { quantiles: [{ run_id: named, column_name: "age", p50: named === "r1" ? 3 : 4 }] },
    });
  });
  await page.route("**/admin/tables/*/profile-checks", (route) =>
    route.fulfill({ json: { drift: [], expectations: [] } }),
  );

  // Not `?open=`: that already opens the table, and the row click would then close it.
  await page.goto("/tables");
  const row = page.locator('[data-table-row="pet-store-sqlite.pets"]');
  await expect(row).toBeVisible({ timeout: 60_000 });
  await row.click();
  await expect(page).toHaveURL(/[?&]table=\d+/);
  const id = new URL(page.url()).searchParams.get("table");

  const link = `/tables?table=${id}&section=quality&profile=r1&profileView=quantiles`;
  await page.goto(link);
  const modal = page.getByTestId("profile-runs-modal");
  await expect(modal).toBeVisible({ timeout: 60_000 });
  await expect(page.getByTestId("profile-tab-quantiles")).toHaveAttribute("aria-current", "true");
  await expect(page.getByTestId("profile-panel-quantiles")).toContainText("age");
  const first = page.getByTestId("profile-runs-list").locator("tr.row-selected");
  await expect(first).toHaveCount(1);
  await expect(first).toContainText(new Date("2026-10-20T10:00:00Z").toLocaleString());

  // The same after a reload.
  await page.reload();
  await expect(page.getByTestId("profile-panel-quantiles")).toBeVisible({ timeout: 60_000 });
  await expect(page.getByTestId("profile-runs-list").locator("tr.row-selected")).toContainText(
    new Date("2026-10-20T10:00:00Z").toLocaleString(),
  );

  // A view and a run chosen are the URL, without a history entry each.
  const entries = await page.evaluate(() => history.length);
  await page.getByTestId("profile-tab-columns").click();
  await expect(page).not.toHaveURL(/profileView=/);
  await page
    .getByTestId("profile-runs-list")
    .locator("tr", { hasText: new Date("2026-10-21T10:00:00Z").toLocaleString() })
    .click();
  await expect(page).toHaveURL(/[?&]profile=r2(&|$)/);
  expect(await page.evaluate(() => history.length)).toBe(entries);

  // Closed, it leaves the table and its section.
  await page.keyboard.press("Escape");
  await expect(modal).toHaveCount(0);
  await expect(page).not.toHaveURL(/profile=|profileView=/);
  await expect(page).toHaveURL(/[?&]table=\d+/);
  await expect(page).toHaveURL(/section=quality/);
});
