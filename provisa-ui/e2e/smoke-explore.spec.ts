// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

// REQ-1989: the smoke check's browser step. The app loads with no console error (the `test` of
// ./coverage fails on any), Explore opens from the menu, and one query answers the demo's rows.

import { test, expect } from "./coverage";
import { runSqlOnPage } from "./source-to-query-helpers";

test("the app loads, Explore opens and a query shows the demo's rows", async ({ page }) => {
  await page.goto("/");
  await page.getByTestId("nav-group-explore").click();
  await page.getByRole("link", { name: "SQL", exact: true }).click();
  await expect(page).toHaveURL(/\/sql$/);

  const rows = await runSqlOnPage(page, "SELECT id, name FROM pet_store.pets ORDER BY id");

  expect(rows.map((r) => r[1])).toEqual(
    expect.arrayContaining(["Cat 1", "Cat 2", "Dog 1", "Rabbit 1"]),
  );
});
