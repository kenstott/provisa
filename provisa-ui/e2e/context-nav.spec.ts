// Copyright (c) 2026 Kenneth Stott
// Canary: 3e63e1d7-0eda-46b6-8ca2-5c3c7eddede8
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

// REQ-2259: the Context group replaces the top-level Ontology and Library links. Its pages are
// reached from the group's subnav at their /context/ routes, and Context Sync is no longer in Admin.

import { test, expect } from "./coverage";

test("the Context group opens the Ontology, Documents and Context Sync pages", async ({ page }) => {
  await page.goto("/");
  await expect(page.locator(".navbar-links").getByRole("link", { name: "Ontology" })).toHaveCount(0);
  await expect(page.locator(".navbar-links").getByRole("link", { name: "Library" })).toHaveCount(0);

  await page.getByTestId("nav-group-context").click();
  const subnav = page.locator(".subnav");
  await subnav.getByRole("link", { name: "Ontology", exact: true }).click();
  await expect(page).toHaveURL(/\/context\/ontology$/);

  await subnav.getByRole("link", { name: "Documents", exact: true }).click();
  await expect(page).toHaveURL(/\/context\/documents$/);
  await expect(page.getByTestId("page-title")).toHaveText("Documents");

  await subnav.getByRole("link", { name: "Context Sync", exact: true }).click();
  await expect(page).toHaveURL(/\/context\/sync$/);
  await expect(page.getByTestId("syncing")).toBeVisible();

  // The group's pages still to come are listed, not linked.
  for (const label of ["Context Health", "Metrics", "Tags", "Classification"]) {
    await expect(subnav.getByText(`${label} — coming soon`)).toBeVisible();
  }
});

test("Admin no longer lists Syncing", async ({ page }) => {
  await page.goto("/admin/overview");
  const rail = page.getByTestId("admin-rail");
  await expect(rail).toBeVisible();
  await expect(rail.getByRole("link", { name: "Syncing" })).toHaveCount(0);
  await expect(rail.getByRole("link", { name: "Import" })).toBeVisible();
});
