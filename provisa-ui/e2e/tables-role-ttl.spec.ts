// Copyright (c) 2026 Kenneth Stott
// Canary: 6e3b9d27-1a8f-4c54-9d02-b7f4e1c8a365
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

// REQ-1907: an operator edits a table's Role TTL list and it persists across a reload.

import type { Page } from "@playwright/test";
import { test, expect } from "./coverage";

test.beforeEach(async ({ page }) => {
  // Demo mode auto-starts the guided tour in a fresh profile, which navigates away from /tables.
  await page.addInitScript(() => localStorage.setItem("provisa_tour_seen", "true"));
});

async function openPetsEditor(page: Page) {
  await page.goto("/tables");
  await page.waitForSelector(".page-header", { timeout: 15000 });
  const petsRow = page
    .locator("tr")
    .filter({ hasText: "pet-store-sqlite" })
    .filter({ hasText: "pets" })
    .first();
  await petsRow.waitFor({ timeout: 15000 });
  await petsRow.click();
  const editBtn = page.getByTestId("table-read-view-edit").first();
  await editBtn.waitFor({ timeout: 10000 });
  await editBtn.click();
  // The load settings sit in the collapsed "Load Management and Timeliness" panel.
  const toggle = page.getByTestId("load-management-panel-toggle");
  await expect(toggle).toContainText("Load Management and Timeliness");
  await expect(toggle).toHaveAttribute("aria-expanded", "false");
  await toggle.click();
  await page.getByTestId("role-ttl-field").waitFor({ state: "visible", timeout: 15000 });
}

async function save(page: Page) {
  await page.getByTestId("table-edit-save").click();
  await expect(page.getByTestId("table-edit-save")).toBeHidden({ timeout: 15000 });
}

test("a table's role TTL list persists after reload", async ({ page }) => {
  await openPetsEditor(page);
  const field = page.getByTestId("role-ttl-field");
  await expect(field.getByTestId("role-ttl-row")).toHaveCount(0);

  await field.getByRole("button", { name: "Add role TTL" }).click();
  const row = field.getByTestId("role-ttl-row").first();
  const role = await row.getByRole("textbox", { name: "Role" }).inputValue();
  expect(role).not.toBe("");
  const seconds = row.getByRole("textbox", { name: "TTL (seconds)" });
  await seconds.fill("7200");
  await expect(row.getByTestId("role-ttl-effective")).toContainText("7200s");
  await save(page);

  await page.reload();
  await openPetsEditor(page);
  const reloaded = page.getByTestId("role-ttl-field").getByTestId("role-ttl-row");
  await expect(reloaded).toHaveCount(1);
  await expect(reloaded.first().getByRole("textbox", { name: "Role" })).toHaveValue(role);
  await expect(reloaded.first().getByRole("textbox", { name: "TTL (seconds)" })).toHaveValue(
    "7200",
  );

  // Leave the shared demo table as it was.
  await reloaded
    .first()
    .getByRole("button", { name: `Remove role TTL for ${role}` })
    .click();
  await save(page);
  await page.reload();
  await openPetsEditor(page);
  await expect(page.getByTestId("role-ttl-field").getByTestId("role-ttl-row")).toHaveCount(0);
});
