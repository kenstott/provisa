// Copyright (c) 2026 Kenneth Stott
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import { test, expect } from "./coverage";
import type { Page } from "@playwright/test";

const QUERY = "MATCH (n:Inquiries) RETURN n LIMIT 5";

async function seedFavoriteAndOpenPanel(page: Page) {
  await page.goto("/graph");

  // Seed a favorite directly into localStorage so we don't need a live query result
  await page.evaluate((q) => {
    const fav = { id: "test-fav-1", query: q, label: "My Favorite", ts: Date.now() };
    localStorage.setItem("provisa.graph.favorites", JSON.stringify([fav]));
  }, QUERY);
  await page.reload();

  // Click the Favorites tab (tabs are icon-only; identified by title attribute)
  const favTab = page.locator(".graph-sidebar-tab[title='Favorites']");
  await expect(favTab).toBeVisible({ timeout: 15000 });
  await favTab.click();

  // The fav item should appear
  const favItem = page.locator(".graph-fav-item").first();
  await expect(favItem).toBeVisible({ timeout: 5000 });
  return favItem;
}

test("favorites panel shows label and action buttons on hover", async ({ page }) => {
  const favItem = await seedFavoriteAndOpenPanel(page);

  // Label is visible
  const label = favItem.locator(".graph-fav-label");
  await expect(label).toHaveText("My Favorite");

  // Hover to reveal action buttons
  await favItem.hover();

  const runBtn = favItem.locator(".graph-fav-run");
  const renameBtn = favItem.locator(".graph-fav-rename-btn");
  const delBtn = favItem.locator(".graph-fav-del");

  await expect(runBtn).toBeVisible();
  await expect(renameBtn).toBeVisible();
  await expect(delBtn).toBeVisible();
});

test("clicking favorite label loads query into editor (edit mode)", async ({ page }) => {
  const favItem = await seedFavoriteAndOpenPanel(page);

  await favItem.locator(".graph-fav-label").click();

  // Query loads into the top-bar collapsed display (not a frame editor)
  const queryDisplay = page.locator(".gf-header-query-collapsed");
  await expect(queryDisplay).toBeVisible({ timeout: 5000 });
  await expect(queryDisplay).toHaveAttribute("title", QUERY);
});

test("rename inline: click ✎ shows input, Enter commits", async ({ page }) => {
  const favItem = await seedFavoriteAndOpenPanel(page);

  await favItem.hover();
  await favItem.locator(".graph-fav-rename-btn").click();

  const renameInput = favItem.locator(".graph-fav-rename-input");
  await expect(renameInput).toBeVisible();
  await expect(renameInput).toBeFocused();

  await renameInput.fill("Renamed Fav");
  await renameInput.press("Enter");

  // Input should disappear and label shows new name
  await expect(renameInput).not.toBeVisible();
  await expect(favItem.locator(".graph-fav-label")).toHaveText("Renamed Fav");
});

test("delete button removes the favorite", async ({ page }) => {
  const favItem = await seedFavoriteAndOpenPanel(page);

  await favItem.hover();
  await favItem.locator(".graph-fav-del").click();

  await expect(page.locator(".graph-fav-item")).toHaveCount(0);
  await expect(page.locator(".graph-schema-empty")).toBeVisible();
});
