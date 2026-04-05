// Copyright (c) 2025 Kenneth Stott
// Canary: 9f2a1efc-17dc-4298-813f-a3326ba3ea45
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { test, expect } from "./coverage";
import { setupMocks } from "./mocks";

test.describe("AdminPage", () => {
  test.beforeEach(async ({ page }) => {
    await setupMocks(page);
    // /admin is proxied to backend by Vite, navigate via SPA
    await page.goto("/");
    await page.getByRole("link", { name: "Admin" }).click();
    await expect(page.getByRole("heading", { name: "Admin Dashboard" })).toBeVisible({ timeout: 15000 });
  });

  test("shows stats dashboard", async ({ page }) => {
    await expect(page.locator(".stat-card")).toHaveCount(6);
    await expect(page.locator(".stat-label", { hasText: "Sources" })).toBeVisible();
    await expect(page.locator(".stat-label", { hasText: "Tables" })).toBeVisible();
    await expect(page.locator(".stat-label", { hasText: "Roles" })).toBeVisible();
  });

  test("shows platform settings", async ({ page }) => {
    await expect(page.getByRole("heading", { name: "Platform Settings" })).toBeVisible();
    await expect(page.locator("h4", { hasText: "Redirect" })).toBeVisible();
    await expect(page.locator("h4", { hasText: "Naming" })).toBeVisible();
    await expect(page.locator("h4", { hasText: "Sampling" })).toBeVisible();
    await expect(page.locator("h4", { hasText: "Cache" })).toBeVisible();
  });

  test("updates platform settings", async ({ page }) => {
    // Change threshold
    const thresholdInput = page.locator("label", { hasText: "Default Threshold" }).locator("input");
    await thresholdInput.fill("5000");

    // Toggle domain prefix
    const domainCheckbox = page.locator("label", { hasText: "Domain prefix" }).locator("input[type='checkbox']");
    await domainCheckbox.click();

    await page.getByRole("button", { name: "Save Settings" }).click();
    await expect(page.locator(".upload-msg", { hasText: "Updated" })).toBeVisible({ timeout: 5000 });
  });

  test("views and hides config", async ({ page }) => {
    await page.getByRole("button", { name: "View" }).click();
    await expect(page.locator(".config-preview")).toBeVisible({ timeout: 5000 });
    await expect(page.locator(".config-preview")).toContainText("Provisa Config");

    await page.getByRole("button", { name: "Hide" }).click();
    await expect(page.locator(".config-preview")).not.toBeVisible();
  });

  test("downloads config", async ({ page }) => {
    const downloadPromise = page.waitForEvent("download");
    await page.getByRole("button", { name: "Download" }).click();
    const download = await downloadPromise;
    expect(download.suggestedFilename()).toBe("provisa.yaml");
  });

  test("uploads config", async ({ page }) => {
    const fileInput = page.locator("input[type='file']");
    await fileInput.setInputFiles({
      name: "provisa.yaml",
      mimeType: "application/x-yaml",
      buffer: Buffer.from("sources:\n  - id: test\n"),
    });
    await expect(page.locator(".upload-msg", { hasText: "Config uploaded" })).toBeVisible({ timeout: 5000 });
  });

  // ── Phase Y: Admin tabs ──

  test("shows admin tabs", async ({ page }) => {
    await expect(page.locator(".admin-tab", { hasText: "Overview" })).toBeVisible();
    await expect(page.locator(".admin-tab", { hasText: "Materialized Views" })).toBeVisible();
    await expect(page.locator(".admin-tab", { hasText: "Cache" })).toBeVisible();
    await expect(page.locator(".admin-tab", { hasText: "System Health" })).toBeVisible();
  });

  test("MV tab shows materialized views", async ({ page }) => {
    await page.locator(".admin-tab", { hasText: "Materialized Views" }).click();
    await expect(page.locator("code", { hasText: "mv-orders-customers" })).toBeVisible({ timeout: 5000 });
    await expect(page.locator(".status-badge", { hasText: "fresh" })).toBeVisible();
    await expect(page.getByRole("button", { name: "Refresh" })).toBeVisible();
    await expect(page.getByRole("button", { name: "Disable" })).toBeVisible();
  });

  test("Cache tab shows stats and purge controls", async ({ page }) => {
    await page.locator(".admin-tab", { hasText: "Cache" }).click();
    await expect(page.locator(".stat-label", { hasText: "Cached Keys" })).toBeVisible({ timeout: 5000 });
    await expect(page.locator(".stat-label", { hasText: "Hit Rate" })).toBeVisible();
    await expect(page.getByRole("button", { name: "Purge All Cache" })).toBeVisible();
  });

  test("System Health tab shows component statuses", async ({ page }) => {
    await page.locator(".admin-tab", { hasText: "System Health" }).click();
    await expect(page.locator("td", { hasText: "Trino" })).toBeVisible({ timeout: 5000 });
    await expect(page.locator("td", { hasText: "PostgreSQL Pool" })).toBeVisible();
    await expect(page.locator("td", { hasText: "Cache (Redis)" })).toBeVisible();
    await expect(page.locator("td", { hasText: "Connected" }).first()).toBeVisible();
  });
});
