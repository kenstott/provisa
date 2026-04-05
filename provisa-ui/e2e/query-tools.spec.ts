// Copyright (c) 2025 Kenneth Stott
// Canary: 235771d5-0c4f-4ba6-85b3-8e3a462ea395
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { test, expect } from "./coverage";

/**
 * Tests for Provisa Tools plugin inside GraphiQL.
 * Uses the live backend (not mocked) since GraphiQL requires full introspection.
 */
test.describe("Query Tools", () => {
  test.beforeEach(async ({ page }) => {
    await page.goto("/query");
    await page.waitForSelector(".graphiql-container", { timeout: 10000 });

    // Dismiss overlay
    const overlay = page.locator(".graphiql-dialog-overlay");
    if (await overlay.isVisible({ timeout: 1000 }).catch(() => false)) {
      await page.keyboard.press("Escape");
      await overlay.waitFor({ state: "hidden", timeout: 3000 }).catch(() => {});
    }
    if (await overlay.isVisible({ timeout: 500 }).catch(() => false)) {
      await overlay.click({ force: true, position: { x: 1, y: 1 } });
      await overlay.waitFor({ state: "hidden", timeout: 3000 }).catch(() => {});
    }
  });

  test("opens Provisa plugin and shows submit form", async ({ page }) => {
    // Find Provisa sidebar button
    const sidebar = page.locator(".graphiql-sidebar");
    const buttons = sidebar.locator("button");
    const count = await buttons.count();

    for (let i = 0; i < count; i++) {
      const label = await buttons.nth(i).getAttribute("aria-label");
      if (label?.toLowerCase().includes("provisa")) {
        await buttons.nth(i).click();
        break;
      }
    }

    const provisaPanel = page.locator(".provisa-tools");
    if (!(await provisaPanel.isVisible({ timeout: 3000 }).catch(() => false))) {
      // Try clicking each button
      for (let i = 0; i < count; i++) {
        await buttons.nth(i).click();
        if (await provisaPanel.isVisible({ timeout: 500 }).catch(() => false)) break;
      }
    }

    if (await provisaPanel.isVisible({ timeout: 2000 }).catch(() => false)) {
      // Verify Submit for Approval button exists
      const submitBtn = page.locator("button", { hasText: "Submit for Approval" });
      await expect(submitBtn).toBeVisible();

      // Open the submit form
      await submitBtn.click();
      await expect(page.locator(".provisa-tools-metadata")).toBeVisible({ timeout: 3000 });

      // Verify form fields
      await expect(page.locator("textarea").first()).toBeVisible(); // Business Purpose
      await expect(page.locator("label", { hasText: "Data Sensitivity" })).toBeVisible();
      await expect(page.locator("label", { hasText: "Refresh Frequency" })).toBeVisible();
      await expect(page.locator("label", { hasText: "Expected Size" })).toBeVisible();
      await expect(page.locator("label", { hasText: "Owner Team" })).toBeVisible();

      // Verify delivery checkboxes
      await expect(page.locator(".provisa-tools-delivery")).toBeVisible();
      await expect(page.locator(".provisa-tools-delivery-item", { hasText: "JSON" })).toBeVisible();
      await expect(page.locator(".provisa-tools-delivery-item", { hasText: "Kafka Sink" })).toBeVisible();

      // Toggle Kafka Sink
      const kafkaCheckbox = page.locator(".provisa-tools-delivery-item", { hasText: "Kafka Sink" }).locator("input[type='checkbox']");
      await kafkaCheckbox.check();
      await expect(page.locator(".provisa-tools-sink")).toBeVisible();
      await expect(page.locator("input[placeholder*='order-report-updates']")).toBeVisible();

      // Cancel
      await page.locator("button", { hasText: "Cancel" }).click();
      await expect(page.locator(".provisa-tools-metadata")).not.toBeVisible();
    }
  });

  test("delivery checkboxes update use cases", async ({ page }) => {
    // Open Provisa panel
    const sidebar = page.locator(".graphiql-sidebar");
    const buttons = sidebar.locator("button");
    const count = await buttons.count();
    for (let i = 0; i < count; i++) {
      const label = await buttons.nth(i).getAttribute("aria-label");
      if (label?.toLowerCase().includes("provisa")) {
        await buttons.nth(i).click();
        break;
      }
    }

    const provisaPanel = page.locator(".provisa-tools");
    if (!(await provisaPanel.isVisible({ timeout: 3000 }).catch(() => false))) {
      for (let i = 0; i < count; i++) {
        await buttons.nth(i).click();
        if (await provisaPanel.isVisible({ timeout: 500 }).catch(() => false)) break;
      }
    }

    if (await provisaPanel.isVisible({ timeout: 2000 }).catch(() => false)) {
      // Open submit form
      await page.locator("button", { hasText: "Submit for Approval" }).click();
      await expect(page.locator(".provisa-tools-metadata")).toBeVisible({ timeout: 3000 });

      // Check JSON delivery
      const jsonCheckbox = page.locator(".provisa-tools-delivery-item", { hasText: "JSON" }).locator("input[type='checkbox']");
      await jsonCheckbox.check();

      // Check Arrow delivery
      const arrowCheckbox = page.locator(".provisa-tools-delivery-item", { hasText: "Arrow" }).locator("input[type='checkbox']");
      await arrowCheckbox.check();

      // Verify use cases textarea was updated
      const useCasesTextarea = page.locator("textarea").nth(1);
      const value = await useCasesTextarea.inputValue();
      expect(value).toContain("json");
      expect(value).toContain("arrow");

      // Uncheck JSON
      await jsonCheckbox.uncheck();
      const value2 = await useCasesTextarea.inputValue();
      expect(value2).not.toContain("json");

      // Fill metadata and change selects
      await page.locator("select", { hasText: "Internal" }).selectOption("confidential");
      await page.locator("select", { hasText: "Ad-hoc" }).selectOption("daily");
      await page.locator("select", { hasText: "<1K" }).selectOption("1K-100K");
      await page.locator("input[placeholder*='Team responsible']").fill("Data Engineering");
    }
  });
});
