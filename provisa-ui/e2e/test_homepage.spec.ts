// Copyright (c) 2026 Kenneth Stott
// Canary: cbf6c5ad-0a04-4918-ab98-13003030af96
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { test, expect } from "./coverage";
import { setupMocks } from "./mocks";

/**
 * Homepage smoke tests.
 * The app root "/" redirects to "/query", so we treat the query page as the
 * effective homepage.  These tests verify that the shell loads cleanly and the
 * top-level chrome (title, navigation, brand link) is visible.
 */
test.describe("Homepage", () => {
  test.beforeEach(async ({ page }) => {
    await setupMocks(page);
    await page.goto("/");
    // Root redirects to /query — wait for navigation to settle
    await page.waitForURL((url) => url.pathname === "/query", { timeout: 10000 });
  });

  test("page title contains Provisa", async ({ page }) => {
    const title = await page.title();
    // Vite's default project name is used for <title>; the brand must appear.
    // Accept any casing variant — e.g. "Provisa", "PROVISA".
    expect(title).toMatch(/provisa/i);
  });

  test("navigation bar is visible", async ({ page }) => {
    await expect(page.locator("nav.navbar")).toBeVisible({ timeout: 10000 });
  });

  test("brand link to Provisa is present in the navbar", async ({ page }) => {
    await expect(
      page.locator("nav.navbar .navbar-brand a", { hasText: "Provisa" })
    ).toBeVisible({ timeout: 10000 });
  });

  test("redirects from / to /query", async ({ page }) => {
    expect(page.url()).toContain("/query");
  });

  test("query page content loads after redirect", async ({ page }) => {
    // The GraphiQL container must be present — this is the main landing content.
    await expect(page.locator(".graphiql-container")).toBeVisible({ timeout: 15000 });
  });

  test("no unexpected console errors on load", async ({ page }) => {
    const errors: string[] = [];
    page.on("console", (msg) => {
      if (msg.type() === "error") errors.push(msg.text());
    });

    // Re-navigate to trigger a fresh load
    await page.goto("/");
    await page.waitForURL((url) => url.pathname === "/query", { timeout: 10000 });
    await page.locator(".graphiql-container").waitFor({ timeout: 15000 });

    const real = errors.filter(
      (e) =>
        !e.includes("Failed to fetch") &&
        !e.includes("NetworkError") &&
        !e.includes("404") &&
        !e.includes("Event") // Monaco/GraphQL worker false positives
    );
    expect(real).toEqual([]);
  });

  test("role selector is present in the navbar", async ({ page }) => {
    // The RoleSelector renders a trigger button with the text "Role: <name>"
    await expect(
      page.locator("nav.navbar .navbar-role .role-selector-trigger")
    ).toBeVisible({ timeout: 10000 });
  });

  test("at least one navigation link is visible", async ({ page }) => {
    // Depending on capabilities, the navbar should show at least one link.
    const navLinks = page.locator("nav.navbar .navbar-links a");
    const count = await navLinks.count();
    expect(count).toBeGreaterThan(0);
  });
});
