// Copyright (c) 2026 Kenneth Stott
// Canary: e43c1581-275a-47e7-9350-8851294dfd6e
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
 * Navigation tests — verify that clicking links in the NavBar changes the URL
 * and renders the expected page heading.
 *
 * All API endpoints are mocked so the tests do not depend on a running backend.
 * Auth is disabled in the default dev build, so RequireAuth passes through.
 */
test.describe("Navigation", () => {
  test.beforeEach(async ({ page }) => {
    await setupMocks(page);
    // Start at the root which redirects to /query
    await page.goto("/");
    await page.waitForURL((url) => url.pathname === "/query", { timeout: 10000 });
    // Wait for the navbar to be fully rendered
    await page.locator("nav.navbar").waitFor({ timeout: 10000 });
  });

  // ── Link-click navigation ──────────────────────────────────────────────────

  test("clicking Sources link navigates to /sources", async ({ page }) => {
    await page.getByRole("link", { name: "Sources" }).click();
    await page.waitForURL("**/sources", { timeout: 10000 });
    expect(page.url()).toContain("/sources");
    await expect(page.getByRole("heading", { name: "Data Sources" })).toBeVisible({
      timeout: 10000,
    });
  });

  test("clicking Tables link navigates to /tables", async ({ page }) => {
    await page.getByRole("link", { name: "Tables" }).click();
    await page.waitForURL("**/tables", { timeout: 10000 });
    expect(page.url()).toContain("/tables");
    await expect(page.getByRole("heading", { name: "Registered Tables" })).toBeVisible({
      timeout: 10000,
    });
  });

  test("clicking Relationships link navigates to /relationships", async ({ page }) => {
    await page.getByRole("link", { name: "Relationships" }).click();
    await page.waitForURL("**/relationships", { timeout: 10000 });
    expect(page.url()).toContain("/relationships");
    await expect(page.getByRole("heading", { name: "Relationships" })).toBeVisible({
      timeout: 10000,
    });
  });

  test("clicking Views link navigates to /views", async ({ page }) => {
    await page.getByRole("link", { name: "Views" }).click();
    await page.waitForURL("**/views", { timeout: 10000 });
    expect(page.url()).toContain("/views");
    await expect(page.getByRole("heading", { name: "Views" })).toBeVisible({
      timeout: 10000,
    });
  });

  test("clicking Security link navigates to /security", async ({ page }) => {
    await page.getByRole("link", { name: "Security" }).click();
    await page.waitForURL("**/security", { timeout: 10000 });
    expect(page.url()).toContain("/security");
    await expect(page.getByRole("heading", { name: "Roles" })).toBeVisible({
      timeout: 10000,
    });
  });

  test("clicking Query link navigates to /query", async ({ page }) => {
    // First go elsewhere so the click is meaningful
    await page.goto("/sources");
    await page.locator("nav.navbar").waitFor({ timeout: 10000 });

    await page.getByRole("link", { name: "Query" }).click();
    await page.waitForURL("**/query", { timeout: 10000 });
    expect(page.url()).toContain("/query");
    await expect(page.locator(".graphiql-container")).toBeVisible({ timeout: 15000 });
  });

  test("clicking Approvals link navigates to /approvals", async ({ page }) => {
    await page.getByRole("link", { name: "Approvals" }).click();
    await page.waitForURL("**/approvals", { timeout: 10000 });
    expect(page.url()).toContain("/approvals");
    await expect(page.getByRole("heading", { name: "Approval Queue" })).toBeVisible({
      timeout: 10000,
    });
  });

  test("clicking Admin link navigates to /admin", async ({ page }) => {
    await page.getByRole("link", { name: "Admin" }).click();
    await page.waitForURL("**/admin", { timeout: 10000 });
    expect(page.url()).toContain("/admin");
    await expect(page.getByRole("heading", { name: "Admin Dashboard" })).toBeVisible({
      timeout: 15000,
    });
  });

  // ── Back navigation ────────────────────────────────────────────────────────

  test("browser Back button returns to the previous page", async ({ page }) => {
    await page.getByRole("link", { name: "Sources" }).click();
    await page.waitForURL("**/sources", { timeout: 10000 });

    await page.goBack();
    await page.waitForURL("**/query", { timeout: 10000 });
    expect(page.url()).toContain("/query");
  });

  // ── Brand link ─────────────────────────────────────────────────────────────

  test("clicking the Provisa brand link navigates to / (then redirects to /query)", async ({ page }) => {
    // Navigate away first
    await page.goto("/sources");
    await page.locator("nav.navbar").waitFor({ timeout: 10000 });

    await page.locator("nav.navbar .navbar-brand a").click();
    // Root "/" immediately redirects to "/query"
    await page.waitForURL((url) => url.pathname === "/query", { timeout: 10000 });
    expect(page.url()).toContain("/query");
  });

  // ── NavBar link count ──────────────────────────────────────────────────────

  test("all expected nav links are present in the navbar", async ({ page }) => {
    const expectedLinks = [
      "Sources",
      "Tables",
      "Relationships",
      "Views",
      "Security",
      "Query",
      "Approvals",
      "Actions",
      "Admin",
    ];

    for (const name of expectedLinks) {
      await expect(
        page.getByRole("link", { name }),
        `NavBar should contain a "${name}" link`
      ).toBeVisible({ timeout: 5000 });
    }
  });
});
