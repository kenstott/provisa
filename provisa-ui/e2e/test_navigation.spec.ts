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
 *
 * NavBar structure:
 *   Top-level NavLinks: Sources, Tables, Relationships
 *   Group buttons (navigate to first item + expand subnav): Explore, Model, Security, Admin
 *   Subnav items (NavLinks, visible when group is active):
 *     Explore → Schema, GraphQL (/query), Cypher (/graph), SQL (/sql)
 *     Model   → Views, Commands
 *     Security → Policies (/security), Approvals
 *     Admin   → Overview (/admin/overview), Domains, Materialized Views, …
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

  // ── Top-level link navigation ──────────────────────────────────────────────

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
    await expect(page.getByRole("heading", { name: "Relationships", exact: true })).toBeVisible({
      timeout: 10000,
    });
  });

  // ── Group button navigation (navigates to first item in group) ─────────────

  test("clicking Model group button navigates to /views", async ({ page }) => {
    await page.getByRole("button", { name: "Model" }).click();
    await page.waitForURL("**/views", { timeout: 10000 });
    expect(page.url()).toContain("/views");
    await expect(page.getByRole("heading", { name: "Views" })).toBeVisible({
      timeout: 10000,
    });
  });

  test("clicking Security group button navigates to /security", async ({ page }) => {
    await page.getByRole("button", { name: "Security" }).click();
    await page.waitForURL("**/security", { timeout: 10000 });
    expect(page.url()).toContain("/security");
    await expect(page.getByRole("heading", { name: "Roles" })).toBeVisible({
      timeout: 10000,
    });
  });

  test("clicking Admin group button navigates to /admin/overview", async ({ page }) => {
    await expect(page.locator("nav.navbar").getByRole("button", { name: "Admin", exact: true })).toBeVisible({ timeout: 10000 });
    await page.getByRole("button", { name: "Admin" }).click();
    await page.waitForURL("**/admin/**", { timeout: 10000 });
    expect(page.url()).toContain("/admin");
    await expect(page.getByRole("heading", { name: "Admin Dashboard" })).toBeVisible({
      timeout: 15000,
    });
  });

  // ── Subnav item navigation (expand group, then click item) ─────────────────

  test("clicking Approvals subnav navigates to /approvals", async ({ page }) => {
    // Expand Security group
    await page.getByRole("button", { name: "Security" }).click();
    await page.waitForURL("**/security", { timeout: 10000 });
    // Approvals subnav link is now visible
    await page.getByRole("link", { name: "Approvals" }).click();
    await page.waitForURL("**/approvals", { timeout: 10000 });
    expect(page.url()).toContain("/approvals");
    await expect(page.getByRole("heading", { name: "Approval Queue" })).toBeVisible({
      timeout: 10000,
    });
  });

  test("clicking GraphQL subnav navigates to /query", async ({ page }) => {
    // Navigate away first so the click is meaningful
    await page.goto("/sources");
    await page.locator("nav.navbar").waitFor({ timeout: 10000 });

    // Expand Explore group
    await expect(page.locator("nav.navbar").getByRole("button", { name: "Explore", exact: true })).toBeVisible({ timeout: 10000 });
    await page.getByRole("button", { name: "Explore" }).click();
    await page.waitForURL(/\/(schema|query|graph|sql)/, { timeout: 10000 });
    // Click GraphQL subnav
    await page.getByRole("link", { name: "GraphQL" }).click();
    await page.waitForURL("**/query", { timeout: 10000 });
    expect(page.url()).toContain("/query");
    await expect(page.locator(".graphiql-container")).toBeVisible({ timeout: 15000 });
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

  // ── NavBar element presence ────────────────────────────────────────────────

  test("all expected top-level nav elements are present in the navbar", async ({ page }) => {
    const expectedLinks = ["Sources", "Tables", "Relationships"];
    const expectedButtons = ["Explore", "Model", "Security", "Admin"];

    for (const name of expectedLinks) {
      await expect(
        page.getByRole("link", { name }),
        `NavBar should contain a "${name}" link`
      ).toBeVisible({ timeout: 5000 });
    }

    for (const name of expectedButtons) {
      await expect(
        page.locator("nav.navbar").getByRole("button", { name, exact: true }),
        `NavBar should contain a "${name}" group button`
      ).toBeVisible({ timeout: 5000 });
    }
  });
});
