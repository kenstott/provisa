// Copyright (c) 2026 Kenneth Stott
// Canary: 841afc78-67c5-4280-a6c7-cb3a30426f7e
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { test, expect } from "./coverage";

/**
 * Smoke tests for every page in the UI.
 * Verifies each page loads, renders its key heading/elements, and has no console errors.
 */

test.describe("Page smoke tests", () => {
  let consoleErrors: string[];

  test.beforeEach(async ({ page }) => {
    consoleErrors = [];
    page.on("console", (msg) => {
      if (msg.type() === "error") consoleErrors.push(msg.text());
    });
  });

  test.afterEach(async () => {
    const real = consoleErrors.filter(
      (e) =>
        !e.includes("Failed to fetch") &&
        !e.includes("NetworkError") &&
        !e.includes("404")
    );
    expect(real, "Unexpected console errors").toEqual([]);
  });

  test("/sources — Data Sources page loads", async ({ page }) => {
    await page.goto("/sources");
    await expect(page.getByRole("heading", { name: "Data Sources" })).toBeVisible({
      timeout: 10000,
    });
    await expect(page.locator("select").first()).toBeAttached();
  });

  test("/tables — Registered Tables page loads", async ({ page }) => {
    await page.goto("/tables");
    await expect(page.getByRole("heading", { name: "Registered Tables" })).toBeVisible({
      timeout: 10000,
    });
  });

  test("/relationships — Relationships page loads", async ({ page }) => {
    await page.goto("/relationships");
    await expect(page.getByRole("heading", { name: "Relationships" })).toBeVisible({
      timeout: 10000,
    });
  });

  test("/views — Views page loads", async ({ page }) => {
    await page.goto("/views");
    await expect(page.getByRole("heading", { name: "Views" })).toBeVisible({
      timeout: 10000,
    });
  });

  test("/security — Security page loads", async ({ page }) => {
    await page.goto("/security");
    await expect(page.getByRole("heading", { name: "Roles" })).toBeVisible({
      timeout: 10000,
    });
    await expect(page.getByRole("heading", { name: "RLS Rules" })).toBeVisible({
      timeout: 10000,
    });
  });

  test("/query — Query page loads with GraphiQL", async ({ page }) => {
    await page.goto("/query");
    await expect(page.locator(".graphiql-container")).toBeVisible({
      timeout: 10000,
    });
  });

  test("/approvals — Approval Queue page loads", async ({ page }) => {
    await page.goto("/approvals");
    await expect(page.getByRole("heading", { name: "Approval Queue" })).toBeVisible({
      timeout: 10000,
    });
  });

  test("/admin — Admin Dashboard page loads", async ({ page }) => {
    // /admin is proxied to the backend by Vite, so navigate via SPA
    await page.goto("/");
    await page.getByRole("link", { name: "Admin" }).click();
    await expect(page.getByRole("heading", { name: "Admin Dashboard" })).toBeVisible({
      timeout: 15000,
    });
  });

  test("/schema — Schema Explorer page loads", async ({ page }) => {
    await page.goto("/schema");
    await expect(page.locator("iframe[title='GraphQL Voyager']")).toBeVisible({
      timeout: 15000,
    });
  });
});
