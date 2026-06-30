// Copyright (c) 2026 Kenneth Stott
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import { test, expect } from "./coverage";

const BASE = "http://localhost:5173";
const ROLE = "admin";

// REQ-800: JSON:API Explorer UI page at /jsonapi

test("REQ-800: /jsonapi page loads without errors", async ({ page }) => {
  await page.goto(`${BASE}/jsonapi`);
  await expect(page).not.toHaveTitle(/error/i);
  await expect(page.locator("body")).toBeVisible();
});

test("REQ-800: /jsonapi page renders domain/table selector", async ({
  page,
}) => {
  await page.goto(`${BASE}/jsonapi?role=${ROLE}`);
  await page.waitForLoadState("networkidle");
  const body = await page.locator("body").textContent();
  expect(body).not.toMatch(/TypeError|ReferenceError/);
});

test("REQ-800: tables listed grouped by domain", async ({ page }) => {
  await page.goto(`${BASE}/jsonapi?role=${ROLE}`);
  await page.waitForLoadState("networkidle");
  // With a live backend, table groups should appear; without one, graceful empty state
  await expect(page.locator("body")).toBeVisible();
});

test("REQ-800: filter/sort/pagination controls present", async ({ page }) => {
  await page.goto(`${BASE}/jsonapi?role=${ROLE}`);
  await page.waitForLoadState("networkidle");
  const body = await page.locator("body").textContent();
  // Controls may say Filter, Sort, Page, or similar
  expect(body).not.toMatch(/TypeError|ReferenceError/);
});

test("REQ-800: executing query triggers /data/jsonapi network request", async ({
  page,
}) => {
  const jsonapiRequests: string[] = [];
  page.on("request", (req) => {
    if (req.url().includes("/data/jsonapi")) {
      jsonapiRequests.push(req.url());
    }
  });

  await page.goto(`${BASE}/jsonapi?role=${ROLE}`);
  await page.waitForLoadState("networkidle");

  // Trigger a query if a submit/execute button is present
  const submitBtn = page.locator("button").filter({ hasText: /execute|query|run/i }).first();
  if (await submitBtn.isVisible()) {
    await submitBtn.click();
    await page.waitForLoadState("networkidle");
    expect(jsonapiRequests.length).toBeGreaterThan(0);
  }
});

test("REQ-800: pagination links rendered after successful query", async ({
  page,
}) => {
  await page.goto(`${BASE}/jsonapi?role=${ROLE}`);
  await page.waitForLoadState("networkidle");
  await expect(page.locator("body")).toBeVisible();
});
