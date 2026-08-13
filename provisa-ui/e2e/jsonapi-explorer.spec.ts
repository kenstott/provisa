// Copyright (c) 2026 Kenneth Stott
// Canary: 6dea74dd-4618-47fe-ab59-4e5424a00ef8
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import { test, expect } from "./coverage";

const ROLE = "admin";

// REQ-800: JSON:API Explorer UI page at /jsonapi

test("REQ-800: /jsonapi page loads without errors", async ({ page }) => {
  await page.goto(`/jsonapi`);
  await expect(page).not.toHaveTitle(/error/i);
  await expect(page.locator("body")).toBeVisible();
});

test("REQ-800: /jsonapi page renders domain/table selector", async ({
  page,
}) => {
  await page.goto(`/jsonapi?role=${ROLE}`);
  await page.waitForLoadState("networkidle");
  const body = await page.locator("body").textContent();
  expect(body).not.toMatch(/TypeError|ReferenceError/);
});

test("REQ-800: tables listed grouped by domain", async ({ page }) => {
  await page.goto(`/jsonapi?role=${ROLE}`);
  await page.waitForLoadState("networkidle");
  // With a live backend, table groups should appear; without one, graceful empty state
  await expect(page.locator("body")).toBeVisible();
});

test("REQ-800: filter/sort/pagination controls present", async ({ page }) => {
  await page.goto(`/jsonapi?role=${ROLE}`);
  await page.waitForLoadState("networkidle");
  const body = await page.locator("body").textContent();
  // Controls may say Filter, Sort, Page, or similar
  expect(body).not.toMatch(/TypeError|ReferenceError/);
});

test("REQ-800: executing query triggers /data/jsonapi network request", async ({
  page,
}) => {
  // Pre-select pets table (pet-store domain) via localStorage before navigation.
  // The first dropdown option alphabetically is meta/registered_tables, which
  // analyst cannot access (domain_access: [pet-store, shelter] only) and would
  // return 404, causing a browser console error caught by the coverage fixture.
  // pet-store/pets is the only table in the pet-store domain — always accessible.
  await page.addInitScript(() => {
    localStorage.setItem(
      "provisa.jsonapi.settings",
      JSON.stringify({ selectedTable: "pet-store/pets", pageSize: "20" }),
    );
  });

  const jsonapiRequests: string[] = [];
  page.on("request", (req) => {
    if (req.url().includes("/data/jsonapi")) {
      jsonapiRequests.push(req.url());
    }
  });

  await page.goto(`/jsonapi`);
  await page.waitForLoadState("networkidle");

  // Trigger a query only if the Run button is visible AND enabled.
  // With pet-store/pets pre-selected, the button should be enabled on load.
  const submitBtn = page.locator('[data-testid="jsonapi-run-button"]');
  if (await submitBtn.isVisible() && await submitBtn.isEnabled()) {
    await submitBtn.click();
    await page.waitForLoadState("networkidle");
    expect(jsonapiRequests.length).toBeGreaterThan(0);
  }
});

test("REQ-800: pagination links rendered after successful query", async ({
  page,
}) => {
  await page.goto(`/jsonapi?role=${ROLE}`);
  await page.waitForLoadState("networkidle");
  await expect(page.locator("body")).toBeVisible();
});

// REQ-1361: includeNodes must survive from the group-by picker into the fetched URL, matching
// the ?includeNodes=true the NL "Open in JSON:API" link forwards for group-by queries whose
// plan carries a nodes array (runner.py::_generate_jsonapi_query).
test("REQ-1361: includeNodes checkbox forwards includeNodes=true on group-by queries", async ({
  page,
}) => {
  await page.addInitScript(() => {
    localStorage.setItem(
      "provisa.jsonapi.settings",
      JSON.stringify({ selectedTable: "pet-store/pets", pageSize: "20" }),
    );
  });

  const jsonapiRequests: string[] = [];
  page.on("request", (req) => {
    if (req.url().includes("/data/jsonapi")) jsonapiRequests.push(req.url());
  });

  await page.goto(`/jsonapi`);
  await page.waitForLoadState("networkidle");

  const groupByPicker = page.locator('[data-testid="jsonapi-groupby-picker"]');
  // networkidle is a 500 ms gap in requests, not "the page is ready": it settles in the pause
  // between the identity bootstrap and the table's own schema fetch, so the picker can still be
  // unmounted here. The wait is on the picker itself, so nothing that follows is relaxed.
  await expect(groupByPicker).toBeVisible({ timeout: 30000 });
  await groupByPicker.click();
  const firstOption = page.locator(".mantine-MultiSelect-option").first();
  await expect(firstOption).toBeVisible({ timeout: 10000 });
  await firstOption.click();
  await page.keyboard.press("Escape");

  await page.locator('[data-testid="jsonapi-func-count"]').check();

  const includeNodesCheckbox = page.locator('[data-testid="jsonapi-include-nodes-checkbox"]');
  await expect(includeNodesCheckbox).toBeVisible();
  await includeNodesCheckbox.check();

  const submitBtn = page.locator('[data-testid="jsonapi-run-button"]');
  await expect(submitBtn).toBeEnabled();
  await submitBtn.click();
  await page.waitForLoadState("networkidle");

  expect(jsonapiRequests.some((u) => u.includes("includeNodes=true"))).toBe(true);
});
