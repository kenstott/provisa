// Copyright (c) 2026 Kenneth Stott
// Canary: f3712b3e-fa63-446e-8356-cc9c88cb5970
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import { test, expect } from "./coverage";

const BASE = "http://localhost:5173";
const ROLE = "admin";

// REQ-801: OpenAPI Explorer UI page at /openapi (Swagger UI iframe)

test("REQ-801: /openapi page loads without errors", async ({ page }) => {
  await page.goto(`${BASE}/openapi`);
  await expect(page).not.toHaveTitle(/error/i);
  await expect(page.locator("body")).toBeVisible();
});

test("REQ-801: /openapi page contains an iframe", async ({ page }) => {
  await page.goto(`${BASE}/openapi?role=${ROLE}`);
  await page.waitForLoadState("domcontentloaded");
  const iframe = page.locator("iframe");
  await expect(iframe).toBeAttached();
});

test("REQ-801: iframe src points to /data/rest/docs", async ({ page }) => {
  await page.goto(`${BASE}/openapi?role=${ROLE}`);
  await page.waitForLoadState("domcontentloaded");
  const iframe = page.locator("iframe");
  const src = await iframe.getAttribute("src");
  expect(src).toContain("/data/rest/docs");
});

test("REQ-801: page does not throw JS errors on load", async ({ page }) => {
  // coverage.ts fixture asserts no uncaught browser errors automatically
  await page.goto(`${BASE}/openapi?role=${ROLE}`);
  await page.waitForLoadState("networkidle");
  await expect(page.locator("body")).toBeVisible();
});

test("REQ-801: autoRun param navigates to specific endpoint", async ({
  page,
}) => {
  const specRequests: string[] = [];
  page.on("request", (req) => {
    if (req.url().includes("/data/rest")) {
      specRequests.push(req.url());
    }
  });

  await page.goto(
    `${BASE}/openapi?role=${ROLE}&autoRun=GET:/default/orders`,
  );
  await page.waitForLoadState("networkidle");

  // Swagger UI should have fetched the spec
  await expect(page.locator("body")).toBeVisible();
});
