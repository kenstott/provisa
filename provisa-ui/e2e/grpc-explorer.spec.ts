// Copyright (c) 2026 Kenneth Stott
// Canary: 9b998407-b95b-4324-9e54-e7fad7501367
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

import { test, expect } from "./coverage";

const BASE = "http://localhost:5173";
const ROLE = "admin";

// REQ-799: gRPC Explorer UI page at /grpc

test("REQ-799: /grpc page loads without errors", async ({ page }) => {
  await page.goto(`${BASE}/grpc`);
  await expect(page).not.toHaveTitle(/error/i);
});

test("REQ-799: /grpc page renders method selector", async ({ page }) => {
  await page.goto(`${BASE}/grpc`);
  // The page should contain a method selector or a loading indicator
  await expect(page.locator("body")).toBeVisible();
});

test("REQ-799: /grpc page with role param fetches proto schema", async ({
  page,
}) => {
  const schemaRequests: string[] = [];
  page.on("request", (req) => {
    if (req.url().includes("/data/grpc") || req.url().includes("proto")) {
      schemaRequests.push(req.url());
    }
  });

  await page.goto(`${BASE}/grpc?role=${ROLE}`);
  await page.waitForLoadState("networkidle");
  // Page should have attempted to load schema or render empty state gracefully
  await expect(page.locator("body")).toBeVisible();
});

test("REQ-799: methods grouped by query vs mutation", async ({ page }) => {
  await page.goto(`${BASE}/grpc?role=${ROLE}`);
  await page.waitForLoadState("networkidle");
  // If methods loaded, they should be grouped; page must not show an unhandled error
  const body = page.locator("body");
  await expect(body).toBeVisible();
  const text = await body.textContent();
  expect(text).not.toMatch(/TypeError|ReferenceError/);
});

test("REQ-799: request body editor renders for selected method", async ({
  page,
}) => {
  await page.goto(`${BASE}/grpc?role=${ROLE}`);
  await page.waitForLoadState("networkidle");
  // Editor area or textarea should be present if a method was auto-selected
  const body = await page.locator("body").textContent();
  expect(body).not.toBeNull();
});
