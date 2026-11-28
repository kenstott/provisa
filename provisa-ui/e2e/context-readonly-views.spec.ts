// Copyright (c) 2026 Kenneth Stott
// Canary: 7f3a2c91-0d4e-4b8a-9e15-6c2d8b4f1a07
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

// REQ-2259: the Context group's Metrics, Tags and Classification pages are read-only views of the
// components that edit them; editing stays in Derived and Admin. Each is reached from the group's
// subnav, shows its content, and offers no control that writes.

import { test, expect } from "./coverage";

test("Context Tags lists each tag with where it is applied and what depends on it", async ({
  page,
}) => {
  const read = await page.request.get("/admin/context/tags");
  expect(read.ok()).toBe(true);
  const body = (await read.json()) as { tags: { id: string; hiddenUsages: number }[] };
  const pii = body.tags.find((t) => t.id === "pii");
  expect(pii).toBeDefined();

  await page.goto("/");
  await page.getByTestId("nav-group-context").click();
  await page.locator(".subnav").getByRole("link", { name: "Tags", exact: true }).click();
  await expect(page).toHaveURL(/\/context\/tags$/);
  // REQ-2277: the scope is a tab, named in the URL.
  await page.getByTestId("tags-scope-column").click();
  await expect(page).toHaveURL(/\/context\/tags\?scope=column$/);
  // REQ-2208: the list holds one line of counts per tag; the panel names them.
  await expect(page.getByTestId("tags-where-summary-pii")).toBeVisible();
  await expect(page.getByTestId("tags-dependents-summary-pii")).toBeVisible();
  await expect(page.getByRole("columnheader", { name: "Where applied" })).toBeVisible();
  await expect(page.getByTestId("tags-create-button")).toHaveCount(0);
  await expect(page.getByTestId("tags-delete-pii")).toHaveCount(0);
  // A row opens the tag's panel, read, in the URL; it offers no Edit or Delete.
  await page.getByTestId("tags-row-pii").click();
  await expect(page).toHaveURL(/[?&]tag=pii\b/);
  await expect(page.getByTestId("tags-detail-pii")).toBeVisible();
  await expect(page.getByTestId("tags-where-pii")).toBeVisible();
  await expect(page.getByTestId("tags-dependents-pii")).toBeVisible();
  await expect(page.getByTestId("tags-edit-pii")).toHaveCount(0);
  await expect(page.getByTestId("tags-delete-pii")).toHaveCount(0);
});

test("Context Classification lists the kinds and where each was detected, without the switch", async ({
  page,
}) => {
  const read = await page.request.get("/admin/context/classification");
  expect(read.ok()).toBe(true);
  const body = (await read.json()) as { kinds: { id: string }[] };
  expect(body.kinds.map((k) => k.id)).toContain("us_ssn");

  await page.goto("/context/classification");
  const row = page.getByTestId("classification-kind-us_ssn");
  await expect(row).toBeVisible();
  await expect(row.getByTestId("classification-kind-detected-us_ssn")).toBeVisible();
  await expect(page.getByTestId("classification-kind-enabled-us_ssn")).toHaveCount(0);
  await expect(page.getByRole("switch")).toHaveCount(0);
  // A row opens the kind's panel, in the URL. REQ-2147 (amended 2026-11-25): a holder of
  // sensitive_data -- this suite's caller -- changes a kind in its panel, so the panel carries Edit.
  await row.click();
  await expect(page).toHaveURL(/[?&]kind=us_ssn\b/);
  await expect(page.getByTestId("classification-kind-detail-us_ssn")).toBeVisible();
  await expect(page.getByTestId("classification-kind-edit-us_ssn")).toBeVisible();
});

test("Context Metrics shows the metrics with no control that writes", async ({ page }) => {
  await page.goto("/context/metrics");
  await expect(page.getByTestId("metrics-table")).toBeVisible();
  await expect(page.getByTestId("metrics-new-button")).toHaveCount(0);
  await expect(page.getByTestId("metrics-bulk")).toHaveCount(0);
});
