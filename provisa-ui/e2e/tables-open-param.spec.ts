// Copyright (c) 2026 Kenneth Stott
// Canary: 3d6f0a52-8a7e-4c19-9b0e-71c2f4a5d9e8
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

// REQ-2277 rule 4: /tables?open=<source>.<table> leaves the URL naming the open table.

import { test, expect } from "./coverage";

test("/tables?open=pet-store-sqlite.pets becomes ?table=<id>", async ({ page }) => {
  await page.goto("/tables?open=pet-store-sqlite.pets");
  await page.waitForURL(/[?&]table=\d+/, { timeout: 30000 });
  const params = new URL(page.url()).searchParams;
  expect(params.get("table")).toMatch(/^\d+$/);
  expect(params.has("open")).toBe(false);
});
