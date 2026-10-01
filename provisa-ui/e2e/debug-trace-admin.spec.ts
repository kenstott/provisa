// Copyright (c) 2026 Kenneth Stott
// Canary: 4b7e0d39-5a16-4c82-9f3d-e1c6a8b2d705
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { test, expect, BACKEND_URL } from "./coverage";
import type { Page } from "@playwright/test";

/**
 * REQ-1910: the operator's debug-trace control, driven end to end in a browser against the real
 * admin API and its control-plane store — nothing is stubbed.
 *
 * The page starts a debug window with a scope and a stated duration, shows it with the time it
 * has left, stops it, and permits and revokes the per-request hint for a role. What the request
 * path then does with those settings is covered by tests/integration/test_trace_scope_e2e.py.
 */

// openPanel budgets 60 s for the on-demand compile of the admin chunk, which the 30 s default
// per-test timeout would cut short.
test.describe.configure({ timeout: 120_000, mode: "serial" });

const API = `${BACKEND_URL}/admin/platform/debug-trace`;

interface DebugTraceState {
  windows: { id: string; scope: string; org_id: string; target: string | null }[];
  hint_roles: { org_id: string; role_id: string }[];
}

/** Leave the deployment with no window open and no role permitted, whatever a test did. */
async function clearSettings(page: Page) {
  const state: DebugTraceState = await (await page.request.get(API)).json();
  for (const w of state.windows) {
    expect((await page.request.delete(`${API}/windows/${w.id}`)).ok()).toBe(true);
  }
  for (const r of state.hint_roles) {
    expect(
      (await page.request.put(`${API}/hint-roles`, { data: { ...r, permitted: false } })).ok(),
    ).toBe(true);
  }
}

async function openPanel(page: Page) {
  await page.goto("/admin/observability");
  await page.getByTestId("observability-debug-trace-tab").click({ timeout: 60000 });
  await expect(page.getByTestId("debug-trace-panel")).toBeVisible();
  // The org list has loaded once the org select holds an option to start a window for.
  await expect(page.getByTestId("debug-trace-org").locator("option")).not.toHaveCount(0);
}

test.beforeEach(async ({ page }) => clearSettings(page));
test.afterEach(async ({ page }) => clearSettings(page));

test("a debug window is started for an org, counts down, and is stopped", async ({ page }) => {
  await openPanel(page);
  await expect(page.getByTestId("debug-trace-no-windows")).toBeVisible();

  const orgId = await page.getByTestId("debug-trace-org").inputValue();
  await page.getByTestId("debug-trace-minutes").fill("20");
  await page.getByTestId("debug-trace-start").click();

  const windows = page.getByTestId("debug-trace-windows");
  await expect(windows).toContainText(orgId);
  await expect(page.getByTestId("debug-trace-no-windows")).toHaveCount(0);

  // The control plane holds the window the page shows, with the stated duration.
  const state: DebugTraceState & { windows: { remaining_seconds: number }[] } = await (
    await page.request.get(API)
  ).json();
  expect(state.windows).toHaveLength(1);
  expect(state.windows[0].scope).toBe("org");
  expect(state.windows[0].org_id).toBe(orgId);
  expect(state.windows[0].remaining_seconds).toBeGreaterThan(19 * 60);
  expect(state.windows[0].remaining_seconds).toBeLessThanOrEqual(20 * 60);

  // Time remaining is shown and counts down by itself.
  const remaining = page.getByTestId(`debug-trace-remaining-${state.windows[0].id}`);
  await expect(remaining).toHaveText(/^(19:\d\d|20:00)$/);
  const first = await remaining.textContent();
  await expect(remaining).not.toHaveText(first ?? "", { timeout: 5000 });

  await page.getByTestId(`debug-trace-stop-${state.windows[0].id}`).click();
  await expect(page.getByTestId("debug-trace-no-windows")).toBeVisible();
  expect(((await (await page.request.get(API)).json()) as DebugTraceState).windows).toEqual([]);
});

test("a role window carries the role it covers", async ({ page }) => {
  await openPanel(page);
  await page.getByTestId("debug-trace-scope").selectOption("role");
  // A role window with no role named cannot be started.
  await expect(page.getByTestId("debug-trace-start")).toBeDisabled();
  await page.getByTestId("debug-trace-target").fill("analyst");
  await page.getByTestId("debug-trace-start").click();

  await expect(page.getByTestId("debug-trace-windows")).toContainText("analyst");
  const state: DebugTraceState = await (await page.request.get(API)).json();
  expect(state.windows.map((w) => [w.scope, w.target])).toEqual([["role", "analyst"]]);
});

test("the per-request hint is permitted for a role and revoked", async ({ page }) => {
  await openPanel(page);
  await expect(page.getByTestId("debug-trace-no-hint-roles")).toBeVisible();

  const orgId = await page.getByTestId("debug-trace-hint-org").inputValue();
  await page.getByTestId("debug-trace-hint-role").fill("analyst");
  await page.getByTestId("debug-trace-hint-permit").click();

  await expect(page.getByTestId("debug-trace-hint-roles")).toContainText("analyst");
  expect(((await (await page.request.get(API)).json()) as DebugTraceState).hint_roles).toEqual([
    { org_id: orgId, role_id: "analyst" },
  ]);

  await page.getByTestId(`debug-trace-hint-revoke-${orgId}-analyst`).click();
  await expect(page.getByTestId("debug-trace-no-hint-roles")).toBeVisible();
  expect(((await (await page.request.get(API)).json()) as DebugTraceState).hint_roles).toEqual([]);
});
