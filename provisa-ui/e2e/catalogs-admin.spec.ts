// Copyright (c) 2026 Kenneth Stott
// Canary: 6d2b8f41-a3e7-4c19-9f05-e81c4d7a2b36
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { test, expect, BACKEND_URL } from "./coverage";
import type { APIRequestContext, Page } from "@playwright/test";

/**
 * REQ-2122: the Catalogs panel, driven in a browser against the real server.
 *
 * A catalog is connected once: its provider, address, credential and the domains that work with
 * it. What is checked is what the server keeps and answers: the catalog comes back after a reload
 * with its credential reported as set and never shown, and the connection test reports the reason
 * the address refused it.
 */

test.describe.configure({ mode: "serial", timeout: 120_000 });

const CATALOGS = `${BACKEND_URL}/admin/catalogs`;

/** Nothing listens on the discard port, so the connection test is refused by the host itself. */
const UNREACHABLE = "http://127.0.0.1:9";

async function domainId(request: APIRequestContext): Promise<string> {
  const answer = await request.post(`${BACKEND_URL}/admin/graphql`, {
    data: { query: "{ domains { id } }" },
  });
  const domains = ((await answer.json()) as { data: { domains: { id: string }[] } }).data.domains;
  expect(domains.length, "the e2e org has a domain to assign").toBeGreaterThan(0);
  return domains[0].id;
}

/** Remove ``id`` from the org's catalogs, keeping every other one as it is (each credential left
 * out of the body is kept). */
async function forget(request: APIRequestContext, id: string) {
  const held = (await (await request.get(CATALOGS)).json()) as {
    catalogs: Record<string, unknown>[];
  };
  const kept = held.catalogs
    .filter((c) => c.id !== id)
    .map((c) => Object.fromEntries(Object.entries(c).filter(([k]) => !k.endsWith("_set"))));
  const answer = await request.put(CATALOGS, { data: { catalogs: kept } });
  expect(answer.ok(), await answer.text()).toBe(true);
}

async function openTab(page: Page) {
  await page.goto("/admin/catalogs");
  await expect(page.getByTestId("catalogs-add")).toBeVisible({ timeout: 60000 });
}

test("a catalog is connected once, kept with its domains, and its credential never shown", async ({
  page,
  request,
}) => {
  const id = `e2e-catalog-${Date.now().toString(36)}`;
  const domain = await domainId(request);
  try {
    await openTab(page);
    const before = await page.locator('[data-testid^="catalog-entry-"]').count();
    await page.getByTestId("catalogs-add").click();
    const i = before;
    await page.getByTestId(`catalog-id-${i}`).fill(id);
    await page.getByTestId(`catalog-provider-${i}`).click();
    await page.getByRole("option", { name: "openmetadata", exact: true }).click();
    await page.getByTestId(`catalog-domains-${i}`).click();
    await page.getByRole("option", { name: domain, exact: true }).click();
    await page.getByTestId(`catalog-endpoint-${i}`).fill(UNREACHABLE);
    await page.getByTestId(`catalog-api-key-${i}`).fill("typed-key");
    await page.getByTestId("catalogs-save").click();
    await expect(page.getByTestId("catalogs-saved")).toBeVisible();

    // What the server kept, read back as the panel reads it.
    const held = (await (await request.get(CATALOGS)).json()) as {
      catalogs: Record<string, unknown>[];
    };
    const kept = held.catalogs.find((c) => c.id === id);
    expect(kept).toMatchObject({
      provider: "openmetadata",
      endpoint: UNREACHABLE,
      domains: [domain],
      api_key_set: true,
    });
    expect(kept && "api_key" in kept).toBe(false);

    await page.reload();
    await expect(page.getByTestId(`catalog-id-${i}`)).toHaveValue(id, { timeout: 60000 });
    await expect(page.getByTestId(`catalog-api-key-${i}`)).toHaveValue("");
    await expect(page.getByTestId(`catalog-api-key-${i}`)).toHaveAttribute(
      "placeholder",
      "Set — leave blank to keep",
    );

    // The connection test is the provider's own health check against the address as kept.
    await page.getByTestId(`catalog-test-${i}`).click();
    const health = page.getByTestId(`catalog-health-${i}`);
    await expect(health).toBeVisible({ timeout: 60000 });
    await expect(health).not.toContainText("The catalog accepted the connection.");
    await expect(health).toContainText(/Connect|refused/i);
  } finally {
    await forget(request, id);
  }
});

test("a catalog with no domain is refused when it is saved", async ({ page, request }) => {
  const id = `e2e-catalog-nodomain-${Date.now().toString(36)}`;
  try {
    await openTab(page);
    const i = await page.locator('[data-testid^="catalog-entry-"]').count();
    await page.getByTestId("catalogs-add").click();
    await page.getByTestId(`catalog-id-${i}`).fill(id);
    await page.getByTestId(`catalog-provider-${i}`).click();
    await page.getByRole("option", { name: "openmetadata", exact: true }).click();
    await page.getByTestId(`catalog-endpoint-${i}`).fill(UNREACHABLE);
    await page.getByTestId("catalogs-save").click();

    await expect(page.getByTestId("catalogs-error")).toContainText("assigned to no domain");
    const held = (await (await request.get(CATALOGS)).json()) as { catalogs: { id: string }[] };
    expect(held.catalogs.map((c) => c.id)).not.toContain(id);
  } finally {
    await forget(request, id);
  }
});
