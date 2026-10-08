// Copyright (c) 2026 Kenneth Stott
// Canary: 8fe55e15-84e6-4594-92fe-76c2f4c997b5
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1923: Stripe, the first brand carried by the OpenAPI source, through the same three screens
// every source goes through (Sources form -> Register Table form -> SQL page), against a live
// Stripe sandbox. The secret key is STRIPE_API_KEY in the repo-root .env; it is never printed, and
// the run refuses a key that is not a test-mode one.
//
// A sandbox starts empty and there is no fixture for Stripe itself, so the customers this reads
// are created through Stripe's own API before the UI is driven and deleted after -- the same idea
// as source-to-query-cloud-warehouse.spec.ts's seed and teardown. They are created three to a
// run and read two to a page, so the read follows Stripe's paging to its end.

import type { Page } from "@playwright/test";

import { test, expect } from "./coverage";
import {
  createDomain,
  openRegisterForm,
  openSourcesForm,
  pickSchemaAndTable,
  runSqlOnPage,
  setSourceCacheTtl,
  submitRegisterAndExpectListed,
  submitSourceAndExpectListed,
} from "./source-to-query-helpers";

const KEY = process.env.STRIPE_API_KEY ?? "";
const STRIPE = "https://api.stripe.com/v1";

async function stripe(method: "POST" | "DELETE", path: string, form?: Record<string, string>) {
  const res = await fetch(`${STRIPE}${path}`, {
    method,
    headers: { Authorization: `Bearer ${KEY}` },
    body: form ? new URLSearchParams(form) : undefined,
  });
  const body = await res.json();
  if (!res.ok) throw new Error(`Stripe ${method} ${path}: ${res.status} ${JSON.stringify(body)}`);
  return body as { id: string };
}

/** Set the page size of the table being registered, in the Paging section the form proposes. */
async function setPageSize(page: Page, size: number) {
  const paging = page.getByTestId("paging-field");
  await expect(paging.getByRole("textbox", { name: "Starts-after parameter" })).toBeVisible({
    timeout: 30000,
  });
  await paging.getByRole("textbox", { name: "Page size", exact: true }).fill(String(size));
}

test.describe("Stripe through the UI (REQ-1923)", () => {
  test.skip(!KEY, "no Stripe sandbox key in this environment (STRIPE_API_KEY)");

  const stamp = Date.now();
  const names = [0, 1, 2].map((n) => `Provisa e2e ${stamp} ${n}`);
  const customers: string[] = [];

  test.beforeAll(async () => {
    if (!/^(sk|rk)_test_/.test(KEY)) throw new Error("STRIPE_API_KEY is not a test-mode key");
    for (const name of names) customers.push((await stripe("POST", "/customers", { name })).id);
  });

  test.afterAll(async () => {
    for (const id of customers) await stripe("DELETE", `/customers/${id}`);
  });

  test("stripe: add the source, register a table, query it on the SQL page", async ({ page }) => {
    test.setTimeout(300000);
    const sourceId = `e2e_stripe_${stamp}`;

    // 1. Sources form -- the brand asks for a secret key and nothing else: no spec, no address.
    await openSourcesForm(page);
    await page.getByTestId("sources-id-input").fill(sourceId);
    await page.getByTestId("sources-type-select").selectOption("stripe");
    await expect(page.getByTestId("openapi-spec-path-input")).toHaveCount(0);
    await page.getByTestId("brand-token-input").fill(KEY);
    // REQ-1907: the native engine serves this source from a replica -- it needs a landing clock.
    await setSourceCacheTtl(page, 300);
    await submitSourceAndExpectListed(page, sourceId);

    // The key is stored; what the admin reports of the source's auth never carries it (REQ-320).
    const listed = await page.request.get("/admin/openapi/list");
    expect(listed.ok(), await listed.text()).toBeTruthy();
    const text = await listed.text();
    expect(text).not.toContain(KEY);
    const mine = (JSON.parse(text) as { source_id: string; auth_config: unknown }[]).find(
      (s) => s.source_id === sourceId,
    );
    expect(mine?.auth_config).toEqual({ type: "bearer" });

    // 2. Register Table form -- adding the source registered nothing (REQ-316); the customers
    // list is picked from what the shipped spec offers, with the paging the spec proposes for it.
    const domain = `e2e-stripe-${stamp}`;
    await createDomain(page, domain);
    await openRegisterForm(page, sourceId, domain);
    await pickSchemaAndTable(page, "openapi", "GetCustomers");
    await expect(page.getByTestId("register-table-col-selected-id")).toBeVisible();
    await setPageSize(page, 2);
    const registered = await submitRegisterAndExpectListed(page, sourceId);

    // 3. SQL page -- the three customers of this run, read from Stripe two to a page.
    const rows = await runSqlOnPage(
      page,
      `SELECT id, name FROM ${domain.replace(/-/g, "_")}.${registered} ` +
        `WHERE name LIKE 'Provisa e2e ${stamp} %' ORDER BY name`,
    );
    expect(rows).toEqual(names.map((name, n) => [customers[n], name]));
  });
});
