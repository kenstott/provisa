// Copyright (c) 2026 Kenneth Stott
// Canary: 5a0d7c62-8e31-4f9b-b74a-1c3e6d05af28
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { test, expect } from "./coverage";
import type { Page, Route } from "@playwright/test";

/**
 * REQ-2247, REQ-1074: publishing from the Context Sync page, driven end to end in a browser.
 *
 * The Syncing endpoints are stubbed so the run does not depend on the e2e org holding the
 * premium entitlement (REQ-1073) or on a catalog being reachable from the test host. What the
 * page does with those responses -- which fields it renders, and exactly which fields it sends
 * back -- is the real thing under test, and is what a stub cannot fake.
 */

// openPage budgets 60 s for the on-demand compile of the admin chunk. Under the 30 s default
// per-test timeout that budget could never be spent.
test.describe.configure({ timeout: 120_000 });

const ENTRY = {
  type: "data_catalog",
  direction: "export",
  endpoint: "",
  auth_mode: "api_key",
  username: "",
  entra_tenant_id: "",
  entra_client_id: "",
  gcp_project: "",
  gcp_location: "",
  aws_region: "",
  datazone_domain_id: "",
  timeout_seconds: 30,
  domains: ["sales"],
  role: "admin",
  datazone_project_id: "",
  delta_share: false,
  import_cron: "",
  api_key_set: false,
  token_set: false,
  entra_client_secret_set: false,
  google_service_account_json_set: false,
  aws_access_key_id_set: false,
  aws_secret_access_key_set: false,
  last_run: null,
  last_import: null,
  // REQ-2259: each data product's latest outcome on this entry; none recorded yet.
  products: [],
};

const PREMIUM_STATE = {
  entitled: true,
  required_tier: "premium",
  providers: ["datazone", "openlineage", "openmetadata"],
  beta_providers: [],
  // REQ-2152: the roles an entry may be published as.
  publishable_roles: ["admin"],
  reconcile_cron: "0 * * * *",
  entries: [
    { ...ENTRY, id: "marquez", provider: "openlineage", endpoint: "http://marquez:5000" },
    {
      ...ENTRY,
      id: "datazone",
      provider: "datazone",
      aws_region: "us-east-1",
      datazone_domain_id: "dzd_1",
      aws_access_key_id_set: true,
      aws_secret_access_key_set: true,
      direction: "import",
      role: null,
    },
  ],
};

/** Capture of the PUT body, so the assertion is on what left the browser. */
type Sent = { body: { entries?: Record<string, unknown>[] } | null };

async function stubSyncing(
  page: Page,
  state: unknown,
  sent: Sent,
  extra: { publish?: unknown; health?: unknown } = {},
) {
  await page.route("**/admin/syncing", async (route: Route) => {
    const request = route.request();
    // The API path and the SPA route are the same URL; only the Accept header tells them apart
    // (vite.config.ts serves index.html for the navigation and proxies everything else). A stub
    // that answered the navigation would replace the page under test with its own JSON.
    if (request.resourceType() === "document") {
      return route.continue();
    }
    if (request.method() === "PUT") {
      sent.body = request.postDataJSON();
      return route.fulfill({ json: { success: true, entries: ["marquez", "datazone"] } });
    }
    return route.fulfill({ json: state });
  });
  await page.route("**/admin/syncing/*/health", (route: Route) =>
    route.fulfill({
      json: extra.health ?? { id: "marquez", provider: "openlineage", ok: true },
    }),
  );
  await page.route("**/admin/syncing/*/publish", (route: Route) =>
    route.fulfill({
      json: extra.publish ?? {
        ok: true,
        entries: [
          {
            id: "marquez",
            provider: "openlineage",
            ok: true,
            published: { dataset: 3 },
            total_published: 3,
            errors: [],
          },
        ],
      },
    }),
  );
}

/**
 * Navigate to the page and wait for it to render. The admin page is a lazily-loaded chunk, and
 * whichever spec navigates to `/admin` first pays Vite's on-demand compile of it, which runs past
 * the 5s default an `expect` would allow.
 */
async function openPage(page: Page) {
  await page.goto("/context/sync");
  await expect(page.getByTestId("syncing-row-marquez")).toBeVisible({ timeout: 60000 });
}

test("the nav reaches the page and the page lists each entry under its type", async ({ page }) => {
  await stubSyncing(page, PREMIUM_STATE, { body: null });
  await openPage(page);

  await expect(
    page.getByTestId("syncing-section-data_catalog").getByTestId("syncing-row-datazone"),
  ).toBeVisible();
  await expect(page.getByTestId("syncing-reconcile-cron")).toHaveValue("0 * * * *");
});

test("turning a DataZone catalog's Export on asks for its owning project and role", async ({
  page,
}) => {
  const sent: Sent = { body: null };
  await stubSyncing(page, PREMIUM_STATE, sent);
  await openPage(page);

  await page.getByTestId("syncing-row-datazone").click();
  await page.getByTestId("syncing-edit").click();
  await page.getByTestId("syncing-direction").click();
  await page.getByRole("option", { name: "Export and Import", exact: true }).click();
  await page.getByTestId("syncing-datazone-project-id").fill("prj_1");
  // REQ-2152: an entry is published as a role whose reads the saver holds.
  await page.getByTestId("syncing-role").click();
  await page.getByRole("option", { name: "admin", exact: true }).click();
  await page.getByTestId("syncing-save").click();

  await expect.poll(() => sent.body).not.toBeNull();
  const datazone = sent.body?.entries?.find((e) => e.id === "datazone");
  expect(datazone).toMatchObject({
    direction: "both",
    datazone_project_id: "prj_1",
    role: "admin",
  });
  // A credential nobody typed is not sent back.
  expect(datazone && "aws_access_key_id" in datazone).toBe(false);
});

test("an org below the tier is told which plan opens publishing", async ({ page }) => {
  // REQ-1073: the gate is enforced server-side on every publish; the page explains it rather than
  // offering a Publish now the server would refuse.
  await stubSyncing(page, { ...PREMIUM_STATE, entitled: false }, { body: null });
  await openPage(page);

  await expect(page.getByTestId("syncing-not-entitled")).toContainText("premium");
  await page.getByTestId("syncing-row-marquez").click();
  await expect(page.getByTestId("syncing-test")).toBeVisible();
  await expect(page.getByTestId("syncing-publish")).toHaveCount(0);
});

test("a refused connection reports the reason the catalog gave", async ({ page }) => {
  await stubSyncing(
    page,
    PREMIUM_STATE,
    { body: null },
    {
      health: {
        id: "marquez",
        provider: "openlineage",
        ok: false,
        error: "ConnectError: Name or service not known",
      },
    },
  );
  await openPage(page);

  await page.getByTestId("syncing-row-marquez").click();
  await page.getByTestId("syncing-test").click();
  await expect(page.getByTestId("syncing-health")).toContainText("Name or service not known");
});

test("a partial publish lists the assets the catalog rejected", async ({ page }) => {
  const outcome = {
    id: "marquez",
    provider: "openlineage",
    ok: false,
    published: { dataset: 2 },
    total_published: 2,
    errors: [{ asset: "wh.public.orders", message: "422 unknown field type" }],
  };
  // After the publish the page reads the entries again, each with its last run.
  let published = false;
  const after = {
    ...PREMIUM_STATE,
    entries: PREMIUM_STATE.entries.map((e) =>
      e.id === "marquez" ? { ...e, last_run: outcome } : e,
    ),
  };
  await page.route("**/admin/syncing", async (route: Route) => {
    if (route.request().resourceType() === "document") return route.continue();
    return route.fulfill({ json: published ? after : PREMIUM_STATE });
  });
  await page.route("**/admin/syncing/*/publish", (route: Route) => {
    published = true;
    return route.fulfill({ json: { ok: false, entries: [outcome] } });
  });
  await openPage(page);

  await page.getByTestId("syncing-row-marquez").click();
  await page.getByTestId("syncing-publish").click();
  const errors = page.getByTestId("syncing-last-run-errors");
  await expect(errors).toContainText("wh.public.orders");
  await expect(errors).toContainText("422 unknown field type");
});
