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
 * REQ-1074, REQ-2122: the metadata-export admin page, driven end to end in a browser.
 *
 * The export endpoints are stubbed so the run does not depend on the e2e org holding the
 * premium entitlement (REQ-1073) or on a catalog being reachable from the test host. What the
 * page does with those responses — which fields it renders, and exactly which fields it sends
 * back — is the real thing under test, and is what a stub cannot fake.
 */

// openTab budgets 60 s for the on-demand compile of the admin chunk. Under the 30 s default
// per-test timeout that budget could never be spent — the test clock expired first, reporting a
// test timeout while the wait was still well inside its own allowance.
test.describe.configure({ timeout: 120_000 });

const PREMIUM_STATE = {
  entitled: true,
  required_tier: "premium",
  providers: ["openlineage", "openmetadata"],
  beta_providers: [],
  // REQ-2152: the roles a target may be published as.
  publishable_roles: ["admin"],
  config: {
    enabled: true,
    reconcile_cron: "0 * * * *",
    // REQ-2118, REQ-2122: an org's targets, each naming a catalog with its export settings, and
    // the role it is published as (REQ-2152).
    targets: [{ catalog: "marquez", datazone_project_id: "", delta_share: false, role: "admin" }],
  },
  last_publish: null,
};

/** The org's catalogs, as the Catalogs panel reports them (REQ-2122). */
const CATALOGS = {
  providers: ["datazone", "openlineage"],
  catalogs: [
    {
      id: "marquez",
      provider: "openlineage",
      endpoint: "http://marquez:5000",
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
      api_key_set: true,
      token_set: false,
      entra_client_secret_set: false,
      google_service_account_json_set: false,
      aws_access_key_id_set: false,
      aws_secret_access_key_set: false,
    },
    {
      id: "datazone",
      provider: "datazone",
      endpoint: "",
      auth_mode: "api_key",
      username: "",
      entra_tenant_id: "",
      entra_client_id: "",
      gcp_project: "",
      gcp_location: "",
      aws_region: "us-east-1",
      datazone_domain_id: "dzd_1",
      timeout_seconds: 30,
      domains: ["sales"],
      api_key_set: false,
      token_set: false,
      entra_client_secret_set: false,
      google_service_account_json_set: false,
      aws_access_key_id_set: true,
      aws_secret_access_key_set: true,
    },
  ],
};

/** Capture of the PUT body, so the assertion is on what left the browser. */
type Sent = { body: { targets?: Record<string, unknown>[] } | null };

async function stubExport(
  page: Page,
  state: unknown,
  sent: Sent,
  extra: { publish?: unknown; health?: unknown } = {},
) {
  await page.route("**/admin/metadata-export", async (route: Route) => {
    const request = route.request();
    // The API path and the SPA route are the same URL; only the Accept header tells them apart
    // (vite.config.ts serves index.html for the navigation and proxies everything else). A stub
    // that answered the navigation would replace the page under test with its own JSON.
    if (request.resourceType() === "document") {
      return route.continue();
    }
    if (request.method() === "PUT") {
      sent.body = request.postDataJSON();
      return route.fulfill({
        json: { success: true, enabled: true, targets: ["marquez"] },
      });
    }
    return route.fulfill({ json: state });
  });
  await page.route("**/admin/catalogs", (route: Route) => route.fulfill({ json: CATALOGS }));
  await page.route("**/admin/metadata-export/health", (route: Route) =>
    route.fulfill({
      json: extra.health ?? {
        ok: true,
        targets: [{ id: "marquez", provider: "openlineage", ok: true }],
      },
    }),
  );
  await page.route("**/admin/metadata-export/publish", (route: Route) =>
    route.fulfill({
      json: extra.publish ?? {
        ok: true,
        targets: [
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
 * Navigate to the tab and wait for it to render.
 *
 * The admin page is a lazily-loaded chunk, and global-setup's pre-warm fetches only the index —
 * so whichever spec navigates to `/admin` first pays Vite's on-demand compile of it, which runs
 * past the 5s default an `expect` would allow. The wait is on the tab having rendered either of
 * its two shapes, so nothing about the assertions that follow is relaxed.
 */
async function openTab(page: Page) {
  await page.goto("/admin/metadata-export");
  await expect(
    page
      .getByTestId("metadata-export-catalog-0")
      .or(page.getByTestId("metadata-export-not-entitled")),
  ).toBeVisible({ timeout: 60000 });
}

test("the nav reaches the tab and the tab shows each target by its catalog", async ({ page }) => {
  await stubExport(page, PREMIUM_STATE, { body: null });
  await openTab(page);

  await expect(page.getByTestId("metadata-export-catalog-0")).toHaveValue("marquez · openlineage");
  await expect(page.getByTestId("metadata-export-reconcile-cron")).toHaveValue("0 * * * *");
});

test("a target is added by picking a catalog, with the settings its provider needs", async ({
  page,
}) => {
  // REQ-2122: the endpoint and credentials are the catalog's; a DataZone export names the project
  // that owns its products, and nothing else is asked for.
  const sent: Sent = { body: null };
  await stubExport(page, PREMIUM_STATE, sent);
  await openTab(page);

  await page.getByTestId("metadata-export-add-target").click();
  await page.getByTestId("metadata-export-catalog-1").click();
  await page.getByRole("option", { name: "datazone · datazone" }).click();
  await page.getByTestId("metadata-export-datazone-project-id-1").fill("prj_1");
  // REQ-2152: a target is published as a role whose reads the saver holds.
  await page.getByTestId("metadata-export-role-1").click();
  await page.getByRole("option", { name: "admin" }).click();
  await page.getByTestId("metadata-export-save").click();

  await expect(page.getByTestId("metadata-export-saved")).toBeVisible();
  expect(sent.body?.targets).toEqual([
    { catalog: "marquez", datazone_project_id: "", delta_share: false, role: "admin" },
    { catalog: "datazone", datazone_project_id: "prj_1", delta_share: false, role: "admin" },
  ]);
});

test("an org below the tier is told which plan opens the feature", async ({ page }) => {
  // REQ-1073: the gate is enforced server-side on every operating endpoint; the page explains it
  // rather than offering a form whose save would be refused.
  await stubExport(page, { ...PREMIUM_STATE, entitled: false }, { body: null });
  await openTab(page);

  await expect(page.getByTestId("metadata-export-not-entitled")).toContainText("premium");
  await expect(page.getByTestId("metadata-export-catalog-0")).toHaveCount(0);
});

test("a refused connection reports the reason the target gave", async ({ page }) => {
  await stubExport(
    page,
    PREMIUM_STATE,
    { body: null },
    {
      health: {
        ok: false,
        targets: [
          {
            id: "marquez",
            provider: "openlineage",
            ok: false,
            error: "ConnectError: Name or service not known",
          },
        ],
      },
    },
  );
  await openTab(page);

  await page.getByTestId("metadata-export-health").click();
  await expect(page.getByTestId("metadata-export-health-marquez")).toContainText(
    "Name or service not known",
  );
});

test("a partial publish lists the assets the catalog rejected", async ({ page }) => {
  await stubExport(
    page,
    PREMIUM_STATE,
    { body: null },
    {
      publish: {
        ok: false,
        targets: [
          {
            id: "marquez",
            provider: "openlineage",
            ok: false,
            published: { dataset: 2 },
            total_published: 2,
            errors: [{ asset: "wh.public.orders", message: "422 unknown field type" }],
          },
        ],
      },
    },
  );
  await openTab(page);

  await page.getByTestId("metadata-export-publish").click();
  const errors = page.getByTestId("metadata-export-publish-errors");
  await expect(errors).toContainText("wh.public.orders");
  await expect(errors).toContainText("422 unknown field type");
});
