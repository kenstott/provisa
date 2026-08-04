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
 * REQ-1074: the metadata-egress admin page, driven end to end in a browser.
 *
 * The egress endpoints are stubbed so the run does not depend on the e2e org holding the
 * premium entitlement (REQ-1073) or on a catalog being reachable from the test host. What the
 * page does with those responses — which fields it renders, and exactly which fields it sends
 * back — is the real thing under test, and is what a stub cannot fake.
 */

const PREMIUM_STATE = {
  entitled: true,
  required_tier: "premium",
  providers: ["openlineage", "openmetadata"],
  config: {
    enabled: true,
    provider: "openlineage",
    endpoint: "http://marquez:5000",
    auth_mode: "api_key",
    username: "",
    entra_tenant_id: "",
    entra_client_id: "",
    reconcile_cron: "0 * * * *",
    timeout_seconds: 30,
    api_key_set: true,
    token_set: false,
    entra_client_secret_set: false,
  },
  last_publish: null,
};

/** Capture of the PUT body, so the assertion is on what left the browser. */
type Sent = { body: Record<string, unknown> | null };

async function stubEgress(
  page: Page,
  state: unknown,
  sent: Sent,
  extra: { publish?: unknown; health?: unknown } = {},
) {
  await page.route("**/admin/metadata-egress", async (route: Route) => {
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
        json: { success: true, provider: "openlineage", enabled: true },
      });
    }
    return route.fulfill({ json: state });
  });
  await page.route("**/admin/metadata-egress/health", (route: Route) =>
    route.fulfill({
      json: extra.health ?? { ok: true, provider: "openlineage" },
    }),
  );
  await page.route("**/admin/metadata-egress/publish", (route: Route) =>
    route.fulfill({
      json:
        extra.publish ?? {
          provider: "openlineage",
          ok: true,
          published: { dataset: 3 },
          total_published: 3,
          errors: [],
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
  await page.goto("/admin/metadata-egress");
  await expect(
    page
      .getByTestId("metadata-egress-endpoint")
      .or(page.getByTestId("metadata-egress-not-entitled")),
  ).toBeVisible({ timeout: 60000 });
}

test("the nav reaches the tab and the tab shows the configured target", async ({ page }) => {
  await stubEgress(page, PREMIUM_STATE, { body: null });
  await openTab(page);

  await expect(page.getByTestId("metadata-egress-endpoint")).toHaveValue("http://marquez:5000");
  await expect(page.getByTestId("metadata-egress-reconcile-cron")).toHaveValue("0 * * * *");
});

test("editing the endpoint does not send back the stored credential", async ({ page }) => {
  // The stored key is reported only as set, so the password field is empty. Sending that empty
  // field would clear the credential, and the loss would only surface at the next publish.
  const sent: Sent = { body: null };
  await stubEgress(page, PREMIUM_STATE, sent);
  await openTab(page);

  await expect(page.getByTestId("metadata-egress-api-key")).toHaveValue("");
  await page.getByTestId("metadata-egress-endpoint").fill("http://marquez:5001");
  await page.getByTestId("metadata-egress-save").click();

  await expect(page.getByTestId("metadata-egress-saved")).toBeVisible();
  expect(sent.body?.endpoint).toBe("http://marquez:5001");
  expect(sent.body && "api_key" in sent.body).toBe(false);
});

test("a credential the administrator types is sent", async ({ page }) => {
  const sent: Sent = { body: null };
  await stubEgress(page, PREMIUM_STATE, sent);
  await openTab(page);

  await page.getByTestId("metadata-egress-api-key").fill("typed-key");
  await page.getByTestId("metadata-egress-save").click();

  await expect(page.getByTestId("metadata-egress-saved")).toBeVisible();
  expect(sent.body?.api_key).toBe("typed-key");
});

test("an org below the tier is told which plan opens the feature", async ({ page }) => {
  // REQ-1073: the gate is enforced server-side on every operating endpoint; the page explains it
  // rather than offering a form whose save would be refused.
  await stubEgress(page, { ...PREMIUM_STATE, entitled: false }, { body: null });
  await openTab(page);

  await expect(page.getByTestId("metadata-egress-not-entitled")).toContainText("premium");
  await expect(page.getByTestId("metadata-egress-endpoint")).toHaveCount(0);
});

test("a refused connection reports the reason the target gave", async ({ page }) => {
  await stubEgress(page, PREMIUM_STATE, { body: null }, {
    health: { ok: false, provider: "openlineage", error: "ConnectError: Name or service not known" },
  });
  await openTab(page);

  await page.getByTestId("metadata-egress-health").click();
  await expect(page.getByTestId("metadata-egress-health-result")).toContainText(
    "Name or service not known",
  );
});

test("a partial publish lists the assets the catalog rejected", async ({ page }) => {
  await stubEgress(page, PREMIUM_STATE, { body: null }, {
    publish: {
      provider: "openlineage",
      ok: false,
      published: { dataset: 2 },
      total_published: 2,
      errors: [{ asset: "wh.public.orders", message: "422 unknown field type" }],
    },
  });
  await openTab(page);

  await page.getByTestId("metadata-egress-publish").click();
  const errors = page.getByTestId("metadata-egress-publish-errors");
  await expect(errors).toContainText("wh.public.orders");
  await expect(errors).toContainText("422 unknown field type");
});
