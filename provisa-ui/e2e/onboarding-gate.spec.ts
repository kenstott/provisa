// Copyright (c) 2026 Kenneth Stott
// Canary: 5a91e37c-6b04-42d8-95f1-e0c837b249da
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
 * REQ-1286 / REQ-1288 / REQ-1289 / REQ-1291 / REQ-1292 / REQ-1326: the routing gate a browser
 * with a stored credential passes through before it sees anything.
 *
 * Each of these is a dead end when it goes wrong, and each dead end looks like a different bug to
 * the person hitting it: a rejected credential that renders "ask your administrator" on a
 * deployment whose administrator slot is unclaimed; a valid credential that skips sign-in — the
 * only place the first-login disclosure appears — and lands on "create an organization" for a
 * deployment nobody administers yet; a backend that is merely down being reported as an access
 * decision.
 *
 * The component tests cover the same branches in isolation. What only a browser shows is the
 * whole path: real localStorage, the real router, the real order of the identity bootstrap and
 * the slot probe — which is where the "valid token skips the disclosure" defect lived.
 */

const ORG_ID = "carolco";

type Identity = {
  user_id: string | null;
  active_org_id: string | null;
  org_memberships: { org_id: string; org_name: string }[];
  assignments: { role_id: string; domain_id: string }[];
};

const MEMBER: Identity = {
  user_id: "carol",
  active_org_id: ORG_ID,
  org_memberships: [{ org_id: ORG_ID, org_name: "Carol Co" }],
  assignments: [{ role_id: "org_admin", domain_id: "*" }],
};

async function stubDeployment(
  page: Page,
  opts: {
    identity?: Identity;
    identityStatus?: number;
    slotUnclaimed?: boolean;
    token?: string;
  } = {},
) {
  const { identity, identityStatus, slotUnclaimed = false, token = "tok-carol" } = opts;

  await page.addInitScript(
    ([t, org]) => {
      localStorage.setItem("provisa_token", t as string);
      // A session that predates this visit — the gate must clear these, not carry them into the
      // next identity (REQ-1326).
      localStorage.setItem("provisa_org", org as string);
      localStorage.setItem("provisa_role", "org_admin");
    },
    [token, "stale-org"],
  );

  await page.route("**/setup/status", (route: Route) =>
    route.fulfill({ json: { needs_setup: false, demo_mode: false, auth_enabled: true } }),
  );
  await page.route("**/firebase-config.js", (route: Route) =>
    route.fulfill({ body: "", contentType: "application/javascript" }),
  );
  await page.route("**/auth/me", (route: Route) =>
    identityStatus !== undefined
      ? route.fulfill({ status: identityStatus, json: { detail: "nope" } })
      : route.fulfill({ json: { ...identity, email: "carol@example.com", dev_mode: false } }),
  );
  await page.route("**/auth/bootstrap-status", (route: Route) =>
    route.fulfill({ json: { unclaimed: slotUnclaimed } }),
  );
  await page.route("**/admin/graphql", (route: Route) =>
    route.fulfill({ json: { data: { roles: [], domains: [] } } }),
  );
}

const stored = (page: Page, key: string) =>
  page.evaluate((k) => localStorage.getItem(k), key);

/** The sign-in screen, whichever provider the deployment renders it with. */
const signIn = (page: Page) => page.getByRole("heading", { name: "Login" });

// Each test below deliberately drives a refused or failing identity response, so the browser
// logs the failed fetch however correctly the app handles it. Anything else still fails.
test.use({
  allowedBrowserErrors: [
    "status of 401",
    "status of 403",
    "status of 500",
    "Failed to load resource",
  ],
});

test("a credential the deployment rejects returns the visitor to sign-in", async ({ page }) => {
  // REQ-1289: 401 means the token is stale — expired, or from before this deployment knew this
  // identity. Nothing on a "no access" panel fixes that, and on a deployment with no administrator
  // yet, the administrator it tells them to ask does not exist.
  await stubDeployment(page, { identityStatus: 401 });

  await page.goto("/");

  await expect(signIn(page)).toBeVisible();
  expect(await stored(page, "provisa_token")).toBeNull();
});

test("a rejected credential clears the whole session, not just the token", async ({ page }) => {
  // REQ-1326: a leftover provisa_org rides on the next request's org header, binding the next
  // identity to an org it may not belong to.
  await stubDeployment(page, { identityStatus: 403 });

  await page.goto("/");
  await expect(signIn(page)).toBeVisible();

  expect(await stored(page, "provisa_token")).toBeNull();
  expect(await stored(page, "provisa_org")).toBeNull();
  expect(await stored(page, "provisa_role")).toBeNull();
});

test("a failing backend is not reported as an access decision", async ({ page }) => {
  // REQ-1286: /auth/me returning 500 means no access decision was ever made. Telling the visitor
  // their account lacks access states one that did not happen, and sends them to ask for an
  // invitation that would not help.
  await stubDeployment(page, { identityStatus: 500 });

  await page.goto("/");

  await expect(page.getByTestId("identity-unavailable")).toBeVisible();
  await expect(page.getByTestId("identity-unavailable-retry")).toBeVisible();
  // The credential is intact: it was never rejected.
  expect(await stored(page, "provisa_token")).toBe("tok-carol");
});

test("an unclaimed platform-admin slot outranks a valid session", async ({ page }) => {
  // REQ-1292/REQ-1288: a browser holding a good credential would otherwise skip sign-in — the one
  // place the first-login disclosure renders — and land inside a deployment nobody administers.
  await stubDeployment(page, { identity: { ...MEMBER }, slotUnclaimed: true });

  await page.goto("/");

  // The sign-in page renders in its claim state — REQ-1288's disclosure, which is the whole
  // reason the credential is dropped rather than followed into the app.
  await expect(page.getByTestId("first-login-notice")).toBeVisible();
  await expect(page.getByRole("heading", { name: "Set up this deployment" })).toBeVisible();
  expect(await stored(page, "provisa_token")).toBeNull();
  expect(await stored(page, "provisa_org")).toBeNull();
});

test("a member with a claimed slot is routed into the app", async ({ page }) => {
  // REQ-1291: the identity bootstrap re-runs when the token changes, so a member does not sit on
  // the gate waiting for a refresh that never comes.
  await stubDeployment(page, { identity: { ...MEMBER }, slotUnclaimed: false });

  await page.goto("/");

  await expect(signIn(page)).toHaveCount(0);
  await expect(page.getByTestId("onboard-org-page")).toHaveCount(0);
  expect(await stored(page, "provisa_token")).toBe("tok-carol");
});

test("a member-less user is offered onboarding rather than an empty app", async ({ page }) => {
  await stubDeployment(page, {
    identity: { user_id: "carol", active_org_id: null, org_memberships: [], assignments: [] },
    slotUnclaimed: false,
  });

  await page.goto("/");

  await expect(page.getByTestId("onboard-org-id")).toBeVisible();
});
