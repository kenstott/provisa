// Copyright (c) 2026 Kenneth Stott
// Canary: 9d4f1b8a-6e27-4c30-b95a-1f8e0c3a7d62
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { test, expect } from "./coverage";

/**
 * REQ-1266: a member-less authenticated user is routed to self-service org creation, fills the
 * form (with demo data), sees "provisioning…" while the create+poll runs, and on ready the app
 * binds the new org and navigates into the shell. Backend routes are stubbed so this isolates the
 * UI onboarding contract (create body carries include_demo, poll drives ready, org is bound); the
 * full two-org data plane is covered by backend integration tests.
 */

const ORG_ID = "carolco";

test("member-less user onboards a new org with demo data and is routed into the app", async ({
  page,
}) => {
  // Auth enabled + a token present, but /auth/me reports no org memberships → onboarding gate.
  await page.addInitScript(() => {
    localStorage.setItem("provisa_token", "tok-carol");
  });

  await page.route("**/setup/status", (route) =>
    route.fulfill({ json: { needs_setup: false, demo_mode: false, auth_enabled: true } }),
  );
  // Firebase config probe from index.html is absent under the stubbed harness.
  await page.route("**/firebase-config.js", (route) =>
    route.fulfill({ body: "", contentType: "application/javascript" }),
  );

  // Membership is empty until the org is created; after that, /auth/me reports the new org so the
  // post-create refresh clears the onboarding gate.
  let created = false;
  await page.route("**/auth/me", (route) =>
    route.fulfill({
      json: {
        user_id: "carol",
        email: "carol@example.com",
        display_name: "Carol",
        dev_mode: false,
        active_org_id: created ? ORG_ID : null,
        org_memberships: created ? [{ org_id: ORG_ID, org_name: "Carol Co" }] : [],
        assignments: created ? [{ role_id: "org_admin", domain_id: "*" }] : [],
      },
    }),
  );
  // Silence the GraphQL role/domain bootstrap the AuthProvider fires and the shell's queries.
  await page.route("**/admin/graphql", (route) =>
    route.fulfill({ json: { data: { roles: [], domains: [] } } }),
  );

  let createBody: Record<string, unknown> | null = null;
  await page.route("**/admin/orgs", (route) => {
    if (route.request().method() !== "POST") return route.continue();
    createBody = route.request().postDataJSON();
    created = true;
    return route.fulfill({
      json: { id: ORG_ID, name: "Carol Co", provisioning_state: "provisioning" },
    });
  });

  // First status poll still provisioning, second flips to ready.
  let statusCalls = 0;
  await page.route(`**/admin/orgs/${ORG_ID}/status`, (route) => {
    statusCalls += 1;
    const state = statusCalls >= 2 ? "ready" : "provisioning";
    return route.fulfill({ json: { id: ORG_ID, name: "Carol Co", provisioning_state: state } });
  });

  await page.goto("/");

  const idInput = page.getByTestId("onboard-org-id");
  await expect(idInput).toBeVisible();
  await idInput.fill(ORG_ID);
  await page.getByTestId("onboard-org-name").fill("Carol Co");
  await page.getByTestId("onboard-org-demo").check();
  await page.getByTestId("onboard-org-submit").click();

  await expect(page.getByTestId("onboard-org-provisioning")).toBeVisible();

  // Once provisioning is ready the app binds the org, refetches identity, and routes into the shell.
  await page.waitForURL("**/query");
  await expect(page.getByTestId("onboard-org-page")).toHaveCount(0);

  expect(createBody).toEqual(expect.objectContaining({ id: ORG_ID, name: "Carol Co", include_demo: true }));
  expect(await page.evaluate(() => localStorage.getItem("provisa_org"))).toBe(ORG_ID);
});

test("member-less user joins an existing org with an invite code and is routed in", async ({
  page,
}) => {
  await page.addInitScript(() => {
    localStorage.setItem("provisa_token", "tok-dave");
  });

  await page.route("**/setup/status", (route) =>
    route.fulfill({ json: { needs_setup: false, demo_mode: false, auth_enabled: true } }),
  );
  await page.route("**/firebase-config.js", (route) =>
    route.fulfill({ body: "", contentType: "application/javascript" }),
  );

  let joined = false;
  await page.route("**/auth/me", (route) =>
    route.fulfill({
      json: {
        user_id: "dave",
        email: "dave@example.com",
        display_name: "Dave",
        dev_mode: false,
        active_org_id: joined ? ORG_ID : null,
        org_memberships: joined ? [{ org_id: ORG_ID, org_name: "Carol Co" }] : [],
        assignments: joined ? [{ role_id: "org_admin", domain_id: "*" }] : [],
      },
    }),
  );
  await page.route("**/admin/graphql", (route) =>
    route.fulfill({ json: { data: { roles: [], domains: [] } } }),
  );

  let redeemBody: Record<string, unknown> | null = null;
  await page.route("**/auth/redeem-invite", (route) => {
    redeemBody = route.request().postDataJSON();
    joined = true;
    return route.fulfill({ json: { user_id: "dave", org_id: ORG_ID, role_id: "org_admin" } });
  });

  await page.goto("/");

  await expect(page.getByTestId("onboard-org-mode")).toBeVisible();
  await page.getByText("Join with an invite").click();
  await page.getByTestId("onboard-org-invite").fill("invite-tok-xyz");
  await page.getByTestId("onboard-org-join-submit").click();

  await page.waitForURL("**/query");
  await expect(page.getByTestId("onboard-org-page")).toHaveCount(0);

  expect(redeemBody).toEqual({ token: "invite-tok-xyz" });
  expect(await page.evaluate(() => localStorage.getItem("provisa_org"))).toBe(ORG_ID);
});
