// Copyright (c) 2026 Kenneth Stott
// Canary: 1f45115d-fd19-4d4e-8a6a-2db4799ace24
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1285: an org that turns auto-join on admits a person whose email its rule matches the first
// time they sign in, with the configured role and no invite. A real basic-auth instance
// (auth-flows-instance.ts). The org's own administrator (joined by invite) sets the policy and
// creates the account; the person then signs in through the login form, is told on screen that
// the org's email rule admitted them, and /auth/me shows the membership and the analyst role.

import { test, expect } from "./coverage";
import {
  getJson,
  inviteAndRegister,
  launch,
  login,
  patch,
  post,
  superuserToken,
  type AuthFlowsInstance,
} from "./auth-flows-instance";

// The login page asks who is signed in before anyone is: those probes answer 401 by design.
test.use({ allowedBrowserErrors: ["status of 401"] });

let instance: AuthFlowsInstance;
let orgAdmin: { userId: string; token: string };
const NEWCOMER = "newcomer";
const NEWCOMER_PASSWORD = `pw-${Math.random().toString(36).slice(2)}-${Date.now()}`;

test.beforeAll(async () => {
  test.setTimeout(540_000); // one-time UI bundle build in a fresh worktree, then the node's boot
  instance = await launch();
  const su = await superuserToken(instance.api);
  orgAdmin = await inviteAndRegister(
    instance.api,
    su,
    "default",
    "org_admin",
    "flowsadmin",
    "admin@flows.example.com",
  );
  // The org's join policy, set by its own administrator: anyone at autojoin.example.com joins
  // as an analyst. Anchored at both ends, so it names one domain and needs no breadth consent.
  await patch(
    instance.api,
    "/admin/orgs/default/settings",
    {
      email_rule: "^[^@]+@autojoin\\.example\\.com$",
      auto_join: true,
      auto_join_role: "analyst",
    },
    orgAdmin.token,
    "default",
  );
  // An account with no membership anywhere: only the email rule can admit it.
  await post(
    instance.api,
    "/admin/users/",
    { username: NEWCOMER, password: NEWCOMER_PASSWORD, email: "newcomer@autojoin.example.com" },
    orgAdmin.token,
    "default",
  );
});

test.afterAll(() => {
  instance?.stop();
});

test("a person whose email matches the org's rule is auto-joined on first sign-in", async ({
  page,
}) => {
  await page.goto(`${instance.ui}/`);
  await page.getByTestId("username-input").fill(NEWCOMER);
  await page.getByTestId("password-input").fill(NEWCOMER_PASSWORD);
  await page.getByTestId("login-button").click();

  // REQ-1478: the person is told which org admitted them and why, before anything else.
  const reason = page.getByTestId("join-notice-reason");
  await expect(reason).toBeVisible({ timeout: 30_000 });
  await page.getByTestId("join-notice-ack").click();
  await expect(page.getByRole("dialog")).toHaveCount(0);

  const me = await getJson(
    instance.api,
    "/auth/me",
    await login(instance.api, NEWCOMER, NEWCOMER_PASSWORD),
  );
  expect((me.org_memberships as { org_id: string }[]).map((m) => m.org_id)).toEqual(["default"]);
  expect((me.assignments as { role_id: string }[]).map((a) => a.role_id)).toContain("analyst");
});
