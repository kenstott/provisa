// Copyright (c) 2026 Kenneth Stott
// Canary: 2822b4d4-6c34-42f7-8da0-d66906ea3e8b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1306: a person leaves an org on their own, from the screen, and lands where the server now
// says they belong. A real basic-auth instance (auth-flows-instance.ts); the member is created the
// way a person joins (an invite into the org, redeemed at /auth/register), signs in, opens their
// profile and leaves. Their only org gone, the next screen is onboarding, not a broken session, and
// /auth/me agrees: no membership left.

import { test, expect } from "./coverage";
import {
  getJson,
  inviteAndRegister,
  launch,
  superuserToken,
  type AuthFlowsInstance,
} from "./auth-flows-instance";

let instance: AuthFlowsInstance;
let member: { userId: string; token: string };

test.beforeAll(async () => {
  test.setTimeout(540_000); // one-time UI bundle build in a fresh worktree, then the node's boot
  instance = await launch();
  const su = await superuserToken(instance.api);
  member = await inviteAndRegister(
    instance.api,
    su,
    "default",
    "analyst",
    "leaver",
    "leaver@flows.example.com",
  );
});

test.afterAll(() => {
  instance?.stop();
});

test("a member leaves their only org from the profile and lands on onboarding", async ({
  page,
}) => {
  const before = await getJson(instance.api, "/auth/me", member.token);
  expect((before.org_memberships as { org_id: string }[]).map((m) => m.org_id)).toEqual([
    "default",
  ]);

  await page.addInitScript((t) => localStorage.setItem("provisa_token", t as string), member.token);
  await page.goto(`${instance.ui}/`);

  await page.getByTestId("navbar-user-trigger").click();
  await page.getByRole("menuitem", { name: "Profile" }).click();
  await expect(page.getByTestId("user-profile-modal")).toBeVisible();
  await page.getByTestId("profile-leave-default").click();

  // The modal reloads into identity bootstrap; with no membership the app routes to onboarding.
  await expect(page.getByTestId("onboard-org-page")).toBeVisible({ timeout: 30_000 });
  const after = await getJson(instance.api, "/auth/me", member.token);
  expect(after.org_memberships).toEqual([]);
});
