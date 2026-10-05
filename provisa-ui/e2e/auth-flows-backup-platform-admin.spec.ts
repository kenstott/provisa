// Copyright (c) 2026 Kenneth Stott
// Canary: 95a7e29c-96b5-432d-8038-370d8f6504dc
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1298: a backup platform_admin is made in two steps, never by a second bootstrap claim. The
// deployment's platform_admin (the one who claimed the bootstrap slot) invites a person into the
// root org, the person redeems the invite, and the platform_admin assigns them platform_admin in
// root from the Local Users screen. The backup then reaches the platform-only Orgs screen, and the
// bootstrap slot is still held by its original claimant. A real basic-auth instance
// (auth-flows-instance.ts) with its slot left open (--bootstrap).

import { test, expect } from "./coverage";
import {
  getJson,
  inviteAndRegister,
  launch,
  post,
  superuserToken,
  type AuthFlowsInstance,
} from "./auth-flows-instance";

// The login page asks who is signed in before anyone is: those probes answer 401 by design.
test.use({ allowedBrowserErrors: ["status of 401"] });

const ROOT = "default";
let instance: AuthFlowsInstance;
let first: { userId: string; password: string; token: string };
let backup: { userId: string; password: string; token: string };

test.beforeAll(async () => {
  test.setTimeout(540_000); // one-time UI bundle build in a fresh worktree, then the node's boot
  instance = await launch({ bootstrap: true });
  const su = await superuserToken(instance.api);
  // The deployment's first administrator: a member of root who then takes the open slot.
  first = await inviteAndRegister(
    instance.api,
    su,
    ROOT,
    "org_admin",
    "firstadmin",
    "first@flows.example.com",
  );
  const claim = await post(instance.api, "/auth/claim-bootstrap", {}, first.token);
  expect(claim.claimed).toBe(true);
  // Step one of REQ-1298: the platform_admin invites the backup into root as an ordinary member.
  backup = await inviteAndRegister(
    instance.api,
    first.token,
    ROOT,
    "analyst",
    "backupadmin",
    "backup@flows.example.com",
  );
});

test.afterAll(() => {
  instance?.stop();
});

async function signIn(page: import("@playwright/test").Page, username: string, password: string) {
  await page.goto(`${instance.ui}/`);
  await page.getByTestId("username-input").fill(username);
  await page.getByTestId("password-input").fill(password);
  await page.getByTestId("login-button").click();
  // REQ-1478: a member invited into an org is first told which org and why.
  const acknowledge = page.getByTestId("join-notice-ack");
  await expect(acknowledge).toBeVisible({ timeout: 30_000 });
  await acknowledge.click();
  await expect(page.getByRole("dialog")).toHaveCount(0);
}

test("the platform_admin makes a root member a backup platform_admin from Local Users", async ({
  browser,
}) => {
  const before = await getJson(instance.api, "/auth/me", backup.token, ROOT);
  expect((before.assignments as { role_id: string }[]).map((a) => a.role_id)).not.toContain(
    "platform_admin",
  );

  // Step two: the platform_admin assigns the role in root, on the screen.
  const adminPage = await (await browser.newContext()).newPage();
  await signIn(adminPage, "firstadmin", first.password);
  await adminPage.goto(`${instance.ui}/admin/local-users`);
  await adminPage.getByRole("button", { name: "Show assignments for backupadmin" }).click();
  await adminPage.getByRole("textbox", { name: "Role", exact: true }).click();
  await adminPage.getByRole("option", { name: "platform_admin", exact: true }).click();
  await adminPage.getByRole("button", { name: "Add", exact: true }).click();
  await expect(adminPage.getByText("platform_admin:*", { exact: true })).toBeVisible();

  // The backup now holds platform rights: the Orgs screen (cross_org) opens for them.
  const backupPage = await (await browser.newContext()).newPage();
  await signIn(backupPage, "backupadmin", backup.password);
  await backupPage.goto(`${instance.ui}/admin/orgs`);
  await expect(backupPage.getByTestId("org-create-toggle")).toBeVisible({ timeout: 30_000 });

  // Never a second claim: the slot stays with the deployment's first administrator.
  const after = await getJson(instance.api, "/auth/me", backup.token, ROOT);
  expect((after.assignments as { role_id: string }[]).map((a) => a.role_id)).toContain(
    "platform_admin",
  );
  const status = await getJson(instance.api, "/auth/bootstrap-status", backup.token);
  expect(status.unclaimed).toBe(false);
  const reclaim = await post(instance.api, "/auth/claim-bootstrap", {}, backup.token);
  expect(reclaim).toMatchObject({ claimed: false, claimed_by: first.userId });
});
