// Copyright (c) 2026 Kenneth Stott
// Canary: 4591753f-b692-464a-aaf6-1b88a433b475
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The auth-flows project's instance: one node, no regions, basic auth enforced
// (scripts/launch-demo-regions.sh --test --auth-flows). Each spec brings up its own and seeds the
// users and orgs it needs through the authenticated admin API, never a shortcut into the store, so
// what a spec proves is the path a real deployment takes. The launcher never prints a secret: it
// names a 0600 file holding the per-run superuser password.

import { spawn, type ChildProcess } from "node:child_process";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { request as pwRequest, expect, type APIRequestContext } from "@playwright/test";

const HERE = path.dirname(fileURLToPath(import.meta.url));

export interface AuthFlowsInstance {
  ui: string;
  api: string;
  stop(): void;
}

let proc: ChildProcess | null = null;
let adminPassword = "";

/** Start the instance; resolves once the node answers and its credentials file is written.
 * ``bootstrap`` leaves the platform-admin slot open for the spec to claim (REQ-1290). */
export function launch(opts: { bootstrap?: boolean } = {}): Promise<AuthFlowsInstance> {
  const script = path.resolve(HERE, "../../scripts/launch-demo-regions.sh");
  const args = [script, "--test", "--auth-flows", ...(opts.bootstrap ? ["--bootstrap"] : [])];
  // The node's log lands beside the spec's results so a failure's server traceback survives the
  // launcher's cleanup (and the CI lane uploads it).
  const logDir = path.resolve(HERE, "../test-results");
  fs.mkdirSync(logDir, { recursive: true });
  const nodeLog = path.join(logDir, `auth-flows-node-${process.pid}-${Date.now()}.log`);
  proc = spawn("bash", args, {
    stdio: ["ignore", "pipe", "inherit"],
    env: { ...process.env, AUTH_FLOWS_NODE_LOG: nodeLog },
  });
  return new Promise<AuthFlowsInstance>((resolve, reject) => {
    // Generous: a fresh worktree builds the UI bundle once (provisa/_ui) before the node starts.
    const timer = setTimeout(
      () => reject(new Error("auth-flows instance did not start in 480s")),
      480_000,
    );
    let buf = "";
    proc!.stdout!.on("data", (d: Buffer) => {
      buf += d.toString();
      const p = buf.match(/PORTS ui=(\d+) api=(\d+)/);
      const c = buf.match(/CREDS_FILE (\S+)/);
      if (p && c) {
        clearTimeout(timer);
        adminPassword = (JSON.parse(fs.readFileSync(c[1], "utf8")) as { admin: string }).admin;
        resolve({
          ui: `http://127.0.0.1:${p[1]}`,
          api: `http://127.0.0.1:${p[2]}`,
          stop: () => proc?.kill("SIGINT"), // the launcher's EXIT trap stops the node, wipes the dir
        });
      }
    });
    proc!.on("exit", (code) => reject(new Error(`auth-flows launcher exited early (${code})`)));
  });
}

/** The org a tenant-plane request is addressed to. A multitenant node reads it from the Host
 * subdomain, or from this header on a bare host (127.0.0.1) such as this instance's. */
function headers(token?: string, org?: string): Record<string, string> {
  return {
    ...(token ? { Authorization: `Bearer ${token}` } : {}),
    ...(org ? { "x-org-provisa": org } : {}),
  };
}

async function post(
  api: string,
  route: string,
  body: unknown,
  token?: string,
  org?: string,
): Promise<Record<string, unknown>> {
  const ctx: APIRequestContext = await pwRequest.newContext();
  try {
    const r = await ctx.post(`${api}${route}`, { data: body, headers: headers(token, org) });
    expect(r.ok(), `POST ${route}: ${r.status()} ${await r.text()}`).toBeTruthy();
    return (await r.json()) as Record<string, unknown>;
  } finally {
    await ctx.dispose();
  }
}

/** The break-glass superuser's bearer (control plane only): it opens invites, nothing more. */
export async function superuserToken(api: string): Promise<string> {
  const r = await post(api, "/auth/superuser-login", {
    username: "admin",
    password: adminPassword,
  });
  return r.access_token as string;
}

/** A basic-auth user created the way a person joins: an invite into ``orgId``, redeemed at
 * /auth/register. Returns the new user's id, password and bearer. */
export async function inviteAndRegister(
  api: string,
  inviterToken: string,
  orgId: string,
  roleId: string,
  username: string,
  email: string,
): Promise<{ userId: string; password: string; token: string }> {
  const invite = await post(
    api,
    "/admin/invites/",
    { org_id: orgId, role_id: roleId },
    inviterToken,
    orgId,
  );
  const password = `pw-${Math.random().toString(36).slice(2)}-${Date.now()}`;
  const reg = await post(api, "/auth/register", {
    username,
    password,
    email,
    invite_token: invite.token,
  });
  const login = await post(api, "/auth/login", { username, password });
  return { userId: reg.user_id as string, password, token: login.access_token as string };
}

/** GET with a bearer, parsed. */
export async function getJson(
  api: string,
  route: string,
  token: string,
  org?: string,
): Promise<Record<string, unknown>> {
  const ctx = await pwRequest.newContext();
  try {
    const r = await ctx.get(`${api}${route}`, { headers: headers(token, org) });
    expect(r.ok(), `GET ${route}: ${r.status()} ${await r.text()}`).toBeTruthy();
    return (await r.json()) as Record<string, unknown>;
  } finally {
    await ctx.dispose();
  }
}

/** PATCH with a bearer, parsed. */
export async function patch(
  api: string,
  route: string,
  body: unknown,
  token: string,
  org?: string,
): Promise<Record<string, unknown>> {
  const ctx = await pwRequest.newContext();
  try {
    const r = await ctx.patch(`${api}${route}`, { data: body, headers: headers(token, org) });
    expect(r.ok(), `PATCH ${route}: ${r.status()} ${await r.text()}`).toBeTruthy();
    return (await r.json()) as Record<string, unknown>;
  } finally {
    await ctx.dispose();
  }
}

/** A basic-auth user's bearer. */
export async function login(api: string, username: string, password: string): Promise<string> {
  const r = await post(api, "/auth/login", { username, password });
  return r.access_token as string;
}

export { post };
