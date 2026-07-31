// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1348: cross-subdomain sign-in, end to end across two real browser origins.
//
// `crossSubdomainAuth.test.ts` covers the host classification and the origin predicate as pure
// functions. What it cannot cover is the thing that actually carries the credential: a hidden
// iframe on the control-plane origin, embedded by an org subdomain, exchanging postMessages across
// an origin boundary that only a browser enforces. jsdom has no such boundary — it would pass this
// suite with the origin check deleted.
//
// The origins are real: Chromium's `--host-resolver-rules` maps the two reserved `.test` zones to
// 127.0.0.1, so `cloud.provisa.test:3000` and `kstott.provisa.test:3000` are separate origins
// served by the one dev server (`server.allowedHosts` in vite.config.ts names both zones).
// `evil.example.test:3000` is the same server under a different base domain — the non-sibling case,
// which is what stops an unrelated site from embedding the relay and reading a live bearer.

import { expect, test } from "./coverage";

const CONTROL_PLANE = "http://cloud.provisa.test:3000";
const ORG_SUBDOMAIN = "http://kstott.provisa.test:3000";
const FOREIGN = "http://evil.example.test:3000";

const RELAY_PATH = "/auth-relay.html";
const TOKEN_FIXTURE = "relay-e2e-bearer";

// The embedding page is the relay document itself, served from the embedder's own origin. What is
// under test is an origin boundary, and the parent's content has no bearing on it — loading the SPA
// instead would boot AuthContext against a backend that knows nothing of these hosts and drown the
// run in console noise the coverage fixture (correctly) fails on. The relay is inert when it is the
// top document: it announces READY to `window.parent`, which is itself, and the listener below
// discards that because its origin is the embedder's, not the control plane's.
const INERT_PAGE = RELAY_PATH;

// `.test` is reserved and resolves nowhere, so every lookup has to be mapped here.
test.use({
  launchOptions: {
    args: ["--host-resolver-rules=MAP *.provisa.test 127.0.0.1, MAP *.example.test 127.0.0.1"],
  },
});

/**
 * Embed the relay served from `relayOrigin` and run the parent half of the protocol against it.
 *
 * Deliberately hand-written rather than calling `acquireTokenFromControlPlane`: the helper is what
 * a legitimate parent does, and a hostile embedder would not use it. Driving the wire protocol
 * directly is what lets the foreign-origin case be expressed at all.
 *
 * Resolves to the token, `null` when the relay answers with no session, or `"timeout"` when it
 * never answers — which is the correct outcome for a requester it refuses to serve.
 */
async function requestTokenFrom(page: import("@playwright/test").Page, relayOrigin: string) {
  return page.evaluate(
    ([origin, path]) =>
      new Promise<string | null | "timeout">((resolve) => {
        const frame = document.createElement("iframe");
        frame.style.display = "none";
        frame.src = `${origin}${path}`;

        const timer = setTimeout(() => finish("timeout"), 6000);
        function finish(value: string | null | "timeout") {
          clearTimeout(timer);
          window.removeEventListener("message", onMessage);
          frame.remove();
          resolve(value);
        }
        function onMessage(event: MessageEvent) {
          if (event.origin !== origin) return;
          if (event.data?.type === "provisa-auth-ready") {
            frame.contentWindow?.postMessage({ type: "provisa-auth-request" }, origin);
            return;
          }
          if (event.data?.type === "provisa-auth-token") finish(event.data.token ?? null);
        }

        window.addEventListener("message", onMessage);
        document.body.appendChild(frame);
      }),
    [relayOrigin, RELAY_PATH] as const,
  );
}

/** Put a bearer in the control plane's origin-scoped storage — what a completed sign-in leaves. */
async function signInOnControlPlane(page: import("@playwright/test").Page, token: string) {
  await page.goto(`${CONTROL_PLANE}${RELAY_PATH}`);
  await page.evaluate((t) => localStorage.setItem("provisa_token", t), token);
}

test.describe("REQ-1348 cross-subdomain auth relay", () => {
  test("hands the control plane's bearer to a sibling org subdomain", async ({ page }) => {
    await signInOnControlPlane(page, TOKEN_FIXTURE);

    await page.goto(`${ORG_SUBDOMAIN}${INERT_PAGE}`);
    expect(await requestTokenFrom(page, CONTROL_PLANE)).toBe(TOKEN_FIXTURE);
  });

  test("answers null — not a timeout — when the control plane has no session either", async ({
    page,
  }) => {
    // A real answer of "no session" is what sends the user to the control-plane login. A silent
    // relay would instead present as a broken deployment, so the two must stay distinguishable.
    await page.goto(`${CONTROL_PLANE}${RELAY_PATH}`);
    await page.evaluate(() => localStorage.removeItem("provisa_token"));

    await page.goto(`${ORG_SUBDOMAIN}${INERT_PAGE}`);
    expect(await requestTokenFrom(page, CONTROL_PLANE)).toBeNull();
  });

  test("never answers an embedder from another base domain", async ({ page }) => {
    await signInOnControlPlane(page, TOKEN_FIXTURE);

    await page.goto(`${FOREIGN}${INERT_PAGE}`);
    // Not merely "not the token": the relay must not reply at all, so nothing about the session —
    // including whether one exists — leaks to a site that is not part of this deployment.
    expect(await requestTokenFrom(page, CONTROL_PLANE)).toBe("timeout");
  });

  test("is served as the relay, not swallowed by the SPA or the /auth proxy", async ({ page }) => {
    // The dev proxy matched "/auth" as a prefix and forwarded "/auth-relay.html" to the API, which
    // has no such route; in production the SPA fallback would serve index.html instead. Either way
    // the frame loads something that never posts READY, and every org subdomain hangs for the full
    // relay timeout and reports a broken control plane.
    const response = await page.goto(`${CONTROL_PLANE}${RELAY_PATH}`);
    expect(response?.status()).toBe(200);
    expect(await page.content()).toContain("authRelay");
  });
});
