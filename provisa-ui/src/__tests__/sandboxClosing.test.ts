// Copyright (c) 2026 Kenneth Stott
// Canary: 5f8e46cd-95d9-4f35-bfc2-0f51b5891b10
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2065: a visitor's tab says it is closing as it unloads (a keepalive POST carrying the
// visitor's token); another tab of the sandbox, told over a BroadcastChannel, sends one request so
// the sandbox is kept. A tab with no visitor token says nothing.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { endVisit } from "../lib/browserStorage";
import { adoptSandboxTokenFromLocation } from "../lib/sandboxSession";
import { installSandboxClosing } from "../lib/sandboxClosing";

let removals: (() => void)[] = [];

function visitor() {
  adoptSandboxTokenFromLocation({
    hash: "#provisa_sandbox=tok-1&provisa_sandbox_org=sbx-1",
    pathname: "/",
    search: "",
  } as Location);
}

function install() {
  const fetch = vi.fn().mockResolvedValue(new Response(null, { status: 204 }));
  removals.push(installSandboxClosing(fetch as unknown as typeof window.fetch));
  return fetch;
}

beforeEach(() => {
  localStorage.clear();
  sessionStorage.clear();
});

afterEach(() => {
  removals.forEach((r) => r());
  removals = [];
  endVisit();
});

describe("the sandbox closing signal", () => {
  it("is sent as the page hides, kept alive, with the visitor's token and org", () => {
    visitor();
    const fetch = install();
    window.dispatchEvent(new Event("pagehide"));
    expect(fetch).toHaveBeenCalledTimes(1);
    const [url, init] = fetch.mock.calls[0];
    expect(url).toBe("/sandboxes/closing");
    expect(init).toMatchObject({ method: "POST", keepalive: true });
    expect(init.headers).toMatchObject({
      Authorization: "Bearer tok-1",
      "X-Org-Provisa": "sbx-1",
    });
  });

  it("is not sent from a tab with no visitor token", () => {
    const fetch = install();
    window.dispatchEvent(new Event("pagehide"));
    expect(fetch).not.toHaveBeenCalled();
  });

  it("makes another tab of the sandbox send one request, which keeps it", async () => {
    visitor();
    const fetch = install();
    const other = new BroadcastChannel("provisa-sandbox");
    other.postMessage("closing");
    await vi.waitFor(() => expect(fetch).toHaveBeenCalledTimes(1));
    expect(fetch.mock.calls[0][0]).toBe("/auth/me");
    expect(fetch.mock.calls[0][1].headers).toMatchObject({ Authorization: "Bearer tok-1" });
    other.close();
  });

  it("sends a request when the page comes back from the back/forward cache", () => {
    visitor();
    const fetch = install();
    const shown = new Event("pageshow") as PageTransitionEvent;
    Object.defineProperty(shown, "persisted", { value: true });
    window.dispatchEvent(shown);
    expect(fetch.mock.calls.map((c) => c[0])).toEqual(["/auth/me"]);
  });
});
