// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1603: signing out from an org subdomain. The session an org host shows is borrowed from the
// control plane (REQ-1348), so a sign-out performed here ends nothing: the Firebase session that
// mints the bearer lives in the control-plane origin's storage, and the reload that follows borrows
// the same session straight back. These tests pin the hand-off that makes the sign-out reach it.

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

const signOutFirebase = vi.fn(async () => {});
const installFirebaseTokenSync = vi.fn(async () => {});
vi.mock("../lib/firebase", () => ({ signOutFirebase, installFirebaseTokenSync }));
vi.mock("../apolloClient", () => ({ clearPersistedAdminCache: vi.fn() }));

const realLocation = window.location;

function setLocation(href: string) {
  const u = new URL(href);
  const assign = vi.fn();
  Object.defineProperty(window, "location", {
    value: {
      protocol: u.protocol,
      hostname: u.hostname,
      port: u.port,
      origin: u.origin,
      href: u.href,
      search: u.search,
      pathname: u.pathname,
      assign,
      replace: vi.fn(),
    },
    writable: true,
    configurable: true,
  });
  return assign;
}

describe("signOut", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    localStorage.setItem("provisa_token", "borrowed");
    localStorage.setItem("provisa_org", "kstott");
  });

  afterEach(() => {
    Object.defineProperty(window, "location", {
      value: realLocation,
      writable: true,
      configurable: true,
    });
    localStorage.clear();
  });

  it("hands an org subdomain's sign-out to the control plane that holds the session", async () => {
    const assign = setLocation("https://kstott.provisa.dev/query");
    const { signOut } = await import("../lib/session");
    await signOut();

    expect(assign).toHaveBeenCalledWith("https://cloud.provisa.dev/logout");
    // Nothing to sign out of on this origin -- Firebase never ran here.
    expect(signOutFirebase).not.toHaveBeenCalled();
    // The borrowed credential does not outlive the navigation.
    expect(localStorage.getItem("provisa_token")).toBeNull();
    expect(localStorage.getItem("provisa_org")).toBeNull();
  });

  it("ends the real session on the control plane and returns to the public entry point", async () => {
    const assign = setLocation("https://cloud.provisa.dev/logout");
    const { signOut } = await import("../lib/session");
    await signOut();

    expect(signOutFirebase).toHaveBeenCalled();
    expect(assign).toHaveBeenCalledWith("/");
    expect(localStorage.getItem("provisa_token")).toBeNull();
  });

  it("settles the restored user before signing it out", async () => {
    // /logout is a cold document load: Firebase has not read the persisted user back out of
    // IndexedDB yet, and a sign-out issued before that lands on nobody.
    const order: string[] = [];
    installFirebaseTokenSync.mockImplementation(async () => void order.push("sync"));
    signOutFirebase.mockImplementation(async () => void order.push("signout"));
    setLocation("https://cloud.provisa.dev/logout");
    const { signOut } = await import("../lib/session");
    await signOut();

    expect(order).toEqual(["sync", "signout"]);
  });
});
