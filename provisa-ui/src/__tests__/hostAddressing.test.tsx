// Copyright (c) 2026 Kenneth Stott
// Canary: 7c2e94b1-5a3d-4f08-9e61-d2b8a0c47f93
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1276/REQ-1348: org-subdomain addressing belongs to a deployment that declares its base
// domain. Any other host — a GitHub Codespace's forwarded port, localhost, a custom single host — is
// the deployment's one address: it names no org, borrows no session from a control plane, and signs
// in where it is. main.tsx is booted on each kind of host and the path it took is observed.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { waitFor } from "@testing-library/react";
import { declareBaseDomain } from "./declaredBaseDomain";

const CODESPACE = "https://fluffy-space-umbrella-5g4r9x-3000.app.github.dev/";

const crossSubdomain = vi.hoisted(() => ({ establishOrgSubdomainSession: vi.fn() }));
const firebase = vi.hoisted(() => ({ installFirebaseTokenSync: vi.fn() }));

vi.mock("../lib/crossSubdomainAuth.ts", async (importOriginal) => ({
  ...(await importOriginal<object>()),
  establishOrgSubdomainSession: crossSubdomain.establishOrgSubdomainSession,
}));
vi.mock("../lib/firebase.ts", async (importOriginal) => ({
  ...(await importOriginal<object>()),
  installFirebaseTokenSync: firebase.installFirebaseTokenSync,
}));

/** The jsdom instance vitest's environment runs on; reconfiguring it moves the real location. */
const dom = (globalThis as unknown as { jsdom: { reconfigure(o: { url: string }): void } }).jsdom;
const startUrl = window.location.href;

beforeEach(() => {
  vi.resetModules();
  localStorage.clear();
  document.body.innerHTML = '<div id="root"></div>';
  crossSubdomain.establishOrgSubdomainSession.mockReset().mockResolvedValue(true);
  firebase.installFirebaseTokenSync.mockReset().mockResolvedValue(undefined);
  // A backend answering a caller with no session.
  vi.stubGlobal(
    "fetch",
    vi.fn(async () => new Response(JSON.stringify({ detail: "no session" }), { status: 401 })),
  );
  vi.spyOn(console, "error").mockImplementation(() => undefined);
});

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
  declareBaseDomain(null);
  dom.reconfigure({ url: startUrl });
  localStorage.clear();
});

async function bootOn(url: string): Promise<HTMLElement> {
  dom.reconfigure({ url });
  await import("../main.tsx");
  const root = document.getElementById("root")!;
  await waitFor(() => expect(root.childElementCount).toBeGreaterThan(0), { timeout: 30000 });
  expect(root.textContent).not.toContain("failed to start");
  return root;
}

/** The org main.tsx recorded from the host, if any (setStored global scope). */
function storedOrg(): string | null {
  for (let i = 0; i < localStorage.length; i++) {
    const key = localStorage.key(i)!;
    if (key.endsWith("provisa_org")) return localStorage.getItem(key);
  }
  return null;
}

describe("a host that is not a declared multi-org deployment", () => {
  it("starts a GitHub Codespace without a relay and without an org", async () => {
    await bootOn(CODESPACE);
    expect(crossSubdomain.establishOrgSubdomainSession).not.toHaveBeenCalled();
    expect(document.querySelector('iframe[src*="auth-relay"]')).toBeNull();
    expect(storedOrg()).toBeNull();
    // It signs in where it is.
    expect(firebase.installFirebaseTokenSync).toHaveBeenCalledOnce();
  }, 60000);

  it("starts on localhost the same way", async () => {
    await bootOn("http://localhost:3000/");
    expect(crossSubdomain.establishOrgSubdomainSession).not.toHaveBeenCalled();
    expect(storedOrg()).toBeNull();
    expect(firebase.installFirebaseTokenSync).toHaveBeenCalledOnce();
  }, 60000);

  it("is a single address even when the deployment declares some other base domain", async () => {
    declareBaseDomain("provisa.dev");
    await bootOn(CODESPACE);
    expect(crossSubdomain.establishOrgSubdomainSession).not.toHaveBeenCalled();
    expect(storedOrg()).toBeNull();
    expect(firebase.installFirebaseTokenSync).toHaveBeenCalledOnce();
  }, 60000);
});

describe("a deployment that declares its base domain", () => {
  beforeEach(() => declareBaseDomain("provisa.dev"));

  it("borrows an org subdomain's session from the control plane", async () => {
    await bootOn("https://kstott.provisa.dev/query");
    expect(crossSubdomain.establishOrgSubdomainSession).toHaveBeenCalledOnce();
    expect(firebase.installFirebaseTokenSync).not.toHaveBeenCalled();
  }, 60000);

  it("signs in on the control plane itself", async () => {
    await bootOn("https://cloud.provisa.dev/login");
    expect(crossSubdomain.establishOrgSubdomainSession).not.toHaveBeenCalled();
    expect(firebase.installFirebaseTokenSync).toHaveBeenCalledOnce();
  }, 60000);
});
