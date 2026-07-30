// Copyright (c) 2026 Kenneth Stott
// Canary: 3d5a1c88-7e64-4a19-b0f2-9c41d7e6a205
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1348: cross-subdomain sign-in. Two things here are security decisions rather than
// conveniences, and both are what these tests are for: which host is treated as an org (a wrong
// answer sends a sign-in to a host the IdP will reject, or treats the control plane as a tenant),
// and which origins may exchange a bearer (a wrong answer hands a live credential to whoever asks).

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import {
  baseDomain,
  controlPlaneOrigin,
  isControlPlaneHost,
  isOrgSubdomainHost,
  isSiblingOrigin,
  orgFromHost,
} from "../lib/authHost";

/** A stand-in for `window.location` — only the fields authHost reads. */
function loc(href: string): Location {
  const u = new URL(href);
  return {
    protocol: u.protocol,
    hostname: u.hostname,
    port: u.port,
    origin: u.origin,
    href: u.href,
    search: u.search,
  } as unknown as Location;
}

describe("authHost host classification", () => {
  it("treats the leftmost label as the org, matching the server's rule", () => {
    expect(isOrgSubdomainHost("kstott.provisa.dev")).toBe(true);
    expect(orgFromHost("kstott.provisa.dev")).toBe("kstott");
    expect(baseDomain("kstott.provisa.dev")).toBe("provisa.dev");
  });

  it("does not treat the control plane as an org", () => {
    expect(isControlPlaneHost("cloud.provisa.dev")).toBe(true);
    expect(isOrgSubdomainHost("cloud.provisa.dev")).toBe(false);
    expect(() => orgFromHost("cloud.provisa.dev")).toThrow();
  });

  it("does not invent an org for hosts that name none", () => {
    // An apex, a bare host, and an IP literal all resolve to no org — the same hosts a dev server
    // and a direct-to-node deployment run on, which must keep signing in locally.
    for (const host of ["provisa.dev", "localhost", "136.119.234.142"]) {
      expect(isOrgSubdomainHost(host)).toBe(false);
    }
  });

  it("derives the control-plane origin by replacing the org label", () => {
    expect(controlPlaneOrigin(loc("https://kstott.provisa.dev/query"))).toBe("https://cloud.provisa.dev");
    expect(controlPlaneOrigin(loc("http://acme.provisa.test:5173/"))).toBe("http://cloud.provisa.test:5173");
  });

  it("refuses to guess an origin for a host with no base domain", () => {
    // Guessing here would point sign-in at a host we may not own; there is no safe default.
    expect(() => controlPlaneOrigin(loc("http://localhost:5173/"))).toThrow();
  });
});

describe("isSiblingOrigin", () => {
  const self = loc("https://cloud.provisa.dev/auth-relay.html");

  it("accepts a sibling org host of the same deployment", () => {
    expect(isSiblingOrigin("https://kstott.provisa.dev", self)).toBe(true);
    expect(isSiblingOrigin("https://cloud.provisa.dev", self)).toBe(true);
  });

  it("rejects anything that is not a single label under the same base domain", () => {
    const hostile = [
      "https://provisa.dev.evil.com", // base domain as a prefix of someone else's
      "https://evil.com",
      "https://kstott.provisa.dev.evil.com",
      "https://provisa.dev", // apex: no org label
      "http://kstott.provisa.dev", // scheme downgrade
      "https://kstott.provisa.dev:8443", // different port
      "https://deep.kstott.provisa.dev", // base domain would be kstott.provisa.dev, not ours
      "not a url",
    ];
    for (const origin of hostile) {
      expect(isSiblingOrigin(origin, self), origin).toBe(false);
    }
  });
});

/**
 * Rewrites `window.location` for the modules that read it directly (the relay and the parent both
 * decide who they are from the URL), and restores it afterwards.
 */
function setLocation(href: string): void {
  Object.defineProperty(window, "location", {
    value: { ...loc(href), replace: vi.fn(), assign: vi.fn() },
    writable: true,
    configurable: true,
  });
}

describe("acquireTokenFromControlPlane", () => {
  const realLocation = window.location;

  beforeEach(() => {
    vi.resetModules();
    setLocation("https://kstott.provisa.dev/query");
  });

  afterEach(() => {
    Object.defineProperty(window, "location", { value: realLocation, writable: true, configurable: true });
    document.body.innerHTML = "";
  });

  it("requests from the relay on READY and resolves with the token it returns", async () => {
    const { acquireTokenFromControlPlane } = await import("../lib/crossSubdomainAuth");
    const pending = acquireTokenFromControlPlane(1000);

    const frame = document.querySelector("iframe");
    expect(frame?.getAttribute("src")).toBe("https://cloud.provisa.dev/auth-relay.html");
    // jsdom gives the frame a real contentWindow; intercept what the parent sends it.
    const post = vi.fn();
    Object.defineProperty(frame!, "contentWindow", { value: { postMessage: post }, configurable: true });

    window.dispatchEvent(
      new MessageEvent("message", { data: { type: "provisa-auth-ready" }, origin: "https://cloud.provisa.dev" }),
    );
    expect(post).toHaveBeenCalledWith({ type: "provisa-auth-request" }, "https://cloud.provisa.dev");

    window.dispatchEvent(
      new MessageEvent("message", {
        data: { type: "provisa-auth-token", token: "bearer-abc" },
        origin: "https://cloud.provisa.dev",
      }),
    );
    await expect(pending).resolves.toBe("bearer-abc");
    // The frame is a credential channel, not part of the page: it must not outlive the exchange.
    expect(document.querySelector("iframe")).toBeNull();
  });

  it("ignores a token offered by any origin other than the control plane", async () => {
    const { acquireTokenFromControlPlane } = await import("../lib/crossSubdomainAuth");
    const pending = acquireTokenFromControlPlane(50);

    window.dispatchEvent(
      new MessageEvent("message", {
        data: { type: "provisa-auth-token", token: "attacker-token" },
        origin: "https://evil.com",
      }),
    );
    // No resolution from the impostor: the request times out as if nothing had been said.
    await expect(pending).rejects.toThrow(/did not respond/);
  });

  it("resolves null — not an error — when the control plane has no session", async () => {
    const { acquireTokenFromControlPlane } = await import("../lib/crossSubdomainAuth");
    const pending = acquireTokenFromControlPlane(1000);
    window.dispatchEvent(
      new MessageEvent("message", {
        data: { type: "provisa-auth-token", token: null },
        origin: "https://cloud.provisa.dev",
      }),
    );
    await expect(pending).resolves.toBeNull();
  });

  it("rejects when the relay never answers, rather than presenting as signed out", async () => {
    const { acquireTokenFromControlPlane } = await import("../lib/crossSubdomainAuth");
    await expect(acquireTokenFromControlPlane(20)).rejects.toThrow(/did not respond/);
  });
});

describe("nextParam", () => {
  const realLocation = window.location;

  beforeEach(() => {
    vi.resetModules();
    setLocation("https://cloud.provisa.dev/login");
  });

  afterEach(() => {
    Object.defineProperty(window, "location", { value: realLocation, writable: true, configurable: true });
  });

  it("accepts a return-to URL on a host of this deployment", async () => {
    const { nextParam } = await import("../lib/crossSubdomainAuth");
    expect(nextParam("?next=" + encodeURIComponent("https://kstott.provisa.dev/query"))).toBe(
      "https://kstott.provisa.dev/query",
    );
  });

  it("refuses to forward a fresh session anywhere else", async () => {
    const { nextParam } = await import("../lib/crossSubdomainAuth");
    for (const raw of ["https://evil.com/steal", "http://kstott.provisa.dev/", "/relative", "javascript:alert(1)"]) {
      expect(nextParam("?next=" + encodeURIComponent(raw)), raw).toBeNull();
    }
    expect(nextParam("")).toBeNull();
  });
});

describe("auth relay (control-plane side)", () => {
  const realLocation = window.location;
  // The relay registers its listener at import. Each test re-imports it against a fresh module
  // registry, but the listeners land on the one shared jsdom window, so without this every test
  // after the first would be answered by every earlier copy too.
  const added: EventListener[] = [];

  beforeEach(() => {
    vi.resetModules();
    localStorage.clear();
    setLocation("https://cloud.provisa.dev/auth-relay.html");
    added.length = 0;
    const realAdd = window.addEventListener.bind(window);
    vi.spyOn(window, "addEventListener").mockImplementation((type, listener, opts) => {
      if (type === "message") added.push(listener as EventListener);
      realAdd(type, listener, opts);
    });
  });

  afterEach(() => {
    vi.restoreAllMocks();
    for (const listener of added) window.removeEventListener("message", listener);
    Object.defineProperty(window, "location", { value: realLocation, writable: true, configurable: true });
    localStorage.clear();
  });

  /** Deliver a token request as if it came from `origin`, and report what the relay replied. */
  async function request(origin: string): Promise<unknown[]> {
    const replies: unknown[] = [];
    const source = { postMessage: (data: unknown) => replies.push(data) };
    // MessageEvent's `source` only accepts a real WindowProxy, so the stub the relay replies to is
    // attached to the constructed event rather than passed through the initializer.
    const event = new MessageEvent("message", { data: { type: "provisa-auth-request" }, origin });
    Object.defineProperty(event, "source", { value: source, configurable: true });
    window.dispatchEvent(event);
    return replies;
  }

  it("hands the stored bearer to a sibling org subdomain", async () => {
    localStorage.setItem("provisa_token", "bearer-abc");
    await import("../authRelay");
    const replies = await request("https://kstott.provisa.dev");
    expect(replies).toEqual([{ type: "provisa-auth-token", token: "bearer-abc" }]);
  });

  it("says nothing to an origin outside this deployment", async () => {
    localStorage.setItem("provisa_token", "bearer-abc");
    await import("../authRelay");
    const replies = await request("https://evil.com");
    expect(replies).toEqual([]);
  });

  it("reports no session as null rather than staying silent", async () => {
    await import("../authRelay");
    const replies = await request("https://kstott.provisa.dev");
    expect(replies).toEqual([{ type: "provisa-auth-token", token: null }]);
  });
});
