// Copyright (c) 2026 Kenneth Stott
// Canary: e4a7d610-1c52-47b9-a0c8-37f92b6d5e01
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The browser's storage can never crash the app: a full, blocked or corrupt store leaves the app on
// its defaults, logged and noticed once, except the session's identity, whose failure is a failure.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { fullStorage, installStorage, throwingStorage } from "./mockStorage";

type Module = typeof import("../lib/browserStorage");

const SCOPE = "cp:org:";
let undo: (() => void)[] = [];
let storage: Module;
let errors: ReturnType<typeof vi.spyOn>;

beforeEach(async () => {
  vi.resetModules(); // the notice is once per page load
  storage = await import("../lib/browserStorage");
  storage.bindStorageScope({ controlPlaneId: "cp", orgId: "org" });
  errors = vi.spyOn(console, "error").mockImplementation(() => undefined);
});

afterEach(() => {
  undo.forEach((u) => u());
  undo = [];
  errors.mockRestore();
});

const install = (name: "localStorage" | "sessionStorage", s: Storage) => {
  undo.push(installStorage(name, s));
  return s;
};

describe("a full store", () => {
  it("makes room from the old result frames, the largest first, and retries once", () => {
    const frames = "x".repeat(40);
    const local = install(
      "localStorage",
      fullStorage(120, {
        [`${SCOPE}provisa.graph.state`]: frames,
        [`${SCOPE}provisa.sparql.state`]: "x".repeat(10),
        [`${SCOPE}nl-question`]: "kept",
      }),
    );
    storage.setStored("provisa_nav_last_item", "y".repeat(30));
    expect(local.getItem(`${SCOPE}provisa_nav_last_item`)).toBe("y".repeat(30));
    expect(local.getItem(`${SCOPE}provisa.graph.state`)).toBeNull();
    expect(local.getItem(`${SCOPE}provisa.sparql.state`)).toBe("x".repeat(10));
    expect(local.getItem(`${SCOPE}nl-question`)).toBe("kept");
  });

  it("gives up old frames in any scope before anything else disposable", () => {
    const local = install(
      "localStorage",
      fullStorage(170, {
        "other:org:provisa.graph.state": "x".repeat(20),
        [`${SCOPE}provisa.graph.counts.v3.a`]: "c".repeat(60),
      }),
    );
    storage.setStored("setting", "y".repeat(20));
    expect(local.getItem("other:org:provisa.graph.state")).toBeNull();
    expect(local.getItem(`${SCOPE}provisa.graph.counts.v3.a`)).toBe("c".repeat(60));
  });

  it("never evicts identity or session keys, nor the firebase session", () => {
    const seed = {
      provisa_token: "t".repeat(30),
      provisa_su_token: "s".repeat(30),
      provisa_org: "o",
      provisa_role: "r",
      "firebase:authUser:key:[DEFAULT]": "f".repeat(30),
    };
    const local = install("localStorage", fullStorage(160, seed));
    storage.setStored("setting", "y".repeat(100));
    for (const [k, v] of Object.entries(seed)) expect(local.getItem(k)).toBe(v);
    expect(local.getItem(`${SCOPE}setting`)).toBeNull();
    expect(errors).toHaveBeenCalled();
  });

  it("logs the failure and raises the notice once, when nothing can go", () => {
    install("localStorage", fullStorage(5));
    const heard: string[] = [];
    storage.setStored("a", "too large");
    storage.setStored("b", "too large");
    storage.listenForStorageNotice((kind) => heard.push(kind));
    storage.setStored("c", "too large");
    expect(heard).toEqual(["settings"]);
    const saves = errors.mock.calls.filter((call: unknown[]) => String(call[0]).includes("save"));
    expect(saves).toHaveLength(3);
  });

  it("does not repeat a notice when the listener is replaced", () => {
    install("localStorage", fullStorage(5));
    const heard: string[] = [];
    storage.listenForStorageNotice((kind) => heard.push(kind))();
    storage.setStored("a", "too large");
    storage.listenForStorageNotice((kind) => heard.push(kind))();
    storage.listenForStorageNotice((kind) => heard.push(kind))();
    expect(heard).toEqual(["settings"]);
  });

  it("is a session failure for the session's identity, never a success", () => {
    install("localStorage", fullStorage(5));
    const heard: string[] = [];
    storage.listenForStorageNotice((kind) => heard.push(kind));
    expect(() => storage.setStored("provisa_token", "a bearer", { global: true })).toThrow(
      storage.SessionStorageError,
    );
    expect(heard).toEqual(["session"]);
  });
});

describe("a store that throws on every call", () => {
  beforeEach(() => {
    install("localStorage", throwingStorage());
    install("sessionStorage", throwingStorage());
  });

  it("reads as nothing saved, and every setting's write and removal goes on", () => {
    expect(storage.getStored("a")).toBeNull();
    expect(storage.getStoredJson("a")).toBeNull();
    expect(storage.storedKeys()).toEqual([]);
    expect(storage.visitorActive()).toBe(false);
    expect(() => storage.setStored("a", "1")).not.toThrow();
    expect(() => storage.removeStored("a")).not.toThrow();
    expect(() => storage.removeStoredEverywhere("a")).not.toThrow();
    expect(errors).toHaveBeenCalled();
  });

  it("fails the session's identity, written or removed", () => {
    const { SessionStorageError, removeStored, setStored } = storage;
    expect(() => setStored("provisa_org", "o", { global: true })).toThrow(SessionStorageError);
    expect(() => removeStored("provisa_role", { global: true })).toThrow(SessionStorageError);
    expect(storage.getStored("provisa_token", { global: true })).toBeNull();
  });

  it("fails a visit: a tab that could not become a visitor must not write the login's keys", () => {
    expect(() => storage.startVisit()).toThrow(storage.SessionStorageError);
  });
});

describe("a store blocked at the door", () => {
  it("copes with the accessor itself throwing", () => {
    const real = window.localStorage;
    Object.defineProperty(window, "localStorage", {
      configurable: true,
      get: () => {
        throw new DOMException("denied", "SecurityError");
      },
    });
    undo.push(() =>
      Object.defineProperty(window, "localStorage", { configurable: true, value: real }),
    );
    expect(storage.getStored("a")).toBeNull();
    expect(() => storage.setStored("a", "1")).not.toThrow();
  });
});

describe("a read before the scope is bound", () => {
  it("is still a defect, not a storage failure", () => {
    storage.bindStorageScope(null);
    expect(() => storage.getStored("a")).toThrow(/neither is known yet/);
    expect(() => storage.setStored("a", "1")).toThrow(/neither is known yet/);
  });
});

describe("corrupt JSON", () => {
  it("reads as nothing saved, logged and noticed", () => {
    install("localStorage", fullStorage(1000, { [`${SCOPE}a`]: "{not json" }));
    const heard: string[] = [];
    storage.listenForStorageNotice((kind) => heard.push(kind));
    expect(storage.getStoredJson("a")).toBeNull();
    expect(heard).toEqual(["settings"]);
    expect(errors).toHaveBeenCalled();
  });
});

describe("a visitor's identity in sessionStorage", () => {
  it("keeps working while localStorage is full, and survives eviction", () => {
    install("localStorage", fullStorage(5));
    const session = install("sessionStorage", fullStorage(200));
    storage.startVisit();
    storage.setStored("provisa_token", "visitor bearer", { global: true });
    storage.setStored("provisa_org", "sbx", { global: true });
    expect(session.getItem("visitor:provisa_token")).toBe("visitor bearer");
    expect(storage.getStored("provisa_token", { global: true })).toBe("visitor bearer");

    storage.setStored("big", "x".repeat(60), { session: true });
    storage.setStored("bigger", "x".repeat(1000), { session: true });
    expect(storage.getStored("provisa_token", { global: true })).toBe("visitor bearer");
    expect(storage.getStored("provisa_org", { global: true })).toBe("sbx");

    storage.endVisit();
    expect(storage.visitorActive()).toBe(false);
    expect(session.getItem("visitor:provisa_token")).toBeNull();
  });

  it("gives a chat's saved results up before the visitor's identity", () => {
    const session = install(
      "sessionStorage",
      fullStorage(150, { [`${SCOPE}mcp.chat.analyst`]: "x".repeat(60) }),
    );
    storage.startVisit();
    storage.setStored("provisa_token", "t".repeat(60), { global: true });
    expect(session.getItem("visitor:provisa_token")).toBe("t".repeat(60));
    expect(session.getItem(`${SCOPE}mcp.chat.analyst`)).toBeNull();
  });
});
