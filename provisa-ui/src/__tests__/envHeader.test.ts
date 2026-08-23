// Copyright (c) 2026 Kenneth Stott
// Canary: 4a8e2c05-71bd-4f36-8091-53df6b7a91ce
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1487: the selected environment reaching the server on every request.
//
// The header is what turns a chosen branch into the model a request is answered from, and it is
// attached in ONE place for the ~100 REST call sites rather than threaded through each. What is
// asserted here is that the interceptor attaches it under the conditions the design names — and
// that it attaches it with no bearer at all, because a deployment with auth disabled branches its
// model exactly as an authenticated one does and would otherwise be pinned to prod.

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

vi.mock("../lib/firebase", () => ({ currentFirebaseToken: vi.fn() }));
vi.mock("../lib/sessionToken", () => ({ storedToken: vi.fn() }));

import { currentFirebaseToken } from "../lib/firebase";
import { storedToken } from "../lib/sessionToken";
import { ENV_HEADER, ENV_STORAGE_KEY, ORG_HEADER, installAuthFetch } from "../lib/authFetch";

const mockFirebase = vi.mocked(currentFirebaseToken);
const mockStored = vi.mocked(storedToken);

const original = window.fetch;
let seen: Headers | null = null;

beforeEach(() => {
  vi.clearAllMocks();
  localStorage.clear();
  seen = null;
  mockFirebase.mockResolvedValue(null);
  mockStored.mockReturnValue(null);
  window.fetch = vi.fn(async (_input: RequestInfo | URL, init?: RequestInit) => {
    seen = new Headers(init?.headers);
    return new Response("{}", { status: 200 });
  }) as typeof window.fetch;
  installAuthFetch();
  Object.defineProperty(window, "location", {
    configurable: true,
    value: { ...window.location, reload: vi.fn() },
  });
});

afterEach(() => {
  window.fetch = original;
});

describe("the environment header", () => {
  it("names the selected environment", async () => {
    localStorage.setItem(ENV_STORAGE_KEY, "dev");
    await window.fetch("/admin/orgs");
    expect(seen?.get(ENV_HEADER)).toBe("dev");
  });

  it("rides with no bearer at all", async () => {
    // An auth-disabled deployment holds no token; gating the header on one would leave it unable
    // to read any environment but prod.
    localStorage.setItem(ENV_STORAGE_KEY, "dev");
    await window.fetch("/admin/orgs");
    expect(seen?.get(ENV_HEADER)).toBe("dev");
    expect(seen?.get("authorization")).toBeNull();
  });

  it("is absent while nothing is selected, which the server serves as prod", async () => {
    mockStored.mockReturnValue("tok");
    await window.fetch("/admin/orgs");
    expect(seen?.get(ENV_HEADER)).toBeNull();
  });

  it("never leaves this origin", async () => {
    localStorage.setItem(ENV_STORAGE_KEY, "dev");
    mockStored.mockReturnValue("tok");
    await window.fetch("https://accounts.google.com/token");
    expect(seen?.get(ENV_HEADER)).toBeNull();
    expect(seen?.get("authorization")).toBeNull();
  });

  it("leaves the org header on its own terms", async () => {
    // The org still rides only with a bearer, unchanged by this addition.
    localStorage.setItem("provisa_org", "acme");
    localStorage.setItem(ENV_STORAGE_KEY, "dev");
    await window.fetch("/admin/orgs");
    expect(seen?.get(ORG_HEADER)).toBeNull();
    mockStored.mockReturnValue("tok");
    await window.fetch("/admin/orgs");
    expect(seen?.get(ORG_HEADER)).toBe("acme");
  });

  it("does not clobber a header the caller already set", async () => {
    localStorage.setItem(ENV_STORAGE_KEY, "dev");
    await window.fetch("/admin/orgs", { headers: { [ENV_HEADER]: "staging" } });
    expect(seen?.get(ENV_HEADER)).toBe("staging");
  });
});

describe("a selection the server no longer has", () => {
  // REQ-1487: an unknown environment is refused, never served as prod — so a deleted branch left
  // in localStorage 404s EVERY request, including the /setup/status the app boots on. The switcher
  // that would repair it never mounts, so the repair has to live where every request passes.
  function answer(status: number, body: string) {
    window.fetch = vi.fn(async () => new Response(body, { status })) as typeof window.fetch;
    installAuthFetch();
  }

  const unknownEnv = JSON.stringify({
    error: { code: "env.unknown", message: "org 'default' has no environment named 'dev'" },
  });

  it("is dropped when the server says it does not know the environment", async () => {
    localStorage.setItem(ENV_STORAGE_KEY, "dev");
    answer(404, unknownEnv);
    await window.fetch("/setup/status");
    expect(localStorage.getItem(ENV_STORAGE_KEY)).toBeNull();
    expect(window.location.reload).toHaveBeenCalled();
  });

  it("survives an ordinary 404, which says nothing about the environment", async () => {
    localStorage.setItem(ENV_STORAGE_KEY, "dev");
    answer(404, JSON.stringify({ detail: "table not found" }));
    await window.fetch("/admin/tables/nope");
    expect(localStorage.getItem(ENV_STORAGE_KEY)).toBe("dev");
    expect(window.location.reload).not.toHaveBeenCalled();
  });

  it("survives a 404 that carries no JSON at all", async () => {
    localStorage.setItem(ENV_STORAGE_KEY, "dev");
    answer(404, "<!doctype html>not found");
    await window.fetch("/admin/tables/nope");
    expect(localStorage.getItem(ENV_STORAGE_KEY)).toBe("dev");
    expect(window.location.reload).not.toHaveBeenCalled();
  });
});
