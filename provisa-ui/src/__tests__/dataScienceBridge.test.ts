// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.

// REQ-2071: the Data Science area's Python reads as the page's user: the bearer, org, environment
// and acting roles the request interceptor attaches, worked out in the one place it is.

import { describe, it, expect, vi, beforeEach } from "vitest";

vi.mock("../lib/firebase", () => ({ currentFirebaseToken: vi.fn() }));
vi.mock("../lib/sessionToken", () => ({ storedToken: vi.fn() }));

import { currentFirebaseToken } from "../lib/firebase";
import { storedToken } from "../lib/sessionToken";
import { ENV_STORAGE_KEY } from "../lib/authFetch";
import { setActingRoles } from "../lib/actingRole";
import { bridgeAuth } from "../dataScience/bridge";

const mockFirebase = vi.mocked(currentFirebaseToken);
const mockStored = vi.mocked(storedToken);

beforeEach(() => {
  vi.clearAllMocks();
  localStorage.clear();
  mockFirebase.mockResolvedValue(null);
  mockStored.mockReturnValue(null);
  setActingRoles(null);
});

describe("bridgeAuth", () => {
  it("is the page's own origin, bearer, org, environment and acting roles", async () => {
    mockFirebase.mockResolvedValue("live-token");
    localStorage.setItem("provisa_org", "acme");
    localStorage.setItem(ENV_STORAGE_KEY, "staging");
    setActingRoles("analyst_eu,auditor");
    expect(await bridgeAuth()).toEqual({
      baseUrl: "",
      bearer: "live-token",
      org: "acme",
      env: "staging",
      role: "analyst_eu,auditor",
    });
  });

  it("names no org without a bearer, as the interceptor sends none", async () => {
    localStorage.setItem("provisa_org", "acme");
    expect(await bridgeAuth()).toEqual({
      baseUrl: "",
      bearer: null,
      org: null,
      env: null,
      role: null,
    });
  });

  it("is read anew each time: a rotated token and a newly picked role are what it says", async () => {
    mockStored.mockReturnValue("first");
    setActingRoles("analyst_eu");
    expect(await bridgeAuth()).toMatchObject({ bearer: "first", role: "analyst_eu" });
    mockStored.mockReturnValue("second");
    setActingRoles("org_admin");
    expect(await bridgeAuth()).toMatchObject({ bearer: "second", role: "org_admin" });
  });
});
