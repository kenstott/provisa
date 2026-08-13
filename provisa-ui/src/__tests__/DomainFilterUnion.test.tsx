// Copyright (c) 2026 Kenneth Stott
// Canary: 7a5ba058-bc14-435d-a7d7-376f0952cf03
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1319: the domain filter must span EVERY active role, not an arbitrary one.
//
// The class of defect: a user holds several roles at once, the code picks `roles[0]`, and which
// role wins is an accident of list order. The symptom was silent and looked like missing data —
// a user holding analyst (pet-store, shelter) plus org_admin (*) saw two domains and lost meta and
// ops, with no error anywhere. Every test below holds the role SET fixed and varies only its ORDER
// or its size, because order-independence is the property that was actually broken.

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { render, screen, waitFor } from "../test-utils/render";
import { DomainFilterProvider, useDomainFilter } from "../context/DomainFilterContext";
import type { Role, Capability } from "../types/auth";

vi.mock("../context/AuthContext", () => ({ useAuth: vi.fn() }));
vi.mock("../api/admin", () => ({ fetchSettings: vi.fn() }));

import { useAuth } from "../context/AuthContext";
import { fetchSettings } from "../api/admin";

const mockUseAuth = vi.mocked(useAuth);
const mockFetchSettings = vi.mocked(fetchSettings);

const ANALYST: Role = {
  id: "analyst",
  capabilities: ["query_development"] as Capability[],
  domain_access: ["pet-store", "shelter"],
};
const ORG_ADMIN: Role = {
  id: "org_admin",
  capabilities: ["user_management"] as Capability[],
  domain_access: ["*"],
};
const OPS: Role = {
  id: "ops",
  capabilities: ["usage"] as Capability[],
  domain_access: ["ops"],
};

// Every domain the server knows about — what a wildcard role is entitled to.
const ALL_DOMAINS = ["meta", "ops", "shelter", "pet-store"];

function Probe() {
  const { domains } = useDomainFilter();
  return <div data-testid="domains">{[...domains].sort().join(",")}</div>;
}

function renderWith(selectedRoles: Role[]) {
  mockUseAuth.mockReturnValue({ selectedRoles } as unknown as ReturnType<typeof useAuth>);
  return render(
    <DomainFilterProvider>
      <Probe />
    </DomainFilterProvider>,
  );
}

async function domainsShown(): Promise<string[]> {
  const el = await screen.findByTestId("domains");
  await waitFor(() => expect(el.textContent).not.toBe(""));
  return el.textContent!.split(",");
}

beforeEach(() => {
  localStorage.clear();
  // @ts-expect-error -- jsdom has no fetch; the provider calls /data/domains for wildcard roles
  global.fetch = vi.fn(() => Promise.resolve({ json: () => Promise.resolve(ALL_DOMAINS) }));
  mockFetchSettings.mockResolvedValue({ naming: { use_domains: true } } as never);
});

afterEach(() => {
  vi.clearAllMocks();
});

describe("DomainFilterContext domain resolution", () => {
  it("gives a wildcard role the whole catalog", async () => {
    renderWith([ORG_ADMIN]);
    expect(await domainsShown()).toEqual([...ALL_DOMAINS].sort());
  });

  it("gives a named-domain role exactly its domains", async () => {
    renderWith([ANALYST]);
    expect(await domainsShown()).toEqual(["pet-store", "shelter"]);
  });

  it("a wildcard role anywhere in the set means the whole catalog", async () => {
    // The reported regression: analyst sorts first, so `roles[0]` collapsed the filter to its two
    // domains and meta + ops vanished even though org_admin entitled the user to them.
    renderWith([ANALYST, ORG_ADMIN]);
    expect(await domainsShown()).toEqual([...ALL_DOMAINS].sort());
  });

  it("does not depend on role order", async () => {
    const { unmount } = renderWith([ANALYST, ORG_ADMIN]);
    const analystFirst = await domainsShown();
    unmount();
    localStorage.clear();
    renderWith([ORG_ADMIN, ANALYST]);
    expect(await domainsShown()).toEqual(analystFirst);
  });

  it("unions the named domains of several non-wildcard roles", async () => {
    renderWith([ANALYST, OPS]);
    expect(await domainsShown()).toEqual(["ops", "pet-store", "shelter"]);
  });

  it("adding a role never removes a domain the user already had", async () => {
    // The monotonicity the union guarantees and `roles[0]` did not: acquiring a second role must
    // only ever widen what the user can see.
    const { unmount } = renderWith([ANALYST]);
    const before = await domainsShown();
    unmount();
    localStorage.clear();
    renderWith([ANALYST, OPS]);
    const after = await domainsShown();
    for (const d of before) expect(after).toContain(d);
  });
});
