// Copyright (c) 2026 Kenneth Stott
// Canary: 5f2c9a41-7b6e-4d03-9a18-c3e50b7f2d64
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1326: client state that scopes the app to one signed-in session must not survive into the
// next one. Reported failure: the control plane was wiped and the operator signed in fresh and
// claimed the platform-admin slot — the server wrote both planes and answered 200 to every request,
// but the app showed no rights. `provisa_org` in localStorage still named the DELETED org from the
// previous session; it is read straight into `selectedOrg` and attached to every request as
// X-Org-Provisa, so the whole app rendered against an org this identity is not a member of.
// Logging out cleared `provisa_org` and hard-reloaded, which is exactly why logout/login "fixed" it.
import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor } from "../test-utils/render";
import { AuthProvider, useAuth } from "../context/AuthContext";

vi.mock("../api/admin", async (importOriginal) => {
  const actual = await importOriginal<typeof import("../api/admin")>();
  return { ...actual, fetchMe: vi.fn(), fetchBootstrapStatus: vi.fn() };
});
vi.mock("../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../hooks/useAdminQueries")>()),
  useRoles: () => ({ refetch: vi.fn().mockResolvedValue({ data: { roles: [] } }) }),
  useDomains: () => ({ refetch: vi.fn().mockResolvedValue({ data: { domains: [] } }) }),
}));

import { fetchMe } from "../api/admin";
import { clearSessionState, startSession, SESSION_KEYS } from "../lib/session";

const mockFetchMe = vi.mocked(fetchMe);

function identity(spec: {
  userId: string;
  orgMemberships?: { org_id: string; org_name: string }[];
  assignments?: { role_id: string; domain_id: string }[];
  activeOrgId?: string | null;
}) {
  return {
    user_id: spec.userId,
    dev_mode: false,
    assignments: spec.assignments ?? [],
    org_memberships: spec.orgMemberships ?? [],
    active_org_id: spec.activeOrgId ?? null,
  } as unknown as Awaited<ReturnType<typeof fetchMe>>;
}

function ActiveOrg() {
  const { activeOrgId, loading } = useAuth();
  return <div data-testid="active-org">{loading ? "loading" : (activeOrgId ?? "none")}</div>;
}

describe("session-scoped client state (REQ-1326)", () => {
  beforeEach(() => {
    mockFetchMe.mockReset();
    localStorage.clear();
  });

  it("startSession drops the previous session keys and the persisted admin snapshot", () => {
    for (const key of SESSION_KEYS) localStorage.setItem(key, "previous");
    localStorage.setItem("apollo-cache", '{"ROOT_QUERY":{}}');
    localStorage.setItem("admin-schema-version", "17");

    startSession("fresh-token");

    expect(localStorage.getItem("provisa_token")).toBe("fresh-token");
    expect(localStorage.getItem("provisa_org")).toBeNull();
    expect(localStorage.getItem("provisa_role")).toBeNull();
    expect(localStorage.getItem("apollo-cache")).toBeNull();
    expect(localStorage.getItem("admin-schema-version")).toBeNull();
  });

  it("clearSessionState leaves nothing behind for the next identity", () => {
    for (const key of SESSION_KEYS) localStorage.setItem(key, "previous");
    localStorage.setItem("apollo-cache", '{"ROOT_QUERY":{}}');

    clearSessionState();

    for (const key of SESSION_KEYS) expect(localStorage.getItem(key)).toBeNull();
    expect(localStorage.getItem("apollo-cache")).toBeNull();
  });

  // The regression itself: a stored org the server does not report membership of.
  it("discards a stored org this identity is not a member of and uses the server answer", async () => {
    localStorage.setItem("provisa_token", "tok");
    localStorage.setItem("provisa_org", "deleted-org");
    mockFetchMe.mockResolvedValue(
      identity({
        userId: "u1",
        activeOrgId: "default",
        orgMemberships: [{ org_id: "default", org_name: "Default" }],
        assignments: [{ role_id: "platform_admin", domain_id: "*" }],
      }),
    );

    render(
      <AuthProvider authEnabled authSettled>
        <ActiveOrg />
      </AuthProvider>,
    );

    await waitFor(() => expect(screen.getByTestId("active-org")).toHaveTextContent("default"));
    expect(localStorage.getItem("provisa_org")).toBeNull();
  });

  it("resolves to no org when the stored one is stale and the server names none", async () => {
    localStorage.setItem("provisa_token", "tok");
    localStorage.setItem("provisa_org", "deleted-org");
    mockFetchMe.mockResolvedValue(identity({ userId: "u1" }));

    render(
      <AuthProvider authEnabled authSettled>
        <ActiveOrg />
      </AuthProvider>,
    );

    await waitFor(() => expect(screen.getByTestId("active-org")).toHaveTextContent("none"));
    expect(localStorage.getItem("provisa_org")).toBeNull();
  });

  it("keeps a stored org the identity really is a member of", async () => {
    localStorage.setItem("provisa_token", "tok");
    localStorage.setItem("provisa_org", "acme");
    mockFetchMe.mockResolvedValue(
      identity({
        userId: "u1",
        activeOrgId: "other",
        orgMemberships: [
          { org_id: "acme", org_name: "Acme" },
          { org_id: "other", org_name: "Other" },
        ],
      }),
    );

    render(
      <AuthProvider authEnabled authSettled>
        <ActiveOrg />
      </AuthProvider>,
    );

    await waitFor(() => expect(screen.getByTestId("active-org")).toHaveTextContent("acme"));
    expect(localStorage.getItem("provisa_org")).toBe("acme");
  });
});
