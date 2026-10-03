// Copyright (c) 2026 Kenneth Stott
// Canary: 4d6970bf-f43c-4c42-9640-09ae98605711
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1235: under multi-tenancy the server refuses a request that names no org; it no longer
// picks the org of a user who belongs to exactly one. The request interceptors (lib/authFetch.ts,
// apolloClient.ts) read the org from localStorage `provisa_org`, so the org sign-in settles on
// has to be written there BEFORE the next request goes out — the roles query is that request.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, waitFor } from "../../test-utils/render";
import { AuthProvider, useAuth } from "../AuthContext";

const fetchMe = vi.fn();
const refetchRoles = vi.fn();
const refetchDomains = vi.fn();

vi.mock("../../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../api/admin")>()),
  fetchMe: (...args: unknown[]) => fetchMe(...args),
}));

vi.mock("../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useAdminQueries")>()),
  useRoles: () => ({ refetch: refetchRoles }),
  useDomains: () => ({ refetch: refetchDomains }),
}));

function Probe() {
  const { role, activeOrgId } = useAuth();
  return (
    <div>
      <span data-testid="role">{role?.id ?? "none"}</span>
      <span data-testid="org">{activeOrgId ?? "none"}</span>
    </div>
  );
}

const ROLES = {
  data: {
    roles: [
      { id: "org_admin", capabilities: ["user_management"], demonstrated: [], domainAccess: ["*"] },
    ],
  },
};

function identity(orgs: string[], activeOrgId: string | null) {
  return {
    dev_mode: false,
    user_id: "alice",
    assignments: [{ role_id: "org_admin", domain_id: "*" }],
    org_memberships: orgs.map((org_id) => ({ org_id, role: "org_admin" })),
    active_org_id: activeOrgId,
  };
}

describe("the org a sign-in settles on is named on the requests that follow (REQ-1235)", () => {
  let orgWhenRolesWereFetched: string | null | undefined;

  beforeEach(() => {
    localStorage.clear();
    vi.clearAllMocks();
    orgWhenRolesWereFetched = undefined;
    refetchDomains.mockResolvedValue({ data: { domains: [] } });
    refetchRoles.mockImplementation(async () => {
      orgWhenRolesWereFetched = localStorage.getItem("provisa_org");
      return ROLES;
    });
  });

  function renderAuth() {
    return render(
      <AuthProvider authEnabled authSettled>
        <Probe />
      </AuthProvider>,
    );
  }

  it("stores a lone membership's org before the roles query is sent", async () => {
    // The server reports no active org: nothing named one on /auth/me.
    fetchMe.mockResolvedValue(identity(["kstott"], null));
    renderAuth();
    await waitFor(() => expect(screen.getByTestId("role")).toHaveTextContent("org_admin"));
    expect(orgWhenRolesWereFetched).toBe("kstott");
    expect(localStorage.getItem("provisa_org")).toBe("kstott");
    expect(screen.getByTestId("org")).toHaveTextContent("kstott");
  });

  it("stores the org the server reports as active", async () => {
    fetchMe.mockResolvedValue(identity(["acme", "beta"], "beta"));
    renderAuth();
    await waitFor(() => expect(screen.getByTestId("role")).toHaveTextContent("org_admin"));
    expect(orgWhenRolesWereFetched).toBe("beta");
  });

  it("keeps a stored org the user still belongs to", async () => {
    localStorage.setItem("provisa_org", "acme");
    fetchMe.mockResolvedValue(identity(["acme", "beta"], null));
    renderAuth();
    await waitFor(() => expect(screen.getByTestId("role")).toHaveTextContent("org_admin"));
    expect(orgWhenRolesWereFetched).toBe("acme");
  });

  it("names no org for a user in several orgs who has not chosen one", async () => {
    fetchMe.mockResolvedValue(identity(["acme", "beta"], null));
    renderAuth();
    await waitFor(() => expect(refetchRoles).toHaveBeenCalled());
    expect(orgWhenRolesWereFetched).toBeNull();
    expect(localStorage.getItem("provisa_org")).toBeNull();
  });
});
