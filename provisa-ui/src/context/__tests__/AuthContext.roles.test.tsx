// Copyright (c) 2026 Kenneth Stott
// Canary: 6f0b31ad-27c4-4e9b-8a15-c3d9e6208b74
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1295: which role the client puts in X-Provisa-Role. The server honors that header only for
// roles the caller is assigned (tests/integration/test_org_creator_role_request.py), so a client
// that invents `admin` when its role list is empty turns one failed roles query into a session
// where every request 403s — the "Role 'admin' is not assigned to this user" screen a self-service
// org creator hit. Under enforced auth the role list must contain only assigned roles, or nothing.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen } from "../../test-utils/render";
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
  const { role, availableRoles, error, devMode } = useAuth();
  return (
    <div>
      <span data-testid="role">{role?.id ?? "none"}</span>
      <span data-testid="available">{availableRoles.map((r) => r.id).join(",") || "none"}</span>
      <span data-testid="error">{error ?? "none"}</span>
      <span data-testid="dev">{String(devMode)}</span>
    </div>
  );
}

function renderAuth(authEnabled: boolean) {
  return render(
    <AuthProvider authEnabled={authEnabled}>
      <Probe />
    </AuthProvider>,
  );
}

const ORG_ADMIN_IDENTITY = {
  dev_mode: false,
  user_id: "alice",
  assignments: [{ role_id: "org_admin", domain_id: "*" }],
  org_memberships: [{ org_id: "kstott", role: "org_admin" }],
  active_org_id: "kstott",
};

const ORG_ROLE_CATALOG = {
  data: {
    roles: [
      { id: "admin", capabilities: ["admin"], domainAccess: ["*"] },
      { id: "analyst", capabilities: ["usage"], domainAccess: ["*"] },
      { id: "org_admin", capabilities: ["user_management"], domainAccess: ["*"] },
    ],
  },
};

describe("AuthProvider role resolution (REQ-1295)", () => {
  beforeEach(() => {
    localStorage.clear();
    vi.clearAllMocks();
    refetchDomains.mockResolvedValue({ data: { domains: [] } });
  });

  it("offers only the roles the user is assigned, never the seeded `admin` row", async () => {
    fetchMe.mockResolvedValue(ORG_ADMIN_IDENTITY);
    refetchRoles.mockResolvedValue(ORG_ROLE_CATALOG);

    renderAuth(true);

    // `admin` and `analyst` exist in the org's role catalog; alice holds neither. Selecting one
    // would put it in X-Provisa-Role and 403 the request.
    expect(await screen.findByTestId("available")).toHaveTextContent("org_admin");
    expect(screen.getByTestId("available")).not.toHaveTextContent("analyst");
    expect(screen.getByTestId("role")).toHaveTextContent("org_admin");
    expect(screen.getByTestId("error")).toHaveTextContent("none");
  });

  it("reports a failed roles query instead of inventing an admin role", async () => {
    fetchMe.mockResolvedValue(ORG_ADMIN_IDENTITY);
    refetchRoles.mockRejectedValue(new Error("Org selection required"));

    renderAuth(true);

    expect(await screen.findByTestId("error")).toHaveTextContent(/Could not load roles/);
    expect(screen.getByTestId("error")).toHaveTextContent(/Org selection required/);
    expect(screen.getByTestId("available")).toHaveTextContent("none");
    expect(screen.getByTestId("role")).toHaveTextContent("none");
  });

  it("ignores a stored role the user is not assigned", async () => {
    // A prior session in this browser left `admin` behind — it must not be re-selected.
    localStorage.setItem("provisa_role", "admin");
    fetchMe.mockResolvedValue(ORG_ADMIN_IDENTITY);
    refetchRoles.mockResolvedValue(ORG_ROLE_CATALOG);

    renderAuth(true);

    expect(await screen.findByTestId("role")).toHaveTextContent("org_admin");
  });

  it("acts as the data-plane role, not platform_admin, when both are assigned", async () => {
    // REQ-1297: the bootstrap super-admin holds platform_admin AND org_admin in the root org. The
    // role list came back in Postgres heap order with platform_admin first, so "All roles" sent
    // platform_admin in X-Provisa-Role and the data surfaces got a schema built for a role whose
    // capabilities are only the platform bypass — every demo table granted to analyst/org_admin
    // disappeared from the GraphQL schema.
    fetchMe.mockResolvedValue({
      ...ORG_ADMIN_IDENTITY,
      assignments: [
        { role_id: "platform_admin", domain_id: "*" },
        { role_id: "org_admin", domain_id: "*" },
      ],
    });
    refetchRoles.mockResolvedValue({
      data: {
        roles: [
          { id: "platform_admin", capabilities: ["admin", "superadmin"], domainAccess: ["*"] },
          { id: "org_admin", capabilities: ["user_management"], domainAccess: ["*"] },
        ],
      },
    });

    renderAuth(true);

    expect(await screen.findByTestId("role")).toHaveTextContent("org_admin");
    // Both stay selectable — only the default acting role changes.
    expect(screen.getByTestId("available")).toHaveTextContent("org_admin,platform_admin");
  });

  it("still grants the full-capability role where auth is not enforced", async () => {
    // No auth means the server grants every capability to every caller; the client mirrors that.
    fetchMe.mockRejectedValue(new Error("no identity"));
    refetchRoles.mockResolvedValue({ data: { roles: [] } });

    renderAuth(false);

    expect(await screen.findByTestId("dev")).toHaveTextContent("true");
    expect(screen.getByTestId("role")).toHaveTextContent("admin");
    expect(screen.getByTestId("error")).toHaveTextContent("none");
  });
});
