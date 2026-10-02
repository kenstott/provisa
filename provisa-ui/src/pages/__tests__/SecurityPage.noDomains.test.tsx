// Copyright (c) 2026 Kenneth Stott
// Canary: ac6393d0-4565-4700-91ce-22164bb8262d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A role reaches the domains it lists and no others; "All Domains" is the only way to say all.
// The editor starts a new role with none, so it must say that such a role reaches no data rather
// than let an empty picker read as "unrestricted" — and the roles table says the same of a saved
// role that lists none.

import { describe, it, expect, vi } from "vitest";
import { render, screen, fireEvent } from "../../test-utils/render";

const ROLES = [
  {
    id: "analyst",
    capabilities: [],
    demonstrated: [],
    domain_access: ["sales"],
    parentRoleId: null,
  },
  {
    id: "everything",
    capabilities: [],
    demonstrated: [],
    domain_access: ["*"],
    parentRoleId: null,
  },
  { id: "newrole", capabilities: [], demonstrated: [], domain_access: [], parentRoleId: null },
];

vi.mock("../../context/DomainFilterContext", () => ({
  useDomainFilter: () => ({
    setDomains: vi.fn(),
    setSelectedDomain: vi.fn(),
    checkedDomains: new Set<string>(),
  }),
}));

vi.mock("../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useAdminQueries")>()),
  useRoles: () => ({ roles: ROLES, loading: false, refetch: vi.fn() }),
  useTables: () => ({ tables: [], loading: false, refetch: vi.fn() }),
  useDomains: () => ({ domains: [{ id: "sales" }], loading: false, refetch: vi.fn() }),
}));

vi.mock("../../hooks/useSecurityQueries", () => ({
  useRLSRules: () => ({ rlsRules: [], loading: false, refetch: vi.fn() }),
  useUpsertRole: () => ({ upsertRole: vi.fn(), loading: false }),
  useDeleteRole: () => ({ deleteRole: vi.fn(), loading: false }),
  useUpsertRlsRule: () => ({ upsertRlsRule: vi.fn(), loading: false }),
  useDeleteRlsRule: () => ({ deleteRlsRule: vi.fn(), loading: false }),
}));

import { SecurityPage } from "../SecurityPage";

describe("SecurityPage — a role with no domains", () => {
  it("says a new role with no domains selected reaches no data", () => {
    render(<SecurityPage />);
    expect(screen.queryByTestId("role-no-domains-note")).toBeNull();
    fireEvent.click(screen.getByTestId("toggle-role-form"));
    expect(screen.getByTestId("role-no-domains-note")).toHaveTextContent("reaches no data");
  });

  it("names a saved role that lists no domains, and leaves the others as they are", () => {
    render(<SecurityPage />);
    expect(screen.getByTestId("role-domains-newrole")).toHaveTextContent("reaches no data");
    expect(screen.getByTestId("role-domains-analyst")).toHaveTextContent("sales");
    expect(screen.getByTestId("role-domains-analyst")).not.toHaveTextContent("reaches no data");
    expect(screen.getByTestId("role-domains-everything")).toHaveTextContent("*");
  });

  it("drops the note when a saved role that lists domains is edited", () => {
    render(<SecurityPage />);
    fireEvent.click(screen.getByText("analyst"));
    fireEvent.click(screen.getByTestId("edit-role-analyst"));
    expect(screen.queryByTestId("role-no-domains-note")).toBeNull();
  });

  it("shows the note when a saved role with no domains is edited", () => {
    render(<SecurityPage />);
    fireEvent.click(screen.getByText("newrole"));
    fireEvent.click(screen.getByTestId("edit-role-newrole"));
    expect(screen.getByTestId("role-no-domains-note")).toBeInTheDocument();
  });
});
