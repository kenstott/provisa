// Copyright (c) 2026 Kenneth Stott
// Canary: e69e0e68-de15-4e8b-ac2e-d08c054a6dd3
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A role someone holds, or a grant names, is not deleted (REQ-1918). The page used to drop the
// mutation's result, so a refused delete looked like nothing happened. It now shows what still
// refers to the role, and shows the server's message for any other refusal.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "../../test-utils/render";

const deleteRoleSpy = vi.fn();

const ROLES = [
  {
    id: "seller",
    description: "What seller is for.",
    capabilities: [],
    demonstrated: [],
    domain_access: ["sales"],
    parentRoleId: null,
  },
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
  useDeleteRole: () => ({ deleteRole: deleteRoleSpy, loading: false }),
  useUpsertRlsRule: () => ({ upsertRlsRule: vi.fn(), loading: false }),
  useDeleteRlsRule: () => ({ deleteRlsRule: vi.fn(), loading: false }),
  useRevokeRoleGrants: () => ({ revokeFromTable: vi.fn(), revokeFromObject: vi.fn() }),
}));

vi.mock("../../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../api/admin")>()),
  fetchDerivedRoles: vi.fn(async () => ({ roles: [], source: "deployment" })),
}));

import { SecurityRolesPage } from "../SecurityPage";

beforeEach(() => deleteRoleSpy.mockReset());

// The delete action is in the role's expanded row.
async function openRole() {
  fireEvent.click(await screen.findByText("seller"));
}

// The trash icon asks first (the shared confirmation, naming the role); Confirm deletes.
async function confirmDelete() {
  fireEvent.click(await screen.findByTestId("delete-role-seller"));
  const dialog = await screen.findByRole("dialog");
  expect(dialog.textContent).toContain('Delete role "seller"?');
  expect(deleteRoleSpy).not.toHaveBeenCalled();
  fireEvent.click(screen.getByRole("button", { name: "Confirm" }));
}

describe("deleting a role on the Security page", () => {
  it("asks before it deletes, and deletes nothing on cancel", async () => {
    render(<SecurityRolesPage />);
    await openRole();
    fireEvent.click(await screen.findByTestId("delete-role-seller"));
    await screen.findByRole("dialog");
    fireEvent.click(screen.getByRole("button", { name: "Cancel" }));
    await waitFor(() => expect(screen.queryByRole("dialog")).toBeNull());
    expect(deleteRoleSpy).not.toHaveBeenCalled();
  });

  it("shows what still refers to a role whose delete is refused", async () => {
    deleteRoleSpy.mockResolvedValue({
      success: false,
      message: "Role 'seller' is still referred to by: role_assignment 7, column 41",
      code: "schema.role_has_dependents",
      params: {
        role: "seller",
        dependents: [
          {
            kind: "role_assignment",
            id: 7,
            name: "alice holds seller",
            via: ["user_role_assignments.role_id"],
          },
          { kind: "column", id: 41, name: "orders.amount", via: ["table_columns.visible_to"] },
        ],
      },
    });
    render(<SecurityRolesPage />);

    await openRole();
    await confirmDelete();

    expect(await screen.findByText("Cannot delete seller yet")).toBeInTheDocument();
    expect(screen.getByText("alice holds seller")).toBeInTheDocument();
    expect(screen.getByText("orders.amount")).toBeInTheDocument();
    expect(deleteRoleSpy).toHaveBeenCalledWith("seller");

    fireEvent.click(screen.getByRole("button", { name: "Close" }));
    await waitFor(() => expect(screen.queryByText("Cannot delete seller yet")).toBeNull());
  });

  it("shows the server's message for a refusal that lists no dependents", async () => {
    deleteRoleSpy.mockResolvedValue({
      success: false,
      message: "Role 'seller' is a system role and cannot be deleted",
      code: "schema.role_is_system",
      params: { role: "seller", dependents: [] },
    });
    render(<SecurityRolesPage />);

    await openRole();
    await confirmDelete();

    expect(await screen.findByTestId("security-roles-error")).toHaveTextContent(
      "Role 'seller' is a system role and cannot be deleted",
    );
    expect(screen.queryByText("Cannot delete seller yet")).toBeNull();
  });
});
