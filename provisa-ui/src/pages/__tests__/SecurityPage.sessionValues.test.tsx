// Copyright (c) 2026 Kenneth Stott
// Canary: 0dc44bbb-1b18-4ba8-83b9-6ee2a377a960
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1682: the role editor's session values -- name and value rows, added and removed, saved as
// the whole set; a name the server refuses is reported in the reader's words.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "../../test-utils/render";

const upsertRoleSpy = vi.fn();

vi.mock("../../context/DomainFilterContext", () => ({
  useDomainFilter: () => ({
    setDomains: vi.fn(),
    setSelectedDomain: vi.fn(),
    checkedDomains: new Set<string>(),
  }),
}));

// Stable: a hook that answered a new array each render would re-run the page's effects.
const ROLES = [
  {
    id: "seller",
    capabilities: [],
    demonstrated: [],
    domain_access: ["sales"],
    parentRoleId: null,
    sessionVars: [{ name: "tier", value: "gold", __typename: "SessionVarType" }],
  },
];

vi.mock("../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useAdminQueries")>()),
  useRoles: () => ({ roles: ROLES, loading: false, refetch: vi.fn() }),
  useTables: () => ({ tables: [], loading: false, refetch: vi.fn() }),
  useDomains: () => ({ domains: [{ id: "sales" }], loading: false, refetch: vi.fn() }),
}));

vi.mock("../../hooks/useRegionQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useRegionQueries")>()),
  useRegionChoices: () => ({ regions: [], connected: null }),
}));

vi.mock("../../hooks/useSecurityQueries", () => ({
  useRLSRules: () => ({ rlsRules: [], loading: false, refetch: vi.fn() }),
  useUpsertRole: () => ({ upsertRole: upsertRoleSpy, loading: false }),
  useDeleteRole: () => ({ deleteRole: vi.fn(), loading: false }),
  useUpsertRlsRule: () => ({ upsertRlsRule: vi.fn(), loading: false }),
  useDeleteRlsRule: () => ({ deleteRlsRule: vi.fn(), loading: false }),
  useRevokeRoleGrants: () => ({ revokeFromTable: vi.fn(), revokeFromObject: vi.fn() }),
}));

import { SecurityRolesPage } from "../SecurityPage";

beforeEach(() => {
  upsertRoleSpy.mockReset();
  upsertRoleSpy.mockResolvedValue({ success: true, message: "" });
});

async function openRole() {
  render(<SecurityRolesPage />);
  fireEvent.click(within(await screen.findByTestId("roles-list")).getByText("seller"));
}

async function saved() {
  fireEvent.click(screen.getByTestId("save-role-seller"));
  await waitFor(() => expect(upsertRoleSpy).toHaveBeenCalled());
  return upsertRoleSpy.mock.calls[0][0];
}

describe("the role editor's session values (REQ-1682)", () => {
  it("shows the role's values in its read view", async () => {
    await openRole();
    expect(screen.getByTestId("role-session-values-seller")).toHaveTextContent("tier = gold");
  });

  it("edits the values the role holds and saves them without the server's type name", async () => {
    await openRole();
    fireEvent.click(screen.getByTestId("edit-role-seller"));
    expect(screen.getByTestId("role-session-value-name-0")).toHaveValue("tier");
    fireEvent.change(screen.getByTestId("role-session-value-value-0"), {
      target: { value: "platinum" },
    });
    expect((await saved()).sessionVars).toEqual([{ name: "tier", value: "platinum" }]);
  });

  it("adds a value and removes one, saving the whole set", async () => {
    await openRole();
    fireEvent.click(screen.getByTestId("edit-role-seller"));
    fireEvent.click(screen.getByTestId("role-session-value-add"));
    fireEvent.change(screen.getByTestId("role-session-value-name-1"), {
      target: { value: "desk" },
    });
    fireEvent.change(screen.getByTestId("role-session-value-value-1"), {
      target: { value: "7" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Remove tier" }));
    expect(screen.getAllByTestId("role-session-value-row")).toHaveLength(1);
    expect((await saved()).sessionVars).toEqual([{ name: "desk", value: "7" }]);
  });

  it("reports a name the server refuses, in the reader's words", async () => {
    upsertRoleSpy.mockResolvedValue({
      success: false,
      message: "raw",
      code: "schema.role_session_var_name_reserved",
      params: { role: "seller", name: "user_id" },
    });
    await openRole();
    fireEvent.click(screen.getByTestId("edit-role-seller"));
    fireEvent.change(screen.getByTestId("role-session-value-name-0"), {
      target: { value: "user_id" },
    });
    await saved();
    expect(
      await screen.findByText(
        "'user_id' is bound by the request or the node and cannot be a role's session value",
      ),
    ).toBeInTheDocument();
  });
});
