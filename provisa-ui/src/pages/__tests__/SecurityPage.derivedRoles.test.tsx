// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2078, REQ-2079, REQ-2080: derived roles in the Roles list -- each attribute a parent marked
// derived, its value roles under it; its panel shows the values seen and what names it, with no
// grant controls; "Add role" declares one, with a description, and re-reads the caller's roles.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "../../test-utils/render";

const rebind = vi.fn(async () => {});
vi.mock("../../context/AuthContext", () => ({ useAuth: () => ({ rebind }) }));
vi.mock("../../context/DomainFilterContext", () => ({
  useDomainFilter: () => ({
    setDomains: vi.fn(),
    setSelectedDomain: vi.fn(),
    checkedDomains: new Set<string>(),
  }),
}));
vi.mock("../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useAdminQueries")>()),
  useRoles: () => ({
    roles: [
      {
        id: "analyst",
        description: "Queries the sales domain.",
        capabilities: ["query_development"],
        demonstrated: [],
        domain_access: ["sales"],
      },
    ],
    loading: false,
    refetch: vi.fn(),
  }),
  useDomains: () => ({ domains: [], loading: false, refetch: vi.fn() }),
}));
const upsertRole = vi.fn(async () => ({ success: true, message: "" }));
vi.mock("../../hooks/useSecurityQueries", () => ({
  useUpsertRole: () => ({ upsertRole, loading: false }),
  useDeleteRole: () => ({ deleteRole: vi.fn(), loading: false }),
  useRevokeRoleGrants: () => ({ revokeFromTable: vi.fn(), revokeFromObject: vi.fn() }),
}));
const REGION = {
  attribute: "region",
  description: "The subsidiary a member works for.",
  values: ["eu", "us"],
  uses: [{ kind: "column", name: "customers.email", via: ["table_columns.masks"] }],
};
const fetchDerivedRoles = vi.fn(async () => ({ roles: [REGION], source: "org" as const }));
const saveDerivedRole = vi.fn(async (_attribute: string, _description: string) => {});
vi.mock("../../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../api/admin")>()),
  fetchDerivedRoles: () => fetchDerivedRoles(),
  saveDerivedRole: (a: string, d: string) => saveDerivedRole(a, d),
  deleteDerivedRole: vi.fn(async () => {}),
}));

import { SecurityRolesPage } from "../SecurityPage";

beforeEach(() => {
  vi.clearAllMocks();
});

describe("derived roles on the Roles page", () => {
  it("lists each derived role, marked derived, with its value roles under it", async () => {
    render(<SecurityRolesPage />);
    const parent = await screen.findByTestId("derived-role-row-region");
    expect(within(parent).getByTestId("derived-badge-region")).toHaveTextContent("derived");
    expect(parent).toHaveTextContent("The subsidiary a member works for.");
    expect(screen.getByTestId("derived-value-row-region__eu")).toHaveTextContent(
      "Held when the user's region is eu",
    );
    expect(screen.getByTestId("derived-value-row-region__us")).toBeTruthy();
    expect(screen.getByTestId("role-description-analyst")).toHaveTextContent(
      "Queries the sales domain.",
    );
  });

  it("shows a derived role's values seen and what names it, with no grant controls", async () => {
    render(<SecurityRolesPage />);
    fireEvent.click(await screen.findByTestId("derived-role-row-region"));
    const panel = within(screen.getByTestId("roles-split-panel"));
    expect(panel.getByTestId("derived-values-region")).toHaveTextContent("region__eu, region__us");
    expect(panel.getByTestId("derived-uses-region")).toHaveTextContent("Column customers.email");
    expect(panel.queryByText("Capabilities")).toBeNull();
    expect(panel.queryByText("Domain Access")).toBeNull();
  });

  it("declares a derived role with its description, then re-reads the caller's roles", async () => {
    render(<SecurityRolesPage />);
    fireEvent.click(await screen.findByTestId("toggle-role-form"));
    fireEvent.click(await screen.findByTestId("add-derived-role"));
    const save = await screen.findByTestId("save-derived-role");
    fireEvent.change(screen.getByTestId("derived-role-attribute"), {
      target: { value: "department" },
    });
    // No description, no save: a role nobody can explain is one nobody can review.
    expect(save).toBeDisabled();
    fireEvent.change(screen.getByTestId("derived-role-description"), {
      target: { value: "The member's department." },
    });
    fireEvent.click(save);
    await waitFor(() =>
      expect(saveDerivedRole).toHaveBeenCalledWith("department", "The member's department."),
    );
    await waitFor(() => expect(rebind).toHaveBeenCalledTimes(1));
  });

  it("keeps a new granted role from saving until it says what it is for", async () => {
    render(<SecurityRolesPage />);
    fireEvent.click(await screen.findByTestId("toggle-role-form"));
    fireEvent.click(await screen.findByTestId("add-granted-role"));
    fireEvent.change(await screen.findByTestId("role-id-input"), { target: { value: "intern" } });
    expect(screen.getByTestId("save-role")).toBeDisabled();
    fireEvent.change(screen.getByTestId("role-description-input"), {
      target: { value: "Interns read the sales domain." },
    });
    expect(screen.getByTestId("role-description-input")).toHaveValue(
      "Interns read the sales domain.",
    );
  });
});
