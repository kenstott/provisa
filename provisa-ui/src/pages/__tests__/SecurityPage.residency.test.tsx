// Copyright (c) 2026 Kenneth Stott
// Canary: a1185c2a-24bd-4ccd-81b5-961b71496e30
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1921: the role editor's data_residency right — a checkbox and the region values its grant
// covers (the org's regions and "No region"), shown only when the platform declares regions.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent, waitFor } from "../../test-utils/render";

const upsertRoleSpy = vi.fn();
const choices = vi.hoisted(() => ({
  value: { regions: ["eu", "us"], connected: "eu" as string | null },
}));

vi.mock("../../context/DomainFilterContext", () => ({
  useDomainFilter: () => ({
    setDomains: vi.fn(),
    setSelectedDomain: vi.fn(),
    checkedDomains: new Set<string>(),
  }),
}));

vi.mock("../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useAdminQueries")>()),
  useRoles: () => ({ roles: [], loading: false, refetch: vi.fn() }),
  useTables: () => ({ tables: [], loading: false, refetch: vi.fn() }),
  useDomains: () => ({ domains: [{ id: "sales" }], loading: false, refetch: vi.fn() }),
}));

vi.mock("../../hooks/useRegionQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useRegionQueries")>()),
  useRegionChoices: () => choices.value,
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
  choices.value = { regions: ["eu", "us"], connected: "eu" };
});

async function newRole(id: string) {
  render(<SecurityRolesPage />);
  fireEvent.click(await screen.findByTestId("toggle-role-form"));
  fireEvent.change(screen.getByTestId("role-id-input"), { target: { value: id } });
  fireEvent.click(screen.getByRole("textbox", { name: "Domain Access" }));
  fireEvent.click(await screen.findByRole("option", { name: "sales", hidden: true }));
}

describe("the role editor's data_residency right (REQ-1921)", () => {
  it("grants the right with the region values chosen, No region among them", async () => {
    await newRole("eu_steward");
    fireEvent.click(screen.getByTestId("residency-grant-checkbox"));
    fireEvent.click(screen.getByRole("textbox", { name: "Region values covered" }));
    fireEvent.click(await screen.findByRole("option", { name: "eu", hidden: true }));
    fireEvent.click(await screen.findByRole("option", { name: "No region", hidden: true }));
    fireEvent.click(screen.getByTestId("save-role"));
    await waitFor(() => expect(upsertRoleSpy).toHaveBeenCalled());
    const saved = upsertRoleSpy.mock.calls[0][0];
    expect(saved.capabilities).toContain("data_residency");
    expect(saved.residencyValues).toEqual(["eu", "no_region"]);
  });

  it("saves no values for a role without the right", async () => {
    await newRole("analyst2");
    fireEvent.click(screen.getByTestId("save-role"));
    await waitFor(() => expect(upsertRoleSpy).toHaveBeenCalled());
    const saved = upsertRoleSpy.mock.calls[0][0];
    expect(saved.capabilities).not.toContain("data_residency");
    expect(saved.residencyValues).toEqual([]);
  });

  it("offers no data_residency when the platform declares no regions", async () => {
    choices.value = { regions: [], connected: null };
    await newRole("analyst3");
    expect(screen.queryByTestId("residency-grant")).toBeNull();
    expect(screen.queryByLabelText("data_residency")).toBeNull();
  });
});
