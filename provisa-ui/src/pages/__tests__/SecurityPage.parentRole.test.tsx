// Copyright (c) 2026 Kenneth Stott
// Canary: 5d7b2f9e-8a3c-4e1d-b6f0-9c2a4e7d1b35
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1677: the role form offers "Inherits from" (one parent, chosen among the other roles) and
// stages it through upsertRole; the expanded role row shows the parent.

import { describe, it, expect, vi } from "vitest";
import { render, screen, fireEvent, waitFor, within } from "../../test-utils/render";
import { openNewRole } from "../../test-utils/securityRoles";

const upsertRoleSpy = vi.fn(async (_input: Record<string, unknown>) => ({
  success: true,
  message: "",
}));
const ROLES = [
  {
    id: "analyst",
    description: "What analyst is for.",
    capabilities: [],
    demonstrated: [],
    domain_access: ["*"],
    parents: ["viewer"],
    standard: true,
  },
  {
    id: "junior",
    description: "What junior is for.",
    capabilities: [],
    demonstrated: [],
    domain_access: ["sales"],
    parents: ["analyst", "auditor"],
    standard: false,
  },
  {
    id: "auditor",
    description: "What auditor is for.",
    capabilities: [],
    demonstrated: [],
    domain_access: ["sales"],
    parents: [],
    standard: false,
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
  useDomains: () => ({ domains: [], loading: false, refetch: vi.fn() }),
}));

vi.mock("../../hooks/useSecurityQueries", () => ({
  useRLSRules: () => ({ rlsRules: [], loading: false, refetch: vi.fn() }),
  useUpsertRole: () => ({ upsertRole: upsertRoleSpy, loading: false }),
  useDeleteRole: () => ({ deleteRole: vi.fn(), loading: false }),
  useUpsertRlsRule: () => ({ upsertRlsRule: vi.fn(), loading: false }),
  useDeleteRlsRule: () => ({ deleteRlsRule: vi.fn(), loading: false }),
  useRevokeRoleGrants: () => ({ revokeFromTable: vi.fn(), revokeFromObject: vi.fn() }),
}));

vi.mock("../../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../api/admin")>()),
  fetchDerivedRoles: vi.fn(async () => ({ roles: [], source: "deployment" })),
}));

import { SecurityPage } from "../SecurityPage";

// A role must list a domain before it can be saved (the server refuses an empty list), so a test
// that saves a new role chooses "All Domains" in the picker first.
// Mantine MultiSelect in jsdom: floating-ui hides the detached dropdown (all rects are 0), so
// the options are found through the input's aria-controls listbox with hidden: true.
async function choose(label: string, option: string) {
  const picker = screen
    .getAllByLabelText(label)
    .find((el) => el.tagName === "INPUT") as HTMLElement;
  fireEvent.click(picker);
  await waitFor(() => {
    if (!picker.getAttribute("aria-controls")) throw new Error("dropdown not open");
  });
  const listbox = document.getElementById(picker.getAttribute("aria-controls") as string);
  fireEvent.click(
    within(listbox as HTMLElement).getByRole("option", { name: option, hidden: true }),
  );
}

const input = (testid: string) => screen.getByTestId(testid) as HTMLInputElement;

describe("SecurityPage — role inheritance (REQ-1677, REQ-2155)", () => {
  it("shows every parent in the expanded role row", () => {
    render(<SecurityPage />);
    fireEvent.click(screen.getByText("junior"));
    expect(screen.getByTestId("role-parent-junior")).toHaveTextContent("analyst, auditor");
  });

  it("stages the chosen parents, in order, through upsertRole", async () => {
    upsertRoleSpy.mockClear();
    render(<SecurityPage />);
    await openNewRole();
    fireEvent.change(input("role-id-input"), { target: { value: "intern" } });
    await choose("Domain Access", "All Domains");
    await choose("Inherits from", "analyst");
    await choose("Inherits from", "auditor");
    fireEvent.click(screen.getByTestId("save-role"));
    await waitFor(() => expect(upsertRoleSpy).toHaveBeenCalled());
    expect(upsertRoleSpy.mock.calls[0][0]).toMatchObject({
      id: "intern",
      parents: ["analyst", "auditor"],
    });
  });

  it("sends no parents when none is chosen", async () => {
    upsertRoleSpy.mockClear();
    render(<SecurityPage />);
    await openNewRole();
    fireEvent.change(input("role-id-input"), { target: { value: "solo" } });
    await choose("Domain Access", "All Domains");
    fireEvent.click(screen.getByTestId("save-role"));
    await waitFor(() => expect(upsertRoleSpy).toHaveBeenCalled());
    expect(upsertRoleSpy.mock.calls[0][0]).toMatchObject({ id: "solo", parents: [] });
  });

  it("offers a standard role no edit or delete, and says why", () => {
    render(<SecurityPage />);
    fireEvent.click(screen.getByText("analyst"));
    expect(screen.queryByTestId("edit-role-analyst")).toBeNull();
    expect(screen.queryByTestId("delete-role-analyst")).toBeNull();
    expect(screen.getByTestId("role-standard-analyst")).toHaveTextContent("Provisa's own");
  });

  it("offers an organisation's own role edit and delete", () => {
    render(<SecurityPage />);
    fireEvent.click(screen.getByText("junior"));
    expect(screen.getByTestId("edit-role-junior")).toBeTruthy();
    expect(screen.getByTestId("delete-role-junior")).toBeTruthy();
  });
});
