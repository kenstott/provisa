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

const upsertRoleSpy = vi.fn(async (_input: Record<string, unknown>) => ({
  success: true,
  message: "",
}));
const ROLES = [
  {
    id: "analyst",
    capabilities: [],
    demonstrated: [],
    domain_access: ["sales"],
    parentRoleId: null,
  },
  { id: "junior", capabilities: [], demonstrated: [], domain_access: [], parentRoleId: "analyst" },
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
}));

import { SecurityPage } from "../SecurityPage";

// A role must list a domain before it can be saved (the server refuses an empty list), so a test
// that saves a new role chooses "All Domains" in the picker first.
// Mantine MultiSelect in jsdom: floating-ui hides the detached dropdown (all rects are 0), so
// the options are found through the input's aria-controls listbox with hidden: true.
async function chooseAllDomains() {
  const picker = screen
    .getAllByLabelText("Domain Access")
    .find((el) => el.tagName === "INPUT") as HTMLElement;
  fireEvent.click(picker);
  await waitFor(() => {
    if (!picker.getAttribute("aria-controls")) throw new Error("dropdown not open");
  });
  const listbox = document.getElementById(picker.getAttribute("aria-controls") as string);
  fireEvent.click(
    within(listbox as HTMLElement).getByRole("option", { name: "All Domains", hidden: true }),
  );
}

const input = (testid: string) => screen.getByTestId(testid) as HTMLInputElement;

describe("SecurityPage — role inheritance (REQ-1677)", () => {
  it("shows the parent in the expanded role row", () => {
    render(<SecurityPage />);
    fireEvent.click(screen.getByText("junior"));
    expect(screen.getByTestId("role-parent-junior")).toHaveTextContent("analyst");
  });

  it("stages the chosen parent through upsertRole", async () => {
    upsertRoleSpy.mockClear();
    render(<SecurityPage />);
    fireEvent.click(screen.getByTestId("toggle-role-form"));
    fireEvent.change(input("role-id-input"), { target: { value: "intern" } });
    const select = input("role-parent-select");
    fireEvent.click(select);
    fireEvent.change(select, { target: { value: "analyst" } });
    fireEvent.click(await screen.findByRole("option", { name: "analyst" }));
    fireEvent.click(screen.getByTestId("save-role"));
    await waitFor(() => expect(upsertRoleSpy).toHaveBeenCalled());
    expect(upsertRoleSpy.mock.calls[0][0]).toMatchObject({ id: "intern", parentRoleId: "analyst" });
  });

  it("sends null when no parent is chosen", async () => {
    upsertRoleSpy.mockClear();
    render(<SecurityPage />);
    fireEvent.click(screen.getByTestId("toggle-role-form"));
    fireEvent.change(input("role-id-input"), { target: { value: "solo" } });
    await chooseAllDomains();
    fireEvent.click(screen.getByTestId("save-role"));
    await waitFor(() => expect(upsertRoleSpy).toHaveBeenCalled());
    expect(upsertRoleSpy.mock.calls[0][0]).toMatchObject({ id: "solo", parentRoleId: null });
  });
});
