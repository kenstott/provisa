// Copyright (c) 2026 Kenneth Stott
// Canary: ac6393d0-4565-4700-91ce-22164bb8262d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A role is always one or more domains, or all: the server refuses a role saved with none. The
// editor starts a new role with none, so it states the requirement and withholds Save until a
// domain (or "All Domains") is chosen — an empty picker never reads as "unrestricted". An
// EXISTING role that has ended up with none (the server's backstop: it reads no data) is told
// so in the table and in its edit form.

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
  useUpsertRole: () => ({ upsertRole: upsertRoleSpy, loading: false }),
  useDeleteRole: () => ({ deleteRole: vi.fn(), loading: false }),
  useUpsertRlsRule: () => ({ upsertRlsRule: vi.fn(), loading: false }),
  useDeleteRlsRule: () => ({ deleteRlsRule: vi.fn(), loading: false }),
  useRevokeRoleGrants: () => ({ revokeFromTable: vi.fn(), revokeFromObject: vi.fn() }),
}));

import { SecurityPage } from "../SecurityPage";

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

describe("SecurityPage — a role lists at least one domain", () => {
  it("states the requirement and withholds Save while a new role has no domain", () => {
    upsertRoleSpy.mockClear();
    render(<SecurityPage />);
    expect(screen.queryByTestId("role-domains-required")).toBeNull();
    fireEvent.click(screen.getByTestId("toggle-role-form"));
    fireEvent.change(screen.getByTestId("role-id-input"), { target: { value: "fresh" } });

    expect(screen.getByTestId("role-domains-required")).toHaveTextContent(
      "must list at least one domain",
    );
    // A new role is told what is required, not that it "reaches no data": it does not exist yet.
    expect(screen.queryByTestId("role-no-domains-note")).toBeNull();
    expect(screen.getByTestId("save-role")).toBeDisabled();
    fireEvent.click(screen.getByTestId("save-role"));
    expect(upsertRoleSpy).not.toHaveBeenCalled();
  });

  it("offers All Domains as an explicit choice, which lifts the requirement", async () => {
    upsertRoleSpy.mockClear();
    render(<SecurityPage />);
    fireEvent.click(screen.getByTestId("toggle-role-form"));
    fireEvent.change(screen.getByTestId("role-id-input"), { target: { value: "fresh" } });
    await chooseAllDomains();

    expect(screen.queryByTestId("role-domains-required")).toBeNull();
    expect(screen.getByTestId("save-role")).not.toBeDisabled();
    fireEvent.click(screen.getByTestId("save-role"));
    await waitFor(() => expect(upsertRoleSpy).toHaveBeenCalledTimes(1));
    expect(upsertRoleSpy.mock.calls[0][0]).toMatchObject({ id: "fresh", domainAccess: ["*"] });
  });

  it("names a saved role that lists no domains, and leaves the others as they are", () => {
    render(<SecurityPage />);
    expect(screen.getByTestId("role-domains-newrole")).toHaveTextContent("reaches no data");
    expect(screen.getByTestId("role-domains-analyst")).toHaveTextContent("sales");
    expect(screen.getByTestId("role-domains-analyst")).not.toHaveTextContent("reaches no data");
    expect(screen.getByTestId("role-domains-everything")).toHaveTextContent("*");
  });

  it("REQ-1940: the roles list renders through the shared list component, rows striped by item", () => {
    const { container } = render(<SecurityPage />);
    const list = container.querySelector("[data-list-table]");
    expect(list).not.toBeNull();
    expect(list!.classList.contains("data-table")).toBe(true);
    const rows = container.querySelectorAll("tbody tr.list-row");
    expect(rows).toHaveLength(ROLES.length);
    // Expanding a role adds a detail row that is not an item row, so the alternation is unchanged.
    fireEvent.click(screen.getByText("analyst"));
    expect(container.querySelectorAll("tbody tr.list-row")).toHaveLength(ROLES.length);
    expect(container.querySelectorAll("tbody tr.list-expand")).toHaveLength(1);
  });

  it("shows neither message when a saved role that lists domains is edited", () => {
    render(<SecurityPage />);
    fireEvent.click(screen.getByText("analyst"));
    fireEvent.click(screen.getByTestId("edit-role-analyst"));
    expect(screen.queryByTestId("role-no-domains-note")).toBeNull();
    expect(screen.queryByTestId("role-domains-required")).toBeNull();
    expect(screen.getByTestId("save-role-analyst")).not.toBeDisabled();
  });

  it("tells an existing role that has ended up with none that it reaches no data", () => {
    render(<SecurityPage />);
    fireEvent.click(screen.getByText("newrole"));
    fireEvent.click(screen.getByTestId("edit-role-newrole"));
    expect(screen.getByTestId("role-no-domains-note")).toHaveTextContent("reaches no data");
    expect(screen.queryByTestId("role-domains-required")).toBeNull();
    expect(screen.getByTestId("save-role-newrole")).toBeDisabled();
  });
});
