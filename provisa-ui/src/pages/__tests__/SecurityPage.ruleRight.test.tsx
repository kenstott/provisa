// Copyright (c) 2026 Kenneth Stott
// Canary: 9f6d2b17-4c83-4ae1-b7f5-2e0a8c1d6f33
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2188: a row rule has a right. The list and the detail show it, the form sets it, an action
// offers none (its rows are only read), an existing rule keeps its own (another right is another
// rule), and deleting a rule names its right.

import { describe, it, expect, vi } from "vitest";
import userEvent from "@testing-library/user-event";
import { render, screen, fireEvent, waitFor, within } from "../../test-utils/render";

const upsertRlsRuleSpy = vi.fn(async (_input: Record<string, unknown>) => ({
  success: true,
  message: "",
}));
const deleteRlsRuleSpy = vi.fn(async () => ({ success: true, message: "" }));

vi.mock("../../context/DomainFilterContext", () => ({
  useDomainFilter: () => ({
    setDomains: vi.fn(),
    setSelectedDomain: vi.fn(),
    selectedDomain: "all",
    checkedDomains: new Set<string>(),
  }),
}));

vi.mock("../../api/actions", () => ({
  fetchActions: async () => ({ functions: [], webhooks: [] }),
}));

vi.mock("../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useAdminQueries")>()),
  useRoles: () => ({
    roles: [
      {
        id: "pets_team",
        description: "What pets_team is for.",
        capabilities: [],
        demonstrated: [],
        domain_access: ["*"],
      },
    ],
    loading: false,
    refetch: vi.fn(),
  }),
  useTables: () => ({ tables: [], loading: false, refetch: vi.fn() }),
  useDomains: () => ({ domains: [{ id: "pets" }], loading: false, refetch: vi.fn() }),
}));

vi.mock("../../hooks/useSecurityQueries", () => ({
  useRLSRules: () => ({
    rlsRules: [
      {
        id: 7,
        tableId: null,
        domainId: "pets",
        actionName: null,
        roleId: "pets_team",
        filterExpr: "owner_role IN (current_setting('provisa.acting_roles'))",
        description: "Its own products.",
        right: "U",
      },
    ],
    loading: false,
    refetch: vi.fn(),
  }),
  useUpsertRole: () => ({ upsertRole: vi.fn(), loading: false }),
  useDeleteRole: () => ({ deleteRole: vi.fn(), loading: false }),
  useUpsertRlsRule: () => ({ upsertRlsRule: upsertRlsRuleSpy, loading: false }),
  useDeleteRlsRule: () => ({ deleteRlsRule: deleteRlsRuleSpy, loading: false }),
  useRevokeRoleGrants: () => ({ revokeFromTable: vi.fn(), revokeFromObject: vi.fn() }),
}));

import { SecurityRlsPage } from "../security/SecurityRlsPage";

async function selectOption(combobox: HTMLElement, name: string | RegExp) {
  await userEvent.click(combobox);
  const listboxId = combobox.getAttribute("aria-controls");
  const listbox = listboxId ? document.getElementById(listboxId) : null;
  if (!listbox) throw new Error(`No listbox for combobox ${combobox.getAttribute("data-testid")}`);
  const option = await within(listbox).findByRole("option", { name, hidden: true });
  await userEvent.click(option);
}

async function fillIn(right?: RegExp) {
  fireEvent.click(screen.getByTestId("toggle-rule-form"));
  await selectOption(screen.getByTestId("rule-apply-to"), /Domain/);
  await selectOption(screen.getByTestId("rule-domain-select"), "pets");
  await selectOption(screen.getByTestId("rule-role-select"), "pets_team");
  if (right) await selectOption(screen.getByTestId("rule-right-select"), right);
  fireEvent.change(screen.getByLabelText("Filter Expression"), { target: { value: "1 = 1" } });
  fireEvent.change(screen.getByTestId("rule-description"), { target: { value: "For the test." } });
  fireEvent.click(screen.getByTestId("save-rule"));
  await waitFor(() => expect(upsertRlsRuleSpy).toHaveBeenCalled());
}

describe("SecurityPage: a row rule's right (REQ-2188)", () => {
  it("shows the right in the list", () => {
    render(<SecurityRlsPage />);
    expect(screen.getByTestId("rule-right-7")).toHaveTextContent("Update");
  });

  it("shows the right in the rule's detail", async () => {
    render(<SecurityRlsPage />);
    fireEvent.click(screen.getByTestId("rule-right-7"));
    expect((await screen.findByTestId("rule-read-7")).textContent).toContain("Update");
  });

  it("saves a new rule as a read rule unless told otherwise", async () => {
    upsertRlsRuleSpy.mockClear();
    render(<SecurityRlsPage />);
    await fillIn();
    expect(upsertRlsRuleSpy.mock.calls[0][0]).toMatchObject({ right: "R" });
  });

  it("sets the right a rule is saved with", async () => {
    upsertRlsRuleSpy.mockClear();
    render(<SecurityRlsPage />);
    await fillIn(/Delete/);
    expect(upsertRlsRuleSpy.mock.calls[0][0]).toMatchObject({ right: "D", domainId: "pets" });
  });

  it("offers an action no right, and saves it a read rule", async () => {
    render(<SecurityRlsPage />);
    fireEvent.click(screen.getByTestId("toggle-rule-form"));
    await selectOption(screen.getByTestId("rule-right-select"), /Update/);
    await selectOption(screen.getByTestId("rule-apply-to"), /Action/);
    expect(screen.queryByTestId("rule-right-select")).toBeNull();
  });

  it("keeps an existing rule's right: another right is another rule", async () => {
    render(<SecurityRlsPage />);
    fireEvent.click(screen.getByTestId("rule-right-7"));
    fireEvent.click(await screen.findByTestId("edit-rule-7"));
    expect(screen.getByTestId("rule-right-select")).toBeDisabled();
    expect(screen.getByTestId("rule-right-select")).toHaveValue("Update");
  });

  it("deletes the rule of its own right", async () => {
    deleteRlsRuleSpy.mockClear();
    render(<SecurityRlsPage />);
    fireEvent.click(screen.getByTestId("rule-right-7"));
    fireEvent.click(await screen.findByTestId("delete-rule-7"));
    const dialog = await screen.findByRole("dialog");
    fireEvent.click(within(dialog).getByRole("button", { name: "Confirm" }));
    await waitFor(() =>
      expect(deleteRlsRuleSpy).toHaveBeenCalledWith("pets_team", null, "pets", null, null, "U"),
    );
  });
});
