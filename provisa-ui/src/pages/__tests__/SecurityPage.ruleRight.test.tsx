// Copyright (c) 2026 Kenneth Stott
// Canary: 9f6d2b17-4c83-4ae1-b7f5-2e0a8c1d6f33
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2188: a row rule carries a set of rights. The list and the detail show them, three checkboxes
// set them, an action offers none (its rows are only read), an edit saves that rule by its id, a
// right already in another rule is refused with the server's words, and a delete names the rule.

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
        rights: ["U", "D"],
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

async function fillIn(...toggle: string[]) {
  fireEvent.click(screen.getByTestId("toggle-rule-form"));
  await selectOption(screen.getByTestId("rule-apply-to"), /Domain/);
  await selectOption(screen.getByTestId("rule-domain-select"), "pets");
  await selectOption(screen.getByTestId("rule-role-select"), "pets_team");
  for (const right of toggle) fireEvent.click(screen.getByTestId(`rule-right-${right}`));
  fireEvent.change(screen.getByLabelText("Filter Expression"), { target: { value: "1 = 1" } });
  fireEvent.change(screen.getByTestId("rule-description"), { target: { value: "For the test." } });
  fireEvent.click(screen.getByTestId("save-rule"));
}

describe("SecurityPage: a row rule's rights (REQ-2188)", () => {
  it("shows every right in the list", () => {
    render(<SecurityRlsPage />);
    expect(screen.getByTestId("rule-right-7")).toHaveTextContent("Update, Delete");
  });

  it("shows every right in the rule's detail", async () => {
    render(<SecurityRlsPage />);
    fireEvent.click(screen.getByTestId("rule-right-7"));
    expect((await screen.findByTestId("rule-read-7")).textContent).toContain("Update, Delete");
  });

  it("offers three checkboxes, Read alone ticked on a new rule", () => {
    render(<SecurityRlsPage />);
    fireEvent.click(screen.getByTestId("toggle-rule-form"));
    expect(screen.getByTestId("rule-right-R")).toBeChecked();
    expect(screen.getByTestId("rule-right-U")).not.toBeChecked();
    expect(screen.getByTestId("rule-right-D")).not.toBeChecked();
  });

  it("saves a new rule as a read rule unless told otherwise", async () => {
    upsertRlsRuleSpy.mockClear();
    render(<SecurityRlsPage />);
    await fillIn();
    await waitFor(() => expect(upsertRlsRuleSpy).toHaveBeenCalled());
    expect(upsertRlsRuleSpy.mock.calls[0][0]).toMatchObject({ rights: ["R"], id: null });
  });

  it("saves one filter with several rights, in their one order", async () => {
    upsertRlsRuleSpy.mockClear();
    render(<SecurityRlsPage />);
    await fillIn("D", "U");
    await waitFor(() => expect(upsertRlsRuleSpy).toHaveBeenCalled());
    expect(upsertRlsRuleSpy.mock.calls[0][0]).toMatchObject({
      rights: ["R", "U", "D"],
      domainId: "pets",
    });
  });

  it("names the rights in the footer when none is ticked, and saves nothing", async () => {
    upsertRlsRuleSpy.mockClear();
    render(<SecurityRlsPage />);
    await fillIn("R");
    expect(await screen.findByTestId("form-problem")).toHaveTextContent("Right");
    expect(upsertRlsRuleSpy).not.toHaveBeenCalled();
  });

  it("shows the refusal when a right is already in another rule", async () => {
    upsertRlsRuleSpy.mockClear();
    upsertRlsRuleSpy.mockResolvedValueOnce({
      success: false,
      message: "taken",
      code: "schema.rls_rule_right_taken",
      params: { role: "pets_team", right: "U", rule: 7 },
    } as never);
    render(<SecurityRlsPage />);
    await fillIn("U");
    expect(await screen.findByTestId("form-problem")).toHaveTextContent(
      "Right U of role 'pets_team' is already in rule 7",
    );
  });

  it("offers an action no rights", async () => {
    render(<SecurityRlsPage />);
    fireEvent.click(screen.getByTestId("toggle-rule-form"));
    fireEvent.click(screen.getByTestId("rule-right-U"));
    await selectOption(screen.getByTestId("rule-apply-to"), /Action/);
    expect(screen.queryByTestId("rule-right-U")).toBeNull();
  });

  it("edits an existing rule's rights, saving that rule by its id", async () => {
    upsertRlsRuleSpy.mockClear();
    render(<SecurityRlsPage />);
    fireEvent.click(screen.getByTestId("rule-right-7"));
    fireEvent.click(await screen.findByTestId("edit-rule-7"));
    expect(screen.getByTestId("rule-right-U")).toBeChecked();
    expect(screen.getByTestId("rule-right-D")).toBeChecked();
    fireEvent.click(screen.getByTestId("rule-right-D"));
    fireEvent.click(screen.getByRole("button", { name: "Save" }));
    await waitFor(() => expect(upsertRlsRuleSpy).toHaveBeenCalled());
    expect(upsertRlsRuleSpy.mock.calls[0][0]).toMatchObject({ rights: ["U"], id: 7 });
  });

  it("deletes the one rule by its id", async () => {
    deleteRlsRuleSpy.mockClear();
    render(<SecurityRlsPage />);
    fireEvent.click(screen.getByTestId("rule-right-7"));
    fireEvent.click(await screen.findByTestId("delete-rule-7"));
    const dialog = await screen.findByRole("dialog");
    fireEvent.click(within(dialog).getByRole("button", { name: "Confirm" }));
    await waitFor(() =>
      expect(deleteRlsRuleSpy).toHaveBeenCalledWith("pets_team", null, "pets", null, null, 7),
    );
  });
});
