// Copyright (c) 2026 Kenneth Stott
// Canary: 6e1a9d4c-3b7f-4c2e-9a8d-5f0b2c7e1d94
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1679: an RLS rule may target a tracked function or webhook. The form offers "Action" as a
// scope, lists the registered actions, stages actionName through upsertRlsRule, and the rules
// table shows an action-scoped rule as such.

import { describe, it, expect, vi } from "vitest";
import userEvent from "@testing-library/user-event";
import { render, screen, fireEvent, waitFor, within } from "../../test-utils/render";

const upsertRlsRuleSpy = vi.fn(async () => ({ success: true, message: "" }));
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
  fetchActions: async () => ({
    functions: [{ name: "customer_lookup", domainId: "sales" }],
    webhooks: [{ name: "notify_ops", domainId: "ops" }],
  }),
}));

vi.mock("../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useAdminQueries")>()),
  useRoles: () => ({
    roles: [{ id: "analyst", capabilities: [], demonstrated: [], domain_access: [] }],
    loading: false,
    refetch: vi.fn(),
  }),
  useTables: () => ({ tables: [], loading: false, refetch: vi.fn() }),
  useDomains: () => ({ domains: [], loading: false, refetch: vi.fn() }),
}));

vi.mock("../../hooks/useSecurityQueries", () => ({
  useRLSRules: () => ({
    rlsRules: [
      {
        id: 9,
        tableId: null,
        domainId: null,
        actionName: "customer_lookup",
        roleId: "analyst",
        filterExpr: "region = 'east'",
      },
    ],
    loading: false,
    refetch: vi.fn(),
  }),
  useUpsertRole: () => ({ upsertRole: vi.fn(), loading: false }),
  useDeleteRole: () => ({ deleteRole: vi.fn(), loading: false }),
  useUpsertRlsRule: () => ({ upsertRlsRule: upsertRlsRuleSpy, loading: false }),
  useDeleteRlsRule: () => ({ deleteRlsRule: deleteRlsRuleSpy, loading: false }),
}));

import { SecurityRlsPage } from "../SecurityPage";

// Mantine Select portals its listbox; jsdom applies no layout so it reads as hidden.
async function selectOption(combobox: HTMLElement, name: string | RegExp) {
  await userEvent.click(combobox);
  const listboxId = combobox.getAttribute("aria-controls");
  const listbox = listboxId ? document.getElementById(listboxId) : null;
  if (!listbox) throw new Error(`No listbox for combobox ${combobox.getAttribute("data-testid")}`);
  const option = await within(listbox).findByRole("option", { name, hidden: true });
  await userEvent.click(option);
}

describe("SecurityPage — RLS rules on actions (REQ-1679)", () => {
  it("lists an action-scoped rule with its action", () => {
    render(<SecurityRlsPage />);
    expect(screen.getByTestId("rule-scope-9")).toHaveTextContent("customer_lookup");
  });

  it("stages actionName when the scope is an action", async () => {
    upsertRlsRuleSpy.mockClear();
    render(<SecurityRlsPage />);
    fireEvent.click(screen.getByTestId("toggle-rule-form"));
    await selectOption(screen.getByTestId("rule-apply-to"), /Action/);
    await selectOption(screen.getByTestId("rule-action-select"), "notify_ops");
    await selectOption(screen.getByTestId("rule-role-select"), "analyst");
    fireEvent.change(screen.getByLabelText("Filter Expression"), {
      target: { value: "1 = 1" },
    });
    fireEvent.click(screen.getByTestId("save-rule"));
    await waitFor(() => expect(upsertRlsRuleSpy).toHaveBeenCalled());
    expect(upsertRlsRuleSpy.mock.calls[0][0]).toMatchObject({
      actionName: "notify_ops",
      tableId: null,
      domainId: null,
      roleId: "analyst",
      filterExpr: "1 = 1",
    });
  });
});
