// Copyright (c) 2026 Kenneth Stott
// Canary: 2a7c5e91-d40b-4f86-b3a2-8e61f0c94d17
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1384: an RLS rule may target a tag and then covers every table carrying it. The form
// offers "Tag" as a scope, lists the user tags that apply to tables, stages tagId through
// upsertRlsRule, deletes by tagId, and the rules table shows a tag-scoped rule as such.

import { describe, it, expect, vi } from "vitest";
import userEvent from "@testing-library/user-event";
import { render, screen, fireEvent, waitFor, within } from "../../test-utils/render";

const upsertRlsRuleSpy = vi.fn(async (_input: Record<string, unknown>) => ({
  success: true,
  message: "",
}));
const deleteRlsRuleSpy = vi.fn(async (..._args: unknown[]) => ({ success: true, message: "" }));

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
        id: "analyst",
        description: "What analyst is for.",
        capabilities: [],
        demonstrated: [],
        domain_access: ["*"],
      },
    ],
    loading: false,
    refetch: vi.fn(),
  }),
  useTables: () => ({ tables: [], loading: false, refetch: vi.fn() }),
  useDomains: () => ({ domains: [], loading: false, refetch: vi.fn() }),
}));

const tag = (id: string, over: Record<string, unknown> = {}) => ({
  id,
  description: "",
  appliesTo: ["table"],
  isSystem: false,
  derived: false,
  reasonPolicy: "hidden",
  expiresPolicy: "hidden",
  paramPolicy: "none",
  paramValues: [],
  ...over,
});

vi.mock("../../hooks/useTagQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useTagQueries")>()),
  useTags: () => ({
    tags: [
      tag("regional"),
      tag("gold"),
      tag("pii", { isSystem: true }),
      tag("column_only", { appliesTo: ["column"] }),
    ],
    loading: false,
    refetch: vi.fn(),
  }),
  useTagAssignments: () => ({
    tagAssignments: [
      { tagId: "regional", objectType: "table", tableId: 1 },
      { tagId: "regional", objectType: "table", tableId: 2 },
      { tagId: "regional", objectType: "column", tableId: 3, columnName: "region" },
    ],
    loading: false,
  }),
}));

vi.mock("../../hooks/useSecurityQueries", () => ({
  useRLSRules: () => ({
    rlsRules: [
      {
        id: 12,
        tableId: null,
        domainId: null,
        actionName: null,
        tagId: "regional",
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
  useRevokeRoleGrants: () => ({ revokeFromTable: vi.fn(), revokeFromObject: vi.fn() }),
}));

import { SecurityRlsPage } from "../security/SecurityRlsPage";

// Mantine Select portals its listbox; jsdom applies no layout so it reads as hidden.
async function openListbox(combobox: HTMLElement) {
  await userEvent.click(combobox);
  const listboxId = combobox.getAttribute("aria-controls");
  const listbox = listboxId ? document.getElementById(listboxId) : null;
  if (!listbox) throw new Error(`No listbox for combobox ${combobox.getAttribute("data-testid")}`);
  return listbox;
}

async function selectOption(combobox: HTMLElement, name: string | RegExp) {
  const listbox = await openListbox(combobox);
  await userEvent.click(await within(listbox).findByRole("option", { name, hidden: true }));
}

describe("SecurityPage — RLS rules on tags (REQ-1384)", () => {
  it("lists a tag-scoped rule with its tag and how many tables carry it", async () => {
    render(<SecurityRlsPage />);
    expect(screen.getByTestId("rule-scope-12")).toHaveTextContent("tag: regional");
    await userEvent.click(screen.getByTestId("rule-scope-12"));
    // Two table-level assignments; the column-level one does not tag its table.
    expect(await screen.findByText(/regional \(on 2 tables\)/)).toBeInTheDocument();
  });

  it("offers only user tags that apply to tables", async () => {
    render(<SecurityRlsPage />);
    fireEvent.click(screen.getByTestId("toggle-rule-form"));
    await selectOption(screen.getByTestId("rule-apply-to"), /Tag/);
    const listbox = await openListbox(screen.getByTestId("rule-tag-select"));
    const offered = within(listbox)
      .getAllByRole("option", { hidden: true })
      .map((o) => o.textContent);
    expect(offered).toEqual(["regional", "gold"]);
  });

  it("stages tagId when the scope is a tag", async () => {
    upsertRlsRuleSpy.mockClear();
    render(<SecurityRlsPage />);
    fireEvent.click(screen.getByTestId("toggle-rule-form"));
    await selectOption(screen.getByTestId("rule-apply-to"), /Tag/);
    await selectOption(screen.getByTestId("rule-tag-select"), "gold");
    await selectOption(screen.getByTestId("rule-role-select"), "analyst");
    fireEvent.change(screen.getByLabelText("Filter Expression"), {
      target: { value: "1 = 1" },
    });
    fireEvent.change(screen.getByTestId("rule-description"), {
      target: { value: "Every row, for the test." },
    });
    fireEvent.click(screen.getByTestId("save-rule"));
    await waitFor(() => expect(upsertRlsRuleSpy).toHaveBeenCalled());
    expect(upsertRlsRuleSpy.mock.calls[0][0]).toMatchObject({
      tagId: "gold",
      tableId: null,
      domainId: null,
      actionName: null,
      roleId: "analyst",
      filterExpr: "1 = 1",
    });
  });

  it("deletes a tag-scoped rule by its tag", async () => {
    deleteRlsRuleSpy.mockClear();
    render(<SecurityRlsPage />);
    await userEvent.click(screen.getByTestId("rule-scope-12"));
    await userEvent.click(await screen.findByTestId("delete-rule-12"));
    const dialog = await screen.findByRole("dialog");
    await userEvent.click(within(dialog).getByRole("button", { name: "Confirm" }));
    await waitFor(() => expect(deleteRlsRuleSpy).toHaveBeenCalled());
    expect(deleteRlsRuleSpy.mock.calls[0]).toEqual(["analyst", null, null, null, "regional"]);
  });
});
