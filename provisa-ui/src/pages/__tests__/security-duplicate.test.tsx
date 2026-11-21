// Copyright (c) 2026 Kenneth Stott
// Canary: 791ead22-6799-41f6-b292-4a210dedbf82
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Creating a role, a derived role and a row rule: "+" opens an empty form whatever object was
// selected or edited before it, and Duplicate (the panel head, beside Edit) opens the same create
// form filled from the selected object (rule: components/list/__tests__/panelDuplicateGuard.test.ts).
// A role's copy is its rights and limits, not its members.

import { describe, it, expect, vi, beforeEach } from "vitest";
import userEvent from "@testing-library/user-event";
import { render, screen, fireEvent, waitFor, within } from "../../test-utils/render";
import { expectHoldsNoneOf } from "../../test-utils/createForm";

const upsertRole = vi.fn(async (_input: Record<string, unknown>, _creating?: boolean) => ({
  success: true,
  message: "",
}));
const upsertRlsRule = vi.fn(async (_input: Record<string, unknown>) => ({
  success: true,
  message: "",
}));
const saveDerivedRole = vi.fn(async (_attribute: string, _description: string) => {});

const ROLES = [
  {
    id: "analyst",
    description: "Reads the sales domain for reporting.",
    capabilities: ["query_development"],
    demonstrated: [],
    domain_access: ["sales"],
    parents: [],
    standard: false,
    detailsHidden: false,
    rateLimit: { requestsPerSecond: 7, maxQueryComplexity: null, maxQueryTimeMs: null },
  },
];
const RULES = [
  {
    id: 4,
    tableId: null,
    domainId: "sales",
    actionName: null,
    tagId: null,
    roleId: "analyst",
    filterExpr: "region = 'eu'",
    rights: ["R"],
    description: "EU analysts see EU rows",
  },
];
const REGION = {
  attribute: "region",
  description: "The subsidiary a member works for.",
  values: ["eu"],
  uses: [],
};

vi.mock("../../context/AuthContext", () => ({ useAuth: () => ({ rebind: vi.fn() }) }));
vi.mock("../../context/DomainFilterContext", () => ({
  useDomainFilter: () => ({
    domains: [],
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
  useRLSRules: () => ({ rlsRules: RULES, loading: false, refetch: vi.fn() }),
  useUpsertRole: () => ({ upsertRole, loading: false }),
  useDeleteRole: () => ({ deleteRole: vi.fn(), loading: false }),
  useDeleteRoles: () => vi.fn(),
  useUpsertRlsRule: () => ({ upsertRlsRule, loading: false }),
  useDeleteRlsRule: () => ({ deleteRlsRule: vi.fn(), loading: false }),
  useRevokeRoleGrants: () => ({ revokeFromTable: vi.fn(), revokeFromObject: vi.fn() }),
}));
vi.mock("../../api/actions", () => ({
  fetchActions: vi.fn().mockResolvedValue({ functions: [], webhooks: [] }),
}));
vi.mock("../../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../api/admin")>()),
  fetchDerivedRoles: vi.fn(async () => ({ roles: [REGION], source: "org" as const })),
  saveDerivedRole: (a: string, d: string) => saveDerivedRole(a, d),
  deleteDerivedRole: vi.fn(async () => {}),
}));

import { SecurityRolesPage } from "../SecurityPage";
import { SecurityRlsPage } from "../security/SecurityRlsPage";

beforeEach(() => vi.clearAllMocks());

const ROLE_VALUES = ["analyst", "Reads the sales domain for reporting.", "7", "analyst-copy"];
const rowOf = (list: string, text: string) =>
  within(screen.getByTestId(list)).getByText(text).closest("tr")!;

async function openAddRole() {
  fireEvent.click(await screen.findByTestId("toggle-role-form"));
  fireEvent.click(await screen.findByTestId("add-granted-role"));
  await screen.findByTestId("role-id-input");
}

describe("+ Role opens an empty form", () => {
  it("holds none of the values of the role selected before it", async () => {
    render(<SecurityRolesPage />);
    fireEvent.click(rowOf("roles-list", "analyst"));
    await openAddRole();
    expectHoldsNoneOf(screen.getByTestId("roles-split-panel"), ROLE_VALUES);
  });

  it("holds none of them after the role was edited and the edit cancelled", async () => {
    render(<SecurityRolesPage />);
    fireEvent.click(rowOf("roles-list", "analyst"));
    fireEvent.click(await screen.findByTestId("edit-role-analyst"));
    fireEvent.click(await screen.findByText("Cancel"));
    await openAddRole();
    expectHoldsNoneOf(screen.getByTestId("roles-split-panel"), ROLE_VALUES);
  });
});

describe("Duplicate, in a role's panel head", () => {
  it("is beside Edit and Delete, named Duplicate", () => {
    render(<SecurityRolesPage />);
    fireEvent.click(rowOf("roles-list", "analyst"));
    expect(screen.getByTestId("duplicate-role-analyst")).toHaveAccessibleName("Duplicate");
  });

  it("opens the new-role form filled from the role, under a new id", async () => {
    render(<SecurityRolesPage />);
    fireEvent.click(rowOf("roles-list", "analyst"));
    fireEvent.click(screen.getByTestId("duplicate-role-analyst"));
    expect(screen.getByTestId("roles-split-title")).toHaveTextContent("New");
    expect(await screen.findByTestId("role-id-input")).toHaveValue("analyst-copy");
    expect(screen.getByTestId("role-description-input")).toHaveValue(
      "Reads the sales domain for reporting.",
    );
  });

  it("creates the copy with the role's rights and limits, as a create", async () => {
    render(<SecurityRolesPage />);
    fireEvent.click(rowOf("roles-list", "analyst"));
    fireEvent.click(screen.getByTestId("duplicate-role-analyst"));
    fireEvent.click(await screen.findByTestId("save-role"));
    await waitFor(() => expect(upsertRole).toHaveBeenCalled());
    expect(upsertRole).toHaveBeenCalledWith(
      expect.objectContaining({
        id: "analyst-copy",
        description: "Reads the sales domain for reporting.",
        capabilities: ["query_development"],
        domainAccess: ["sales"],
        rateLimit: expect.objectContaining({ requestsPerSecond: 7 }),
      }),
      true,
    );
  });
});

describe("derived roles", () => {
  const DERIVED_VALUES = ["region", "The subsidiary a member works for.", "region_copy"];

  it("+ derived role opens empty after a derived role was selected", async () => {
    render(<SecurityRolesPage />);
    fireEvent.click(await screen.findByTestId("derived-role-row-region"));
    fireEvent.click(await screen.findByTestId("toggle-role-form"));
    fireEvent.click(await screen.findByTestId("add-derived-role"));
    expectHoldsNoneOf(await screen.findByTestId("derived-role-form"), DERIVED_VALUES);
  });

  it("offers Duplicate beside Edit and Delete", async () => {
    render(<SecurityRolesPage />);
    fireEvent.click(await screen.findByTestId("derived-role-row-region"));
    expect(await screen.findByTestId("duplicate-derived-role-region")).toHaveAccessibleName(
      "Duplicate",
    );
  });

  it("opens the declare form filled with the description, under region_copy", async () => {
    render(<SecurityRolesPage />);
    fireEvent.click(await screen.findByTestId("derived-role-row-region"));
    fireEvent.click(await screen.findByTestId("duplicate-derived-role-region"));
    expect(await screen.findByTestId("derived-role-attribute")).toHaveValue("region_copy");
    expect(screen.getByTestId("derived-role-description")).toHaveValue(
      "The subsidiary a member works for.",
    );
    fireEvent.click(screen.getByTestId("save-derived-role"));
    await waitFor(() =>
      expect(saveDerivedRole).toHaveBeenCalledWith(
        "region_copy",
        "The subsidiary a member works for.",
      ),
    );
  });

  it("opens the + form empty again after a duplicate was cancelled", async () => {
    render(<SecurityRolesPage />);
    fireEvent.click(await screen.findByTestId("derived-role-row-region"));
    fireEvent.click(await screen.findByTestId("duplicate-derived-role-region"));
    fireEvent.click(await screen.findByText("Cancel"));
    fireEvent.click(await screen.findByTestId("toggle-role-form"));
    fireEvent.click(await screen.findByTestId("add-derived-role"));
    expectHoldsNoneOf(await screen.findByTestId("derived-role-form"), DERIVED_VALUES);
  });
});

describe("row rules", () => {
  const RULE_VALUES = ["analyst", "region = 'eu'", "EU analysts see EU rows"];
  const rulePanel = () => screen.getByTestId("rules-split-panel");

  it("+ rule opens empty after a rule was selected", async () => {
    render(<SecurityRlsPage />);
    fireEvent.click(rowOf("rules-list", "analyst"));
    fireEvent.click(screen.getByTestId("toggle-rule-form"));
    expectHoldsNoneOf(rulePanel(), RULE_VALUES);
  });

  it("+ rule opens empty after a rule was edited and the edit cancelled", async () => {
    render(<SecurityRlsPage />);
    fireEvent.click(rowOf("rules-list", "analyst"));
    fireEvent.click(screen.getByTestId("edit-rule-4"));
    fireEvent.click(await screen.findByText("Cancel"));
    fireEvent.click(screen.getByTestId("toggle-rule-form"));
    expectHoldsNoneOf(rulePanel(), RULE_VALUES);
  });

  it("offers Duplicate beside Edit and Delete", () => {
    render(<SecurityRlsPage />);
    fireEvent.click(rowOf("rules-list", "analyst"));
    expect(screen.getByTestId("duplicate-rule-4")).toHaveAccessibleName("Duplicate");
  });

  it("opens the new-rule form filled from the rule, and saves it as a create", async () => {
    render(<SecurityRlsPage />);
    fireEvent.click(rowOf("rules-list", "analyst"));
    await userEvent.click(screen.getByTestId("duplicate-rule-4"));
    expect(screen.getByTestId("rules-split-title")).toHaveTextContent("New RLS rule");
    expect(screen.getByTestId("rule-description")).toHaveValue("EU analysts see EU rows");
    expect(within(rulePanel()).getByDisplayValue("region = 'eu'")).toBeInTheDocument();
    await userEvent.click(screen.getByTestId("save-rule"));
    await waitFor(() => expect(upsertRlsRule).toHaveBeenCalled());
    expect(upsertRlsRule).toHaveBeenCalledWith(
      expect.objectContaining({
        roleId: "analyst",
        domainId: "sales",
        filterExpr: "region = 'eu'",
        description: "EU analysts see EU rows",
        id: null,
        creating: true,
      }),
    );
  });
});
