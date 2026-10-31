// Copyright (c) 2026 Kenneth Stott
// Canary: c41e7a02-96bd-4d3f-8a15-07f2e6b9d354
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1132: a system domain is held as a capability, so the role form and the RLS rule form offer
// data domains only, whatever the catalog of domains holds.

import { describe, it, expect, vi } from "vitest";
import userEvent from "@testing-library/user-event";
import { render, screen, fireEvent, within } from "../../test-utils/render";
import { openNewRole } from "../../test-utils/securityRoles";

vi.mock("../../context/DomainFilterContext", () => ({
  useDomainFilter: () => ({
    domains: [],
    setDomains: vi.fn(),
    setSelectedDomain: vi.fn(),
    checkedDomains: new Set<string>(),
  }),
}));
vi.mock("../../api/actions", () => ({
  fetchActions: async () => ({ functions: [], webhooks: [] }),
}));
vi.mock("../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useAdminQueries")>()),
  useRoles: () => ({ roles: [], loading: false, refetch: vi.fn() }),
  useTables: () => ({ tables: [], loading: false, refetch: vi.fn() }),
  useDomains: () => ({
    domains: [{ id: "meta" }, { id: "ontology" }, { id: "ops" }, { id: "sales" }, { id: "hr" }],
    loading: false,
    refetch: vi.fn(),
  }),
}));
vi.mock("../../hooks/useSecurityQueries", () => ({
  useRLSRules: () => ({ rlsRules: [], loading: false, refetch: vi.fn() }),
  useUpsertRole: () => ({ upsertRole: vi.fn(), loading: false }),
  useDeleteRole: () => ({ deleteRole: vi.fn(), loading: false }),
  useUpsertRlsRule: () => ({ upsertRlsRule: vi.fn(), loading: false }),
  useDeleteRlsRule: () => ({ deleteRlsRule: vi.fn(), loading: false }),
  useRevokeRoleGrants: () => ({ revokeFromTable: vi.fn(), revokeFromObject: vi.fn() }),
}));

import { SecurityRolesPage } from "../SecurityPage";
import { SecurityRlsPage } from "../security/SecurityRlsPage";

describe("domain pickers offer data domains only", () => {
  it("the RLS rule form's Domain dropdown", async () => {
    render(<SecurityRlsPage />);
    fireEvent.click(screen.getByTestId("toggle-rule-form"));
    const combobox = screen.getByTestId("rule-domain-select");
    await userEvent.click(combobox);
    const listbox = document.getElementById(combobox.getAttribute("aria-controls")!)!;
    const offered = within(listbox)
      .getAllByRole("option", { hidden: true })
      .map((o) => o.textContent);
    expect(offered).toEqual(["sales", "hr"]);
  });

  it("the role form's Domain Access", async () => {
    render(<SecurityRolesPage />);
    await openNewRole();
    fireEvent.click(screen.getByRole("textbox", { name: "Domain Access" }));
    expect(await screen.findByRole("option", { name: "sales", hidden: true })).toBeTruthy();
    for (const system of ["meta", "ontology", "ops"]) {
      expect(screen.queryByRole("option", { name: system, hidden: true })).toBeNull();
    }
  });
});
