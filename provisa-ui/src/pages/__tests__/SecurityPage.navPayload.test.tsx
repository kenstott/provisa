// Copyright (c) 2026 Kenneth Stott
// Canary: 2e7bf90a-f89e-4842-ae91-bfbc929ef89a
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// "Rules for this table" hands the table to the security page; the rules list is filtered by it,
// whether the page was just opened or was already open.

import { describe, it, expect, vi } from "vitest";
import { act, screen, waitFor } from "@testing-library/react";
import { useNavigate } from "react-router-dom";
import { render } from "../../test-utils/render";

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
vi.mock("../../hooks/useSecurityQueries", () => ({
  useRLSRules: () => ({ rlsRules: [], loading: false, refetch: vi.fn() }),
  useUpsertRole: () => ({ upsertRole: vi.fn(), loading: false }),
  useDeleteRole: () => ({ deleteRole: vi.fn(), loading: false }),
  useUpsertRlsRule: () => ({ upsertRlsRule: vi.fn(), loading: false }),
  useDeleteRlsRule: () => ({ deleteRlsRule: vi.fn(), loading: false }),
  useRevokeRoleGrants: () => ({ revokeFromTable: vi.fn(), revokeFromObject: vi.fn() }),
}));

import { SecurityRlsPage } from "../SecurityPage";

function RulesFor({ table }: { table: string }) {
  const navigate = useNavigate();
  return (
    <button onClick={() => navigate("/security", { state: { tableFilter: table } })}>
      {table}
    </button>
  );
}

describe("SecurityPage — a table handed over while the page is open", () => {
  it("filters the rules by it", async () => {
    render(
      <>
        <RulesFor table="orders" />
        <SecurityRlsPage />
      </>,
      { initialEntries: ["/security"] },
    );
    const filter = await screen.findByPlaceholderText(/Filter by role or table/);
    expect(filter).toHaveValue("");
    act(() => screen.getByText("orders").click());
    await waitFor(() => expect(filter).toHaveValue("orders"));
  });
});
