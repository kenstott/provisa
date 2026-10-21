// Copyright (c) 2026 Kenneth Stott
// Canary: c389b863-313f-45b8-92ab-7b34223938d9
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Admin > Domains, the domain tree (REQ-1363): each domain's parent is listed, a domain with no
// steward shows the one it inherits, and a domain is added under the parent chosen.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { screen, fireEvent, waitFor, within } from "@testing-library/react";
import { render } from "../../test-utils/render";

const deleteDomain = vi.fn(async (_id: string) => ({ success: true, message: "ok" }));
const createDomain =
  vi.fn<
    (
      id: string,
      description: string,
      alias: string | null,
      parent: string | null,
    ) => Promise<Record<string, unknown>>
  >();
const refetch = vi.fn(async () => undefined);
// One object per hook, the same on every render: the page derives state from these in effects.
const hooks = vi.hoisted(() => ({
  sources: { loading: false, sources: [] },
  domains: {
    loading: false,
    domains: [
      {
        id: "retail",
        description: "Business unit",
        steward: "bu_steward",
        effectiveSteward: "bu_steward",
      },
      {
        id: "sales",
        description: "Sales",
        parentDomainId: "retail",
        steward: null,
        effectiveSteward: "bu_steward",
      },
    ],
    refetch: undefined as unknown,
  },
  tables: { loading: false, tables: [] },
  relationships: { loading: false, relationships: [] },
  rls: { loading: false, rlsRules: [] },
  roles: { roles: [] },
}));

vi.mock("../../hooks/useAdminQueries", async (orig) => ({
  ...(await orig<typeof import("../../hooks/useAdminQueries")>()),
  useSources: () => hooks.sources,
  useDomains: () => ({ ...hooks.domains, domains: hooks.domains.domains, refetch }),
  useTables: () => hooks.tables,
  useRelationships: () => hooks.relationships,
  useRLSRules: () => hooks.rls,
  useRoles: () => hooks.roles,
  useCreateDomain: () => ({ createDomain }),
  useDeleteDomain: () => ({ deleteDomain }),
}));

vi.mock("../../context/AuthContext", () => ({
  useAuth: () => ({
    role: { id: "org_admin", capabilities: ["org_settings", "domain_admin"], domain_access: ["*"] },
    capabilities: ["org_settings", "domain_admin"],
    selectedRoles: ["org_admin"],
    user: { id: "u", username: "u" },
  }),
}));

vi.mock("../../api/admin", async (orig) => ({
  ...(await orig<typeof import("../../api/admin")>()),
  fetchSettings: vi.fn(async () => ({
    naming: { use_domains: true },
    federation: { enabled: false, subgraph_name: "provisa", role: null },
  })),
}));

import { AdminPage } from "../AdminPage";

describe("Admin > Domains: the domain tree", () => {
  beforeEach(() => {
    createDomain.mockReset();
  });

  it("lists each domain's parent and the steward a domain inherits", async () => {
    render(<AdminPage />, { initialEntries: ["/admin/domains"] });
    const row = (await screen.findByText("Sales")).closest("tr") as HTMLElement;
    expect(row).toHaveTextContent("retail");
    expect(row).toHaveTextContent("bu_steward");
    expect(within(row).getByTestId("steward-inherited")).toBeInTheDocument();
    const unit = screen.getByText("Business unit").closest("tr") as HTMLElement;
    expect(within(unit).queryByTestId("steward-inherited")).toBeNull();
  });

  it("adds a domain under the parent chosen", async () => {
    createDomain.mockResolvedValueOnce({ success: true, message: "created" });
    render(<AdminPage />, { initialEntries: ["/admin/domains"] });
    fireEvent.click(await screen.findByTestId("new-domain-open"));
    fireEvent.change(screen.getByTestId("new-domain-id"), { target: { value: "outlets" } });
    fireEvent.click(screen.getByTestId("new-domain-parent"));
    fireEvent.click(await screen.findByRole("option", { name: "retail" }));
    fireEvent.click(screen.getByTestId("new-domain-save"));
    await waitFor(() => expect(createDomain).toHaveBeenCalledWith("outlets", "", null, "retail"));
  });
});
