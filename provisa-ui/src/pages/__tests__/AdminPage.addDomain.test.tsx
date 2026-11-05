// Copyright (c) 2026 Kenneth Stott
// Canary: 5c1e8b92-4f37-4a06-9d2b-7e6a3f0c1d48
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Admin > Domains: adding a domain. A refused add (an id that exists) keeps the form open and says
// why; the page never reports a domain added that was not.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { screen, fireEvent, waitFor } from "@testing-library/react";
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
    domains: [{ id: "sales", description: "Sales", alias: null }],
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

async function addDomain(id: string) {
  render(<AdminPage />, { initialEntries: ["/admin/domains"] });
  fireEvent.click(await screen.findByTestId("new-domain-open"));
  fireEvent.change(screen.getByTestId("new-domain-id"), { target: { value: id } });
  fireEvent.click(screen.getByTestId("new-domain-save"));
}

describe("Admin > Domains: adding a domain", () => {
  beforeEach(() => {
    createDomain.mockReset();
    refetch.mockClear();
  });

  it("keeps the form open on a refused add and says why", async () => {
    createDomain.mockResolvedValueOnce({
      success: false,
      message: "Domain 'sales' already exists",
      code: "schema.domain_id_taken",
      params: { domain: "sales" },
    });
    await addDomain("sales");
    expect(await screen.findByTestId("domain-msg")).toHaveTextContent("sales");
    expect(screen.getByTestId("new-domain-form")).toBeInTheDocument();
    expect(screen.queryByText(/Domain "sales" added|Domain 'sales' added/)).toBeNull();
    expect(refetch).not.toHaveBeenCalled();
  });

  it("closes the form and lists the domain once it is created", async () => {
    createDomain.mockResolvedValueOnce({ success: true, message: "Domain 'finance' created" });
    await addDomain("finance");
    await waitFor(() => expect(screen.queryByTestId("new-domain-form")).toBeNull());
    expect(createDomain).toHaveBeenCalledWith("finance", "", null, null);
    expect(refetch).toHaveBeenCalled();
  });
});
