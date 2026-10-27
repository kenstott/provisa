// Copyright (c) 2026 Kenneth Stott
// Canary: a4fa49c1-d796-4660-9f9d-00e630d6745f
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Admin > Domains: the trash icon asks before it deletes. A click opens the shared confirmation
// naming the domain; nothing is deleted until it is confirmed, and Cancel deletes nothing.

import { describe, it, expect, vi, beforeEach } from "vitest";
import { screen, fireEvent, waitFor } from "@testing-library/react";
import { render } from "../../test-utils/render";

const deleteDomain = vi.fn(async (_id: string) => ({ success: true, message: "ok" }));
const createDomain = vi.fn(async (_id: string, _description: string, _alias: string | null) => ({
  success: true,
  message: "ok",
}));
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

// A domain the server refuses (a reserved word, an id in
// use) is said as refused, in red, the form left open to correct -- never reported as added.
describe("Admin > Domains: adding a domain", () => {
  beforeEach(() => {
    createDomain.mockClear();
    refetch.mockClear();
  });

  async function add(id: string) {
    render(<AdminPage />, { initialEntries: ["/admin/domains"] });
    fireEvent.click(await screen.findByTestId("new-domain-open"));
    fireEvent.change(screen.getByTestId("new-domain-id"), { target: { value: id } });
    fireEvent.click(screen.getByTestId("new-domain-save"));
  }

  it("says a refusal as refused, and keeps the form open", async () => {
    createDomain.mockImplementationOnce(async () => ({
      success: false,
      message: "Domain id 'select' is a reserved word",
    }));
    await add("select");
    const msg = await screen.findByTestId("domain-msg");
    expect(msg).toHaveTextContent("Domain id 'select' is a reserved word");
    expect(msg.closest(".mantine-Alert-root")?.getAttribute("style") ?? "").toContain("red");
    expect(screen.getByTestId("new-domain-form")).toBeInTheDocument();
    expect(refetch).not.toHaveBeenCalled();
  });

  it("says why an add raised", async () => {
    createDomain.mockImplementationOnce(async () => {
      throw new Error("Domain 'sales' already exists");
    });
    await add("sales");
    expect(await screen.findByTestId("domain-msg")).toHaveTextContent(
      "Domain 'sales' already exists",
    );
  });

  it("says a domain added as added", async () => {
    await add("finance");
    await waitFor(() => expect(createDomain).toHaveBeenCalledWith("finance", "", null, null));
    expect(await screen.findByTestId("domain-msg")).toHaveTextContent("finance");
    expect(screen.queryByTestId("new-domain-form")).toBeNull();
  });
});
