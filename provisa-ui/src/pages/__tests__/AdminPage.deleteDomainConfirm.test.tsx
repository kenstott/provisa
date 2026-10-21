// Copyright (c) 2026 Kenneth Stott
// Canary: 7d07e241-1d48-41e4-b88f-740057ed77cd
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
import { screen, fireEvent, waitFor, within } from "@testing-library/react";
import { render } from "../../test-utils/render";

const deleteDomain = vi.fn(async (_id: string) => ({ success: true, message: "ok" }));
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
  useCreateDomain: () => ({ createDomain: vi.fn() }),
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

describe("Admin > Domains: deleting a domain", () => {
  beforeEach(() => {
    deleteDomain.mockClear();
    refetch.mockClear();
  });

  async function openConfirmation() {
    render(<AdminPage />, { initialEntries: ["/admin/domains"] });
    fireEvent.click(await screen.findByRole("button", { name: "Delete domain sales" }));
    return screen.findByRole("dialog");
  }

  it("asks first, naming the domain, and deletes nothing on the click", async () => {
    const dialog = await openConfirmation();
    expect(within(dialog).getByText('Delete domain "sales"?')).toBeInTheDocument();
    expect(
      within(dialog).getByText(
        "The domain is removed. A domain that is still in use cannot be deleted.",
      ),
    ).toBeInTheDocument();
    expect(deleteDomain).not.toHaveBeenCalled();
  });

  it("deletes on confirm", async () => {
    const dialog = await openConfirmation();
    fireEvent.click(within(dialog).getByRole("button", { name: "Confirm" }));
    await waitFor(() => expect(deleteDomain).toHaveBeenCalledWith("sales"));
    await waitFor(() => expect(refetch).toHaveBeenCalled());
  });

  it("deletes nothing on cancel", async () => {
    const dialog = await openConfirmation();
    fireEvent.click(within(dialog).getByRole("button", { name: "Cancel" }));
    await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
    expect(deleteDomain).not.toHaveBeenCalled();
  });
});
