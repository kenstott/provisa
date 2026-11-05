// Copyright (c) 2026 Kenneth Stott
// Canary: 91c4d7e3-5a28-4b6f-a0d9-2e8f3b1c7a45
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2187: a focus link opens the Relationships page's ERD in place, on a direct load too: its
// table highlighted; a table the diagram does not hold is said so in the bar, none other taken.

import { describe, expect, it, vi } from "vitest";
import { screen, waitFor } from "@testing-library/react";
import { render } from "../test-utils/render";
import { RelationshipsPage } from "../pages/RelationshipsPage";

const table = (id: number, name: string) => ({
  id,
  domainId: "sales",
  tableName: name,
  schemaName: "public",
  description: "",
  columns: [],
});
const RELS = { relationships: [], loading: false, refetch: vi.fn() };
const TABLES = { tables: [table(1, "orders"), table(2, "customers")], loading: false };
const DOMAINS = { domains: [{ id: "sales", description: "" }] };
const AUTH = { capabilities: [] };
const FILTER = { domainsEnabled: false, domains: [], checkedDomains: new Set<string>() };

vi.mock("../hooks/useAdminQueries", () => ({
  useRelationships: () => RELS,
  useAllRelationships: () => RELS,
  useTables: () => TABLES,
  useDomains: () => DOMAINS,
  useUpsertRelationship: () => ({ upsertRelationship: vi.fn() }),
  useDeleteRelationship: () => ({ deleteRelationship: vi.fn() }),
}));
vi.mock("../context/AuthContext", () => ({ useAuth: () => AUTH }));
vi.mock("../context/DomainFilterContext", () => ({ useDomainFilter: () => FILTER }));
vi.mock("../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../api/admin")>()),
  fetchCandidates: vi.fn().mockResolvedValue([]),
  fetchRejectedCount: vi.fn().mockResolvedValue(0),
}));
vi.mock("../api/actions", () => ({
  fetchActions: vi.fn().mockResolvedValue({ functions: [], webhooks: [] }),
}));
// jsdom has no canvas: the real cytoscape draws on a context that accepts every call.
HTMLCanvasElement.prototype.getContext = function (this: HTMLCanvasElement) {
  const state: Record<string, unknown> = {};
  return new Proxy(state, {
    get: (t, k: string) =>
      k in t
        ? t[k]
        : k === "measureText"
          ? () => ({ width: 10 })
          : k === "canvas"
            ? this
            : () => undefined,
    set: (t, k: string, v) => {
      t[k] = v;
      return true;
    },
  });
} as unknown as typeof HTMLCanvasElement.prototype.getContext;
vi.mock("../components/TagControl", () => ({ TagControl: () => null }));

describe("RelationshipsPage opened on a focus link", () => {
  it("opens the ERD in place with the table in the link highlighted", async () => {
    render(<RelationshipsPage />, {
      initialEntries: ["/relationships?view=erd&objects=sales.customers"],
    });
    expect(await screen.findByTestId("erd-panel")).toBeInTheDocument();
    expect(screen.queryByRole("dialog")).toBeNull();
    await waitFor(() =>
      expect(screen.getByTestId("erd-panel")).toHaveAttribute("data-highlighted", "2"),
    );
    expect(screen.queryByTestId("erd-focus-missing")).toBeNull();
  });

  it("says so for a table the ERD does not hold, with no other table taken", async () => {
    render(<RelationshipsPage />, {
      initialEntries: ["/relationships?view=erd&objects=sales.ghosts"],
    });
    expect(await screen.findByTestId("erd-focus-missing")).toHaveTextContent(
      "Not found: sales.ghosts",
    );
    expect(screen.getByTestId("erd-panel")).toHaveAttribute("data-highlighted", "");
  });

  it("stays on the list without the parameter", () => {
    render(<RelationshipsPage />, { initialEntries: ["/relationships"] });
    expect(screen.queryByTestId("erd-panel")).toBeNull();
  });
});
