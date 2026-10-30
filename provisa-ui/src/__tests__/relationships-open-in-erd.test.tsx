// Copyright (c) 2026 Kenneth Stott
// Canary: 91c4d7e3-5a28-4b6f-a0d9-2e8f3b1c7a45
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2130: `/relationships?erd=<table id>` (the Ontology's "Open in ERD") opens the ERD on that
// table -- selected and centred -- on a direct load too; a table the ERD does not hold is said so.

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
const DOMAINS = { domains: [{ domainId: "sales", label: "Sales", description: "" }] };
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

describe("RelationshipsPage opened on a table's ERD", () => {
  it("opens the ERD with the table in the parameter selected and centred", async () => {
    render(<RelationshipsPage />, { initialEntries: ["/relationships?erd=2"] });
    expect(await screen.findByTestId("erd-modal")).toBeInTheDocument();
    await waitFor(() => expect(screen.getByTestId("erd-panel")).toHaveAttribute("data-focus", "found"));
    expect(screen.queryByTestId("erd-table-not-found")).toBeNull();
  });

  it("says so for a table the ERD does not hold, with no other table taken", async () => {
    render(<RelationshipsPage />, { initialEntries: ["/relationships?erd=99"] });
    expect(await screen.findByTestId("erd-table-not-found")).toHaveTextContent(
      "No table with id 99 is in the ERD.",
    );
    expect(screen.getByTestId("erd-panel")).toHaveAttribute("data-focus", "missing");
  });

  it("stays closed without the parameter", () => {
    render(<RelationshipsPage />, { initialEntries: ["/relationships"] });
    expect(screen.queryByTestId("erd-modal")).toBeNull();
  });
});
