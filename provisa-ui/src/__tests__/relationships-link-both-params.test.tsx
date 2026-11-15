// Copyright (c) 2026 Kenneth Stott
// Canary: 28fc3fb6-6c45-4d0d-a406-554dd1f9b62b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The Associations link of a table's relationship (pages/tables/TableAssociations) names two
// things: the search narrowing the list to the source table and the relationship whose panel opens.
// Both are read together on load.

import { describe, expect, it, vi } from "vitest";
import { screen, within } from "@testing-library/react";
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
const rel = (id: number, source: string, target: string) => ({
  id,
  sourceTableId: id,
  targetTableId: 9,
  sourceTableName: source,
  sourceDomainId: "sales",
  targetTableName: target,
  sourceColumn: "k",
  targetColumn: "id",
  cardinality: "many-to-one",
  materialize: false,
  refreshInterval: 300,
  targetFunctionName: null,
  functionArg: null,
  alias: null,
  graphqlAlias: null,
  physicalName: null,
  computedCypherAlias: null,
  cypherType: null,
  autoSuggested: false,
  disableCypher: false,
  sameEntity: false,
  viaTableId: null,
  viaTableName: null,
  viaSourceColumn: null,
  viaTargetColumn: null,
  viaTypeColumn: null,
  viaTypeValue: null,
  viaLabelSource: null,
  parameters: [],
});
const RELS = {
  relationships: [rel(1, "orders", "customers"), rel(2, "returns", "customers")],
  loading: false,
  refetch: vi.fn(),
};
const TABLES = { tables: [table(1, "orders"), table(2, "returns")], loading: false };
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
  useDeleteRelationships: () => vi.fn(),
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
vi.mock("../components/TagControl", () => ({ TagControl: () => null }));

describe("RelationshipsPage opened on a table's relationship link", () => {
  it("filters the list to the table and opens that relationship's panel", async () => {
    render(<RelationshipsPage />, {
      initialEntries: ["/relationships?relationship=2&search=returns"],
    });
    const list = await screen.findByTestId("relationships-list");
    expect(within(list).getByText(/returns/)).toBeInTheDocument();
    expect(within(list).queryByText(/orders/)).toBeNull();
    expect(await screen.findByTestId("relationships-split-panel")).toBeInTheDocument();
  });
});
