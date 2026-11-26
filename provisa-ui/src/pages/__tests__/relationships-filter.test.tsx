// Copyright (c) 2026 Kenneth Stott
// Canary: 6e0b3f58-a2c4-4d91-8f7e-15b9d2c64a07
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The Relationships filter finds a relationship by every text its row shows (maintainer
// 2026-11-26: "neighborhood" found nothing beside building_permits.neighborhood), and a list the
// filter empties says nothing matches, never that nothing is registered.

import { describe, it, expect, vi } from "vitest";
import { screen, fireEvent } from "@testing-library/react";
import { render } from "../../test-utils/render";
import { expectFilterFindsEveryCell } from "../../test-utils/listFilter";
import { RelationshipsPage } from "../RelationshipsPage";

const BASE = {
  targetFunctionName: null,
  functionArg: null,
  viaTableName: null,
  cypherType: null,
  computedCypherAlias: null,
  properties: [] as string[],
};
const RELS = {
  relationships: [
    {
      ...BASE,
      id: "permits-to-budget",
      sourceTableId: 1,
      targetTableId: 2,
      sourceDomainId: "civic",
      sourceTableName: "building_permits",
      targetTableName: "budget_2026__budget",
      sourceColumn: "neighborhood",
      targetColumn: "district",
      cardinality: "many-to-one",
      materialize: true,
      refreshInterval: 900,
      alias: "PERMIT_BUDGET",
      graphqlAlias: "permitBudget",
      shape: "direct",
      autoSuggested: true,
    },
    {
      ...BASE,
      id: "orders-to-customers",
      sourceTableId: 3,
      targetTableId: 4,
      sourceDomainId: "shop",
      sourceTableName: "orders",
      targetTableName: "customers",
      sourceColumn: "customer_id",
      targetColumn: "id",
      cardinality: "one-to-one",
      materialize: false,
      refreshInterval: 300,
      alias: null,
      computedCypherAlias: "PLACED_BY",
      graphqlAlias: null,
      shape: "junction",
      viaTableName: "order_links",
      cypherType: "LINKED",
      properties: ["weight"],
    },
  ],
  loading: false,
  refetch: vi.fn(),
};
const table = (id: number, domainId: string, tableName: string) => ({
  id,
  domainId,
  sourceId: `${domainId}-db`,
  schemaName: domainId,
  tableName,
  alias: null,
  columns: [],
});
const TABLES = {
  refetch: () => Promise.resolve(),
  tables: [
    table(1, "civic", "building_permits"),
    table(2, "civic", "budget_2026__budget"),
    table(3, "shop", "orders"),
    table(4, "shop", "customers"),
  ],
  loading: false,
};
const FILTER = { domainsEnabled: true, domains: [], checkedDomains: new Set<string>() };

vi.mock("../../hooks/useAdminQueries", () => ({
  useCodeResolvers: () => ({
    codeResolvers: [],
    loading: false,
    saveCodeResolver: vi.fn(),
    retireCodeResolver: vi.fn(),
  }),
  useRelationships: () => RELS,
  useAllRelationships: () => RELS,
  useTables: () => TABLES,
  useDomains: () => ({ domains: [] }),
  useUpsertRelationship: () => ({ upsertRelationship: vi.fn() }),
  useDeleteRelationship: () => ({ deleteRelationship: vi.fn() }),
  useDeleteRelationships: () => vi.fn(),
}));
vi.mock("../../context/AuthContext", () => ({ useAuth: () => ({ capabilities: [] }) }));
vi.mock("../../context/DomainFilterContext", () => ({ useDomainFilter: () => FILTER }));
vi.mock("../../api/admin", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../api/admin")>()),
  fetchRejectedCount: vi.fn().mockResolvedValue(0),
  fetchSettings: vi.fn().mockResolvedValue({
    naming: { convention: "apollo_graphql", use_domains: true, default_domain: "" },
  }),
  fetchDiscoveryRuns: vi.fn().mockResolvedValue([]),
}));
vi.mock("../../api/actions", () => ({
  fetchActions: vi.fn().mockResolvedValue({ functions: [], webhooks: [] }),
}));
vi.mock("../../components/TagControl", () => ({ TagControl: () => null }));

const filterBox = () => screen.getByTestId("filter-input");
const list = () => screen.getByTestId("relationships-list");

describe("RelationshipsPage filter", () => {
  it("finds a relationship by a column name its row shows", async () => {
    render(<RelationshipsPage />);
    await screen.findByText("building_permits.neighborhood");
    fireEvent.change(filterBox(), { target: { value: "Neighborhood" } });
    expect(screen.getByText("building_permits.neighborhood")).toBeTruthy();
    expect(screen.queryByText("orders.customer_id")).toBeNull();
  });

  it("finds each relationship by every text its row shows", async () => {
    render(<RelationshipsPage />);
    await screen.findByText("building_permits.neighborhood");
    await expectFilterFindsEveryCell(filterBox(), list);
  });

  it("says nothing matches when the filter empties the list", async () => {
    render(<RelationshipsPage />);
    await screen.findByText("building_permits.neighborhood");
    fireEvent.change(filterBox(), { target: { value: "no-such-thing" } });
    expect(screen.getByTestId("relationships-empty").textContent).toBe(
      "No relationships match the filter.",
    );
  });
});
