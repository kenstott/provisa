// Copyright (c) 2026 Kenneth Stott
// Canary: 8b2d6f14-9c37-4a50-b1e8-3f6a9d2c7e05
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1666: the product detail's lineage is the member tables (every column an output), their
// whole upstream ancestry, and one hop downstream — never the checker's results tables.

import { describe, it, expect, vi } from "vitest";
import { render, waitFor } from "../../test-utils/render";
import type { LineageGraphData } from "../../api/lineage";

vi.mock("../../context/AuthContext", () => ({
  useAuth: () => ({ role: { id: "admin", capabilities: [] } }),
}));
vi.mock("../../hooks/useCapability", () => ({ useCapability: () => true }));
vi.mock("../../api/actions", () => ({
  fetchActions: () => Promise.resolve({ functions: [] }),
  saveFunction: vi.fn(),
}));
vi.mock("../../api/glossary", () => ({ fetchRelatedGlossaryTerms: () => Promise.resolve([]) }));

const col = (relation: string, column: string, kind = "source") => ({
  id: `${relation}.${column}`,
  column,
  relation,
  kind,
  materialized: false,
});
const edge = (source: string, target: string) => ({ source, target, transform: "", ops: [] });

// breeds → pets → dim_pet → fact_pet (two hops downstream), plus the checker's results table.
const FEDERATION: LineageGraphData = {
  nodes: [
    col("pet_store.breeds", "name"),
    col("pet_store.pets", "name"),
    col("pet_store.pets", "id"),
    col("pet_store.dim_pet", "id", "derived"),
    col("pet_store.fact_pet", "pet_id", "derived"),
    col("pet_store.pets_quality", "scan_id", "derived"),
  ],
  edges: [
    edge("pet_store.breeds.name", "pet_store.pets.name"),
    edge("pet_store.pets.id", "pet_store.dim_pet.id"),
    edge("pet_store.dim_pet.id", "pet_store.fact_pet.pet_id"),
    edge("pet_store.pets.id", "pet_store.pets_quality.scan_id"),
  ],
  outputs: [],
};
vi.mock("../../api/lineage", () => ({ fetchFederationGraph: () => Promise.resolve(FEDERATION) }));

const seen: LineageGraphData[] = [];
vi.mock("../data-products/DataProductDetailPanel", () => ({
  DataProductDetailPanel: ({ lineageGraph }: { lineageGraph: LineageGraphData | null }) => {
    if (lineageGraph) seen.push(lineageGraph);
    return <div data-testid="detail-panel" />;
  },
}));

const products = [
  {
    id: "pet_health",
    domainId: "pet-store",
    name: "Pet Health",
    purpose: "",
    limitations: "",
    usage: "",
    customProperties: {},
  },
];
const table = (id: number, tableName: string, extra: Record<string, unknown> = {}) => ({
  id,
  domainId: "pet-store",
  schemaName: "pet_store",
  tableName,
  productId: "pet_health",
  dqContract: null,
  columns: [{ columnName: "id" }, { columnName: "name" }, { columnName: "price" }],
  ...extra,
});
const tables = [
  table(1, "pets"),
  table(2, "pets_quality", { dqContract: "name: x", columns: [{ columnName: "scan_id" }] }),
];
const none: never[] = [];
vi.mock("../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useAdminQueries")>()),
  useDataProducts: () => ({
    dataProducts: products,
    loading: false,
    error: null,
    refetch: vi.fn(),
  }),
  useDomains: () => ({ domains: none, loading: false, refetch: vi.fn() }),
  useTables: () => ({ tables, loading: false, refetch: vi.fn() }),
  useSources: () => ({ sources: none, loading: false, refetch: vi.fn() }),
  useRelationships: () => ({ relationships: none, loading: false, refetch: vi.fn() }),
  useRoles: () => ({ roles: none, loading: false, refetch: vi.fn() }),
  useCreateDataProduct: () => ({ createDataProduct: vi.fn(), loading: false }),
  useDeleteDataProduct: () => ({ deleteDataProduct: vi.fn(), loading: false }),
  useUpdateTable: () => ({ updateTable: vi.fn(), loading: false }),
}));

import { DataProductsPage } from "../DataProductsPage";

describe("product detail lineage scope (REQ-1666)", () => {
  it("keeps members, ancestry and one hop downstream; drops checker tables; every member column is an output", async () => {
    render(<DataProductsPage />, { initialEntries: ["/data-products?product=pet_health"] });
    await waitFor(() => expect(seen.length).toBeGreaterThan(0));
    const g = seen[seen.length - 1];
    const relations = new Set(g.nodes.map((n) => n.relation));
    expect(relations).toEqual(new Set(["pet_store.breeds", "pet_store.pets", "pet_store.dim_pet"]));
    // price was never in the federation graph; it is still a column of the published table.
    expect(g.nodes.map((n) => n.id)).toContain("pet_store.pets.price");
    expect(new Set(g.outputs)).toEqual(
      new Set(["pet_store.pets.id", "pet_store.pets.name", "pet_store.pets.price"]),
    );
    expect(g.edges.map((e) => e.target)).not.toContain("pet_store.pets_quality.scan_id");
    expect(g.edges.map((e) => e.target)).not.toContain("pet_store.fact_pet.pet_id");
  });
});
