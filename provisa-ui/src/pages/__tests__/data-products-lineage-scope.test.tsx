// Copyright (c) 2026 Kenneth Stott
// Canary: 8b2d6f14-9c37-4a50-b1e8-3f6a9d2c7e05
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1640, REQ-1666: the product detail's lineage is the LineageDag the Lineage page draws, over the
// member tables (every column an output), their whole upstream ancestry and one hop downstream --
// never the checker's results tables -- full width below the cards, for a role with
// view_governance only.

import { describe, it, expect, vi } from "vitest";
import { fireEvent, render, screen, waitFor, within } from "../../test-utils/render";
import type { LineageGraphData, LineageNode } from "../../api/lineage";
import type { DataProduct } from "../../types/admin";
import type { ProductCore } from "../data-products/productCore";
import { lifecycleProps } from "../data-products/__tests__/lifecycleProps";

vi.mock("../../context/AuthContext", () => ({
  useAuth: () => ({ role: { id: "admin", capabilities: [] } }),
}));
vi.mock("../../hooks/useCapability", () => ({ useCapability: () => true }));
vi.mock("../../api/actions", () => ({
  fetchActions: () => Promise.resolve({ functions: [] }),
  saveFunction: vi.fn(),
}));
vi.mock("../../components/graph3d/KnowledgeGraphPanel", () => ({
  KnowledgeGraphPanel: () => null,
  ProductMapButton: () => null,
}));

const col = (
  relation: string,
  column: string,
  kind: LineageNode["kind"] = "source",
): LineageNode => ({
  id: `${relation}.${column}`,
  column,
  relation,
  kind,
  materialized: false,
});
const edge = (source: string, target: string) => ({ source, target, transform: "", ops: [] });

// breeds → pets → dim_pet → fact_pet (two hops downstream), plus the checker's results table; pets
// is read natively from its source (the "native:" node relation is the lineage's internal key).
const FEDERATION: LineageGraphData = {
  nodes: [
    col("native:pet_store.pets", "pet_id", "native"),
    col("pet_store.breeds", "name"),
    col("staging.pet_prices", "price"),
    col("pet_store.pets", "name"),
    col("pet_store.pets", "id"),
    col("pet_store.dim_pet", "id", "derived"),
    col("pet_store.fact_pet", "pet_id", "derived"),
    col("pet_store.pets_quality", "scan_id", "derived"),
  ],
  edges: [
    edge("native:pet_store.pets.pet_id", "pet_store.pets.id"),
    edge("pet_store.breeds.name", "pet_store.pets.name"),
    edge("staging.pet_prices.price", "pet_store.pets.name"),
    edge("pet_store.pets.id", "pet_store.dim_pet.id"),
    edge("pet_store.dim_pet.id", "pet_store.fact_pet.pet_id"),
    edge("pet_store.pets.id", "pet_store.pets_quality.scan_id"),
  ],
  outputs: [],
};
vi.mock("../../api/lineage", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../api/lineage")>()),
  fetchFederationGraph: () => Promise.resolve(FEDERATION),
}));

// The DAG itself is the Lineage page's, held by its own tests: here, what it is given.
type Lanes = { members: ReadonlySet<string>; title: (lane: number) => string };
const drawn: {
  graph: LineageGraphData;
  members?: ReadonlySet<string>;
  lanes?: Lanes;
  relationLabel?: (relation: string) => string;
}[] = [];
vi.mock("../../components/lineage/LineageDag", () => ({
  LineageDag: ({
    graph,
    lanes,
    relationLabel,
  }: {
    graph: LineageGraphData;
    lanes?: Lanes;
    relationLabel?: (relation: string) => string;
  }) => {
    drawn.push({ graph, members: lanes?.members, lanes, relationLabel });
    return <div data-testid="lineage-dag" />;
  },
}));

const product = {
  id: "pet_health",
  domainId: "pet-store",
  name: "Pet Health",
  purpose: "",
  limitations: "",
  usage: "",
  customProperties: {},
} as DataProduct;
const CORE: ProductCore = {
  product,
  tables: [],
  edges: [],
  queries: { graphql: "", sql: "", jsonapi: "" },
};
vi.mock("../data-products/useProductCore", () => ({
  useMetaProducts: () => ({
    data: [product],
    unreadable: false,
    error: null,
    loading: false,
    reload: vi.fn(),
  }),
  useProductCore: () => ({
    data: CORE,
    unreadable: false,
    error: null,
    loading: false,
    reload: vi.fn(),
  }),
  useMetaReader: () => null,
}));

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
  table(3, "breeds", { productId: null, columns: [{ columnName: "name" }] }),
];
const none: never[] = [];
vi.mock("../../hooks/useDataProductMutations", () => ({
  useCreateDataProduct: () => ({ createDataProduct: vi.fn(), loading: false }),
  useUpdateDataProduct: () => ({ updateDataProduct: vi.fn(), loading: false }),
  useDeleteDataProduct: () => ({ deleteDataProduct: vi.fn(), loading: false }),
  useSaveDataContract: () => ({ saveDataContract: vi.fn(), loading: false }),
  useDataProductFindings: () => ({ findings: new Map(), refetch: vi.fn(), error: undefined }),
  useDataProductStatus: () => ({
    publish: vi.fn(),
    deprecate: vi.fn(),
    retire: vi.fn(),
    restore: vi.fn(),
  }),
}));
vi.mock("../../hooks/useAdminQueries", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../hooks/useAdminQueries")>()),
  useDomains: () => ({ domains: none, loading: false, refetch: vi.fn() }),
  useTables: () => ({ tables, loading: false, refetch: vi.fn() }),
  useSources: () => ({ sources: none, loading: false, refetch: vi.fn() }),
  useRelationships: () => ({ relationships: none, loading: false, refetch: vi.fn() }),
  useRoles: () => ({ roles: none, loading: false, refetch: vi.fn() }),
  useUpdateTable: () => ({ updateTable: vi.fn(), loading: false }),
}));

import { DataProductsPage } from "../DataProductsPage";
import { DataProductDetailPanel } from "../data-products/DataProductDetailPanel";

async function opened() {
  render(<DataProductsPage />, { initialEntries: ["/data-products?product=pet_health"] });
  // The DAG is the tallest panel on the page: it starts closed.
  fireEvent.click(await screen.findByTestId("data-product-detail-lineage-toggle-pet_health"));
  await waitFor(() => expect(drawn.length).toBeGreaterThan(0));
  return drawn[drawn.length - 1];
}

describe("product detail lineage scope (REQ-1666)", () => {
  it("keeps members, ancestry and one hop downstream; drops checker tables; every member column is an output", async () => {
    const { graph: g } = await opened();
    const relations = new Set(g.nodes.map((n) => n.relation));
    expect(relations).toEqual(
      new Set([
        "native:pet_store.pets",
        "pet_store.breeds",
        "staging.pet_prices",
        "pet_store.pets",
        "pet_store.dim_pet",
      ]),
    );
    // price was never in the federation graph; it is still a column of the published table.
    expect(g.nodes.map((n) => n.id)).toContain("pet_store.pets.price");
    expect(new Set(g.outputs)).toEqual(
      new Set(["pet_store.pets.id", "pet_store.pets.name", "pet_store.pets.price"]),
    );
    expect(g.edges.map((e) => e.target)).not.toContain("pet_store.pets_quality.scan_id");
    expect(g.edges.map((e) => e.target)).not.toContain("pet_store.fact_pet.pet_id");
  });

  it("builds its swimlanes around the member tables, the checker's left out", async () => {
    const { members } = await opened();
    expect(members).toEqual(new Set(["pet_store.pets"]));
  });

  it("heads each lane in words and names each table as the catalog does", async () => {
    const { lanes, relationLabel } = await opened();
    expect([-2, -1, 0, 1].map((l) => lanes!.title(l))).toEqual([
      "Upstream, hop 2",
      "Input Ports",
      "Output Ports",
      "Downstream, hop 1",
    ]);
    expect(relationLabel!("pet_store.pets")).toBe("pet-store.pets");
  });
});

describe("the product's Input Ports (REQ-1660)", () => {
  it("are one row per upstream table, named as the catalog does, its columns behind an expand", async () => {
    render(<DataProductsPage />, { initialEntries: ["/data-products?product=pet_health"] });
    const ports = await screen.findByTestId("data-product-detail-input-ports-pet_health");
    // pets' native read, breeds and an unregistered staging relation: three datasets, not one row
    // per column. The unregistered one (REQ-1626) is named by its relation.
    expect(ports.children).toHaveLength(3);
    expect(within(ports).getByText("pet-store.pets")).toBeInTheDocument();
    expect(within(ports).getByText("pet-store.breeds")).toBeInTheDocument();
    expect(within(ports).getByText("staging.pet_prices")).toBeInTheDocument();
    const toggle = screen.getByTestId("input-port-toggle-pet_store.pets");
    expect(toggle).toHaveAttribute("aria-expanded", "false");
    expect(screen.queryByTestId("input-port-columns-pet_store.pets")).toBeNull();
    fireEvent.click(toggle);
    expect(toggle).toHaveAttribute("aria-expanded", "true");
    expect(screen.getByTestId("input-port-columns-pet_store.pets")).toHaveTextContent("pet_id");
    fireEvent.click(toggle);
    expect(screen.queryByTestId("input-port-columns-pet_store.pets")).toBeNull();
    expect(document.body.textContent).not.toMatch(/native:/);
  });
});

describe("the product detail's lineage DAG (REQ-1640)", () => {
  const panel = (canSeeLineage: boolean) =>
    render(
      <DataProductDetailPanel
        p={product}
        core={CORE}
        coreError={null}
        reader={null}
        canEdit={false}
        canSeeLineage={canSeeLineage}
        lineageLoading={false}
        lineageError=""
        lineageGraph={FEDERATION}
        inputPorts={[]}
        onEdit={vi.fn()}
        onDelete={vi.fn()}
        {...lifecycleProps()}
      />,
    );

  it("is drawn below the cards, not as one of them", async () => {
    panel(true);
    fireEvent.click(screen.getByTestId("data-product-detail-lineage-toggle-pet_health"));
    expect(await screen.findByTestId("data-product-detail-lineage-pet_health")).toBeInTheDocument();
    expect(screen.queryByTestId("data-product-card-lineage-pet_health")).toBeNull();
  });

  it("is not drawn for a role without view_governance", () => {
    panel(false);
    expect(screen.queryByTestId("data-product-detail-lineage-toggle-pet_health")).toBeNull();
    expect(screen.queryByTestId("data-product-detail-lineage-pet_health")).toBeNull();
  });
});
