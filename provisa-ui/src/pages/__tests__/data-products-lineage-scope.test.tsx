// Copyright (c) 2026 Kenneth Stott
// Canary: 8b2d6f14-9c37-4a50-b1e8-3f6a9d2c7e05
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1640, REQ-1666, REQ-2156: the product detail's lineage is the LineageDag the Lineage page
// draws, over what the product lineage endpoint answers -- the member tables, their ancestry and one
// hop downstream, cut to the reader's rights (the server's, held by its own tests) -- full width
// below the cards, for any reader of the product: no governance right is asked.

import { describe, it, expect, vi } from "vitest";
import { fireEvent, render, screen, waitFor, within } from "../../test-utils/render";
import type {
  LineageGraphData,
  LineageNode,
  ProductInput,
  ProductLineage,
} from "../../api/lineage";
import type { DataProduct } from "../../types/admin";
import type { ProductCore } from "../data-products/productCore";

vi.mock("../../context/AuthContext", () => ({
  useAuth: () => ({ role: { id: "admin", capabilities: [] }, selectedRoles: [{ id: "admin" }] }),
}));
// Every right but view_governance: the product's lineage asks none.
vi.mock("../../hooks/useCapability", () => ({
  useCapability: (cap: string) => cap !== "view_governance",
}));
vi.mock("../../api/actions", () => ({
  fetchActions: () => Promise.resolve({ functions: [] }),
  saveFunction: vi.fn(),
}));
vi.mock("../../components/graph3d/GraphPanel", () => ({
  GraphPanel: () => null,
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

// What the endpoint answers for pet_health: breeds and staging → pets (its member) → dim_pet; pets is
// read natively from its source (the "native:" node relation is the lineage's internal key).
const LINEAGE: ProductLineage = {
  nodes: [
    col("native:pet_store.pets", "pet_id", "native"),
    col("pet_store.breeds", "name"),
    col("staging.pet_prices", "price"),
    col("pet_store.pets", "name"),
    col("pet_store.pets", "id"),
    col("pet_store.dim_pet", "id", "derived"),
  ],
  edges: [
    edge("native:pet_store.pets.pet_id", "pet_store.pets.id"),
    edge("pet_store.breeds.name", "pet_store.pets.name"),
    edge("staging.pet_prices.price", "pet_store.pets.name"),
    edge("pet_store.pets.id", "pet_store.dim_pet.id"),
  ],
  outputs: ["pet_store.pets.id", "pet_store.pets.name"],
  members: ["pet_store.pets"],
  consumers: [],
  inputs: [],
  dq: { expectations: [], promises: [], related: [], unattached: [] },
};
const asked: string[] = [];
vi.mock("../../api/lineage", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../api/lineage")>()),
  fetchProductLineage: (id: string) => {
    asked.push(id);
    return Promise.resolve(LINEAGE);
  },
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

describe("product detail lineage scope (REQ-1666, REQ-2156)", () => {
  it("draws what the product lineage endpoint answers for the expanded product", async () => {
    const { graph } = await opened();
    expect(asked).toContain("pet_health");
    expect(graph).toBe(LINEAGE);
  });

  it("builds its swimlanes around the members the endpoint names", async () => {
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

const input = (relation: string, over: Partial<ProductInput> = {}): ProductInput => ({
  relation,
  kind: "source_table",
  table_id: 1,
  domain_id: "pet-store",
  table_name: relation.split(".")[1],
  alias: null,
  product_id: null,
  source_id: null,
  source_type: null,
  source_schema: null,
  source_table: null,
  ...over,
});

const panel = (lineage: ProductLineage | null) =>
  render(
    <DataProductDetailPanel
      p={product}
      core={CORE}
      coreError={null}
      reader={null}
      canEdit={false}
      owner
      lineageLoading={false}
      lineageError=""
      lineageGraph={lineage}
      onEdit={vi.fn()}
      onDelete={vi.fn()}
    />,
  );

describe("the product's Input Ports, its nearest physical inputs (REQ-2126)", () => {
  const listed = (inputs: ProductInput[]) => {
    panel({ ...LINEAGE, inputs });
    return within(screen.getByTestId("data-product-detail-inputs-pet_health"))
      .getAllByRole("listitem")
      .map((li) => li.textContent);
  };

  it("lists the inputs the endpoint found, a registered one by its model name", () => {
    expect(
      listed([input("pet_store.breeds"), input("pet_store.dim_pet", { alias: "pets_dimension" })]),
    ).toEqual(["pet-store.breeds", "pet-store.pets_dimension"]);
  });

  it("names a member read straight from a source by that source, each row its own", () => {
    const source = (table: number, name: string) =>
      input(`native:pet_store.${name}`, {
        kind: "source",
        table_id: table,
        source_id: "pg",
        source_type: "sqlite",
        source_schema: "sales",
        source_table: name,
      });
    const logged = vi.spyOn(console, "error").mockImplementation(() => {});
    expect(listed([source(1, "orders"), source(2, "customers")])).toEqual([
      "pg/sales/orders (sqlite)",
      "pg/sales/customers (sqlite)",
    ]);
    expect(logged.mock.calls.some((c) => String(c[0]).includes("same key"))).toBe(false);
    logged.mockRestore();
  });

  it("are absent until the product's lineage is read", () => {
    panel(null);
    expect(screen.queryByTestId("data-product-card-inputPorts-pet_health")).toBeNull();
  });
});

describe("the product detail's lineage DAG (REQ-1640)", () => {
  it("is drawn below the cards, not as one of them", async () => {
    panel(LINEAGE);
    fireEvent.click(screen.getByTestId("data-product-detail-lineage-toggle-pet_health"));
    expect(await screen.findByTestId("data-product-detail-lineage-pet_health")).toBeInTheDocument();
    expect(screen.queryByTestId("data-product-card-lineage-pet_health")).toBeNull();
  });
});
