// Copyright (c) 2026 Kenneth Stott
// Canary: 3d9b7f40-6a21-4e8c-95d3-b17e2c0a8f59
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2252: a relationship links to itself on every surface that reads it, in its detail panel and
// in a data product's Access section, by the one shared component: each link opens that surface's
// Explore editor with the relationship's query, run at once.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import type { ReactElement } from "react";
import { Route, Routes, useLocation } from "react-router-dom";
import { fireEvent, render, screen } from "../../../test-utils/render";
import type { ListedTable, Relationship } from "../../../types/admin";
import type { DataProduct } from "../../../types/admin";
import type { ProductCore } from "../../../pages/data-products/productCore";
import { AccessSection } from "../../../pages/data-products/AccessSection";
import { RelationshipDetail } from "../RelationshipDetail";
// The panel links the tables it names (REQ-2259), gated on the viewer's rights.
vi.mock("../../../context/AuthContext", () => ({
  useAuth: () => ({ capabilities: ["table_registration"] }),
}));
import { metaReader } from "../../../api/metaGraph";
import {
  GRAPH,
  TABLES,
  VOCABULARY,
  placedBy,
  reader,
  tagged,
  toDimension,
} from "./relationshipQueryFixtures";

vi.mock("../../../api/metaGraph", async (importOriginal) => ({
  ...(await importOriginal<typeof import("../../../api/metaGraph")>()),
  metaReader: vi.fn(),
}));

const LISTED = TABLES.map((tb) => ({ ...tb, columns: [] })) as unknown as ListedTable[];

/** The page a link opened, and the hand-off it carried. */
function Opened() {
  const location = useLocation();
  return (
    <pre data-testid="opened">
      {JSON.stringify({ to: location.pathname, state: location.state })}
    </pre>
  );
}

function routed(ui: ReactElement) {
  return render(
    <Routes>
      <Route path="/" element={ui} />
      <Route path="*" element={<Opened />} />
    </Routes>,
  );
}

const opened = () => JSON.parse(screen.getByTestId("opened").textContent ?? "null");

let role = 0;
beforeEach(() => {
  vi.stubGlobal(
    "fetch",
    vi.fn((url: string) =>
      Promise.resolve(
        new Response(JSON.stringify(url === "/data/graph-schema" ? GRAPH : VOCABULARY), {
          status: 200,
        }),
      ),
    ),
  );
  vi.mocked(metaReader).mockImplementation(() => Promise.resolve(reader()));
  // A fresh role a test: the surfaces are read once per role for the page's life.
  role += 1;
});
afterEach(() => vi.unstubAllGlobals());

function detail(
  rel = placedBy(),
  tables: ListedTable[] = LISTED,
  refetchTables: () => Promise<unknown> = () => Promise.resolve(),
) {
  return (
    <RelationshipDetail
      rel={rel}
      editingRel={null}
      setEditingRel={vi.fn()}
      canManage={false}
      onStartEdit={vi.fn()}
      onDuplicate={vi.fn()}
      onDelete={vi.fn()}
      onEditSave={vi.fn()}
      saving={null}
      problem=""
      tables={tables}
      refetchTables={refetchTables}
      functions={[]}
      tableDomainById={{}}
      domainsEnabled={false}
      actingRole={`analyst-${role}`}
    />
  );
}

const SURFACE_NAMES: [string, string][] = [
  ["cypher", "Cypher"],
  ["sql", "SQL"],
  ["sparql", "SPARQL"],
  ["graphql", "GraphQL"],
  ["rest", "REST"],
  ["grpc", "gRPC"],
  ["jsonapi", "JSON:API"],
];

describe("the relationship detail panel", () => {
  it("reads the tables again when one of the relationship's is not listed, then shows its links", async () => {
    const refetch = vi.fn(() => Promise.resolve());
    const view = routed(detail(placedBy(), [], refetch));
    await vi.waitFor(() => expect(refetch).toHaveBeenCalledTimes(1));
    expect(screen.queryByTestId("relationship-detail-query-sql")).toBeNull();
    view.rerender(
      <Routes>
        <Route path="/" element={detail(placedBy(), LISTED, refetch)} />
      </Routes>,
    );
    expect(await screen.findByTestId("relationship-detail-query-sql")).toBeInTheDocument();
    expect(refetch).toHaveBeenCalledTimes(1);
  });

  it("says why when the table is still missing after the read", async () => {
    routed(detail(placedBy(), [], () => Promise.resolve()));
    expect(
      await screen.findByTestId("relationship-detail-query-links-unavailable"),
    ).toHaveTextContent("not among the registered tables");
  });

  it("says why when the tables cannot be read", async () => {
    routed(detail(placedBy(), [], () => Promise.reject(new Error("boom"))));
    expect(
      await screen.findByTestId("relationship-detail-query-links-unavailable"),
    ).toHaveTextContent("boom");
  });

  it("links the relationship on every surface, each named by its surface", async () => {
    routed(detail());
    for (const [surface, text] of SURFACE_NAMES) {
      const link = await screen.findByTestId(`relationship-detail-query-${surface}`);
      expect(link).toHaveTextContent(text);
      expect(link).toHaveAccessibleName(`Read placed-by in ${text}`);
    }
    expect(screen.getByTestId("relationship-detail-query-links")).toHaveTextContent("Query it");
  });

  it("links a discovery-made shared dimension on Cypher and SPARQL too", async () => {
    const rel = toDimension();
    routed(detail(rel));
    for (const [surface] of SURFACE_NAMES)
      expect(await screen.findByTestId(`relationship-detail-query-${surface}`)).toBeInTheDocument();
    fireEvent.click(screen.getByTestId("relationship-detail-query-cypher"));
    expect(opened().state.query).toBe(
      "MATCH (a:Shelter:BuildingPermits)-[r:IS_PERMIT_NEIGHBORHOOD_DIMENSION]->" +
        "(b:Shelter:PermitNeighborhood) RETURN a, r, b LIMIT 25",
    );
  });

  it("opens the REST explorer with the relationship in ?fields=, run at once", async () => {
    routed(detail());
    fireEvent.click(await screen.findByTestId("relationship-detail-query-rest"));
    expect(opened()).toEqual({
      to: "/openapi",
      state: {
        openApiUrl: "GET /data/rest/sales/orders?fields=id,customerId,client&limit=25",
        autoRun: true,
      },
    });
  });

  it("opens the gRPC explorer with the related table's field in the read_mask, run at once", async () => {
    routed(detail());
    fireEvent.click(await screen.findByTestId("relationship-detail-query-grpc"));
    expect(opened()).toEqual({
      to: "/grpc",
      state: {
        grpcMethod: "QuerySalesOrders()",
        grpcBody: { read_mask: { paths: ["customer_id", "client.id"] }, limit: 25 },
        autoRun: true,
      },
    });
  });

  it("opens the SQL editor with the join, run at once", async () => {
    routed(detail());
    fireEvent.click(await screen.findByTestId("relationship-detail-query-sql"));
    expect(opened()).toEqual({
      to: "/sql",
      state: {
        sql: 'select * from "sales"."orders" as s join "sales"."clients" as t on s."customer_id" = t."id" limit 25',
        autoRun: true,
      },
    });
  });

  it("opens the Cypher editor with the type between its labels", async () => {
    routed(detail());
    fireEvent.click(await screen.findByTestId("relationship-detail-query-cypher"));
    expect(opened()).toEqual({
      to: "/graph",
      state: {
        query: "MATCH (a:Orders)-[r:PLACED_BY]->(b:Clients) RETURN a, r, b LIMIT 25",
      },
    });
  });

  it("shows no Cypher or SPARQL link for a relationship excluded from Cypher", async () => {
    routed(detail(placedBy({ disableCypher: true })));
    expect(await screen.findByTestId("relationship-detail-query-graphql")).toBeInTheDocument();
    expect(screen.queryByTestId("relationship-detail-query-cypher")).toBeNull();
    expect(screen.queryByTestId("relationship-detail-query-sparql")).toBeNull();
  });

  it("shows no links for a relationship reaching a function", () => {
    routed(detail(placedBy({ targetTableId: null, targetColumn: null, targetFunctionName: "f" })));
    expect(screen.queryByTestId("relationship-detail-query-links")).toBeNull();
  });
});

const PRODUCT = { id: "p1", name: "Orders" } as unknown as DataProduct;

function core(): ProductCore {
  const member = (tb: (typeof TABLES)[number]) => ({
    ...tb,
    sourceId: "pg",
    schemaName: "public",
    description: null,
    dqContract: null,
  });
  // Each relationship of a member as the meta read serves it (productCore's sourceRelationships).
  const metaRel = (rel: Relationship, target: (typeof TABLES)[number]) => ({
    id: String(rel.id),
    alias: rel.alias,
    graphqlField: rel.graphqlAlias,
    cypherType: rel.cypherType as string,
    cardinality: rel.cardinality,
    sourceColumn: rel.sourceColumn,
    targetColumn: rel.targetColumn,
    disableCypher: rel.disableCypher,
    targetTable: { id: target.id, domainId: target.domainId, tableName: target.tableName },
  });
  return {
    product: PRODUCT,
    // orders and customers are the product's; tags is not.
    tables: [
      {
        ...member(TABLES[0]),
        sourceRelationships: [metaRel(placedBy(), TABLES[1]), metaRel(tagged(), TABLES[3])],
      },
      member(TABLES[1]),
    ],
    queries: { graphql: "", sql: "", jsonapi: "" },
  };
}

describe("the data product Access section", () => {
  const access = () => (
    <AccessSection
      core={core()}
      reader={reader()}
      relationships={[placedBy(), tagged()]}
      tables={LISTED}
      actingRole={`analyst-${role}`}
    />
  );

  it("lists each relationship among its tables with the same links as the detail panel", async () => {
    routed(access());
    for (const [surface, text] of SURFACE_NAMES)
      expect(
        await screen.findByTestId(`data-product-detail-relationship-placed-by-query-${surface}`),
      ).toHaveAccessibleName(`Read placed-by in ${text}`);
    // tags is no member: orders -> tags is not the product's.
    expect(screen.queryByTestId("data-product-detail-relationship-tagged-query-links")).toBeNull();
    // The Cypher-only link per relationship type is replaced.
    expect(screen.queryByTestId("data-product-detail-cypher-query-PLACED_BY")).toBeNull();
  });

  it("lists the relationships the meta read serves, linked once the model's are read", async () => {
    routed(
      <AccessSection
        core={core()}
        reader={reader()}
        relationships={[]}
        tables={LISTED}
        actingRole={`analyst-${role}`}
      />,
    );
    const list = screen.getByTestId("data-product-detail-graphql-queries-p1");
    expect(list).toHaveTextContent("placed-by");
    expect(list).not.toHaveTextContent("tagged");
    expect(
      screen.queryByTestId("data-product-detail-relationship-placed-by-query-links"),
    ).toBeNull();
  });

  it("opens the GraphQL editor with the relationship's field nested, run at once", async () => {
    routed(access());
    fireEvent.click(
      await screen.findByTestId("data-product-detail-relationship-placed-by-query-graphql"),
    );
    expect(opened()).toEqual({
      to: "/query",
      state: {
        query: "{ sales__orders(limit: 25) { id customerId client { id name } } }",
        autoRun: true,
      },
    });
  });

  it("opens the JSON:API editor with the relationship included, run at once", async () => {
    routed(access());
    fireEvent.click(
      await screen.findByTestId("data-product-detail-relationship-placed-by-query-jsonapi"),
    );
    expect(opened()).toEqual({
      to: "/jsonapi",
      state: {
        jsonapiUrl: "/data/jsonapi/sales/orders?include=client&page%5Bsize%5D=25",
        autoRun: true,
      },
    });
  });

  it("opens the SPARQL editor with the relationship's predicate, run at once", async () => {
    routed(access());
    fireEvent.click(
      await screen.findByTestId("data-product-detail-relationship-placed-by-query-sparql"),
    );
    const { to, state } = opened();
    expect(to).toBe("/sparql");
    expect(state).not.toHaveProperty("autoRun");
    expect(state.query).toContain("?s a sales:orders . ?s rel:PLACED_BY ?o . ?o a sales:clients .");
  });
});
