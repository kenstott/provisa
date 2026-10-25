// Copyright (c) 2026 Kenneth Stott
// Canary: 3f8a1d62-9c47-4e05-b8d3-6a2e7c1f9b40
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2128: the product graph's meta-domain queries are built from what the role's schema holds,
// with root fields as /data/graph-schema names them; the answer is turned into the graph.

import { describe, expect, it } from "vitest";
import {
  RoleSchema,
  rootKey,
  type GraphqlType,
  type Introspection,
  type MetaReader,
} from "../../../api/metaGraph";
import {
  NO_PRODUCT_FILTERS,
  productGraph,
  productGraphQuery,
  toGraphData,
  type ProductFilters,
  type ProductRow,
} from "../productGraphData";
import { PRODUCT_SCOPE, visibleGraph, NO_FILTERS, type LayerId } from "../graphModel";

const productsQuery = (
  m: MetaReader,
  product: string | null,
  f: ProductFilters = NO_PRODUCT_FILTERS,
) => productGraphQuery(m, product, f);

const scalar: GraphqlType = { kind: "SCALAR", name: "String" };
const list = (name: string): GraphqlType => ({
  kind: "LIST",
  name: null,
  ofType: { kind: "NON_NULL", name: null, ofType: { kind: "OBJECT", name } },
});
const one = (name: string): GraphqlType => ({ kind: "OBJECT", name });
const type = (name: string, fields: Record<string, GraphqlType>) => ({
  name,
  fields: Object.entries(fields).map(([n, t]) => ({ name: n, type: t })),
});

/** A role's schema as the compiler names it: domain alias "m", camelCase columns. */
function reader(opts: { tags: boolean }): MetaReader {
  const raw: Introspection = {
    __schema: {
      queryType: { name: "Query" },
      types: [
        type("Query", {
          m__dataProducts: list("M__DataProducts"),
          m__registeredTables: list("M__RegisteredTables"),
          m__tableColumns: list("M__TableColumns"),
        }),
        type("M__DataProducts", {
          id: scalar,
          name: scalar,
          domainId: scalar,
          memberTables: list("M__RegisteredTables"),
          commands: list("M__TrackedFunctions"),
          ...(opts.tags ? { tags: list("M__TagAssignments") } : {}),
        }),
        type("M__RegisteredTables", {
          id: scalar,
          domainId: scalar,
          tableName: scalar,
          dqContract: scalar,
          productId: scalar,
          tableColumns: list("M__TableColumns"),
          ...(opts.tags ? { tags: list("M__TagAssignments") } : {}),
          profileResults: list("M__RegisteredTables"),
          profiledTable: one("M__RegisteredTables"),
        }),
        type("M__TableColumns", {
          columnName: scalar,
          columnKey: scalar,
          glossaryRef: list("M__GlossaryTermRefs"),
          sensitivity: list("M__SensitiveCoverage"),
          ...(opts.tags ? { tags: list("M__TagAssignments") } : {}),
        }),
        type("M__GlossaryTermRefs", { termId: scalar, term: one("M__GlossaryTerms") }),
        type("M__GlossaryTerms", {
          id: scalar,
          name: scalar,
          edgesOut: list("M__GlossaryTermEdges"),
          edgesIn: list("M__GlossaryTermEdges"),
        }),
        type("M__GlossaryTermEdges", {
          id: scalar,
          fromTermId: scalar,
          toTermId: scalar,
          relType: scalar,
          sourceTerm: one("M__GlossaryTerms"),
          targetTerm: one("M__GlossaryTerms"),
        }),
        type("M__SensitiveCoverage", { tags: scalar }),
        type("M__TrackedFunctions", { name: scalar, domainId: scalar }),
        type("M__TagAssignments", { tagId: scalar }),
      ],
    },
  };
  return {
    schema: new RoleSchema(raw),
    roots: new Map([
      [rootKey("meta", "data_products"), "m__dataProducts"],
      [rootKey("meta", "registered_tables"), "m__registeredTables"],
      [rootKey("meta", "table_columns"), "m__tableColumns"],
    ]),
  };
}

const squash = (q: string | null) => (q ?? "").replace(/\s+/g, " ");

describe("the product graph's query", () => {
  it("reads each view by the root field the server names, each column aliased to its own name", () => {
    const q = squash(productsQuery(reader({ tags: true }), "c360"));
    expect(q).toContain('data_products: m__dataProducts(where: { id: { eq: "c360" } })');
    expect(q).toContain("domain_id: domainId");
    expect(q).toContain("member_tables: memberTables {");
    expect(q).toContain("dq_contract: dqContract");
    // Declared without an alias: found by the view it reaches.
    expect(q).toContain("table_columns: tableColumns { column_name: columnName");
    expect(q).toContain("tags: tags { tag_id: tagId }");
  });

  it("selects nothing the role cannot read", () => {
    const q = squash(productsQuery(reader({ tags: false }), null));
    expect(q).not.toContain("tag_id");
    // Nor what its schema does not have yet (the checked table, the relationships).
    expect(q).not.toContain("checks");
    expect(q).not.toContain("source_relationships");
    expect(q).not.toContain("where");
  });

  it("is one query: the bound terms, without their edges, and each column's sensitivity in it", () => {
    const q = squash(productsQuery(reader({ tags: true }), "c360"));
    expect(q.startsWith("query DataProductGraph {")).toBe(true);
    expect(q.match(/m__/g)).toHaveLength(1);
    // A product's neighbourhood ends at the terms its columns are bound to.
    expect(q).toContain("term: term { id: id name: name }");
    expect(q).not.toContain("edgesOut");
    expect(q).not.toContain("edgesIn");
    expect(q).toContain("sensitivity: sensitivity { tags: tags }");
  });

  it("keeps the chosen domains, tags and sensitivities in its where", () => {
    const q = squash(
      productsQuery(reader({ tags: true }), null, {
        domains: ["sales"],
        tags: ["gold"],
        sensitivities: ["pii"],
        edgeTypes: ["KIND_OF"],
      }),
    );
    const pii =
      '{ _or: [{ tags: { eq: "pii" } }, { tags: { like: "pii, %" } }, { tags: { like: "%, pii" } }, { tags: { like: "%, pii, %" } }] }';
    // The products: in a domain kept, tagged with a tag kept, holding a column sensitive to one.
    expect(q).toContain(
      `m__dataProducts(where: { _and: [{ domainId: { in: ["sales"] } }, { tags: { tagId: { in: ["gold"] } } }, { memberTables: { tableColumns: { sensitivity: ${pii} } } }] })`,
    );
    expect(q).toContain(
      `memberTables(where: { _and: [{ domainId: { in: ["sales"] } }, { tags: { tagId: { in: ["gold"] } } }, { tableColumns: { sensitivity: ${pii} } }] })`,
    );
    expect(q).toContain(
      `tableColumns(where: { _and: [{ tags: { tagId: { in: ["gold"] } } }, { sensitivity: ${pii} }] })`,
    );
    // Only the tags kept are drawn.
    expect(q).toContain('tags: tags(where: { tagId: { in: ["gold"] } }) { tag_id: tagId }');
    expect(q).toContain('commands: commands(where: { domainId: { in: ["sales"] } })');
    // No term edge is read, so an edge type narrows nothing in it.
    expect(q).not.toContain("relType");
  });

  it("is not built when the role cannot read the products view", () => {
    const m = reader({ tags: true });
    m.roots.delete(rootKey("meta", "data_products"));
    expect(productsQuery(m, null)).toBeNull();
  });
});

const ROWS: ProductRow[] = [
  {
    id: "c360",
    name: "customer_360",
    domain_id: "sales",
    tags: [{ tag_id: "gold" }],
    commands: [{ name: "refresh_c360", domain_id: "sales" }],
    member_tables: [
      {
        id: 1,
        domain_id: "sales",
        table_name: "customers",
        dq_contract: null,
        product_id: "c360",
        table_columns: [
          {
            column_name: "email",
            column_key: "1:email",
            glossary_ref: [{ term_id: 7, term: { id: 7, name: "Customer" } }],
          },
        ],
        profile_results: [{ id: 9, table_name: "customers_1_profile_columns" }],
        source_relationships: [
          {
            id: "1",
            target_table: { id: 2, domain_id: "sales", table_name: "orders", product_id: "c360" },
          },
          {
            id: "2",
            target_table: { id: 5, domain_id: "finance", table_name: "invoices", product_id: null },
          },
        ],
      },
      { id: 2, domain_id: "sales", table_name: "orders", dq_contract: null, product_id: "c360" },
      {
        id: 3,
        domain_id: "sales",
        table_name: "orders_dq",
        dq_contract: "dataset: x/y/z",
        product_id: "c360",
        checks: { id: 2 },
      },
    ],
  },
];

describe("the product graph's data", () => {
  it("draws the product, its parts and how they join", () => {
    const g = toGraphData(
      ROWS,
      [
        {
          id: 1,
          from_term_id: 7,
          to_term_id: 8,
          rel_type: "KIND_OF",
          from_term: "Customer",
          to_term: "Party",
        },
      ],
      [{ column_key: "1:email", table_id: 1, tags: "pii, contact" }],
    );
    const byId = new Map(g.nodes.map((n) => [n.id, n]));
    expect(byId.get("p:c360")).toMatchObject({ type: "product", tags: ["gold"] });
    expect(byId.get("t:1")).toMatchObject({ type: "table", sensitivities: ["pii", "contact"] });
    expect(byId.get("c:1:email")).toMatchObject({
      parent: "t:1",
      sensitivities: ["pii", "contact"],
    });
    expect(byId.get("t:2")).toMatchObject({ sensitivities: [] });
    expect(byId.get("k:3")?.type).toBe("checker");
    expect(byId.get("x:5")?.type).toBe("relation");
    const kinds = new Map(g.edges.map((e) => [e.id, e.kind]));
    expect(kinds.get("checks:k:3>t:2")).toBe("checks");
    expect(kinds.get("relationship:t:1>t:2")).toBe("relationship");
    expect(kinds.get("relationship_outside:t:1>x:5")).toBe("relationship_outside");
    expect(kinds.get("term_binding:term:7>c:1:email")).toBe("term_binding");
    expect(g.edges.find((e) => e.kind === "term_edge")?.label).toBe("KIND_OF");
    expect(kinds.get("profiled:r:9>t:1")).toBe("profiled");
  });

  it("reads a one-to-many the server answered null (no rows) as empty", () => {
    const rows = [
      {
        id: "lone",
        name: "lone",
        domain_id: "sales",
        tags: null,
        commands: null,
        edges: null,
        member_tables: [
          {
            id: 6,
            domain_id: "sales",
            table_name: "solo",
            dq_contract: null,
            tags: null,
            table_columns: null,
            profile_results: null,
            source_relationships: null,
          },
        ],
      },
    ] as ProductRow[];
    const g = toGraphData(rows, null, null);
    const byId = new Map(g.nodes.map((n) => [n.id, n]));
    expect(byId.get("p:lone")?.tags).toEqual([]);
    expect(byId.get("t:6")?.tags).toEqual([]);
  });

  it("judges nothing by sensitivity when the role cannot read the sensitive columns", () => {
    const g = toGraphData(ROWS, null, null);
    expect(g.nodes.find((n) => n.id === "t:1")?.sensitivities).toBeUndefined();
    expect(g.edges.some((e) => e.kind === "term_edge")).toBe(false);
  });
});

describe("a product's own graph (REQ-2136)", () => {
  it("is exactly its neighbourhood: the view's focus on its product prunes nothing it read", () => {
    const rows: ProductRow[] = [
      {
        ...ROWS[0],
        member_tables: (ROWS[0].member_tables ?? []).map((t) => ({
          ...t,
          table_columns: (t.table_columns ?? []).map((c) => ({
            ...c,
            sensitivity: [{ tags: "pii" }],
          })),
        })),
        // A linked product is a bare node, in a domain of its own.
        edges: [
          {
            kind: "relationship",
            target_product_id: "billing",
            target_table_id: null,
            hops: 1,
            path_count: 2,
            aliases: "BILLED_TO",
            target_product: { id: "billing", name: "Billing", domain_id: "finance" },
          },
        ],
      },
    ];
    const g = productGraph(rows);
    const layers = new Set(PRODUCT_SCOPE.layerOrder as LayerId[]);
    const ids = (focus: string[]) =>
      visibleGraph(g, layers, { ...NO_FILTERS, focus }, new Set(["t:1"]), PRODUCT_SCOPE)
        .nodes.map((n) => n.id)
        .sort();
    expect(ids(["p:c360"])).toEqual(ids([]));
    // The read stops at the bound terms: the graph holds no term beyond them.
    expect(ids([])).toEqual(expect.arrayContaining(["p:billing", "term:7", "c:1:email"]));
  });
});

describe("the product's links (REQ-2126)", () => {
  const linked: ProductRow[] = [
    {
      id: "p1",
      name: "P1",
      domain_id: "sales",
      edges: [
        {
          kind: "relationship",
          target_product_id: "p2",
          target_table_id: null,
          hops: 1,
          path_count: 2,
          aliases: "BOUGHT, PAID",
          target_product: { id: "p2", name: "P2", domain_id: "finance" },
        },
      ],
    },
  ];

  it("joins a product to another by one edge carrying its path count and aliases", () => {
    const g = toGraphData(linked, null, null);
    expect(g.edges.find((e) => e.kind === "product_link")).toMatchObject({
      a: "p:p1",
      b: "p:p2",
      paths: 2,
      label: "BOUGHT, PAID",
    });
  });

  it("draws no lineage from the product's edges: its lineage is the product lineage endpoint's", () => {
    const g = toGraphData(linked, null, null);
    expect(g.nodes.some((n) => n.id.startsWith("l:"))).toBe(false);
    expect(g.edges.some((e) => e.kind === "lineage")).toBe(false);
  });
});

describe("column lineage (REQ-2129, REQ-2156)", () => {
  it("is read from no meta view: a member column carries no upstream edge here", () => {
    const g = toGraphData(ROWS, null, null);
    expect(g.edges.some((e) => e.kind === "column_lineage")).toBe(false);
  });
});
