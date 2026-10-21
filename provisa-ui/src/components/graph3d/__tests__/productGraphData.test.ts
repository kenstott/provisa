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
import { productsQuery, toGraphData, type ProductRow } from "../productGraphData";

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
          profileResults: list("M__RegisteredTables"),
          profiledTable: one("M__RegisteredTables"),
        }),
        type("M__TableColumns", { columnName: scalar, columnKey: scalar }),
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
    expect(q).not.toContain("tags");
    // Nor what its schema does not have yet (the checked table, the relationships).
    expect(q).not.toContain("checks");
    expect(q).not.toContain("source_relationships");
    expect(q).not.toContain("where");
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
            id: 1,
            target_table: { id: 2, domain_id: "sales", table_name: "orders", product_id: "c360" },
          },
          {
            id: 2,
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

  it("judges nothing by sensitivity when the role cannot read the sensitive columns", () => {
    const g = toGraphData(ROWS, null, null);
    expect(g.nodes.find((n) => n.id === "t:1")?.sensitivities).toBeUndefined();
    expect(g.edges.some((e) => e.kind === "term_edge")).toBe(false);
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
          direction: null,
          target_product_id: "p2",
          target_table_id: null,
          target_relation: null,
          hops: 1,
          path_count: 2,
          aliases: "BOUGHT, PAID",
          target_product: { id: "p2", name: "P2", domain_id: "finance" },
        },
        {
          kind: "lineage",
          direction: "upstream",
          target_product_id: null,
          target_table_id: null,
          target_relation: "raw.d",
          hops: 2,
          path_count: 1,
          aliases: null,
        },
        {
          kind: "lineage",
          direction: "downstream",
          target_product_id: null,
          target_table_id: null,
          target_relation: "mart.e",
          hops: 1,
          path_count: 1,
          aliases: null,
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

  it("stands lineage in lanes: ancestry by its distance upstream, a consumer one lane down", () => {
    const g = toGraphData(linked, null, null);
    const lane = (id: string) => g.nodes.find((n) => n.id === id)?.lane;
    expect([lane("l:raw.d"), lane("p:p1"), lane("l:mart.e")]).toEqual([-2, 0, 1]);
    expect(g.edges.filter((e) => e.kind === "lineage").map((e) => [e.a, e.b])).toEqual([
      ["l:raw.d", "p:p1"],
      ["p:p1", "l:mart.e"],
    ]);
  });
});

describe("column lineage (REQ-2129)", () => {
  it("joins each member column to the columns deriving it, named by their relation", () => {
    const rows: ProductRow[] = [
      {
        id: "p1",
        name: "P1",
        domain_id: "sales",
        member_tables: [
          {
            id: 1,
            domain_id: "sales",
            table_name: "a",
            table_columns: [
              {
                column_name: "total",
                column_key: "1:total",
                upstream: [
                  {
                    source_column_key: "9:amount",
                    source_relation: "finance.b",
                    source_column: "amount",
                    via: "sales.a",
                  },
                ],
              },
            ],
          },
        ],
      },
    ];
    const g = toGraphData(rows, null, null);
    expect(g.nodes.find((n) => n.id === "c:9:amount")?.label).toBe("finance.b.amount");
    expect(g.edges.find((e) => e.kind === "column_lineage")).toMatchObject({
      a: "c:9:amount",
      b: "c:1:total",
      label: "sales.a",
    });
  });
});
