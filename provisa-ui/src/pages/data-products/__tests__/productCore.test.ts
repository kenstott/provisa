// Copyright (c) 2026 Kenneth Stott
// Canary: 6c1e8b37-2f94-4d05-a8e1-7b3d9f2c4a68
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2127: the panel's core is read from the meta domain as the user: the nested GraphQL read,
// the product's row by SQL and by JSON:API (the three queries the panel shows), and the glossary
// terms' closure. What the role's schema does not hold is not selected and its section is absent.

import { afterEach, describe, expect, it, vi } from "vitest";
import {
  RoleSchema,
  rootKey,
  type GraphqlType,
  type Introspection,
  type MetaReader,
} from "../../../api/metaGraph";
import { loadProductCore, panelQueries } from "../productCore";

const s: GraphqlType = { kind: "SCALAR", name: "String" };
const list = (name: string): GraphqlType => ({
  kind: "LIST",
  name: null,
  ofType: { kind: "OBJECT", name },
});
const one = (name: string): GraphqlType => ({ kind: "OBJECT", name });
const type = (name: string, fields: Record<string, GraphqlType>) => ({
  name,
  fields: Object.entries(fields).map(([n, t]) => ({ name: n, type: t })),
});

function reader(): MetaReader {
  const raw: Introspection = {
    __schema: {
      queryType: { name: "Query" },
      types: [
        type("Query", {
          m__dataProducts: list("DP"),
          m__tableColumns: list("TC"),
          ont__termEdges: list("TE"),
          ont__terms: list("GT"),
        }),
        type("DP", {
          id: s,
          memberTables: list("RT"),
          commands: list("TF"),
          // No tags and no edges: this role reads neither.
        }),
        type("RT", {
          id: s,
          sourceId: s,
          domainId: s,
          schemaName: s,
          tableName: s,
          description: s,
          dqContract: s,
          tableColumns: list("TC"),
          checks: one("RT"),
        }),
        type("TC", { columnName: s, glossaryRef: list("GR") }),
        type("GR", { termId: s, term: one("GT") }),
        type("GT", { id: s, name: s, definition: s, isAbstract: s, live: s }),
        type("TE", { fromTermId: s, toTermId: s }),
        type("TF", { name: s, description: s, domainId: s }),
      ],
    },
  };
  return {
    schema: new RoleSchema(raw),
    roots: new Map([
      [rootKey("meta", "data_products"), "m__dataProducts"],
      [rootKey("meta", "table_columns"), "m__tableColumns"],
      [rootKey("ontology", "_term_edges"), "ont__termEdges"],
      [rootKey("ontology", "_terms"), "ont__terms"],
    ]),
  };
}

const PRODUCT_ROW = {
  id: "c360",
  domain_id: "sales",
  name: "Customer 360",
  owner_role: "steward",
  team_role: null,
  purpose: "One customer",
  limitations: "",
  usage: "",
  version: "1",
  status: "live",
  status_reason: null,
  retire_on: null,
  successor_id: null,
  support: null,
  support_contact: null,
  custom_properties: '{"tier":"gold"}',
};

const CUSTOMER = { id: 7, name: "Customer", definition: "A buyer", is_abstract: false, live: true };
const PARTY = { id: 8, name: "Party", definition: "Anyone", is_abstract: true, live: true };

afterEach(() => vi.unstubAllGlobals());

describe("the panel's meta reads", () => {
  it("are the nested GraphQL read and the product's row by SQL and JSON:API", () => {
    const q = panelQueries(reader(), "c360");
    expect(q?.graphql.replace(/\s+/g, " ")).toContain(
      'data_products: m__dataProducts(where: { id: { eq: "c360" } })',
    );
    expect(q?.graphql).toContain("member_tables: memberTables");
    // Built a selection a line, as the panel shows it.
    expect(q?.graphql.split("\n").slice(0, 4)).toEqual([
      "query DataProductPanel {",
      '  data_products: m__dataProducts(where: { id: { eq: "c360" } }) {',
      "    id: id",
      "    member_tables: memberTables {",
    ]);
    expect(q?.graphql).not.toContain("tags");
    expect(q?.graphql).not.toContain("edges");
    expect(q?.sql).toBe(
      'SELECT "id", "domain_id", "name", "owner_role", "team_role", "purpose", "limitations", "usage", "version", "status", "status_reason", "retire_on", "successor_id", "support", "support_contact", "custom_properties" FROM "meta"."data_products" WHERE "id" = \'c360\'',
    );
    expect(q?.jsonapi).toBe("/data/jsonapi/meta/data_products?filter[id]=c360");
  });

  it("are all sent as the user, and fill the panel; what the role cannot read is absent", async () => {
    const seen: { url: string; role: string | null; body: string }[] = [];
    vi.stubGlobal(
      "fetch",
      vi.fn(async (url: string, init?: RequestInit) => {
        const role = new Headers(init?.headers).get("X-Provisa-Role");
        const body = String(init?.body ?? "");
        seen.push({ url, role, body });
        const json = (data: unknown) => new Response(JSON.stringify(data), { status: 200 });
        if (url.endsWith("/data/sql")) {
          return json({ columns: Object.keys(PRODUCT_ROW), data: { sql: [PRODUCT_ROW] } });
        }
        if (url.startsWith("/data/jsonapi/")) {
          return json({
            data: [{ attributes: { ...PRODUCT_ROW, custom_properties: { tier: "gold" } } }],
          });
        }
        if (body.includes("DataProductPanel")) {
          return json({
            data: {
              data_products: [
                {
                  id: "c360",
                  member_tables: [
                    {
                      id: 1,
                      source_id: "pg",
                      domain_id: "sales",
                      schema_name: "public",
                      table_name: "customers",
                      description: null,
                      dq_contract: null,
                      table_columns: [
                        { column_name: "email", glossary_ref: [{ term_id: 7, term: CUSTOMER }] },
                      ],
                    },
                  ],
                  commands: [{ name: "refresh", description: null, domain_id: "sales" }],
                },
              ],
            },
          });
        }
        if (body.includes("TermEdges")) {
          const from7 = body.includes("[7]");
          return json({ data: { _term_edges: from7 ? [{ to_term_id: 8 }] : [] } });
        }
        if (body.includes("query Terms")) return json({ data: { _terms: [PARTY] } });
        throw new Error(`unexpected request ${url} ${body}`);
      }),
    );
    const core = await loadProductCore(reader(), "analyst", "c360");
    expect(seen.every((r) => r.role === "analyst")).toBe(true);
    expect(core?.product).toMatchObject({
      name: "Customer 360",
      ownerRole: "steward",
      customProperties: { tier: "gold" },
    });
    expect(core?.tables?.map((t) => t.table_name)).toEqual(["customers"]);
    // REQ-2157: the product's checks are its lineage's (the product lineage endpoint), not the panel's
    // meta read.
    expect(core).not.toHaveProperty("checks");
    expect(core?.commands?.map((c) => c.name)).toEqual(["refresh"]);
    // The bound term and the abstract term its edge reaches.
    expect(core?.terms?.map((t) => t.name)).toEqual(["Customer", "Party"]);
    // Not in this role's schema: no section.
    expect(core?.tags).toBeUndefined();
    expect(core?.edges).toBeUndefined();
  });
});
