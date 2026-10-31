// Copyright (c) 2026 Kenneth Stott
// Canary: 3f8dcd89-78d0-43ed-9f7d-b140ce9d31e0
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-416: a deployment's naming convention names its GraphQL fields, and never the keys the UI
// reads a row under. Rows arrive under the camelCase keys whatever the schema's spelling; under
// the default convention the two are one, and no alias is emitted.

import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import { parse, type OperationDefinitionNode, type SelectionSetNode } from "graphql";
import { describe, expect, it } from "vitest";
import {
  RoleSchema,
  rootField,
  rootKey,
  selectionSet,
  type GraphqlType,
  type Introspection,
  type MetaReader,
} from "../metaGraph";
import { ontologyCountQuery, ontologyQuery } from "../../components/graph3d/glossaryGraphData";
import type { QueryScope } from "../../components/graph3d/graphModel";
import { productGraphQuery } from "../../components/graph3d/productGraphData";
import { listQuery, panelQueries } from "../../pages/data-products/productCore";

const scalar: GraphqlType = { kind: "SCALAR", name: "String" };
const list = (name: string): GraphqlType => ({
  kind: "LIST",
  name: null,
  ofType: { kind: "OBJECT", name },
});
const field = (name: string, type: GraphqlType) => ({ name, type });

/** One view, as ``columns`` and ``rels`` spell it, with the camelCase keys callers read. */
function small(columns: [string, string], rels: { declared: string; derived: string }): MetaReader {
  const raw: Introspection = {
    __schema: {
      queryType: { name: "Query" },
      types: [
        {
          name: "Query",
          fields: [field("on_terms", list("ON_Terms")), field("m_cols", list("M_Cols"))],
        },
        {
          name: "ON_Terms",
          fields: [
            field("id", scalar),
            field(columns[0], scalar),
            field(rels.declared, list("ON_Terms")),
            field(rels.derived, list("M_Cols")),
          ],
        },
        { name: "M_Cols", fields: [field(columns[1], scalar)] },
      ],
    },
  };
  return {
    schema: new RoleSchema(raw),
    roots: new Map([
      [rootKey("ontology", "_terms"), "on_terms"],
      [rootKey("meta", "table_columns"), "m_cols"],
    ]),
  };
}

const SELECTION = {
  fields: ["id", "domain_id"],
  rels: {
    edgesOut: { sel: { fields: ["id"] } },
    tableColumns: { to: "table_columns", sel: { fields: ["column_name"] } },
  },
};

/** ``_to_snake_case`` of provisa/compiler/naming.py. */
const snake = (name: string) =>
  name
    .replace(/([A-Z]+)([A-Z][a-z])/g, "$1_$2")
    .replace(/([a-z0-9])([A-Z])/g, "$1_$2")
    .toLowerCase();

/** The relationships the meta seed declares a graphql_alias for: schema_gen takes one as written,
 * so these keep their camelCase under ``snake`` and ``hasura_graphql``, and every other field,
 * a derived relationship's among them, takes the convention. */
const DECLARED = new Set(["edgesOut", "edgesIn", "sourceTerm", "targetTerm", "memberTables"]);

interface MetaSchemaDoc {
  introspection: Introspection;
  root_fields: { domain_id: string; table_name: string; graphql_field: string }[];
}

/** The meta model's real schema, as is or spelled as ``name`` spells a field. */
function metaReader(name: (field: string) => string = (f) => f): MetaReader {
  const doc = JSON.parse(
    readFileSync(
      resolve(__dirname, "../../../../tests/fixtures/graphql_golden/meta_schema.json"),
      "utf8",
    ),
  ) as MetaSchemaDoc;
  const types = doc.introspection.__schema.types.map((t) => ({
    ...t,
    fields: t.fields && t.fields.map((f) => ({ ...f, name: name(f.name) })),
  }));
  return {
    schema: new RoleSchema({ __schema: { ...doc.introspection.__schema, types } }),
    roots: new Map(
      doc.root_fields.map((r) => [rootKey(r.domain_id, r.table_name), name(r.graphql_field)]),
    ),
  };
}

const snakeConvention = (f: string) => (DECLARED.has(f) ? f : snake(f));

/** The shape of a query's answer: the key each row's value arrives under, at every level, and
 * whether the field was narrowed. */
function answerKeys(set: SelectionSetNode): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const s of set.selections) {
    if (s.kind !== "Field") throw new Error(`unexpected ${s.kind}`);
    const key = (s.alias ?? s.name).value + (s.arguments?.length ? "(narrowed)" : "");
    out[key] = s.selectionSet ? answerKeys(s.selectionSet) : true;
  }
  return out;
}

function answer(query: string | null): Record<string, unknown> {
  if (query === null) throw new Error("no query was built");
  const op = parse(query).definitions[0] as OperationDefinitionNode;
  return answerKeys(op.selectionSet);
}

const SCOPE: QueryScope = {
  layers: new Set(["ontology", "domains", "data", "columns"]),
  domains: ["sales"],
  edgeTypes: ["KIND_OF"],
  hiddenStatuses: ["retired"],
  tags: [],
  sensitivities: [],
  orphans: true,
  search: ["party"],
};

const FILTERS = {
  domains: ["sales"],
  tags: ["pii"],
  sensitivities: ["confidential"],
  edgeTypes: [],
};

const BUILDERS: [string, (m: MetaReader) => string | null][] = [
  ["the Ontology View", (m) => ontologyQuery(m, SCOPE)],
  ["the Ontology View's count", (m) => ontologyCountQuery(m, SCOPE)],
  ["the product graph", (m) => productGraphQuery(m, null, FILTERS)],
  ["one product's graph", (m) => productGraphQuery(m, "1", FILTERS)],
  ["the product list", listQuery],
  ["a product's panel", (m) => panelQueries(m, "1")?.graphql ?? null],
];

describe("a schema under the snake conventions", () => {
  const m = small(["domain_id", "column_name"], { declared: "edgesOut", derived: "table_columns" });

  it("is read under camelCase keys: a column and a derived relationship are aliased", () => {
    expect(selectionSet(m, "ON_Terms", SELECTION)).toBe(
      [
        "{",
        "  id",
        "  domainId: domain_id",
        "  edgesOut {",
        "    id",
        "  }",
        "  tableColumns: table_columns {",
        "    columnName: column_name",
        "  }",
        "}",
      ].join("\n"),
    );
  });

  it("finds a relationship spelled snake_case by its camelCase key", () => {
    const spelled = small(["domain_id", "column_name"], {
      declared: "edges_out",
      derived: "table_columns",
    });
    expect(selectionSet(spelled, "ON_Terms", SELECTION)).toContain("edgesOut: edges_out {");
  });

  it("keeps the view's own alias on the root field", () => {
    expect(rootField(m, "_terms", { fields: ["domain_id"] })).toBe(
      "_terms: on_terms {\n    domainId: domain_id\n  }",
    );
  });

  it.each(BUILDERS)("answers %s under the default convention's keys", (_name, build) => {
    expect(answer(build(metaReader(snakeConvention)))).toEqual(answer(build(metaReader())));
  });
});

describe("a schema under the default convention", () => {
  const m = small(["domainId", "columnName"], { declared: "edgesOut", derived: "tableColumns" });

  it("is selected with no alias", () => {
    expect(selectionSet(m, "ON_Terms", SELECTION)).toBe(
      [
        "{",
        "  id",
        "  domainId",
        "  edgesOut {",
        "    id",
        "  }",
        "  tableColumns {",
        "    columnName",
        "  }",
        "}",
      ].join("\n"),
    );
  });

  it.each(BUILDERS)("gives %s no alias past the root", (_name, build) => {
    const query = build(metaReader());
    if (query === null) throw new Error("no query was built");
    const op = parse(query).definitions[0] as OperationDefinitionNode;
    const aliases = (set: SelectionSetNode): string[] =>
      set.selections.flatMap((s) =>
        s.kind === "Field"
          ? [
              ...(s.alias ? [s.alias.value] : []),
              ...(s.selectionSet ? aliases(s.selectionSet) : []),
            ]
          : [],
      );
    for (const root of op.selectionSet.selections) {
      if (root.kind === "Field" && root.selectionSet)
        expect(aliases(root.selectionSet)).toEqual([]);
    }
  });
});
