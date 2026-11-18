// Copyright (c) 2026 Kenneth Stott
// Canary: 0c6d3b98-1e47-4a25-b9f3-7d82e5a14c60
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2252: one role's surfaces over orders -> customers (PLACED_BY), and a junction-backed
// orders -> tags (TAGGED) through order_tags, as the server serves them.

import { RoleSchema, type GraphqlType, type MetaReader } from "../../../api/metaGraph";
import type { SparqlVocabulary } from "../../graph/sparql";
import type { GraphSchemaDoc, RelationshipQueryTable } from "../relationshipQueries";
import type { Relationship } from "../../../types/admin";

const scalar = (name: string): GraphqlType => ({ kind: "SCALAR", name });
const object = (name: string): GraphqlType => ({ kind: "OBJECT", name });
const list = (name: string): GraphqlType => ({ kind: "LIST", name: null, ofType: object(name) });

export const ORDERS: RelationshipQueryTable = {
  id: 1,
  domainId: "sales",
  tableName: "orders",
  alias: null,
};
export const CUSTOMERS: RelationshipQueryTable = {
  id: 2,
  domainId: "sales",
  tableName: "customers",
  alias: "clients",
};
export const ORDER_TAGS: RelationshipQueryTable = {
  id: 3,
  domainId: "sales",
  tableName: "order_tags",
  alias: null,
};
export const TAGS: RelationshipQueryTable = {
  id: 4,
  domainId: "sales",
  tableName: "tags",
  alias: null,
};

/** The role's GraphQL schema; ``clientColumns`` are the columns it reads of customers. */
export function reader(clientColumns: string[] = ["id", "name"]): MetaReader {
  const schema = new RoleSchema({
    __schema: {
      queryType: { name: "Query" },
      types: [
        {
          name: "Query",
          fields: [
            { name: "sales__orders", type: list("SalesOrders") },
            { name: "sales__clients", type: list("SalesClients") },
            { name: "sales__orderTags", type: list("SalesOrderTags") },
            { name: "sales__tags", type: list("SalesTags") },
          ],
        },
        {
          name: "SalesOrders",
          fields: [
            { name: "id", type: scalar("Int") },
            { name: "customerId", type: scalar("Int") },
            { name: "client", type: object("SalesClients") },
          ],
        },
        {
          name: "SalesClients",
          fields: clientColumns.map((name) => ({ name, type: scalar("String") })),
        },
        { name: "SalesOrderTags", fields: [{ name: "orderId", type: scalar("Int") }] },
        { name: "SalesTags", fields: [{ name: "id", type: scalar("Int") }] },
      ],
    },
  });
  return {
    schema,
    roots: new Map([
      ["sales.orders", "sales__orders"],
      ["sales.customers", "sales__clients"],
      ["sales.order_tags", "sales__orderTags"],
      ["sales.tags", "sales__tags"],
    ]),
  };
}

export const GRAPH: GraphSchemaDoc = {
  node_labels: [
    { label: "Orders", table_id: 1, pk_columns: ["id"] },
    { label: "Clients", table_id: 2, pk_columns: ["id"] },
    { label: "Tags", table_id: 4, pk_columns: ["id"] },
  ],
  relationship_types: [
    { type: "PLACED_BY", source: "Orders", target: "Clients" },
    { type: "TAGGED", source: "Orders", target: "Tags" },
  ],
};

export const VOCABULARY: SparqlVocabulary = {
  base: "https://provisa.example/",
  prefixes: { sales: "https://provisa.example/sales/", rel: "https://provisa.example/rel/" },
  classes: ["orders", "clients", "tags"].map((name) => ({
    iri: `https://provisa.example/sales/${name}`,
    kind: "table" as const,
    labels: [name[0].toUpperCase() + name.slice(1)],
    parameterized: false,
    parameterized_labels: [],
    superclasses: [],
  })),
  predicates: [],
  edges: [
    {
      iri: "https://provisa.example/rel/PLACED_BY",
      type: "PLACED_BY",
      source: "Orders",
      target: "Clients",
      join: "customer_id=id",
    },
    {
      iri: "https://provisa.example/rel/TAGGED",
      type: "TAGGED",
      source: "Orders",
      target: "Tags",
      join: "id=id",
    },
  ],
  concepts: [],
};

/** orders -> customers as the admin list serves it. */
export function placedBy(more: Partial<Relationship> = {}): Relationship {
  return {
    id: "placed-by",
    sourceTableId: 1,
    targetTableId: 2,
    sourceTableName: "orders",
    sourceDomainId: "sales",
    targetTableName: "customers",
    sourceColumn: "customer_id",
    targetColumn: "id",
    cardinality: "many-to-one",
    materialize: false,
    refreshInterval: 300,
    targetFunctionName: null,
    functionArg: null,
    alias: null,
    graphqlAlias: "client",
    physicalName: "client",
    computedCypherAlias: "PLACED_BY",
    cypherType: "PLACED_BY",
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
    withdrawn: false,
    ...more,
  } as unknown as Relationship;
}

/** orders -> tags through order_tags, as the admin list serves it. */
export const tagged = (): Relationship =>
  placedBy({
    id: "tagged" as unknown as number,
    targetTableId: 4,
    targetTableName: "tags",
    sourceColumn: "id",
    targetColumn: "id",
    graphqlAlias: null,
    physicalName: null,
    computedCypherAlias: "TAGGED",
    cypherType: "TAGGED",
    cardinality: "many-to-many",
    viaTableId: 3,
    viaTableName: "order_tags",
    viaSourceColumn: "order_id",
    viaTargetColumn: "tag_id",
    viaTypeColumn: "kind",
    viaTypeValue: "label",
    viaLabelSource: "fixed",
  });

export const TABLES = [ORDERS, CUSTOMERS, ORDER_TAGS, TAGS];
