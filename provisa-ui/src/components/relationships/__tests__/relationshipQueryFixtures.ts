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
/** A table whose neighborhood a discovery accept joined to a shared dimension (REQ-413). */
export const BUILDING_PERMITS: RelationshipQueryTable = {
  id: 5,
  domainId: "shelter",
  tableName: "building_permits",
  alias: null,
};
/** The shared dimension: a derived view holding each neighborhood once. */
export const PERMIT_NEIGHBORHOOD: RelationshipQueryTable = {
  id: 6,
  domainId: "shelter",
  tableName: "permit_neighborhood",
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
            { name: "shelter__buildingPermits", type: list("ShelterBuildingPermits") },
            { name: "shelter__permitNeighborhood", type: list("ShelterPermitNeighborhood") },
          ],
        },
        {
          name: "SalesOrders",
          fields: [
            { name: "id", type: scalar("Int") },
            { name: "customerId", type: scalar("Int") },
            { name: "client", type: object("SalesClients") },
            { name: "tagged", type: list("SalesTags") },
          ],
        },
        {
          name: "SalesClients",
          fields: clientColumns.map((name) => ({ name, type: scalar("String") })),
        },
        { name: "SalesOrderTags", fields: [{ name: "orderId", type: scalar("Int") }] },
        { name: "SalesTags", fields: [{ name: "id", type: scalar("Int") }] },
        {
          name: "ShelterBuildingPermits",
          fields: [
            { name: "id", type: scalar("Int") },
            { name: "neighborhood", type: scalar("String") },
            { name: "permit_neighborhood_dimension", type: object("ShelterPermitNeighborhood") },
          ],
        },
        {
          name: "ShelterPermitNeighborhood",
          fields: [{ name: "permit_neighborhood", type: scalar("String") }],
        },
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
      ["shelter.building_permits", "shelter__buildingPermits"],
      ["shelter.permit_neighborhood", "shelter__permitNeighborhood"],
    ]),
  };
}

/** An edge's join as /data/graph-schema serves it (provisa/gql/label_map.py
 * RelationshipMapping.join): its two ends sorted, each JSON-encoded as [node key, column, source
 * expression, source constant, junction columns], then the junction table and its type. */
export function joinOf(
  source: [string, string, string[] | null],
  target: [string, string, string[] | null],
  junction: { table: [string, string, string]; type: [string | null, string | null] } | null = null,
): string {
  const ends = [
    JSON.stringify([source[0], source[1], null, null, source[2]]),
    JSON.stringify([target[0], target[1], null, null, target[2]]),
  ].sort();
  return JSON.stringify([ends, junction?.table ?? null, junction?.type ?? null]);
}

export const GRAPH: GraphSchemaDoc = {
  node_labels: [
    {
      label: "Orders",
      table_id: 1,
      pk_columns: ["id"],
      properties: ["id", "customerId"],
      native_filter_columns: [],
    },
    {
      label: "Clients",
      table_id: 2,
      pk_columns: ["id"],
      properties: ["id", "name"],
      native_filter_columns: [],
    },
    {
      label: "Tags",
      table_id: 4,
      pk_columns: ["id"],
      properties: ["id"],
      native_filter_columns: [],
    },
    {
      label: "Shelter:BuildingPermits",
      table_id: 5,
      pk_columns: ["id"],
      properties: ["id", "neighborhood"],
      native_filter_columns: [],
    },
    {
      label: "Shelter:PermitNeighborhood",
      table_id: 6,
      pk_columns: ["permitNeighborhood"],
      properties: ["permitNeighborhood"],
      native_filter_columns: [],
    },
  ],
  relationship_types: [
    {
      type: "PLACED_BY",
      source: "Orders",
      target: "Clients",
      junction_table_name: null,
      join: joinOf(["SalesOrders", "customer_id", null], ["SalesClients", "id", null]),
    },
    {
      type: "TAGGED",
      source: "Orders",
      target: "Tags",
      junction_table_name: "order_tags",
      join: joinOf(["SalesOrders", "id", ["order_id"]], ["SalesTags", "id", ["tag_id"]], {
        table: ["pg", "sales", "order_tags"],
        type: ["kind", "label"],
      }),
    },
    // Served under the type the schema build names an unaliased edge by, from its GraphQL field
    // (IS_<FIELD>), not the stored BELONGS_TO_<COLUMN> the admin list shows.
    {
      type: "IS_PERMIT_NEIGHBORHOOD_DIMENSION",
      source: "Shelter:BuildingPermits",
      target: "Shelter:PermitNeighborhood",
      junction_table_name: null,
      join: joinOf(
        ["ShelterBuildingPermits", "neighborhood", null],
        ["ShelterPermitNeighborhood", "permit_neighborhood", null],
      ),
    },
  ],
};

export const VOCABULARY: SparqlVocabulary = {
  base: "https://provisa.example/",
  prefixes: {
    sales: "https://provisa.example/sales/",
    shelter: "https://provisa.example/shelter/",
    rel: "https://provisa.example/rel/",
  },
  classes: [
    ...["orders", "clients", "tags"].map((name) => ({
      iri: `https://provisa.example/sales/${name}`,
      label: name[0].toUpperCase() + name.slice(1),
    })),
    {
      iri: "https://provisa.example/shelter/building_permits",
      label: "Shelter:BuildingPermits",
    },
    {
      iri: "https://provisa.example/shelter/permit_neighborhood",
      label: "Shelter:PermitNeighborhood",
    },
  ].map(({ iri, label }) => ({
    iri,
    kind: "table" as const,
    labels: [label],
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
    {
      iri: "https://provisa.example/rel/IS_PERMIT_NEIGHBORHOOD_DIMENSION",
      type: "IS_PERMIT_NEIGHBORHOOD_DIMENSION",
      source: "Shelter:BuildingPermits",
      target: "Shelter:PermitNeighborhood",
      join: "neighborhood=permit_neighborhood",
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
    shape: "direct",
    properties: [],
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
    graphqlAlias: "tagged",
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
    shape: "junction",
    properties: [],
  });

/** building_permits -> its shared neighborhood dimension, as a discovery accept made it and the
 * admin list serves it: no alias, so the stored type is derived from its column. */
export const toDimension = (): Relationship =>
  placedBy({
    id: "building_permits-neighborhood-to-permit_neighborhood" as unknown as number,
    sourceTableId: 5,
    targetTableId: 6,
    sourceTableName: "building_permits",
    sourceDomainId: "shelter",
    targetTableName: "permit_neighborhood",
    sourceColumn: "neighborhood",
    targetColumn: "permit_neighborhood",
    graphqlAlias: "permit_neighborhood_dimension",
    physicalName: "permit_neighborhood_dimension",
    computedCypherAlias: "BELONGS_TO_NEIGHBORHOOD",
    cypherType: "BELONGS_TO_NEIGHBORHOOD",
  });

export const TABLES = [ORDERS, CUSTOMERS, ORDER_TAGS, TAGS, BUILDING_PERMITS, PERMIT_NEIGHBORHOOD];
