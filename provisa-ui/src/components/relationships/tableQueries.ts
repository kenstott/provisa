// Copyright (c) 2026 Kenneth Stott
// Canary: 7c2e94a1-5b38-4d0f-a6e1-9f3b82c4d517
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2252 (Amended 2026-11-18): a table's query on every surface it is exposed on, as an Explore
// hand-off run at once: Cypher matches its node label, SQL selects from it, SPARQL reads the
// triples of its class's subjects, GraphQL reads its columns from its root field, REST reads its
// root, gRPC calls its Query method, and JSON:API reads its resources. Each by the names the acting
// role is served and with a row limit; a surface that does not serve the table to the role has no
// query.

import { rootKey, type MetaReader } from "../../api/metaGraph";
import { quoteIdent, tableSqlRef } from "../../naming";
import { allPrefixes, compactIri, withUsedPrefixes, type SparqlVocabulary } from "../graph/sparql";
import { grpcExample, grpcMethod } from "../../pages/data-products/tableExamples";
import {
  RELATIONSHIP_QUERY_LIMIT,
  RELATIONSHIP_SURFACES,
  type GraphSchemaDoc,
  type QueryLink,
  type RelationshipQueryTable,
  type RelationshipSurface,
  type RelationshipSurfaces,
} from "./relationshipQueries";

const LIMIT = RELATIONSHIP_QUERY_LIMIT;

/** The table's node label in the role's graph schema; null when it is not served in Cypher. */
const labelOf = (tb: RelationshipQueryTable, graph: GraphSchemaDoc): string | null =>
  graph.node_labels.find((n) => Number(n.table_id) === tb.id)?.label ?? null;

/** The table's root field in the role's GraphQL schema; null when the role is not served it. */
const rootOf = (tb: RelationshipQueryTable, reader: MetaReader): string | null =>
  reader.roots.get(rootKey(tb.domainId, tb.tableName)) ?? null;

export function tableCypher(tb: RelationshipQueryTable, graph: GraphSchemaDoc): string | null {
  const label = labelOf(tb, graph);
  return label === null ? null : `MATCH (n:${label}) RETURN n LIMIT ${LIMIT}`;
}

export function tableSparql(
  tb: RelationshipQueryTable,
  graph: GraphSchemaDoc,
  vocabulary: SparqlVocabulary,
): string | null {
  const label = labelOf(tb, graph);
  if (label === null) return null;
  const cls = vocabulary.classes.find((c) => c.kind === "table" && c.labels.includes(label));
  if (!cls) return null;
  const namespaces = allPrefixes(vocabulary);
  const body = `SELECT * WHERE {\n  ?s a ${compactIri(cls.iri, namespaces)} . ?s ?p ?o .\n}\nLIMIT ${LIMIT}`;
  return withUsedPrefixes(body, namespaces);
}

export function tableSql(tb: RelationshipQueryTable, reader: MetaReader): string | null {
  return rootOf(tb, reader) === null ? null : `select * from ${tableSqlRef(tb)} limit ${LIMIT}`;
}

export function tableGraphql(tb: RelationshipQueryTable, reader: MetaReader): string | null {
  const root = rootOf(tb, reader);
  const type = root ? reader.schema.rootType(root) : null;
  if (!root || !type) return null;
  const columns = reader.schema.scalars(type);
  if (columns.length === 0) return null;
  return `{ ${root}(limit: ${LIMIT}) { ${columns.join(" ")} } }`;
}

export function tableRest(tb: RelationshipQueryTable, reader: MetaReader): string | null {
  const root = rootOf(tb, reader);
  return root === null ? null : `GET /data/rest/${tb.domainId}/${tb.tableName}?limit=${LIMIT}`;
}

export function tableJsonApi(tb: RelationshipQueryTable, reader: MetaReader): string | null {
  if (rootOf(tb, reader) === null) return null;
  const query = new URLSearchParams({ "page[size]": String(LIMIT) });
  return `/data/jsonapi/${tb.domainId}/${tb.tableName}?${query.toString()}`;
}

export function tableGrpc(tb: RelationshipQueryTable, reader: MetaReader): QueryLink | null {
  const call = grpcExample(reader, tb);
  return call === null
    ? null
    : { to: "/grpc", state: { ...call.state, grpcBody: { limit: LIMIT }, autoRun: true } };
}

/** Each surface's query of ``tb`` as an Explore hand-off, run at once, in the order the
 * relationship links show them; a surface the role is not served the table on is left out. */
export function tableQueryLinks(
  tb: RelationshipQueryTable,
  surfaces: RelationshipSurfaces,
): { surface: RelationshipSurface; link: QueryLink }[] {
  const { reader, graph, vocabulary } = surfaces;
  const run = (to: string, key: string, text: string | null): QueryLink | null =>
    text === null ? null : { to, state: { [key]: text, autoRun: true } };
  const built: Record<RelationshipSurface, QueryLink | null> = {
    cypher: graph ? run("/graph", "query", tableCypher(tb, graph)) : null,
    sql: reader ? run("/sql", "sql", tableSql(tb, reader)) : null,
    sparql:
      graph && vocabulary ? run("/sparql", "query", tableSparql(tb, graph, vocabulary)) : null,
    graphql: reader ? run("/query", "query", tableGraphql(tb, reader)) : null,
    rest: reader ? run("/openapi", "openApiUrl", tableRest(tb, reader)) : null,
    grpc: reader ? tableGrpc(tb, reader) : null,
    jsonapi: reader ? run("/jsonapi", "jsonapiUrl", tableJsonApi(tb, reader)) : null,
  };
  return RELATIONSHIP_SURFACES.flatMap((surface) => {
    const link = built[surface];
    return link ? [{ surface, link }] : [];
  });
}

// REQ-2252 (amended 2026-11-22): a table whose aggregates are enabled has, on its API section, a
// link per surface that serves its aggregates to the acting role, each opening that surface's
// Explore editor with an aggregate query run at once: its count, and with group-by enabled its
// count per value of one of its columns. GraphQL, REST, JSON:API and gRPC serve them through the
// table's aggregate and group-by root fields, which the role's schema has only where the table
// enables them and the role may aggregate; Cypher, SQL and SPARQL count whatever they serve.

/** A table as its aggregate queries are built from it. */
export interface AggregateQueryTable extends RelationshipQueryTable {
  enableGroupBy: boolean;
  columns: { columnName: string; computedSqlAlias: string; isPrimaryKey: boolean }[];
}

type AggregateColumn = AggregateQueryTable["columns"][number];

/** The column a group-by groups on: the table's first that is not its key, else its first; none
 * when group-by is off. */
function groupColumn(tb: AggregateQueryTable): AggregateColumn | null {
  if (!tb.enableGroupBy || tb.columns.length === 0) return null;
  return tb.columns.find((c) => !c.isPrimaryKey) ?? tb.columns[0];
}

const norm = (name: string) => name.replace(/_/g, "").toLowerCase();

/** The table's ``variant`` root field as the role's schema spells it (``x_aggregate`` under
 * snake_case, ``xAggregate`` under camelCase: provisa/compiler/naming.py root_field_variant);
 * null when the role is not served it. */
function variantRoot(
  tb: RelationshipQueryTable,
  reader: MetaReader,
  variant: "aggregate" | "group_by",
): string | null {
  const root = rootOf(tb, reader);
  if (root === null) return null;
  const spellings =
    variant === "aggregate"
      ? [`${root}_aggregate`, `${root}Aggregate`]
      : [`${root}_group_by`, `${root}GroupBy`];
  return spellings.find((f) => reader.schema.rootType(f) !== null) ?? null;
}

/** The group column as the role's GraphQL schema spells it; null when it reads no such column. */
function gqlColumn(tb: AggregateQueryTable, reader: MetaReader, col: AggregateColumn) {
  const root = rootOf(tb, reader);
  const type = root ? reader.schema.rootType(root) : null;
  return type ? reader.schema.column(type, col.computedSqlAlias) : null;
}

export function tableAggregateGraphql(tb: AggregateQueryTable, reader: MetaReader): string | null {
  const col = groupColumn(tb);
  const gb = col ? variantRoot(tb, reader, "group_by") : null;
  const by = col && gb ? gqlColumn(tb, reader, col) : null;
  if (gb && by) return `{ ${gb}(by: [${by}]) { groupKey aggregate { count } } }`;
  const agg = variantRoot(tb, reader, "aggregate");
  return agg === null ? null : `{ ${agg} { aggregate { count } } }`;
}

export function tableAggregateSql(tb: AggregateQueryTable, reader: MetaReader): string | null {
  if (rootOf(tb, reader) === null) return null;
  const col = groupColumn(tb);
  if (col === null) return `select count(*) from ${tableSqlRef(tb)}`;
  const c = quoteIdent(col.computedSqlAlias);
  return `select ${c}, count(*) from ${tableSqlRef(tb)} group by ${c}`;
}

/** The node property the group column is read by; null when the label has no such property. */
function cypherProperty(
  tb: AggregateQueryTable,
  graph: GraphSchemaDoc,
  col: AggregateColumn,
): string | null {
  const node = graph.node_labels.find((n) => Number(n.table_id) === tb.id);
  return node?.properties.find((p) => norm(p) === norm(col.computedSqlAlias)) ?? null;
}

export function tableAggregateCypher(
  tb: AggregateQueryTable,
  graph: GraphSchemaDoc,
): string | null {
  const label = labelOf(tb, graph);
  if (label === null) return null;
  const col = groupColumn(tb);
  const prop = col ? cypherProperty(tb, graph, col) : null;
  return prop
    ? `MATCH (n:${label}) RETURN n.${prop} AS ${prop}, count(n) AS count`
    : `MATCH (n:${label}) RETURN count(n) AS count`;
}

export function tableAggregateSparql(
  tb: AggregateQueryTable,
  graph: GraphSchemaDoc,
  vocabulary: SparqlVocabulary,
): string | null {
  const label = labelOf(tb, graph);
  if (label === null) return null;
  const cls = vocabulary.classes.find((c) => c.kind === "table" && c.labels.includes(label));
  if (!cls) return null;
  const namespaces = allPrefixes(vocabulary);
  const col = groupColumn(tb);
  const prop = col ? cypherProperty(tb, graph, col) : null;
  const pred = prop
    ? vocabulary.predicates.find(
        (p) => p.kind === "column" && p.label === label && p.property === prop,
      )
    : undefined;
  const typed = `?s a ${compactIri(cls.iri, namespaces)} .`;
  const body = pred
    ? `SELECT ?v (COUNT(?s) AS ?count) WHERE {\n  ${typed} ?s ${compactIri(pred.iri, namespaces)} ?v .\n}\nGROUP BY ?v`
    : `SELECT (COUNT(?s) AS ?count) WHERE {\n  ${typed}\n}`;
  return withUsedPrefixes(body, namespaces);
}

/** The REST and JSON:API aggregate parameters: the count, grouped by the column's physical name
 * (the spelling ``groupBy`` takes) where the role is served the table's group-by. */
function aggregateParams(tb: AggregateQueryTable, reader: MetaReader): URLSearchParams | null {
  if (variantRoot(tb, reader, "aggregate") === null) return null;
  const params = new URLSearchParams({ aggregate: "count" });
  const col = groupColumn(tb);
  if (col && variantRoot(tb, reader, "group_by") !== null) params.set("groupBy", col.columnName);
  return params;
}

export function tableAggregateRest(tb: AggregateQueryTable, reader: MetaReader): string | null {
  const params = aggregateParams(tb, reader);
  return params === null
    ? null
    : `GET /data/rest/${tb.domainId}/${tb.tableName}?${params.toString()}`;
}

export function tableAggregateJsonApi(tb: AggregateQueryTable, reader: MetaReader): string | null {
  const params = aggregateParams(tb, reader);
  return params === null
    ? null
    : `/data/jsonapi/${tb.domainId}/${tb.tableName}?${params.toString()}`;
}

export function tableAggregateGrpc(tb: AggregateQueryTable, reader: MetaReader): QueryLink | null {
  const root = rootOf(tb, reader);
  if (root === null || variantRoot(tb, reader, "aggregate") === null) return null;
  const col = groupColumn(tb);
  const state =
    col && variantRoot(tb, reader, "group_by") !== null
      ? {
          grpcMethod: grpcMethod(root, "GroupBy"),
          grpcBody: { by: [col.columnName], funcs: ["count"] },
        }
      : { grpcMethod: grpcMethod(root, "Aggregate"), grpcBody: { funcs: ["count"] } };
  return { to: "/grpc", state: { ...state, autoRun: true } };
}

/** Each surface's aggregate query of ``tb`` as an Explore hand-off, run at once, in the order the
 * table's links show them; a surface that does not serve the role its aggregates is left out. */
export function tableAggregateLinks(
  tb: AggregateQueryTable,
  surfaces: RelationshipSurfaces,
): { surface: RelationshipSurface; link: QueryLink }[] {
  const { reader, graph, vocabulary } = surfaces;
  const run = (to: string, key: string, text: string | null): QueryLink | null =>
    text === null ? null : { to, state: { [key]: text, autoRun: true } };
  const built: Record<RelationshipSurface, QueryLink | null> = {
    cypher: graph ? run("/graph", "query", tableAggregateCypher(tb, graph)) : null,
    sql: reader ? run("/sql", "sql", tableAggregateSql(tb, reader)) : null,
    sparql:
      graph && vocabulary
        ? run("/sparql", "query", tableAggregateSparql(tb, graph, vocabulary))
        : null,
    graphql: reader ? run("/query", "query", tableAggregateGraphql(tb, reader)) : null,
    rest: reader ? run("/openapi", "openApiUrl", tableAggregateRest(tb, reader)) : null,
    grpc: reader ? tableAggregateGrpc(tb, reader) : null,
    jsonapi: reader ? run("/jsonapi", "jsonapiUrl", tableAggregateJsonApi(tb, reader)) : null,
  };
  return RELATIONSHIP_SURFACES.flatMap((surface) => {
    const link = built[surface];
    return link ? [{ surface, link }] : [];
  });
}
