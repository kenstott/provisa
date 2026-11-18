// Copyright (c) 2026 Kenneth Stott
// Canary: 5b8e2d41-7c93-4f6a-a1e0-3d9c7b52f816
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2252: a relationship's query on every surface that reads it, as an Explore hand-off: Cypher
// matches its type between its two node labels, SQL joins its two tables on its columns, SPARQL
// selects subject and object over its predicate, GraphQL reads its source with its field nested,
// JSON:API reads its source with it included. Each by the names the acting role is served and with
// a row limit; a surface that does not serve the relationship to the role has no query.
//
// REST and gRPC have none: a REST read (/data/rest/<field>) and a gRPC Query<Type> call select the
// table's own columns only and never follow a relationship (provisa/api/rest/generator.py,
// provisa/grpc/query_ir.py resolve_read_mask); they reach a related row only inside a group-by's
// nodes, which reads groups, not the relationship.

import type { MetaReader } from "../../api/metaGraph";
import { rootKey } from "../../api/metaGraph";
import { quoteIdent, tableSqlRef } from "../../naming";
import { allPrefixes, compactIri, withUsedPrefixes, type SparqlVocabulary } from "../graph/sparql";

/** The rows a relationship's query reads at most. */
export const RELATIONSHIP_QUERY_LIMIT = 25;

/** Where a query opens: the Explore page and the hand-off it reads. */
export interface QueryLink {
  to: string;
  state: Record<string, unknown>;
}

/** A registered table as its names are built: its id, domain, table name and alias. */
export interface RelationshipQueryTable {
  id: number;
  domainId: string;
  tableName: string;
  alias: string | null;
}

/** A relationship as its queries are built from it. Columns are stored key lists: comma-separated,
 * paired in order (provisa/compiler/sql_types.py key_list). */
export interface RelationshipQueryRef {
  source: RelationshipQueryTable;
  target: RelationshipQueryTable;
  sourceColumn: string;
  targetColumn: string;
  /** REQ-1586: the associative table a junction-backed edge goes through, and its columns pairing
   * with the source's and the target's; null on an FK/PK-backed edge. */
  via: {
    table: RelationshipQueryTable;
    sourceColumn: string;
    targetColumn: string;
    typeColumn: string | null;
    typeValue: string | null;
  } | null;
  /** The type it is served as in Cypher (stored by the server, never derived here). */
  cypherType: string | null;
  disableCypher: boolean;
  /** Its GraphQL field as stored (graphql_field); null for a junction-backed edge. */
  graphqlField: string | null;
  /** REQ-1417: its name on the SQL plane, which JSON:API's ?include= takes; null when none. */
  physicalName: string | null;
}

/** The role's graph schema as /data/graph-schema serves it: the parts a query is named by. */
export interface GraphSchemaDoc {
  node_labels: { label: string; table_id: number; pk_columns: string[] }[];
  relationship_types: { type: string; source: string; target: string }[];
}

/** What the acting role is served, each read on its own; null while unread or refused. */
export interface RelationshipSurfaces {
  reader: MetaReader | null;
  graph: GraphSchemaDoc | null;
  vocabulary: SparqlVocabulary | null;
}

export const RELATIONSHIP_SURFACES = ["cypher", "sql", "sparql", "graphql", "jsonapi"] as const;
export type RelationshipSurface = (typeof RELATIONSHIP_SURFACES)[number];

const keys = (stored: string): string[] =>
  stored
    .split(",")
    .map((k) => k.trim())
    .filter((k) => k !== "");

/** ``a`` joined to ``b`` on their paired columns; null when the lists do not pair. */
function joinOn(a: string, aCols: string, b: string, bCols: string): string | null {
  const left = keys(aCols);
  const right = keys(bCols);
  if (left.length === 0 || left.length !== right.length) return null;
  return left.map((c, i) => `${a}.${quoteIdent(c)} = ${b}.${quoteIdent(right[i])}`).join(" and ");
}

/** SQL: the source joined to the target on the relationship's columns (through its junction). */
export function relationshipSql(rel: RelationshipQueryRef): string | null {
  const limit = `limit ${RELATIONSHIP_QUERY_LIMIT}`;
  if (rel.via === null) {
    const on = joinOn("s", rel.sourceColumn, "t", rel.targetColumn);
    if (on === null) return null;
    return `select * from ${tableSqlRef(rel.source)} as s join ${tableSqlRef(rel.target)} as t on ${on} ${limit}`;
  }
  const v = rel.via;
  const toVia = joinOn("s", rel.sourceColumn, "j", v.sourceColumn);
  const fromVia = joinOn("j", v.targetColumn, "t", rel.targetColumn);
  if (toVia === null || fromVia === null) return null;
  const typed =
    v.typeColumn && v.typeValue !== null
      ? ` and j.${quoteIdent(v.typeColumn)} = '${v.typeValue.replace(/'/g, "''")}'`
      : "";
  return (
    `select * from ${tableSqlRef(rel.source)} as s` +
    ` join ${tableSqlRef(v.table)} as j on ${toVia}${typed}` +
    ` join ${tableSqlRef(rel.target)} as t on ${fromVia} ${limit}`
  );
}

const labelOf = (graph: GraphSchemaDoc, tableId: number): string | null =>
  graph.node_labels.find((n) => Number(n.table_id) === tableId)?.label ?? null;

/** The two node labels and the type the role is served the relationship as; null when its graph
 * schema has no such edge (excluded from Cypher, or an end it cannot read). */
function servedEdge(
  rel: RelationshipQueryRef,
  graph: GraphSchemaDoc,
): { source: string; target: string; type: string } | null {
  if (rel.disableCypher || !rel.cypherType) return null;
  const source = labelOf(graph, rel.source.id);
  const target = labelOf(graph, rel.target.id);
  if (source === null || target === null) return null;
  const served = graph.relationship_types.some(
    (r) => r.type === rel.cypherType && r.source === source && r.target === target,
  );
  return served ? { source, target, type: rel.cypherType } : null;
}

/** Cypher: the relationship's type between its two node labels. */
export function relationshipCypher(rel: RelationshipQueryRef, graph: GraphSchemaDoc): string | null {
  const edge = servedEdge(rel, graph);
  if (edge === null) return null;
  return `MATCH (a:${edge.source})-[r:${edge.type}]->(b:${edge.target}) RETURN a, r, b LIMIT ${RELATIONSHIP_QUERY_LIMIT}`;
}

/** SPARQL: subject and object over the relationship's predicate, each typed by its end's class. */
export function relationshipSparql(
  rel: RelationshipQueryRef,
  graph: GraphSchemaDoc,
  vocabulary: SparqlVocabulary,
): string | null {
  const edge = servedEdge(rel, graph);
  if (edge === null) return null;
  const predicate = vocabulary.edges.find(
    (e) => e.type === edge.type && e.source === edge.source && e.target === edge.target,
  );
  const classOf = (label: string) =>
    vocabulary.classes.find((c) => c.kind === "table" && c.labels.includes(label))?.iri;
  const domain = classOf(edge.source);
  const range = classOf(edge.target);
  if (!predicate || !domain || !range) return null;
  const namespaces = allPrefixes(vocabulary);
  const [d, p, r] = [domain, predicate.iri, range].map((iri) => compactIri(iri, namespaces));
  const body = `SELECT * WHERE {\n  ?s a ${d} . ?s ${p} ?o . ?o a ${r} .\n}\nLIMIT ${RELATIONSHIP_QUERY_LIMIT}`;
  return withUsedPrefixes(body, namespaces);
}

/** The source's root field and type, and the relationship's field on it with the type it reaches,
 * as the role's GraphQL schema names them; null when the role is not served one of them. */
function graphqlPath(rel: RelationshipQueryRef, reader: MetaReader) {
  if (!rel.graphqlField) return null;
  const root = reader.roots.get(rootKey(rel.source.domainId, rel.source.tableName));
  const type = root ? reader.schema.rootType(root) : null;
  if (!root || !type) return null;
  const field = reader.schema.relationship(type, rel.graphqlField);
  if (!field) return null;
  return { root, type, field };
}

/** GraphQL: the source's columns with the relationship's field nested, its target's columns in it. */
export function relationshipGraphql(rel: RelationshipQueryRef, reader: MetaReader): string | null {
  const path = graphqlPath(rel, reader);
  if (path === null) return null;
  const own = reader.schema.scalars(path.type);
  const related = reader.schema.scalars(path.field.type);
  if (related.length === 0) return null;
  return `{ ${path.root}(limit: ${RELATIONSHIP_QUERY_LIMIT}) { ${[...own, `${path.field.name} { ${related.join(" ")} }`].join(" ")} } }`;
}

/** JSON:API: the source's resources with the relationship included. JSON:API serves a relationship
 * GraphQL serves, by its SQL-plane name, to a target whose resources have an id (a key, or an id
 * column). */
export function relationshipJsonApi(
  rel: RelationshipQueryRef,
  reader: MetaReader,
  graph: GraphSchemaDoc | null,
): string | null {
  if (!rel.physicalName) return null;
  const path = graphqlPath(rel, reader);
  if (path === null) return null;
  const keyed =
    (graph?.node_labels.find((n) => Number(n.table_id) === rel.target.id)?.pk_columns.length ?? 0) >
      0 || reader.schema.scalars(path.field.type).includes("id");
  if (!keyed) return null;
  const query = new URLSearchParams({
    include: rel.physicalName,
    "page[size]": String(RELATIONSHIP_QUERY_LIMIT),
  });
  return `/data/jsonapi/${rel.source.domainId}/${rel.source.tableName}?${query.toString()}`;
}

/** Whether the role reads every table the relationship's SQL names: each has a root field in its
 * schema (/data/graph-schema lists every table the role reads, junctions included). */
function readsTables(rel: RelationshipQueryRef, reader: MetaReader): boolean {
  const tables = [rel.source, rel.target, ...(rel.via ? [rel.via.table] : [])];
  return tables.every((tb) => reader.roots.has(rootKey(tb.domainId, tb.tableName)));
}

/** Each surface's query of ``rel`` as an Explore hand-off, run at once, in the order shown; a
 * surface the role is not served the relationship on is left out. */
export function relationshipQueryLinks(
  rel: RelationshipQueryRef,
  surfaces: RelationshipSurfaces,
): { surface: RelationshipSurface; link: QueryLink }[] {
  const { reader, graph, vocabulary } = surfaces;
  const run = (to: string, key: string, text: string | null): QueryLink | null =>
    text === null ? null : { to, state: { [key]: text, autoRun: true } };
  const built: Record<RelationshipSurface, QueryLink | null> = {
    cypher: graph ? run("/graph", "query", relationshipCypher(rel, graph)) : null,
    sql: reader && readsTables(rel, reader) ? run("/sql", "sql", relationshipSql(rel)) : null,
    sparql:
      graph && vocabulary ? run("/sparql", "query", relationshipSparql(rel, graph, vocabulary)) : null,
    graphql: reader ? run("/query", "query", relationshipGraphql(rel, reader)) : null,
    jsonapi: reader ? run("/jsonapi", "jsonapiUrl", relationshipJsonApi(rel, reader, graph)) : null,
  };
  return RELATIONSHIP_SURFACES.flatMap((surface) => {
    const link = built[surface];
    return link ? [{ surface, link }] : [];
  });
}

/** An admin-listed relationship as its queries are built, its tables found in ``tables`` by id;
 * null for one that reaches a function, or whose tables are not listed. */
export function relationshipQueryRef(
  r: {
    sourceTableId: number;
    targetTableId: number | null;
    sourceColumn: string;
    targetColumn: string | null;
    viaTableId: number | null;
    viaSourceColumn: string | null;
    viaTargetColumn: string | null;
    viaTypeColumn: string | null;
    viaTypeValue: string | null;
    cypherType: string | null;
    disableCypher: boolean;
    graphqlAlias: string | null;
    physicalName: string | null;
  },
  tables: readonly RelationshipQueryTable[],
): RelationshipQueryRef | null {
  const byId = (id: number | null) => (id === null ? undefined : tables.find((tb) => tb.id === id));
  const pick = (tb: RelationshipQueryTable): RelationshipQueryTable => ({
    id: tb.id,
    domainId: tb.domainId,
    tableName: tb.tableName,
    alias: tb.alias,
  });
  const source = byId(r.sourceTableId);
  const target = byId(r.targetTableId);
  if (!source || !target || r.targetColumn === null) return null;
  let via: RelationshipQueryRef["via"] = null;
  if (r.viaTableId !== null) {
    const table = byId(r.viaTableId);
    if (!table || r.viaSourceColumn === null || r.viaTargetColumn === null) return null;
    via = {
      table: pick(table),
      sourceColumn: r.viaSourceColumn,
      targetColumn: r.viaTargetColumn,
      typeColumn: r.viaTypeColumn,
      typeValue: r.viaTypeValue,
    };
  }
  return {
    source: pick(source),
    target: pick(target),
    sourceColumn: r.sourceColumn,
    targetColumn: r.targetColumn,
    via,
    cypherType: r.cypherType,
    disableCypher: r.disableCypher,
    // The admin list's graphqlAlias is the pinned alias or the stored GraphQL field
    // (provisa/api/admin/db_queries.py); a junction-backed edge has no GraphQL field.
    graphqlField: r.viaTableId === null ? r.graphqlAlias : null,
    physicalName: r.physicalName,
  };
}
