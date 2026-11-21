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
import { tableSqlRef } from "../../naming";
import { allPrefixes, compactIri, withUsedPrefixes, type SparqlVocabulary } from "../graph/sparql";
import { grpcExample } from "../../pages/data-products/tableExamples";
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
