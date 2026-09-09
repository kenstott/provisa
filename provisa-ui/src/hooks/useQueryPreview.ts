// Copyright (c) 2026 Kenneth Stott
// Canary: 7c2e5d83-4a1f-4b6e-9d0c-2f8a3e7b1c94
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useCallback } from "react";
import { useLazyQuery } from "@apollo/client/react";
import {
  Neo4jPreview as NEO4J_PREVIEW_QUERY,
  SparqlPreview as SPARQL_PREVIEW_QUERY,
} from "./admin.graphql";
import type { QueryPreview, QueryPreviewVars } from "../types/admin";

// REQ-1670 (neo4j) / REQ-1683 (sparql): the query preview behind Register Table on a query-API
// source. A failure is the ``error`` field, never a thrown query error — half-written text is the
// normal state of the field the operator is typing into.
export function useQueryPreview() {
  const [runNeo4j] = useLazyQuery<
    { neo4jPreview: QueryPreview },
    { sourceId: string; cypher: string }
  >(NEO4J_PREVIEW_QUERY, { fetchPolicy: "no-cache" });
  const [runSparql] = useLazyQuery<
    { sparqlPreview: QueryPreview },
    { sourceId: string; query: string }
  >(SPARQL_PREVIEW_QUERY, { fetchPolicy: "no-cache" });
  return {
    preview: useCallback(
      async ({ sourceType, sourceId, query }: QueryPreviewVars): Promise<QueryPreview> => {
        if (sourceType === "sparql") {
          const res = await runSparql({ variables: { sourceId, query } });
          if (res.error) return { rows: [], columns: [], error: res.error.message };
          return res.data?.sparqlPreview ?? { rows: [], columns: [], error: "no preview returned" };
        }
        const res = await runNeo4j({ variables: { sourceId, cypher: query } });
        if (res.error) return { rows: [], columns: [], error: res.error.message };
        return res.data?.neo4jPreview ?? { rows: [], columns: [], error: "no preview returned" };
      },
      [runNeo4j, runSparql],
    ),
  };
}
