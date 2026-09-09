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
import { Neo4jPreview as NEO4J_PREVIEW_QUERY } from "./admin.graphql";
import type { Neo4jPreview, Neo4jPreviewVars } from "../types/admin";

// REQ-1670: the Cypher preview behind Register Table on a neo4j source. A failure is the
// ``error`` field, never a thrown query error — half-written Cypher is the normal state.
export function useNeo4jPreview() {
  const [run] = useLazyQuery<{ neo4jPreview: Neo4jPreview }, Neo4jPreviewVars>(
    NEO4J_PREVIEW_QUERY,
    { fetchPolicy: "no-cache" },
  );
  return {
    preview: useCallback(
      async (vars: Neo4jPreviewVars): Promise<Neo4jPreview> => {
        const res = await run({ variables: vars });
        if (res.error) return { rows: [], columns: [], error: res.error.message };
        return res.data?.neo4jPreview ?? { rows: [], columns: [], error: "no preview returned" };
      },
      [run],
    ),
  };
}
