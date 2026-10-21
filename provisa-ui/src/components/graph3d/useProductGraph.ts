// Copyright (c) 2026 Kenneth Stott
// Canary: 5a8e2c71-f4d3-4b96-8c0a-3e7b1d9f6c42
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2128: the data product graph's data for one product, or every product, read from the meta
// domain as the role the app acts as.

import { useEffect, useState } from "react";
import { useAuth } from "../../context/AuthContext";
import {
  INTROSPECTION_QUERY,
  RoleSchema,
  dataGraphql,
  rootFields,
  type Introspection,
  type MetaReader,
} from "../../api/metaGraph";
import type { GraphData } from "./graphModel";
import {
  boundTermIds,
  memberTableIds,
  productsQuery,
  sensitiveQuery,
  termEdgesQueries,
  toGraphData,
  type ProductRow,
  type SensitiveRow,
  type TermEdgeRow,
} from "./productGraphData";

// One read of each role's schema for the page's life: it changes only with the model.
const readers = new Map<string, Promise<MetaReader>>();

export function metaReader(role: string): Promise<MetaReader> {
  let r = readers.get(role);
  if (!r) {
    r = Promise.all([
      dataGraphql<Introspection>(INTROSPECTION_QUERY, {}, role),
      rootFields(role),
    ]).then(([raw, roots]) => ({ schema: new RoleSchema(raw), roots }));
    r.catch(() => readers.delete(role));
    readers.set(role, r);
  }
  return r;
}

export async function loadProductGraph(role: string, product: string | null): Promise<GraphData> {
  const m = await metaReader(role);
  const query = productsQuery(m, product);
  if (query === null) return { nodes: [], edges: [] };
  const { data_products: products } = await dataGraphql<{
    data_products: ProductRow[];
  }>(query, {}, role);
  const terms = boundTermIds(products);
  const tables = memberTableIds(products);
  const edgeQueries = terms.length > 0 ? termEdgesQueries(m, terms) : null;
  const sensQuery = tables.length > 0 ? sensitiveQuery(m, tables) : null;
  const [termEdges, sensitive] = await Promise.all([
    edgeQueries === null
      ? null
      : Promise.all(
          edgeQueries.map((q) =>
            dataGraphql<{ glossary_term_edges: TermEdgeRow[] }>(q, {}, role).then(
              (r) => r.glossary_term_edges,
            ),
          ),
        ).then((both) => both.flat()),
    sensQuery === null
      ? null
      : dataGraphql<{ sensitive_coverage: SensitiveRow[] }>(sensQuery, {}, role).then(
          (r) => r.sensitive_coverage,
        ),
  ]);
  return toGraphData(products, termEdges, sensitive);
}

export function useProductGraph(product: string | null, enabled: boolean) {
  const { actingRoles } = useAuth();
  const [state, setState] = useState<{ data: GraphData | null; error: string | null }>({
    data: null,
    error: null,
  });
  useEffect(() => {
    if (!enabled || actingRoles === null) return;
    let cancelled = false;
    loadProductGraph(actingRoles, product).then(
      (data) => !cancelled && setState({ data, error: null }),
      (e: unknown) =>
        !cancelled && setState({ data: null, error: e instanceof Error ? e.message : String(e) }),
    );
    return () => {
      cancelled = true;
    };
  }, [actingRoles, product, enabled]);
  return state;
}
