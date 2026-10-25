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

import { useCallback, useEffect, useState } from "react";
import { useAuth } from "../../context/AuthContext";
import {
  INTROSPECTION_QUERY,
  RoleSchema,
  dataGraphql,
  rootFields,
  type Introspection,
  type MetaReader,
} from "../../api/metaGraph";
import { fetchProductLineage } from "../../api/lineage";
import type { GraphData, QueryScope } from "./graphModel";
import {
  NO_PRODUCT_FILTERS,
  productGraph,
  productGraphQuery,
  withProductLineage,
  type ProductFilters,
  type ProductRow,
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

/** ``product``'s graph (every product's for null) as its one query keeps it by ``f``, with that
 * query (REQ-2136); null when the role cannot read the products. One product's graph carries its
 * lineage, from the product lineage endpoint (REQ-2156). */
export async function loadProductGraph(
  role: string,
  product: string | null,
  f: ProductFilters,
): Promise<{ data: GraphData; query: string } | null> {
  const query = productGraphQuery(await metaReader(role), product, f);
  if (query === null) return null;
  const [{ data_products: products }, lineage] = await Promise.all([
    dataGraphql<{ data_products: ProductRow[] }>(query, {}, role),
    product === null ? null : fetchProductLineage(product),
  ]);
  const data = productGraph(products);
  return { data: lineage && product ? withProductLineage(data, product, lineage) : data, query };
}

const EMPTY: GraphData = { nodes: [], edges: [] };

/**
 * ``product``'s graph, read once ``enabled``: ``all`` with no filter (what a panel draws and what a
 * view's controls choose from), and ``scoped`` as the view's controls keep it, read afresh on each
 * ``onScope``, with the query that read it.
 */
export function useProductGraph(product: string | null, enabled: boolean) {
  const { actingRoles } = useAuth();
  const [state, setState] = useState<{ data: GraphData | null; error: string | null }>({
    data: null,
    error: null,
  });
  const [filters, setFilters] = useState<ProductFilters | null>(null);
  const [scoped, setScoped] = useState<{ data: GraphData; query: string } | null>(null);
  useEffect(() => {
    if (!enabled || actingRoles === null) return;
    let cancelled = false;
    loadProductGraph(actingRoles, product, NO_PRODUCT_FILTERS).then(
      (read) => !cancelled && setState({ data: read?.data ?? EMPTY, error: null }),
      (e: unknown) =>
        !cancelled && setState({ data: null, error: e instanceof Error ? e.message : String(e) }),
    );
    return () => {
      cancelled = true;
    };
  }, [actingRoles, product, enabled]);
  useEffect(() => {
    if (!enabled || actingRoles === null || filters === null) return;
    let cancelled = false;
    loadProductGraph(actingRoles, product, filters).then(
      (read) => !cancelled && setScoped(read),
      (e: unknown) =>
        !cancelled &&
        setState((st) => ({ ...st, error: e instanceof Error ? e.message : String(e) })),
    );
    return () => {
      cancelled = true;
    };
  }, [actingRoles, product, enabled, filters]);
  const onScope = useCallback(
    (sc: QueryScope) =>
      setFilters((prev) => {
        const next: ProductFilters = {
          domains: sc.domains,
          tags: sc.tags,
          sensitivities: sc.sensitivities,
          edgeTypes: sc.edgeTypes,
        };
        return prev !== null && JSON.stringify(prev) === JSON.stringify(next) ? prev : next;
      }),
    [],
  );
  return { ...state, scoped, onScope };
}
