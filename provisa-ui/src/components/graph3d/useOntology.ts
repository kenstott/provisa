// Copyright (c) 2026 Kenneth Stott
// Canary: a83c5e17-2d96-4f4b-9c71-0e6b8d2f5a94
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2174 (ONE ONTOLOGY GRAPH): the one graph's data around a preset, read from the ontology
// schema as the user (ontologyData.ts): every layer unfiltered, for the view's controls, and what
// the view's controls keep, read afresh as they change.

import { useCallback, useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { useAuth } from "../../context/AuthContext";
import { ONTOLOGY_SCOPE, type GraphData, type LayerId, type QueryScope } from "./graphModel";
import { dataGraphql, metaReader } from "../../api/metaGraph";
import { fetchProductLineage } from "../../api/lineage";
import {
  flatTerms,
  ontologyCountQuery,
  ontologyGraph,
  ontologyQuery,
  overviewGraph,
  termEdges,
  withProductLineage,
  type CountRow,
  type OntologyRows,
} from "./ontologyData";
import { NODE_BUDGET } from "./neighbourhood";
import type { OntologyPreset } from "./ontologyPresets";

/** What a read of the ontology came to: the graph and the query that read it, or -- past the
 * budget -- how many terms it would have read, with each term's domains. */
export type Loaded =
  | { data: GraphData; query: string; over?: undefined }
  | { over: number; rows: CountRow[]; query: string };

/** The ontology as ``scope`` reads it, in one query (REQ-2136): its terms counted first, and read
 * only within the budget (REQ-2143); null when the role cannot read the glossary. */
export async function loadOntology(role: string, scope: QueryScope): Promise<Loaded | null> {
  const m = await metaReader(role);
  const query = ontologyQuery(m, scope);
  const count = ontologyCountQuery(m, scope);
  if (query === null || count === null) return null;
  const counted = (await dataGraphql<{ _terms: CountRow[] }>(count, {}, role))._terms;
  if (counted.length > NODE_BUDGET) return { over: counted.length, rows: counted, query };
  // No term at the root, so none nested under it: a product with no terms (a new one) reads
  // itself alone, not the empty neighbourhood's nested edges -- seconds of server time.
  const read = counted.length === 0 ? ontologyQuery(m, scope, false) : query;
  const rows: OntologyRows =
    read === null ? { _terms: [] } : await dataGraphql<OntologyRows>(read, {}, role);
  // A search's or a focus's neighbourhood is read nested, a level to each hop: each term once.
  const terms = flatTerms(rows._terms ?? []);
  const products = rows.data_products ?? [];
  const data = ontologyGraph(terms, termEdges(terms), products);
  if (!scope.layers.has("lineage")) return { data, query };
  // REQ-2174 (THE ONE GRAPH'S PRESETS AND LAYERS): under Lineage each product read has its
  // lineage, from the product lineage endpoint (REQ-2156).
  const lineages = await Promise.all(products.map((p) => fetchProductLineage(p.id)));
  return {
    data: products.reduce(
      (g, p, i) => withProductLineage(g, p.id, lineages[i], p.member_tables ?? []),
      data,
    ),
    query,
  };
}

// Before the first scoped read: nothing drawn yet.
const EMPTY: GraphData = { nodes: [], edges: [] };

/** A snippet's layers: the terms, the columns they bind, those columns' tables and the products
 * holding them. */
export const SNIPPET_LAYERS: LayerId[] = ["ontology", "columns", "ownerTables", "data"];

/** Every layer, no filter: what the view's controls choose from. A layer read only once it is on
 * is offered by the node it hangs off (ONTOLOGY_SCOPE.readWhenOn), so it is not read here. */
function everything(preset: OntologyPreset): QueryScope {
  return {
    layers: new Set<LayerId>(
      ONTOLOGY_SCOPE.layerOrder.filter((l) => !(l in ONTOLOGY_SCOPE.readWhenOn)),
    ),
    domains: [],
    edgeTypes: [],
    hiddenStatuses: [],
    orphans: true,
    tags: [],
    sensitivities: [],
    search: [],
    focus: preset.focus,
    depth: preset.depth,
  };
}

/** What a read came to, and how the view says it. */
export interface Read {
  /** Every layer, no filter: what the controls choose from; null when the role cannot read it. */
  all: GraphData | null;
  error: string | null;
  done: boolean;
}

/**
 * The ontology around ``preset``, read once ``enabled``: ``all`` with no filter (what a view's
 * controls choose from) and, read afresh on each ``onScope``, what the view's controls keep, with
 * the query that read it. Polly's glossary tools change the ontology server-side (ChatPanel's
 * provisa:glossary-changed): it is read again then.
 */
export function useOntology(preset: OntologyPreset, enabled: boolean) {
  const { t } = useTranslation();
  const { actingRoles } = useAuth();
  const said = useCallback(
    (domain: string, count: number) =>
      t("graph3d.domainCount", {
        domain: domain === "*" ? t("glossaryTab.enterpriseDomainLabel") : domain,
        count,
      }),
    [t],
  );
  const [read, setRead] = useState<Read>({ all: null, error: null, done: false });
  const [scope, setScope] = useState<QueryScope | null>(null);
  const [scoped, setScoped] = useState<Loaded | null>(null);
  const [changes, setChanges] = useState(0);
  const { focus, depth } = preset;
  useEffect(() => {
    const changed = () => setChanges((n) => n + 1);
    window.addEventListener("provisa:glossary-changed", changed);
    return () => window.removeEventListener("provisa:glossary-changed", changed);
  }, []);
  useEffect(() => {
    if (!enabled || actingRoles === null) return;
    let cancelled = false;
    loadOntology(actingRoles, everything({ focus, depth })).then(
      (all) =>
        !cancelled &&
        setRead({
          // Past the budget the choices are the overview's: the domains.
          all:
            all === null ? null : all.over === undefined ? all.data : overviewGraph(all.rows, said),
          error: null,
          done: true,
        }),
      (e: unknown) =>
        !cancelled &&
        setRead({ all: null, error: e instanceof Error ? e.message : String(e), done: true }),
    );
    return () => {
      cancelled = true;
    };
  }, [enabled, actingRoles, said, changes, focus, depth]);
  useEffect(() => {
    if (!enabled || actingRoles === null || scope === null) return;
    let cancelled = false;
    loadOntology(actingRoles, scope).then(
      (d) => !cancelled && setScoped(d),
      (e: unknown) =>
        !cancelled && setRead((r) => ({ ...r, error: e instanceof Error ? e.message : String(e) })),
    );
    return () => {
      cancelled = true;
    };
  }, [enabled, actingRoles, scope, changes]);
  const onScope = useCallback((s: QueryScope) => setScope(s), []);
  // REQ-2143: past the budget, the whole ontology unsearched is its overview -- each domain with
  // its count, picked to narrow to it -- and a search, a focus or a domain still past it is said.
  const overview =
    scoped?.over !== undefined &&
    scope !== null &&
    scope.search.length === 0 &&
    scope.focus === null &&
    scope.domains.length === 0;
  const drawn: GraphData =
    scoped === null
      ? EMPTY
      : scoped.over === undefined
        ? scoped.data
        : overview
          ? overviewGraph(scoped.rows, said)
          : EMPTY;
  const notice =
    scoped?.over !== undefined && !overview
      ? t("graph3d.overBudget", { count: scoped.over })
      : null;
  const query = (all: GraphData) => ({
    all,
    onScope,
    text: scoped?.query ?? null,
    notice,
    overview,
    ...(focus === null ? {} : { own: focus }),
  });
  return { read, drawn, query };
}
