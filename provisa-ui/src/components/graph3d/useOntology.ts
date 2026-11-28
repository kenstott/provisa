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

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { useAuth } from "../../context/AuthContext";
import type { GraphData, LayerId, QueryScope } from "./graphModel";
import { dataGraphql, metaReader, type MetaReader } from "../../api/metaGraph";
import { fetchProductLineage } from "../../api/lineage";
import {
  ontologyCountQuery,
  ontologyGraph,
  ontologyQuery,
  overviewGraph,
  memberTablesOf,
  readsTerms,
  termLevels,
  withProductLineage,
  type CountRow,
  type OntologyRows,
} from "./ontologyData";
import {
  tableColumnsField,
  tableColumnsQuery,
  withTableColumns,
  type TableColumnsRow,
} from "./tableColumns";
import { readOntology } from "./ontologyRead";
import { allHopsCypher, drawnCypher, termsCypherGraph } from "./ontologyCypher";
import { NODE_BUDGET } from "./neighbourhood";
import { scopeOf, type OntologyPreset } from "./ontologyPresets";

/** The language a view's query is written in: GraphQL, or -- All hops -- Cypher (REQ-2174,
 * amended 2026-11-26). */
export type QueryLanguage = "graphql" | "cypher";

/** What a read of the ontology came to: the graph and the query that read it, in ``language``, or
 * -- past the budget -- how many terms it would have read, with each term's domains; ``hops``: its
 * root was within the budget, and its hops (All's, or a few hops of a dense part) took it past, so
 * it would draw more than the budget, ``over``. ``walk``: the view's terms as one Cypher query
 * reads them, whatever its Hops (allHopsCypher); null when it reads no term (the Product map's
 * products apart). */
export type Loaded =
  | { data: GraphData; query: string; language: QueryLanguage; walk: Walk | null; over?: undefined }
  | {
      over: number;
      rows: CountRow[];
      query: string;
      language: "graphql";
      walk: Walk | null;
      hops?: true;
    };

/** A view's terms as one Cypher query reads them (allHopsCypher): from ``roots`` -- the terms its
 * root reads: a search's matches, a focus's terms, or with neither every term its filters keep --
 * ``depth`` steps over the links of ``edgeTypes`` (every link type, when none). */
export interface Walk {
  roots: number[];
  depth: number;
  edgeTypes: string[];
}

/** The ontology as ``scope`` reads it, opened as one query (REQ-2136), by the rules it reads by
 * (QueryScope.products): its terms counted first, and read only within the budget (REQ-2143);
 * null when the role cannot read the glossary. */
export async function loadOntology(role: string, scope: QueryScope): Promise<Loaded | null> {
  const m = await metaReader(role);
  const query = ontologyQuery(m, scope);
  // A read of no term (the Product map opened on its products) counts none.
  const terms = readsTerms(scope);
  const count = terms ? ontologyCountQuery(m, scope) : null;
  if (query === null || (terms && count === null)) return null;
  const counted =
    count === null ? [] : (await dataGraphql<{ _terms: CountRow[] }>(count, {}, role))._terms;
  // REQ-2136, REQ-2174: the view as one Cypher query, for every Hops value: from the terms its
  // root reads -- those counted -- walked as far as the view reads.
  const walk = (depth: number): Walk | null =>
    terms ? { roots: counted.map((r) => r.id), depth, edgeTypes: [...scope.edgeTypes] } : null;
  // Past the budget at the root, the view draws the terms counted alone (the overview's).
  if (counted.length > NODE_BUDGET)
    return { over: counted.length, rows: counted, query, language: "graphql", walk: walk(0) };
  // No term at the root, so no hop past it: a product with no terms (a new one) reads itself
  // alone. Else a search's or a focus's neighbourhood is kept by its root's where, one read; All's
  // is walked a hop at a time, each term once.
  const { rows, opened, over } = await readOntology(
    m,
    scope,
    (q) => dataGraphql<OntologyRows>(q, {}, role),
    counted.length > 0,
    NODE_BUDGET,
  );
  if (opened === null) return null;
  // A fixed Hops value is its own bound (none without a search or a focus); All's, the steps its
  // walk took to the end of the connected part, or as far as it went.
  const levels = termLevels(scope);
  const walked = walk(
    Number.isFinite(levels) ? levels : opened.language === "cypher" ? opened.depth : 0,
  );
  // REQ-2174 (amended 2026-11-12): All follows the connected part to its end under the budget --
  // past it the reads stop, and that it is past is said, as at the root.
  const past = {
    over: NODE_BUDGET,
    rows: counted,
    query,
    language: "graphql",
    walk: walked,
    hops: true,
  } as const;
  if (over) return past;
  // REQ-2136, REQ-2174 (amended 2026-11-27): what the view opens as is the one query that read it,
  // its hops its reach filter; All's, the Cypher variable-length query from its root's terms.
  const data = ontologyGraph(rows, scope);
  if (levels > 0 && data.nodes.length > NODE_BUDGET) return past;
  const said =
    opened.language === "graphql"
      ? { query: opened.query, language: opened.language, walk: walked }
      : {
          query: allHopsCypher(
            await termsCypherGraph(role),
            opened.roots,
            opened.depth,
            scope.edgeTypes,
          ),
          language: opened.language,
          walk: walked,
        };
  // Only the Product map reads products apart, and draws their lineage.
  const products = rows.data_products ?? [];
  if (!scope.products || !scope.layers.has("lineage")) return { data, ...said };
  // REQ-2174 (THE ONE GRAPH'S PRESETS AND LAYERS): under Lineage each product read has its
  // lineage, from the product lineage endpoint (REQ-2156).
  const lineages = await Promise.all(products.map((p) => fetchProductLineage(p.id)));
  return {
    data: products.reduce(
      (g, p, i) => withProductLineage(g, p.id, lineages[i], memberTablesOf(p)),
      data,
    ),
    ...said,
  };
}

// Before the first scoped read: nothing drawn yet.
const EMPTY: GraphData = { nodes: [], edges: [] };

/** Every layer of the preset's rules, no filter: what the view's controls choose from. A layer
 * read only once it is on is offered by the node it hangs off (GraphScope.readWhenOn), so it is
 * not read here; nor is a layer read on asking (GraphScope.readOnAsking) until it is in ``on``. */
function everything(preset: OntologyPreset, on: readonly LayerId[]): QueryScope {
  const scope = scopeOf(preset);
  const asked = (l: LayerId) => !(l in (scope.readOnAsking ?? {})) || on.includes(l);
  return {
    layers: new Set<LayerId>(scope.layerOrder.filter((l) => !(l in scope.readWhenOn) && asked(l))),
    domains: [],
    edgeTypes: [],
    hiddenStatuses: [],
    orphans: true,
    tags: [],
    sensitivities: [],
    search: [],
    focus: preset.focus,
    depth: preset.depth,
    products: preset.products,
  };
}

/** The view's terms as one Cypher query, written when it is asked for (Open as query): the walk
 * the view's read states (``walk``), or the terms a focus draws within it (``drawn``). ``key``
 * names the walk, so a query written is the view's while the key holds. */
export interface ViewCypher {
  key: string;
  walk: () => Promise<string>;
  drawn: (terms: readonly number[]) => Promise<string>;
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
  // The scope ``scoped`` answers. A new scope (a search typed) is read a hop at a time; until those
  // reads are in, ``scoped`` is still the scope before's, drawn but no longer the view's query.
  const [scopedFor, setScopedFor] = useState<QueryScope | null>(null);
  const [changes, setChanges] = useState(0);
  // The tables the view has expanded, and the columns read of each beyond those its graph read
  // (a term's graph reads only the columns its terms reach): read once on asking, kept while the
  // ontology is unchanged, drawn while the table is expanded.
  const [expandedTables, setExpandedTables] = useState<ReadonlySet<string>>(new Set());
  const [tableColumns, setTableColumns] = useState<ReadonlyMap<string, TableColumnsRow[]>>(
    new Map(),
  );
  const asking = useRef(new Set<string>());
  const [columnsReader, setColumnsReader] = useState<MetaReader | null>(null);
  const { focus, depth, products } = preset;
  // The layers read on asking that the view has on: what the controls choose from grows with them.
  const asked = scopeOf(preset).readOnAsking ?? {};
  const askedOn = [...(scope?.layers ?? [])]
    .filter((l) => l in asked)
    .sort()
    .join();
  useEffect(() => {
    const changed = () => {
      setChanges((n) => n + 1);
      setTableColumns(new Map());
      asking.current.clear();
    };
    window.addEventListener("provisa:glossary-changed", changed);
    return () => window.removeEventListener("provisa:glossary-changed", changed);
  }, []);
  useEffect(() => {
    if (!enabled || actingRoles === null) return;
    let cancelled = false;
    const on = (askedOn === "" ? [] : askedOn.split(",")) as LayerId[];
    loadOntology(actingRoles, everything({ focus, depth, products }, on)).then(
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
  }, [enabled, actingRoles, said, changes, focus, depth, products, askedOn]);
  useEffect(() => {
    if (!enabled || actingRoles === null || scope === null) return;
    let cancelled = false;
    loadOntology(actingRoles, scope).then(
      (d) => {
        if (cancelled) return;
        setScoped(d);
        setScopedFor(scope);
      },
      (e: unknown) =>
        !cancelled && setRead((r) => ({ ...r, error: e instanceof Error ? e.message : String(e) })),
    );
    return () => {
      cancelled = true;
    };
  }, [enabled, actingRoles, scope, changes]);
  // A table expanded with more columns than were read is read for them: that table alone.
  const readData = scoped !== null && scoped.over === undefined ? scoped.data : null;
  useEffect(() => {
    if (!enabled || actingRoles === null || readData === null) return;
    for (const table of expandedTables) {
      if (tableColumns.has(table) || asking.current.has(table)) continue;
      const node = readData.nodes.find((n) => n.id === table);
      if (node?.columns === undefined) continue;
      const had = readData.nodes.filter((n) => n.type === "column" && n.parent === table).length;
      if (node.columns <= had) continue;
      asking.current.add(table);
      metaReader(actingRoles)
        .then(async (m) => {
          const q = tableColumnsQuery(m, table);
          if (q === null) throw new Error(`the role cannot read ${table}'s columns`);
          const rows = (
            await dataGraphql<{ registered_tables: TableColumnsRow[] }>(q, {}, actingRoles)
          ).registered_tables;
          setColumnsReader(m);
          setTableColumns((prev) => new Map(prev).set(table, rows));
        })
        .catch((e: unknown) =>
          setRead((r) => ({ ...r, error: e instanceof Error ? e.message : String(e) })),
        )
        .finally(() => asking.current.delete(table));
    }
  }, [enabled, actingRoles, readData, expandedTables, tableColumns]);
  const onExpanded = useCallback((tables: ReadonlySet<string>) => setExpandedTables(tables), []);
  // The view's controls say what to read; the preset says by which rules.
  const onScope = useCallback((s: QueryScope) => setScope({ ...s, products }), [products]);
  // REQ-2143: past the budget, the whole ontology unsearched is its overview -- each domain with
  // its count, picked to narrow to it -- and a search, a focus or a domain still past it is said.
  const overview =
    scoped?.over !== undefined &&
    scope !== null &&
    scope.search.length === 0 &&
    scope.focus === null &&
    scope.domains.length === 0;
  // The columns read on asking are drawn while their table is expanded, and read with the rest.
  const shownKey = [...expandedTables]
    .filter((t) => tableColumns.has(t))
    .sort()
    .join();
  const shownTables = useMemo(() => (shownKey === "" ? [] : shownKey.split(",")), [shownKey]);
  const withColumns = useMemo(
    () =>
      readData === null || shownTables.length === 0
        ? readData
        : withTableColumns(
            readData,
            shownTables.flatMap((t) => tableColumns.get(t) ?? []),
          ),
    [readData, shownTables, tableColumns],
  );
  const drawn: GraphData =
    scoped === null
      ? EMPTY
      : withColumns !== null
        ? withColumns
        : overview && scoped.over !== undefined
          ? overviewGraph(scoped.rows, said)
          : EMPTY;
  const columnsField =
    shownTables.length === 0 || columnsReader === null
      ? null
      : tableColumnsField(columnsReader, shownTables);
  // REQ-2136: Open as query hands over the query of the scope now asked for, never the one before:
  // none while that scope's read is in flight.
  // A Cypher query (All hops) reads no columns on asking: they are a GraphQL root of their own.
  const text =
    scoped?.query === undefined || scopedFor !== scope
      ? null
      : columnsField === null || scoped.language === "cypher"
        ? scoped.query
        : scoped.query.replace(/\n}$/, `\n  ${columnsField}\n}`);
  // The language ``text`` is written in; none while there is no text.
  const language: QueryLanguage | null = text === null || scoped === null ? null : scoped.language;
  // REQ-2136, REQ-2174: the view opens as Cypher in every state: its terms walked as its read
  // walks them, written with the server's label and link types when it is asked for.
  const walk = text === null || scoped === null ? null : scoped.walk;
  const walkKey = walk === null ? null : JSON.stringify(walk);
  const cypher = useMemo((): ViewCypher | null => {
    if (walkKey === null || actingRoles === null) return null;
    const w = JSON.parse(walkKey) as Walk;
    return {
      key: walkKey,
      walk: async () =>
        allHopsCypher(await termsCypherGraph(actingRoles), w.roots, w.depth, w.edgeTypes),
      drawn: async (terms) => drawnCypher(await termsCypherGraph(actingRoles), terms, w.edgeTypes),
    };
  }, [walkKey, actingRoles]);
  const notice =
    scoped?.over !== undefined && !overview
      ? t(scoped.hops ? "graph3d.overBudgetHops" : "graph3d.overBudget", { count: scoped.over })
      : null;
  const query = (all: GraphData) => ({
    all,
    onScope,
    onExpanded,
    text,
    language,
    cypher,
    notice,
    overview,
    ...(focus === null ? {} : { own: focus }),
  });
  return { read, drawn, query };
}
