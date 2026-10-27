// Copyright (c) 2026 Kenneth Stott
// Canary: f4a2c9b7-3e1d-4f5a-8b2e-7c6d1a9f4e3b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState, useCallback, useRef, useEffect, useMemo } from "react";
import { useNavPayload } from "../hooks/useNavPayload";
import { useTranslation } from "react-i18next";
import { Text } from "@mantine/core";
import { useDomainFilter } from "../context/DomainFilterContext";
import { GraphFrame } from "../components/graph/GraphFrame";
import {
  type FrameData,
  type FrameView,
  type GraphView,
  type RelLineOverride,
  type GNode,
  type GEdge,
} from "../components/graph/graph-model";
import {
  useRelationships,
  useAllRelationships,
  useUpsertRelationship,
  useTables,
} from "../hooks/useAdminQueries";
import { tableRef } from "../components/relationships/relationship-types";
import { useAuth } from "../context/AuthContext";
import { uuid } from "../lib/uuid";
import "./GraphPage.css";
import {
  useLocalStorage,
  graphStateOf,
  saveGraphState,
  settingKey,
  type GraphLanguage,
} from "../components/graph/graph-persistence";
import {
  addPattern,
  compactIri,
  freshVariable,
  sparqlStatement,
  withPrefixes,
  SPARQL_ACCEPT,
  type SparqlVocabulary,
} from "../components/graph/sparql";
import {
  SparqlVocabularyPanel,
  type VocabularyPick,
} from "../components/graph/SparqlVocabularyPanel";
import type { Favorite } from "../components/graph/graph-persistence";
import type {
  SchemaNodeLabel,
  SchemaRel,
  CypherSchema,
  GraphCounts,
} from "../components/graph/graph-schema-types";
import { Sidebar } from "../components/graph/GraphSidebar";
import { QueryBar } from "../components/graph/QueryBar";
import { NativeFilterModal } from "../components/graph/graph-context-menus";
import { Neo4jExportModal } from "../components/graph/Neo4jExportModal";
import { buildDropExpansion, withExpansion } from "./graph-drop";
import { getStored, removeStored, setStored } from "../lib/browserStorage";
import { useMaxNodes } from "../components/graph/graph-drawing";
import {
  cypherError,
  frameStatement,
  showing,
  type CypherAnswer,
} from "../components/graph/frameStatements";

// ── Main page ─────────────────────────────────────────────────────────────────
/** A refused request's error: its body's ``detail`` or ``error`` when it is JSON that has one, else
 *  the body as sent. */
function refusalText(body: string): string {
  let parsed: unknown;
  try {
    parsed = JSON.parse(body);
  } catch {
    return body;
  }
  if (parsed !== null && typeof parsed === "object") {
    const said =
      (parsed as { detail?: unknown; error?: unknown }).detail ??
      (parsed as { error?: unknown }).error;
    if (typeof said === "string") return said;
    if (said !== undefined) return JSON.stringify(said);
  }
  return body;
}

export function GraphPage({ language = "cypher" }: { language?: GraphLanguage } = {}) {
  const { t } = useTranslation();
  // REQ-2135: Cypher and SPARQL share this page, each with its own frames, history, favorites and
  // settings; SPARQL's vocabulary stands where Cypher's labels are.
  const graphState = graphStateOf(language);
  const sparql = language === "sparql";
  const { role, selectedRoles, loading: authLoading } = useAuth();
  // REQ-1620: under "Role: All" the acting set is every active role, sent as one
  // comma-separated X-Provisa-Role so the server unions their domain_access — sending only
  // role.id (the first of the set) silently narrowed every graph-page request to one role.
  const roleHeaderValue =
    selectedRoles.length > 0 ? selectedRoles.map((r) => r.id).join(",") : role?.id;
  const { checkedDomains } = useDomainFilter();
  // A query forwarded from NL "Open in Cypher" or the guided tour, captured ONCE at mount.
  // Consumed after auth resolves so it runs exactly once with the correct role (see below).
  // REQ-1472: the localStorage handoff is READ here but deleted only where it is actually run.
  // OnboardGate renders PageLoading instead of its children while /auth/me is in flight, so this
  // page can mount, unmount and remount before the run effect ever fires; deleting the key at
  // mount lost the query on that remount and the frame never appeared at all.
  // A query to run: the guided tour's localStorage hand-off at mount, then one per navigation
  // hand-off (useNavPayload below), whether the page was just opened or already open. Each has its
  // own sequence number, and the run effect runs each one once.
  const [forwarded, setForwarded] = useState<{
    query: string;
    seq: number;
    view?: FrameView;
  } | null>(() => {
    const pending = getStored(settingKey(language, "pending_query"));
    return pending ? { query: pending, seq: 0 } : null;
  });
  const ranSeqRef = useRef<number | null>(null);
  // The frame the last forwarded query runs in.
  const [handoffFrameId, setHandoffFrameId] = useState<string | null>(null);
  const [frames, setFrames] = useState<FrameData[]>(graphState.frames);
  const [history, setHistory] = useState<string[]>(graphState.history);
  const [historyQuery, setHistoryQuery] = useState<string | null>(null);
  // A view a navigation asks for, by frame: applied by the frame once per seq.
  const [frameViews, setFrameViews] = useState<Record<string, FrameView>>({});
  const viewSeqRef = useRef(0);
  useNavPayload<{ query?: string; autoRun?: boolean; view?: GraphView }>((payload) => {
    const { maxNodes: requestedMax, ...frameView } = payload.view ?? {};
    if (requestedMax !== undefined) setMaxNodes(requestedMax);
    const view: FrameView | undefined =
      Object.keys(frameView).length > 0 ? { ...frameView, seq: ++viewSeqRef.current } : undefined;
    if (payload.query) {
      const query = payload.query;
      if (payload.autoRun) setForwarded((prev) => ({ query, seq: (prev?.seq ?? 0) + 1, view }));
      else setHistoryQuery(query);
      return;
    }
    // No query: the view is for the frame on screen, the newest one, and its query is kept.
    const newest = framesRef.current[0];
    if (view && newest) setFrameViews((views) => ({ ...views, [newest.id]: view }));
  });
  const [schemaNodeLabels, setSchemaNodeLabels] = useState<SchemaNodeLabel[]>([]);
  const [schemaRels, setSchemaRels] = useState<SchemaRel[]>([]);
  // The viewer's max-nodes setting: the most distinct nodes a frame draws.
  const [maxNodes, setMaxNodes] = useMaxNodes();
  const [schemaLoading, setSchemaLoading] = useState(true);
  // Why /data/graph-schema gave no schema (a refused request, or none reached); null when it did.
  const [schemaError, setSchemaError] = useState<string | null>(null);
  // What /data/graph-counts last said for this role and domain selection; null until it has.
  const [counts, setCounts] = useState<GraphCounts | null>(null);
  // Why /data/graph-counts gave no counts (a refused request, or none reached); null when it did.
  const [countsError, setCountsError] = useState<string | null>(null);
  const [sidebarWidth, setSidebarWidth] = useLocalStorage<number>(
    settingKey(language, "sidebarWidth"),
    240,
  );
  const [activeLabel, setActiveLabel] = useState<string | null>(null);
  const [colorOverrides, setColorOverrides] = useLocalStorage<Record<string, string>>(
    settingKey(language, "colorOverrides"),
    {},
  );
  const [sizeOverrides, setSizeOverrides] = useLocalStorage<Record<string, number>>(
    settingKey(language, "sizeOverrides"),
    {},
  );
  const [labelProperty, setLabelProperty] = useLocalStorage<Record<string, string>>(
    settingKey(language, "labelProperty"),
    {},
  );
  const [sizeByProperty, setSizeByProperty] = useLocalStorage<Record<string, string>>(
    settingKey(language, "sizeByProperty"),
    {},
  );
  const [sizeMultiplier, setSizeMultiplier] = useLocalStorage<Record<string, number>>(
    settingKey(language, "sizeMultiplier"),
    {},
  );
  const [autoImpute, setAutoImpute] = useLocalStorage<boolean>(
    settingKey(language, "autoImpute"),
    false,
  );
  const [favorites, setFavorites] = useLocalStorage<Favorite[]>(
    settingKey(language, "favorites"),
    [],
  );
  const [relLineOverrides, setRelLineOverrides] = useLocalStorage<Record<string, RelLineOverride>>(
    settingKey(language, "relLineOverrides"),
    {},
  );
  const { relationships: adminRels, refetch: refetchRelationships } = useRelationships();
  // Graph traversal (drag-drop discovery, expand/collapse, exclude-node rewriting) needs the full
  // relationship set including meta:%-prefixed system relationships (e.g. HAS_TABLE_COLUMNS) — the
  // same set Auto Impute Relationships already queries server-side. `adminRels` stays scoped to the
  // `relationships` GraphQL field (meta:% excluded) because it also backs relationship-alias editing,
  // where system-generated rows must stay read-only.
  const { relationships: graphRels } = useAllRelationships();
  const { upsertRelationship } = useUpsertRelationship();
  // The name a relationship's save takes for each table: its alias, else its table name.
  const { tables } = useTables();
  const [nfModal, setNfModal] = useState<{
    label: string;
    filterColumns: { name: string; type: string }[];
    onConfirm: (params: Record<string, string>) => void;
  } | null>(null);
  const [showNeo4jModal, setShowNeo4jModal] = useState(false);
  const effectiveDataRef = useRef<
    Map<string, { nodes: Map<string, GNode>; edges: Map<string, GEdge> }>
  >(new Map());
  const [numericPropsByLabel, setNumericPropsByLabel] = useState<Record<string, string[]>>({});
  const onEffectiveDataChange = useCallback(
    (frameId: string, nodes: Map<string, GNode>, edges: Map<string, GEdge>) => {
      effectiveDataRef.current.set(frameId, { nodes, edges });
      const acc: Record<string, Set<string>> = {};
      effectiveDataRef.current.forEach((frame) => {
        frame.nodes.forEach((n) => {
          const label = n.tableLabel;
          if (!acc[label]) acc[label] = new Set();
          Object.entries(n.properties).forEach(([k, v]) => {
            if (typeof v === "number") acc[label].add(k);
          });
        });
      });
      setNumericPropsByLabel(Object.fromEntries(Object.entries(acc).map(([k, v]) => [k, [...v]])));
    },
    [],
  );
  const clusterMapRef = useRef<
    Record<string, { scl1: number | null; scl2: number | null; scl3: number | null }>
  >({});

  const handleAddFavorite = useCallback(
    (query: string) => {
      const trimmed = query.trim();
      setFavorites((prev) => {
        const existing = prev.find((f) => f.query === trimmed);
        if (existing) return prev.filter((f) => f.query !== trimmed);
        const label = trimmed.split("\n")[0].slice(0, 50);
        const id = `${Date.now()}-${Math.random().toString(36).slice(2, 7)}`;
        return [...prev, { id, query: trimmed, label, ts: Date.now() }];
      });
    },
    [setFavorites],
  );

  const handleFavoriteDelete = useCallback(
    (id: string) => {
      setFavorites((prev) => prev.filter((f) => f.id !== id));
    },
    [setFavorites],
  );

  const isFavorited = useCallback(
    (query: string) => {
      return favorites.some((f) => f.query === query.trim());
    },
    [favorites],
  );

  // REQ-2135: SPARQL's vocabulary, for the role (read again when the role changes).
  const [vocabulary, setVocabulary] = useState<SparqlVocabulary | null>(null);
  const [vocabularyError, setVocabularyError] = useState<string | null>(null);
  useEffect(() => {
    if (!sparql) return;
    const headers: Record<string, string> = {};
    if (roleHeaderValue) headers["X-Provisa-Role"] = roleHeaderValue;
    let cancelled = false;
    fetch("/data/sparql/vocabulary", { headers })
      .then(async (r) => {
        // A refused request's body is its error, never a vocabulary.
        if (!r.ok) throw new Error(refusalText(await r.text()));
        return r.json() as Promise<SparqlVocabulary>;
      })
      .then(
        (v) => !cancelled && (setVocabulary(v), setVocabularyError(null)),
        (e: unknown) =>
          !cancelled && setVocabularyError(e instanceof Error ? e.message : String(e)),
      );
    return () => {
      cancelled = true;
    };
  }, [sparql, roleHeaderValue]);

  // Fetch schema when role changes via dedicated graph-schema endpoint
  useEffect(() => {
    if (sparql) return;
    setSchemaLoading(true);
    const headers: Record<string, string> = {};
    if (roleHeaderValue) headers["X-Provisa-Role"] = roleHeaderValue;
    fetch("/data/graph-schema", { headers })
      .then(async (r) => {
        // A refused request's body is its error, never a schema.
        if (!r.ok) throw new Error(refusalText(await r.text()));
        return r.json();
      })
      .then((data) => {
        const nodeLabels: SchemaNodeLabel[] = (data.node_labels ?? []).map(
          (n: {
            label: string;
            domain_label: string | null;
            domain_id: string | null;
            table_label: string;
            table_id: number;
            properties: string[];
            pk_columns: string[];
            id_column?: string;
            native_filter_columns?: { name: string; type: string }[];
            property_types?: Record<string, string>;
            scl1?: number | null;
            scl2?: number | null;
            scl3?: number | null;
          }) => ({
            domainLabel: n.domain_label ?? null,
            domainId: n.domain_id ?? null,
            tableLabel: n.table_label,
            tableId: n.table_id,
            properties: n.properties ?? [],
            pkColumns: n.pk_columns ?? [],
            idColumn: n.id_column ?? null,
            nativeFilterColumns: n.native_filter_columns ?? [],
            propertyTypes: n.property_types ?? {},
            scl1: n.scl1 ?? null,
            scl2: n.scl2 ?? null,
            scl3: n.scl3 ?? null,
          }),
        );
        const seenRel = new Set<string>();
        const rels: SchemaRel[] = (data.relationship_types ?? [])
          .filter((r: { type: string; source: string; target: string }) => {
            const key = `${r.type}::${r.source ?? ""}→${r.target ?? ""}`;
            if (seenRel.has(key)) return false;
            seenRel.add(key);
            return true;
          })
          .map((r: { type: string; source: string; target: string; properties?: string[] }) => ({
            type: r.type,
            source: r.source ?? "",
            target: r.target ?? "",
            properties: r.properties ?? [],
          }));
        const seen = new Set<string>();
        const uniqueNodeLabels = nodeLabels.filter((n) => {
          const key = n.domainLabel ? `${n.domainLabel}:${n.tableLabel}` : n.tableLabel;
          if (seen.has(key)) return false;
          seen.add(key);
          return true;
        });
        const newClusterMap: Record<
          string,
          { scl1: number | null; scl2: number | null; scl3: number | null }
        > = {};
        for (const node of uniqueNodeLabels) {
          const entry = { scl1: node.scl1, scl2: node.scl2, scl3: node.scl3 };
          newClusterMap[node.tableLabel] = entry;
          if (node.domainLabel) newClusterMap[`${node.domainLabel}:${node.tableLabel}`] = entry;
        }
        clusterMapRef.current = newClusterMap;
        setSchemaNodeLabels(uniqueNodeLabels);
        setSchemaRels(rels.sort((a, b) => a.type.localeCompare(b.type)));
        setSchemaError(null);
      })
      .catch((e: unknown) => {
        setSchemaNodeLabels([]);
        setSchemaRels([]);
        setSchemaError(e instanceof Error ? e.message : String(e));
      })
      .finally(() => setSchemaLoading(false));
  }, [roleHeaderValue, sparql]);

  useEffect(() => {
    if (schemaNodeLabels.length === 0 && schemaRels.length === 0) return;
    const headers: Record<string, string> = {};
    if (roleHeaderValue) headers["X-Provisa-Role"] = roleHeaderValue;
    const domainsKey = [...checkedDomains].sort().join(",");
    // v3: only counts are stored (a v2 entry could hold a refused request's error body).
    const countsKey = `provisa.graph.counts.v3.${roleHeaderValue ?? "default"}.${domainsKey}`;
    try {
      const cached = getStored(countsKey);
      setCounts(cached ? (JSON.parse(cached) as GraphCounts) : null);
    } catch {
      setCounts(null);
    }
    const domainsParam = checkedDomains.size > 0 ? `?domains=${[...checkedDomains].join(",")}` : "";
    fetch(`/data/graph-counts${domainsParam}`, { headers })
      .then(async (r) => {
        // A refused request's body is its error, never counts: shown, and not stored.
        if (!r.ok) throw new Error(refusalText(await r.text()));
        return (await r.json()) as GraphCounts;
      })
      .then((data) => {
        setCounts(data);
        setCountsError(null);
        try {
          setStored(countsKey, JSON.stringify(data));
        } catch {
          /* quota */
        }
      })
      .catch((e: unknown) => {
        setCounts(null);
        setCountsError(e instanceof Error ? e.message : String(e));
      });
  }, [schemaNodeLabels, schemaRels, roleHeaderValue, checkedDomains]);

  // Run ``query`` and put what it answered in frame ``id``: one result per statement it holds.
  const answer = useCallback(
    async (id: string, query: string, start: number) => {
      if (sparql) {
        // REQ-2135: one SPARQL query, answered in the form it asks for (rows, or triples drawn).
        const hdrs: Record<string, string> = {
          "Content-Type": "application/sparql-query",
          Accept: SPARQL_ACCEPT,
        };
        if (roleHeaderValue) hdrs["X-Provisa-Role"] = roleHeaderValue;
        let statement;
        try {
          const res = await fetch("/data/sparql", { method: "POST", headers: hdrs, body: query });
          statement = await sparqlStatement(query, res, vocabulary?.prefixes ?? {});
        } catch (err) {
          statement = null;
          const message = String(err);
          setFrames((f) => {
            const next = f.map((fr) =>
              fr.id === id ? { ...fr, status: "error" as const, error: message } : fr,
            );
            graphState.frames = next;
            saveGraphState(graphState);
            return next;
          });
        }
        if (statement === null) return;
        const elapsed = Date.now() - start;
        const shown = statement;
        setFrames((f) => {
          const next = f.map((fr) => (fr.id === id ? { ...fr, ...showing(shown), elapsed } : fr));
          graphState.frames = next;
          saveGraphState(graphState);
          return next;
        });
        return;
      }
      try {
        const hdrs: Record<string, string> = { "Content-Type": "application/json" };
        if (roleHeaderValue) hdrs["X-Provisa-Role"] = roleHeaderValue;
        hdrs["X-Provisa-Stats"] = "true";
        // The box may hold several statements, as Neo4j Browser's editor does: the server splits
        // them and answers each on its own, in order; the frame shows one at a time.
        const res = await fetch("/data/cypher", {
          method: "POST",
          headers: hdrs,
          body: JSON.stringify({ query, params: {}, results: "each" }),
        });
        const elapsed = Date.now() - start;
        if (!res.ok) {
          const text = await res.text();
          let refused: { error: string; violations?: string[] };
          try {
            refused = cypherError(JSON.parse(text), res.status);
          } catch {
            refused = { error: text }; // no JSON body (a proxy's page): the response as it came
          }
          setFrames((f) => {
            const next = f.map((fr) =>
              fr.id === id
                ? { ...fr, status: "error" as const, violations: undefined, ...refused }
                : fr,
            );
            graphState.frames = next;
            saveGraphState(graphState);
            return next;
          });
          return;
        }
        const { results } = (await res.json()) as { results: CypherAnswer[] };
        const statements = results.map((r) => frameStatement(r, maxNodes, clusterMapRef.current));
        setFrames((f) => {
          const next = f.map((fr) =>
            fr.id === id
              ? statements.length === 0
                ? { ...fr, status: "done" as const, elapsed }
                : {
                    ...fr,
                    ...showing(statements[0]),
                    elapsed,
                    statements: statements.length > 1 ? statements : undefined,
                    statementIndex: 0,
                  }
              : fr,
          );
          graphState.frames = next;
          saveGraphState(graphState);
          return next;
        });
      } catch (err) {
        setFrames((f) => {
          const next = f.map((fr) =>
            fr.id === id ? { ...fr, status: "error" as const, error: String(err) } : fr,
          );
          graphState.frames = next;
          saveGraphState(graphState);
          return next;
        });
      }
    },
    [roleHeaderValue, maxNodes, sparql, vocabulary, graphState],
  );

  // One statement's result, of the frame's, shown: its drawing, rows and error.
  const showStatement = useCallback((id: string, index: number) => {
    setFrames((f) => {
      const next = f.map((fr) =>
        fr.id === id && fr.statements
          ? { ...fr, ...showing(fr.statements[index]), statementIndex: index }
          : fr,
      );
      graphState.frames = next;
      saveGraphState(graphState);
      return next;
    });
  }, [graphState]);

  const runQuery = useCallback(
    async (query: string, view?: FrameView, frameId?: string) => {
      if (!query) return;
      const id = frameId ?? uuid();
      if (view) setFrameViews((views) => ({ ...views, [id]: view }));
      const start = Date.now();
      setFrames((f) => {
        const next = [
          {
            id,
            query,
            status: "loading" as const,
            nodes: new Map(),
            edges: new Map(),
            rows: [],
            columns: [],
          },
          ...f,
        ];
        graphState.frames = next;
        saveGraphState(graphState);
        return next;
      });
      setHistory((h) => {
        const next = [query, ...h.filter((q) => q !== query).slice(0, 49)];
        graphState.history = next;
        saveGraphState(graphState);
        return next;
      });
      await answer(id, query, start);
    },
    [answer, graphState],
  );

  // Auto-execute a query forwarded from another page (e.g. NL "Open in Cypher", or the Cypher panel).
  useEffect(() => {
    // Run a query forwarded from NL "Open in Cypher" / the guided tour. `runQuery` depends
    // on `role`, which resolves asynchronously — running on mount would fire with no role
    // (wrong/empty result) and again when role loads (a duplicate frame). Wait until auth
    // has resolved, then run exactly once with the correct role.
    if (authLoading || !forwarded || ranSeqRef.current === forwarded.seq) return;
    ranSeqRef.current = forwarded.seq;
    removeStored(settingKey(language, "pending_query"));
    setHistoryQuery(forwarded.query);
    const frameId = uuid();
    setHandoffFrameId(frameId);
    runQuery(forwarded.query, forwarded.view, frameId);
  }, [runQuery, authLoading, forwarded, language]);

  const closeFrame = useCallback((id: string) => {
    setFrames((f) => {
      const next = f.filter((fr) => fr.id !== id);
      graphState.frames = next;
      saveGraphState(graphState);
      return next;
    });
  }, [graphState]);

  const handlePin = useCallback((id: string) => {
    setFrames((f) => {
      const next = f.map((fr) => (fr.id === id ? { ...fr, pinned: !fr.pinned } : fr));
      graphState.frames = next;
      saveGraphState(graphState);
      return next;
    });
  }, [graphState]);

  // pkMap covers ALL schema nodes — pk lookup is independent of the domain visibility filter
  const { pkMap, labelToTableLabel, labelToTableId } = useMemo(() => {
    const pk: Record<string, string[]> = {};
    const l2t: Record<string, string> = {};
    const l2id: Record<string, number> = {};
    for (const node of schemaNodeLabels) {
      const compoundLabel = node.domainLabel
        ? `${node.domainLabel}:${node.tableLabel}`
        : node.tableLabel;
      pk[compoundLabel] =
        node.pkColumns.length > 0 ? node.pkColumns : node.idColumn ? [node.idColumn] : [];
      l2t[compoundLabel] = node.tableLabel;
      l2id[compoundLabel] = node.tableId;
    }
    return { pkMap: pk, labelToTableLabel: l2t, labelToTableId: l2id };
  }, [schemaNodeLabels]);

  const rerunFrame = useCallback(
    async (id: string, query: string) => {
      if (!query) return;
      const start = Date.now();
      setFrames((f) => {
        const next = f.map((fr) =>
          fr.id === id
            ? {
                ...fr,
                query,
                status: "loading" as const,
                nodes: new Map(),
                edges: new Map(),
                rows: [],
                columns: [],
                elapsed: undefined,
                error: undefined,
                redirect: undefined,
                truncated: undefined,
                statements: undefined,
                statementIndex: undefined,
              }
            : fr,
        );
        graphState.frames = next;
        saveGraphState(graphState);
        return next;
      });
      await answer(id, query, start);
    },
    [answer, graphState],
  );

  const framesRef = useRef(frames);
  framesRef.current = frames;

  const NUMERIC_TYPES = new Set([
    "integer",
    "bigint",
    "int",
    "int4",
    "int8",
    "smallint",
    "float",
    "double precision",
    "numeric",
    "decimal",
    "real",
    "float4",
    "float8",
  ]);

  const buildNfWhereClauses = useCallback(
    (
      varName: string,
      filterColumns: { name: string; type: string }[],
      params: Record<string, string>,
    ) =>
      filterColumns
        .filter((col) => params[col.name] !== "")
        .map(({ name, type }) => {
          const v = params[name];
          const lit = NUMERIC_TYPES.has((type ?? "").toLowerCase())
            ? v
            : `'${v.replace(/\\/g, "\\\\").replace(/'/g, "\\'")}'`;
          return `${varName}._nf_${name} = ${lit}`;
        }),
    // eslint-disable-next-line react-hooks/exhaustive-deps -- pure string builder with no reactive deps; memoized once
    [],
  );

  const onTableDrop = useCallback(
    (frameId: string, compoundLabel: string) => {
      const frame = framesRef.current.find((fr) => fr.id === frameId);
      if (!frame) return;

      const { query: newQueryBase, targetVars } = buildDropExpansion(
        frame.query,
        compoundLabel,
        schemaRels,
      );

      const droppedNode = schemaNodeLabels.find((n) => {
        const cl = n.domainLabel ? `${n.domainLabel}:${n.tableLabel}` : n.tableLabel;
        return cl === compoundLabel;
      });
      if (droppedNode && droppedNode.nativeFilterColumns.length > 0) {
        // Every hop lands on its own copy of the same parameterized node, so the arguments the
        // steward supplies once have to bind on each of them — an unbound copy is not a filter
        // that matched nothing, it is a pattern the binder cannot satisfy.
        const tvs = targetVars;
        const nfc = droppedNode.nativeFilterColumns;
        setNfModal({
          label: droppedNode.tableLabel,
          filterColumns: nfc,
          onConfirm: (params) => {
            setNfModal(null);
            const clauses = tvs.flatMap((tv) => buildNfWhereClauses(tv, nfc, params));
            const whereStr = clauses.length > 0 ? `\nWHERE ${clauses.join(" AND ")}` : "";
            const finalQuery = newQueryBase.replace(/(\nRETURN )/, `${whereStr}$1`);
            rerunFrame(frameId, finalQuery);
          },
        });
      } else {
        rerunFrame(frameId, newQueryBase);
      }
    },
    [rerunFrame, schemaRels, schemaNodeLabels, buildNfWhereClauses],
  );

  const onDomainDrop = useCallback(
    (frameId: string, domainLabel: string) => {
      const frame = framesRef.current.find((fr) => fr.id === frameId);
      if (!frame) return;

      const nodeVarMatch = frame.query.match(/\bMATCH\s*\(\s*(\w+)/i);
      const firstVar = nodeVarMatch?.[1] ?? "n";

      const suffix = domainLabel.replace(/[^a-zA-Z0-9]/g, "").slice(0, 12);
      let targetVar = `z${suffix}`;
      const trimmed = frame.query.trim();
      let counter = 2;
      while (trimmed.includes(` ${targetVar}`) || trimmed.includes(`(${targetVar}`)) {
        targetVar = `z${suffix}${counter}`;
        counter++;
      }

      const optMatchPattern = `(${firstVar})-[]-(${targetVar}:${domainLabel})`;
      const newQuery = withExpansion(
        trimmed,
        `OPTIONAL MATCH ${optMatchPattern}`,
        `, ${targetVar}`,
        `${firstVar}, ${targetVar}`,
      );
      rerunFrame(frameId, newQuery);
    },
    [rerunFrame],
  );

  const handleColorChange = useCallback(
    (label: string, color: string) => {
      setColorOverrides((prev) => ({ ...prev, [label]: color }));
    },
    [setColorOverrides],
  );

  const handleSizeChange = useCallback(
    (label: string, size: number) => {
      setSizeOverrides((prev) => ({ ...prev, [label]: size }));
    },
    [setSizeOverrides],
  );

  const handleLabelPropertyChange = useCallback(
    (label: string, prop: string) => {
      setLabelProperty((prev) => ({ ...prev, [label]: prop }));
    },
    [setLabelProperty],
  );

  const handleSizeByPropertyChange = useCallback(
    (label: string, prop: string) => {
      setSizeByProperty((prev) => ({ ...prev, [label]: prop }));
    },
    [setSizeByProperty],
  );

  const handleSizeMultiplierChange = useCallback(
    (label: string, multiplier: number) => {
      setSizeMultiplier((prev) => ({ ...prev, [label]: multiplier }));
    },
    [setSizeMultiplier],
  );

  const handleRelLineChange = useCallback(
    (type: string, override: RelLineOverride) => {
      setRelLineOverrides((prev) => ({ ...prev, [type]: override }));
    },
    [setRelLineOverrides],
  );

  const handleSaveEdgeAlias = useCallback(
    async (relId: number, cqlAlias: string, gqlAlias: string) => {
      const rel = adminRels.find((r) => r.id === relId);
      if (!rel) return null;
      const tableRefOf = (id: number) => {
        const table = tables.find((t) => t.id === id);
        if (!table) throw new Error(`table ${id} of relationship ${rel.id} is not registered`);
        return tableRef(table);
      };
      const result = await upsertRelationship(
        {
          id: String(rel.id),
          sourceTableId: tableRefOf(rel.sourceTableId),
          targetTableId: rel.targetTableId != null ? tableRefOf(rel.targetTableId) : "",
          sourceColumn: rel.sourceColumn,
          targetColumn: rel.targetColumn ?? "",
          cardinality: rel.cardinality,
          materialize: rel.materialize,
          refreshInterval: rel.refreshInterval,
          targetFunctionName: rel.targetFunctionName,
          functionArg: rel.functionArg,
          alias: cqlAlias || null,
          graphqlAlias: gqlAlias || null,
        },
        false, // its aliases changed: an update
      );
      // A refusal (a reserved word, an alias in use) is said, not taken for a save.
      if (!result.success) return result.message;
      await refetchRelationships();
      return null;
    },
    [adminRels, tables, upsertRelationship, refetchRelationships],
  );

  const visibleNodeLabels =
    checkedDomains.size === 0
      ? schemaNodeLabels
      : schemaNodeLabels.filter((n) => !n.domainId || checkedDomains.has(n.domainId));

  const visibleLabelSet = new Set(
    visibleNodeLabels.map((n) =>
      n.domainLabel ? `${n.domainLabel}:${n.tableLabel}` : n.tableLabel,
    ),
  );
  const visibleSchemaRels =
    checkedDomains.size === 0
      ? schemaRels
      : schemaRels.filter((r) => visibleLabelSet.has(r.source) && visibleLabelSet.has(r.target));

  const cypherSchema: CypherSchema = {
    labels: visibleNodeLabels
      .flatMap((n) =>
        n.domainLabel
          ? [`${n.domainLabel}:${n.tableLabel}`, n.domainLabel, n.tableLabel]
          : [n.tableLabel],
      )
      .filter((v, i, a) => a.indexOf(v) === i),
    relationshipTypes: visibleSchemaRels.map((r) => r.type),
    propertyKeys: [
      ...new Set(
        visibleNodeLabels
          .flatMap((n) => [...n.properties, ...n.nativeFilterColumns.map((c) => c.name)])
          .concat(visibleSchemaRels.flatMap((r) => r.properties)),
      ),
    ],
  };

  const handleHistorySelect = useCallback((q: string) => setHistoryQuery(q), []);

  // REQ-2135: a SPARQL pill adds to the query in the bar. Its patterns share one subject, ?s: a
  // class types it, a property or a relationship hangs a fresh object off it; a concept is read for
  // what is said of it; a prefix or a domain is declared.
  const handleVocabularyPick = useCallback(
    (pick: VocabularyPick) => {
      if (!vocabulary) return;
      const p = vocabulary.prefixes;
      const q = graphState.currentQuery;
      let next: string;
      if (pick.kind === "prefix") next = withPrefixes(q, [pick.prefix], p);
      else {
        const term = compactIri(pick.iri, p);
        const pattern =
          pick.kind === "class"
            ? `?s a ${term} .`
            : pick.kind === "concept"
              ? `${term} ${freshVariable(q, "p")} ${freshVariable(q, "o")} .`
              : `?s ${term} ${freshVariable(q, "o")} .`;
        next = addPattern(q, pattern, [term], p);
      }
      graphState.currentQuery = next;
      saveGraphState(graphState);
      setHistoryQuery(next);
    },
    [vocabulary, graphState],
  );
  const handleDomainClick = useCallback(
    (domainId: string) => {
      runQuery(`MATCH (n:${domainId}) RETURN n LIMIT 25`);
    },
    [runQuery],
  );
  const handleLabelClick = useCallback(
    (compoundLabel: string) => {
      if (compoundLabel === "*") {
        runQuery("MATCH (n) RETURN n LIMIT 25");
        return;
      }
      const node = schemaNodeLabels.find((n) => {
        const cl = n.domainLabel ? `${n.domainLabel}:${n.tableLabel}` : n.tableLabel;
        return cl === compoundLabel || n.domainLabel === compoundLabel;
      });
      if (node && node.nativeFilterColumns.length > 0) {
        setNfModal({
          label: node.tableLabel,
          filterColumns: node.nativeFilterColumns,
          onConfirm: (params) => {
            setNfModal(null);
            const clauses = buildNfWhereClauses("n", node.nativeFilterColumns, params);
            const whereStr = clauses.length > 0 ? ` WHERE ${clauses.join(" AND ")}` : "";
            runQuery(`MATCH (n:${compoundLabel})${whereStr} RETURN n LIMIT 25`);
          },
        });
      } else {
        runQuery(`MATCH (n:${compoundLabel}) RETURN n LIMIT 25`);
      }
    },
    [runQuery, schemaNodeLabels, buildNfWhereClauses],
  );

  const handleRelClick = useCallback(
    (type: string) => {
      if (type === "*") {
        runQuery("MATCH p=()-->() RETURN p LIMIT 25");
        return;
      }
      runQuery(`MATCH ()-[r:${type}]->() RETURN r LIMIT 25`);
    },
    [runQuery],
  );

  const handlePropertyKeyClick = useCallback(
    (key: string) => {
      runQuery(
        `MATCH (n)\nWHERE n.${key} IS NOT NULL\nRETURN DISTINCT "node" AS entity, n.${key} AS ${key}\nLIMIT 25\nUNION ALL\nMATCH ()-[r]-()\nWHERE r.${key} IS NOT NULL\nRETURN DISTINCT "relationship" AS entity, r.${key} AS ${key}\nLIMIT 25`,
      );
    },
    [runQuery],
  );

  const handleNfConfirm = useCallback(
    (params: Record<string, string>) => {
      nfModal?.onConfirm(params);
    },
    [nfModal],
  );

  return (
    <div className="graph-page">
      {nfModal && (
        <NativeFilterModal
          label={nfModal.label}
          filterColumns={nfModal.filterColumns}
          onConfirm={handleNfConfirm}
          onCancel={() => setNfModal(null)}
        />
      )}
      {showNeo4jModal &&
        (() => {
          const allNodes = new Map<string, GNode>();
          const allEdges = new Map<string, GEdge>();
          for (const f of frames) {
            const effective = effectiveDataRef.current.get(f.id);
            if (effective) {
              effective.nodes.forEach((n, k) => allNodes.set(k, n));
              effective.edges.forEach((e, k) => allEdges.set(k, e));
            } else {
              f.nodes.forEach((n, k) => allNodes.set(k, n));
              f.edges.forEach((e, k) => allEdges.set(k, e));
            }
          }
          return (
            <Neo4jExportModal
              nodes={[...allNodes.values()]}
              edges={[...allEdges.values()]}
              onClose={() => setShowNeo4jModal(false)}
            />
          );
        })()}
      <Sidebar
        schemaNodeLabels={visibleNodeLabels}
        schemaRels={visibleSchemaRels}
        schemaLoading={schemaLoading}
        schemaError={schemaError}
        history={history}
        colorOverrides={colorOverrides}
        sizeOverrides={sizeOverrides}
        labelProperty={labelProperty}
        sizeByProperty={sizeByProperty}
        sizeMultiplier={sizeMultiplier}
        relLineOverrides={relLineOverrides}
        onHistorySelect={handleHistorySelect}
        favorites={favorites}
        onFavoriteSelect={(q) => setHistoryQuery(q)}
        onFavoriteDelete={handleFavoriteDelete}
        onFavoriteRun={(q) => {
          setHistoryQuery(q);
          runQuery(q);
        }}
        onFavoriteRename={(id, name) =>
          setFavorites((prev) => prev.map((f) => (f.id === id ? { ...f, label: name } : f)))
        }
        onLabelClick={handleLabelClick}
        onDomainClick={handleDomainClick}
        onRelClick={handleRelClick}
        onColorChange={handleColorChange}
        onSizeChange={handleSizeChange}
        onLabelPropertyChange={handleLabelPropertyChange}
        onSizeByPropertyChange={handleSizeByPropertyChange}
        onSizeMultiplierChange={handleSizeMultiplierChange}
        onRelLineChange={handleRelLineChange}
        numericPropsByLabel={numericPropsByLabel}
        onNeo4jExport={() => setShowNeo4jModal(true)}
        width={sidebarWidth}
        onWidthChange={setSidebarWidth}
        highlightedLabel={activeLabel}
        propertyKeys={cypherSchema.propertyKeys}
        onPropertyKeyClick={handlePropertyKeyClick}
        counts={counts}
        countsError={countsError}
        maxNodes={maxNodes}
        onMaxNodesChange={setMaxNodes}
        vocabulary={
          sparql ? (
            <SparqlVocabularyPanel
              vocabulary={vocabulary}
              error={vocabularyError}
              onPick={handleVocabularyPick}
            />
          ) : undefined
        }
      />

      <div className="graph-content">
        <QueryBar
          onRun={runQuery}
          initialQuery={historyQuery ?? graphState.currentQuery}
          onQueryChange={(q) => {
            graphState.currentQuery = q;
            saveGraphState(graphState);
          }}
          cypherSchema={schemaLoading ? undefined : cypherSchema}
          autoImpute={autoImpute}
          onToggleAutoImpute={() => setAutoImpute((v) => !v)}
          language={language}
          key={historyQuery ?? "initial"}
        />

        {/* REQ-2010: the status of the last forwarded query's frame, which a demo tour step waits
            on until it is done. */}
        <div
          className="graph-stream"
          data-handoff-status={frames.find((f) => f.id === handoffFrameId)?.status}
        >
          {frames.length === 0 && (
            <div className="graph-stream-empty" role="status" data-testid="graph-empty-state">
              <div className="graph-stream-empty-icon" aria-hidden="true">
                ⬡
              </div>
              <Text component="div">{t(language === "sparql" ? "graphPage.emptyStateHintSparql" : "graphPage.emptyStateHint")}</Text>
              <Text component="div" className="graph-stream-hint">
                {t("graphPage.emptyStateShortcut")}
              </Text>
            </div>
          )}
          {[...frames]
            .sort((a, b) => (b.pinned ? 1 : 0) - (a.pinned ? 1 : 0))
            .map((frame) => (
              <GraphFrame
                key={frame.id}
                frame={frame}
                requestedView={frameViews[frame.id]}
                onClose={closeFrame}
                onRerun={rerunFrame}
                // A drop, an expansion and an exclusion write Cypher against the model's labels and
                // relationships: none of them acts on a SPARQL frame.
                onTableDrop={sparql ? undefined : onTableDrop}
                onDomainDrop={sparql ? undefined : onDomainDrop}
                colorOverrides={colorOverrides}
                sizeOverrides={sizeOverrides}
                labelProperty={labelProperty}
                sizeByProperty={sizeByProperty}
                sizeMultiplier={sizeMultiplier}
                relLineOverrides={relLineOverrides}
                onColorChange={handleColorChange}
                pkMap={pkMap}
                labelToTableLabel={labelToTableLabel}
                labelToTableId={labelToTableId}
                relationships={sparql ? [] : adminRels}
                graphRelationships={sparql ? [] : graphRels}
                schemaRels={sparql ? [] : schemaRels}
                autoImpute={autoImpute}
                language={language}
                onSaveEdgeAlias={handleSaveEdgeAlias}
                onSelectedLabelChange={setActiveLabel}
                onEffectiveDataChange={onEffectiveDataChange}
                onAddFavorite={handleAddFavorite}
                isFavorited={isFavorited}
                onPin={handlePin}
                onStatement={showStatement}
              />
            ))}
        </div>
      </div>
    </div>
  );
}
