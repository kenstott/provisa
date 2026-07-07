// Copyright (c) 2026 Kenneth Stott
// Canary: f4a2c9b7-3e1d-4f5a-8b2e-7c6d1a9f4e3b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState, useCallback, useRef, useEffect } from "react";
import { useLocation } from "react-router-dom";
import { useDomainFilter } from "../context/DomainFilterContext";
import { GraphFrame } from "../components/graph/GraphFrame";
import {
  extractElements,
  type FrameData,
  type RelLineOverride,
  type GNode,
  type GEdge,
} from "../components/graph/graph-model";
import { useRelationships, useUpsertRelationship } from "../hooks/useAdminQueries";
import { useAuth } from "../context/AuthContext";
import "./GraphPage.css";
import { useLocalStorage, graphState, saveGraphState } from "../components/graph/graph-persistence";
import type { Favorite } from "../components/graph/graph-persistence";
import type {
  SchemaNodeLabel,
  SchemaRel,
  CypherSchema,
} from "../components/graph/graph-schema-types";
import { Sidebar } from "../components/graph/GraphSidebar";
import { QueryBar } from "../components/graph/QueryBar";
import { NativeFilterModal } from "../components/graph/graph-context-menus";
import { Neo4jExportModal } from "../components/graph/Neo4jExportModal";
import { tableLabel as dbTableLabel } from "../naming";

// ── Main page ─────────────────────────────────────────────────────────────────
export function GraphPage() {
  const { role } = useAuth();
  const { checkedDomains } = useDomainFilter();
  const location = useLocation();
  const [frames, setFrames] = useState<FrameData[]>(graphState.frames);
  const [history, setHistory] = useState<string[]>(graphState.history);
  const [historyQuery, setHistoryQuery] = useState<string | null>(
    () => (location.state as { query?: string } | null)?.query ?? null,
  );
  const [schemaNodeLabels, setSchemaNodeLabels] = useState<SchemaNodeLabel[]>([]);
  const [schemaRels, setSchemaRels] = useState<SchemaRel[]>([]);
  const [schemaLoading, setSchemaLoading] = useState(true);
  const [totalNodeCount, setTotalNodeCount] = useState<number | null>(null);
  const [totalRelCount, setTotalRelCount] = useState<number | null>(null);
  const [labelCounts, setLabelCounts] = useState<Record<string, number>>({});
  const [sidebarWidth, setSidebarWidth] = useLocalStorage<number>("provisa.graph.sidebarWidth", 240);
  const [activeLabel, setActiveLabel] = useState<string | null>(null);
  const [colorOverrides, setColorOverrides] = useLocalStorage<Record<string, string>>(
    "provisa.graph.colorOverrides",
    {},
  );
  const [sizeOverrides, setSizeOverrides] = useLocalStorage<Record<string, number>>(
    "provisa.graph.sizeOverrides",
    {},
  );
  const [labelProperty, setLabelProperty] = useLocalStorage<Record<string, string>>(
    "provisa.graph.labelProperty",
    {},
  );
  const [sizeByProperty, setSizeByProperty] = useLocalStorage<Record<string, string>>(
    "provisa.graph.sizeByProperty",
    {},
  );
  const [sizeMultiplier, setSizeMultiplier] = useLocalStorage<Record<string, number>>(
    "provisa.graph.sizeMultiplier",
    {},
  );
  const [autoImpute, setAutoImpute] = useLocalStorage<boolean>("provisa.graph.autoImpute", false);
const [favorites, setFavorites] = useLocalStorage<Favorite[]>("provisa.graph.favorites", []);
  const [relLineOverrides, setRelLineOverrides] = useLocalStorage<Record<string, RelLineOverride>>(
    "provisa.graph.relLineOverrides",
    {},
  );
  const { relationships: adminRels, refetch: refetchRelationships } = useRelationships();
  const { upsertRelationship } = useUpsertRelationship();
  const [nfModal, setNfModal] = useState<{
    label: string;
    filterColumns: { name: string; type: string }[];
    onConfirm: (params: Record<string, string>) => void;
  } | null>(null);
  const [showNeo4jModal, setShowNeo4jModal] = useState(false);
  const effectiveDataRef = useRef<Map<string, { nodes: Map<string, GNode>; edges: Map<string, GEdge> }>>(new Map());
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

  const handleAddFavorite = useCallback((query: string) => {
    const trimmed = query.trim();
    setFavorites((prev) => {
      const existing = prev.find((f) => f.query === trimmed);
      if (existing) return prev.filter((f) => f.query !== trimmed);
      const label = trimmed.split("\n")[0].slice(0, 50);
      const id = `${Date.now()}-${Math.random().toString(36).slice(2, 7)}`;
      return [...prev, { id, query: trimmed, label, ts: Date.now() }];
    });
  }, [setFavorites]);

  const handleFavoriteDelete = useCallback((id: string) => {
    setFavorites((prev) => prev.filter((f) => f.id !== id));
  }, [setFavorites]);

  const isFavorited = useCallback((query: string) => {
    return favorites.some((f) => f.query === query.trim());
  }, [favorites]);

  // Fetch schema when role changes via dedicated graph-schema endpoint
  useEffect(() => {
    setSchemaLoading(true);
    const headers: Record<string, string> = {};
    if (role) headers["X-Provisa-Role"] = role.id;
    fetch("/data/graph-schema", { headers })
      .then((r) => r.json())
      .then((data) => {
        const nodeLabels: SchemaNodeLabel[] = (data.node_labels ?? []).map(
          (n: {
            label: string;
            domain_label: string | null;
            domain_id: string | null;
            table_label: string;
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
          .map((r: { type: string; source: string; target: string }) => ({
            type: r.type,
            source: r.source ?? "",
            target: r.target ?? "",
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
      })
      .catch(() => {})
      .finally(() => setSchemaLoading(false));
    /* eslint-disable-next-line react-hooks/exhaustive-deps --
       keyed on role.id only; the full role object identity changes on unrelated auth refreshes and must not refetch the graph schema */
  }, [role?.id]);

  useEffect(() => {
    if (schemaNodeLabels.length === 0 && schemaRels.length === 0) return;
    const headers: Record<string, string> = {};
    if (role) headers["X-Provisa-Role"] = role.id;
    const domainsKey = [...checkedDomains].sort().join(",");
    const countsKey = `provisa.graph.counts.${role?.id ?? "default"}.${domainsKey}`;
    try {
      const cached = localStorage.getItem(countsKey);
      if (cached) {
        const c = JSON.parse(cached) as { node_count: number; rel_count: number; label_counts: Record<string, number> };
        setTotalNodeCount(c.node_count);
        setTotalRelCount(c.rel_count);
        setLabelCounts(c.label_counts);
      } else {
        setTotalNodeCount(null);
        setTotalRelCount(null);
      }
    } catch {
      setTotalNodeCount(null);
      setTotalRelCount(null);
    }
    const domainsParam =
      checkedDomains.size > 0 ? `?domains=${[...checkedDomains].join(",")}` : "";
    fetch(`/data/graph-counts${domainsParam}`, { headers })
      .then((r) => r.json())
      .then((data) => {
        const node_count = data.node_count ?? 0;
        const rel_count = data.rel_count ?? 0;
        const label_counts = data.label_counts ?? {};
        setTotalNodeCount(node_count);
        setTotalRelCount(rel_count);
        setLabelCounts(label_counts);
        try {
          localStorage.setItem(countsKey, JSON.stringify({ node_count, rel_count, label_counts }));
        } catch { /* quota */ }
      })
      .catch(() => {});
    /* eslint-disable-next-line react-hooks/exhaustive-deps */
  }, [schemaNodeLabels, schemaRels, role?.id, checkedDomains]);


  const runQuery = useCallback(
    async (query: string) => {
      if (!query) return;
      const id = crypto.randomUUID();
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
      try {
        const hdrs: Record<string, string> = { "Content-Type": "application/json" };
        if (role) hdrs["X-Provisa-Role"] = role.id;
        hdrs["X-Provisa-Stats"] = "true";
        const res = await fetch("/data/cypher", {
          method: "POST",
          headers: hdrs,
          body: JSON.stringify({ query, params: {} }),
        });
        const elapsed = Date.now() - start;
        if (!res.ok) {
          const text = await res.text();
          let msg: string;
          try {
            msg = (JSON.parse(text) as { error?: string }).error ?? text;
          } catch {
            msg = text;
          }
          setFrames((f) => {
            const next = f.map((fr) =>
              fr.id === id ? { ...fr, status: "error" as const, error: msg } : fr,
            );
            graphState.frames = next;
            saveGraphState(graphState);
            return next;
          });
          return;
        }
        const data = await res.json();
        const rows: Record<string, unknown>[] = data.rows ?? [];
        const columns: string[] = data.columns ?? [];
        const queryStats: unknown = data.provisa_stats ?? undefined;
        const { nodes, edges } = extractElements(rows);
        nodes.forEach((node) => {
          const clusters = clusterMapRef.current[node.label];
          if (clusters)
            Object.assign(node.properties, {
              scl1: clusters.scl1,
              scl2: clusters.scl2,
              scl3: clusters.scl3,
            });
        });
        setFrames((f) => {
          const next = f.map((fr) =>
            fr.id === id
              ? { ...fr, status: "done" as const, nodes, edges, rows, columns, elapsed, queryStats }
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
    [role],
  );

  // Auto-execute a query forwarded from another page (e.g. NL "Open in Cypher", or the Cypher panel).
  useEffect(() => {
    // NL forwards the query via navigate state with autoRun; run it on arrival.
    const st = location.state as { query?: string; autoRun?: boolean } | null;
    if (st?.query && st.autoRun) {
      setHistoryQuery(st.query);
      runQuery(st.query);
      return;
    }
    const pending = localStorage.getItem("provisa.graph.pending_query");
    if (pending) {
      localStorage.removeItem("provisa.graph.pending_query");
      setHistoryQuery(pending);
      runQuery(pending);
    }
    // Run once on mount for the forwarded query; deps intentionally exclude location.state.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [runQuery]);

  const closeFrame = useCallback((id: string) => {
    setFrames((f) => {
      const next = f.filter((fr) => fr.id !== id);
      graphState.frames = next;
      saveGraphState(graphState);
      return next;
    });
  }, []);

  const handlePin = useCallback((id: string) => {
    setFrames((f) => {
      const next = f.map((fr) => fr.id === id ? { ...fr, pinned: !fr.pinned } : fr);
      graphState.frames = next;
      saveGraphState(graphState);
      return next;
    });
  }, []);

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
              }
            : fr,
        );
        graphState.frames = next;
        saveGraphState(graphState);
        return next;
      });
      try {
        const hdrs2: Record<string, string> = { "Content-Type": "application/json" };
        if (role) hdrs2["X-Provisa-Role"] = role.id;
        hdrs2["X-Provisa-Stats"] = "true";
        const res = await fetch("/data/cypher", {
          method: "POST",
          headers: hdrs2,
          body: JSON.stringify({ query, params: {} }),
        });
        const elapsed = Date.now() - start;
        if (!res.ok) {
          const text = await res.text();
          let msg: string;
          try {
            msg = (JSON.parse(text) as { error?: string }).error ?? text;
          } catch {
            msg = text;
          }
          setFrames((f) => {
            const next = f.map((fr) =>
              fr.id === id ? { ...fr, status: "error" as const, error: msg } : fr,
            );
            graphState.frames = next;
            saveGraphState(graphState);
            return next;
          });
          return;
        }
        const data = await res.json();
        const rows: Record<string, unknown>[] = data.rows ?? [];
        const columns: string[] = data.columns ?? [];
        const queryStats: unknown = data.provisa_stats ?? undefined;
        const { nodes, edges } = extractElements(rows);
        nodes.forEach((node) => {
          const clusters = clusterMapRef.current[node.label];
          if (clusters)
            Object.assign(node.properties, {
              scl1: clusters.scl1,
              scl2: clusters.scl2,
              scl3: clusters.scl3,
            });
        });
        setFrames((f) => {
          const next = f.map((fr) =>
            fr.id === id
              ? { ...fr, status: "done" as const, nodes, edges, rows, columns, elapsed, queryStats }
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
    [role],
  );

  const framesRef = useRef(frames);
  framesRef.current = frames;

  const NUMERIC_TYPES = new Set(["integer", "bigint", "int", "int4", "int8", "smallint", "float", "double precision", "numeric", "decimal", "real", "float4", "float8"]);

  const buildNfWhereClauses = useCallback(
    (varName: string, filterColumns: { name: string; type: string }[], params: Record<string, string>) =>
      filterColumns
        .filter((col) => params[col.name] !== "")
        .map(({ name, type }) => {
          const v = params[name];
          const lit = NUMERIC_TYPES.has((type ?? "").toLowerCase())
            ? v
            : `'${v.replace(/\\/g, "\\\\").replace(/'/g, "\\'")}'`;
          return `${varName}._nf_${name} = ${lit}`;
        }),
    // eslint-disable-next-line react-hooks/exhaustive-deps
    [],
  );

  const onTableDrop = useCallback(
    (frameId: string, compoundLabel: string) => {
      const frame = framesRef.current.find((fr) => fr.id === frameId);
      if (!frame) return;

      const droppedTableName = labelToTableLabel[compoundLabel] ?? compoundLabel;

      // Map each declared Cypher label → its query variable by parsing the MATCH
      // clauses. Use the QUERY (not result nodes): an OPTIONAL MATCH branch that
      // returned no rows still declares its label and must remain matchable.
      const varByLabel: Record<string, string> = {};
      for (const m of frame.query.matchAll(/\(\s*(\w+)\s*:([\w:]+)\s*\)/g)) {
        const [, varName, labels] = m;
        labels.split(":").forEach((l) => {
          varByLabel[l] = varName;
        });
      }

      // Find a relationship whose one endpoint is the dropped table and whose other
      // endpoint is a label already declared in the query. Comparison is exact: the
      // dropped label and dbTableLabel(table_name) are both produced by the same
      // label-derivation function on registered_tables.table_name.
      let sourceVar: string | undefined;
      let relAlias: string | null = null;
      for (const r of adminRels) {
        if (r.disableCypher) continue;
        const srcLabel = dbTableLabel(r.sourceTableName);
        const tgtLabel = r.targetTableName ? dbTableLabel(r.targetTableName) : null;
        // dropped node is the relationship target; existing query node is the source
        if (tgtLabel === droppedTableName && varByLabel[srcLabel]) {
          sourceVar = varByLabel[srcLabel];
          relAlias = (r.alias ?? r.computedCypherAlias ?? "").toUpperCase() || null;
          break;
        }
        // dropped node is the relationship source; existing query node is the target
        if (srcLabel === droppedTableName && tgtLabel && varByLabel[tgtLabel]) {
          sourceVar = varByLabel[tgtLabel];
          relAlias = (r.alias ?? r.computedCypherAlias ?? "").toUpperCase() || null;
          break;
        }
      }
      if (!sourceVar) {
        // No known relationship — fall back to first MATCH variable
        const nodeVarMatch = frame.query.match(/\bMATCH\s*\(\s*(\w+)/i);
        sourceVar = nodeVarMatch?.[1] ?? "n";
      }

      const suffix = droppedTableName.replace(/[^a-zA-Z0-9]/g, "").slice(0, 12);
      let relVar = `r${suffix}`;
      let targetVar = `m${suffix}`;
      const trimmed = frame.query.replace(/\s+LIMIT\s+\d+\s*$/i, "").trim();
      let counter = 2;
      while (
        trimmed.includes(`[${relVar}`) ||
        trimmed.includes(` ${targetVar}`) ||
        trimmed.includes(`(${targetVar}`)
      ) {
        relVar = `r${suffix}${counter}`;
        targetVar = `m${suffix}${counter}`;
        counter++;
      }
      const optMatchPattern = relAlias
        ? `(${sourceVar})-[${relVar}:${relAlias}]-(${targetVar}:${compoundLabel})`
        : `(${targetVar}:${compoundLabel})`;
      const extraReturn = relAlias ? `, ${relVar}, ${targetVar}` : `, ${targetVar}`;
      const returnMatches = [...trimmed.matchAll(/\bRETURN\b/gi)];
      const lastReturn = returnMatches.pop();
      let newQueryBase: string;
      if (!lastReturn || lastReturn.index === undefined) {
        newQueryBase = `${trimmed}\nOPTIONAL MATCH ${optMatchPattern}\nRETURN ${sourceVar}${extraReturn}`;
      } else {
        const beforeReturn = trimmed.slice(0, lastReturn.index).trimEnd();
        const returnClause = trimmed.slice(lastReturn.index + 6).trim();
        newQueryBase = `${beforeReturn}\nOPTIONAL MATCH ${optMatchPattern}\nRETURN ${returnClause}${extraReturn}`;
      }

      const droppedNode = schemaNodeLabels.find((n) => {
        const cl = n.domainLabel ? `${n.domainLabel}:${n.tableLabel}` : n.tableLabel;
        return cl === compoundLabel;
      });
      if (droppedNode && droppedNode.nativeFilterColumns.length > 0) {
        const tv = targetVar;
        const nfc = droppedNode.nativeFilterColumns;
        setNfModal({
          label: droppedNode.tableLabel,
          filterColumns: nfc,
          onConfirm: (params) => {
            setNfModal(null);
            const clauses = buildNfWhereClauses(tv, nfc, params);
            const whereStr = clauses.length > 0 ? `\nWHERE ${clauses.join(" AND ")}` : "";
            const finalQuery = newQueryBase.replace(/(\nRETURN )/, `${whereStr}$1`);
            rerunFrame(frameId, finalQuery);
          },
        });
      } else {
        rerunFrame(frameId, newQueryBase);
      }
    },
    [rerunFrame, adminRels, schemaNodeLabels, buildNfWhereClauses],
  );

  const onDomainDrop = useCallback(
    (frameId: string, domainLabel: string) => {
      const frame = framesRef.current.find((fr) => fr.id === frameId);
      if (!frame) return;

      const nodeVarMatch = frame.query.match(/\bMATCH\s*\(\s*(\w+)/i);
      const firstVar = nodeVarMatch?.[1] ?? "n";

      const suffix = domainLabel.replace(/[^a-zA-Z0-9]/g, "").slice(0, 12);
      let targetVar = `z${suffix}`;
      const trimmed = frame.query.replace(/\s+LIMIT\s+\d+\s*$/i, "").trim();
      let counter = 2;
      while (trimmed.includes(` ${targetVar}`) || trimmed.includes(`(${targetVar}`)) {
        targetVar = `z${suffix}${counter}`;
        counter++;
      }

      const optMatchPattern = `(${firstVar})-[]-(${targetVar}:${domainLabel})`;
      const returnMatches = [...trimmed.matchAll(/\bRETURN\b/gi)];
      const lastReturn = returnMatches.pop();
      let newQuery: string;
      if (!lastReturn || lastReturn.index === undefined) {
        newQuery = `${trimmed}\nOPTIONAL MATCH ${optMatchPattern}\nRETURN ${firstVar}, ${targetVar}`;
      } else {
        const beforeReturn = trimmed.slice(0, lastReturn.index).trimEnd();
        const returnClause = trimmed.slice(lastReturn.index + 6).trim();
        newQuery = `${beforeReturn}\nOPTIONAL MATCH ${optMatchPattern}\nRETURN ${returnClause}, ${targetVar}`;
      }
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
      if (!rel) return;
      await upsertRelationship({
        id: String(rel.id),
        sourceTableId: rel.sourceTableName,
        targetTableId: rel.targetTableName ?? "",
        sourceColumn: rel.sourceColumn,
        targetColumn: rel.targetColumn ?? "",
        cardinality: rel.cardinality,
        materialize: rel.materialize,
        refreshInterval: rel.refreshInterval,
        targetFunctionName: rel.targetFunctionName,
        functionArg: rel.functionArg,
        alias: cqlAlias || null,
        graphqlAlias: gqlAlias || null,
      });
      await refetchRelationships();
    },
    [adminRels, upsertRelationship, refetchRelationships],
  );

  const visibleNodeLabels =
    checkedDomains.size === 0
      ? schemaNodeLabels
      : schemaNodeLabels.filter(
          (n) => !n.domainId || checkedDomains.has(n.domainId),
        );

  const visibleLabelSet = new Set(
    visibleNodeLabels.map((n) =>
      n.domainLabel ? `${n.domainLabel}:${n.tableLabel}` : n.tableLabel,
    ),
  );
  const visibleSchemaRels =
    checkedDomains.size === 0
      ? schemaRels
      : schemaRels.filter(
          (r) => visibleLabelSet.has(r.source) && visibleLabelSet.has(r.target),
        );

  // pkMap covers ALL schema nodes — pk lookup is independent of the domain visibility filter
  const pkMap: Record<string, string[]> = {};
  const labelToTableLabel: Record<string, string> = {};
  for (const node of schemaNodeLabels) {
    const compoundLabel = node.domainLabel
      ? `${node.domainLabel}:${node.tableLabel}`
      : node.tableLabel;
    pkMap[compoundLabel] =
      node.pkColumns.length > 0 ? node.pkColumns : (node.idColumn ? [node.idColumn] : []);
    labelToTableLabel[compoundLabel] = node.tableLabel;
  }

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
      ...new Set(visibleNodeLabels.flatMap((n) => [...n.properties, ...n.nativeFilterColumns.map((c) => c.name)])),
    ],
  };

  const handleHistorySelect = useCallback((q: string) => setHistoryQuery(q), []);
  const handleDomainClick = useCallback(
    (domainId: string) => {
      runQuery(`MATCH (n:${domainId}) RETURN n LIMIT 25`);
    },
    [runQuery],
  );
  const handleLabelClick = useCallback(
    (compoundLabel: string) => {
      if (compoundLabel === "*") { runQuery("MATCH (n) RETURN n LIMIT 25"); return; }
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
      if (type === "*") { runQuery("MATCH p=()-->() RETURN p LIMIT 25"); return; }
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
      {showNeo4jModal && (() => {
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
        onFavoriteRun={(q) => { setHistoryQuery(q); runQuery(q); }}
        onFavoriteRename={(id, name) => setFavorites((prev) => prev.map((f) => f.id === id ? { ...f, label: name } : f))}
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
        totalNodeCount={totalNodeCount}
        totalRelCount={totalRelCount}
        labelCounts={labelCounts}
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
          key={historyQuery ?? "initial"}
        />

        <div className="graph-stream">
          {frames.length === 0 && (
            <div className="graph-stream-empty">
              <div className="graph-stream-empty-icon">⬡</div>
              <div>Run a Cypher query to explore the graph</div>
              <div className="graph-stream-hint">⌘↵ to run</div>
            </div>
          )}
          {[...frames].sort((a, b) => (b.pinned ? 1 : 0) - (a.pinned ? 1 : 0)).map((frame) => (
            <GraphFrame
              key={frame.id}
              frame={frame}
              onClose={closeFrame}
              onRerun={rerunFrame}
              onTableDrop={onTableDrop}
              onDomainDrop={onDomainDrop}
              colorOverrides={colorOverrides}
              sizeOverrides={sizeOverrides}
              labelProperty={labelProperty}
              sizeByProperty={sizeByProperty}
              sizeMultiplier={sizeMultiplier}
              relLineOverrides={relLineOverrides}
              onColorChange={handleColorChange}
              pkMap={pkMap}
              labelToTableLabel={labelToTableLabel}
              relationships={adminRels}
              autoImpute={autoImpute}
              onSaveEdgeAlias={handleSaveEdgeAlias}
              onSelectedLabelChange={setActiveLabel}
              onEffectiveDataChange={onEffectiveDataChange}
              onAddFavorite={handleAddFavorite}
              isFavorited={isFavorited}
              onPin={handlePin}
            />
          ))}
        </div>
      </div>
    </div>
  );
}
