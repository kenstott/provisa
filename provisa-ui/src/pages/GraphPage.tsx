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
import { useDomainFilter } from "../context/DomainFilterContext";
import { GraphFrame } from "../components/graph/GraphFrame";
import {
  extractElements,
  type FrameData,
  type RelLineOverride,
} from "../components/graph/graph-model";
import { fetchRelationships, upsertRelationship } from "../api/admin";
import { useAuth } from "../context/AuthContext";
import type { Relationship } from "../types/admin";
import "./GraphPage.css";
import { useLocalStorage, graphState, saveGraphState } from "../components/graph/graph-persistence";
import type {
  SchemaNodeLabel,
  SchemaRel,
  CypherSchema,
} from "../components/graph/graph-schema-types";
import { Sidebar } from "../components/graph/GraphSidebar";
import { QueryBar } from "../components/graph/QueryBar";
import { NativeFilterModal } from "../components/graph/graph-context-menus";

// ── Main page ─────────────────────────────────────────────────────────────────
export function GraphPage() {
  const { role } = useAuth();
  const { checkedDomains } = useDomainFilter();
  const [frames, setFrames] = useState<FrameData[]>(graphState.frames);
  const [history, setHistory] = useState<string[]>(graphState.history);
  const [historyQuery, setHistoryQuery] = useState<string | null>(null);
  const [schemaNodeLabels, setSchemaNodeLabels] = useState<SchemaNodeLabel[]>([]);
  const [schemaRels, setSchemaRels] = useState<SchemaRel[]>([]);
  const [schemaLoading, setSchemaLoading] = useState(true);
  const [sidebarWidth, setSidebarWidth] = useState(240);
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
  const [autoImpute, setAutoImpute] = useLocalStorage<boolean>("provisa.graph.autoImpute", false);
  const [relLineOverrides, setRelLineOverrides] = useLocalStorage<Record<string, RelLineOverride>>(
    "provisa.graph.relLineOverrides",
    {},
  );
  const [adminRels, setAdminRels] = useState<Relationship[]>([]);
  const [nfModal, setNfModal] = useState<{
    label: string;
    compoundLabel: string;
    filterColumns: string[];
  } | null>(null);
  const frameIdRef = useRef(
    graphState.frames.reduce((max, f) => Math.max(max, parseInt(f.id) || 0), 0),
  );
  const clusterMapRef = useRef<
    Record<string, { scl1: number | null; scl2: number | null; scl3: number | null }>
  >({});

  // Fetch admin relationships for edge alias editing
  useEffect(() => {
    fetchRelationships()
      .then(setAdminRels)
      .catch(() => {});
  }, []);

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
            native_filter_columns?: string[];
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

  const runQuery = useCallback(
    async (query: string) => {
      if (!query) return;
      const id = String(++frameIdRef.current);
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
              ? { ...fr, status: "done" as const, nodes, edges, rows, columns, elapsed }
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

  // Auto-execute a query forwarded from another page (e.g. Cypher panel → Graph).
  useEffect(() => {
    const pending = localStorage.getItem("provisa.graph.pending_query");
    if (pending) {
      localStorage.removeItem("provisa.graph.pending_query");
      setHistoryQuery(pending);
      runQuery(pending);
    }
  }, [runQuery]);

  const closeFrame = useCallback((id: string) => {
    setFrames((f) => {
      const next = f.filter((fr) => fr.id !== id);
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
              ? { ...fr, status: "done" as const, nodes, edges, rows, columns, elapsed }
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

  const onTableDrop = useCallback(
    (frameId: string, compoundLabel: string) => {
      const frame = framesRef.current.find((fr) => fr.id === frameId);
      if (!frame) return;

      // The dropped label's virtual table name is the last segment (after the last colon)
      const droppedTableName = compoundLabel.split(":").pop()!;

      // Find a relationship between any node currently in the frame and the dropped table.
      // Match by virtual table name (alias takes precedence over raw table name).
      const frameNodeLabels = new Set<string>();
      frame.nodes.forEach((node) => {
        node.label.split(":").forEach((l) => frameNodeLabels.add(l));
      });

      // Build a map from node label → variable name by parsing MATCH clauses in the query
      const varByLabel: Record<string, string> = {};
      for (const m of frame.query.matchAll(/\(\s*(\w+)\s*:([\w:]+)\s*\)/g)) {
        const [, varName, labels] = m;
        labels.split(":").forEach((l) => {
          varByLabel[l] = varName;
        });
      }

      // Find a rel where one side is a frame node and the other is the dropped table
      let sourceVar: string;
      let relAlias: string | null = null;
      const rel = adminRels.find((r) => {
        const srcMatch =
          frameNodeLabels.has(r.sourceTableName) && r.targetTableName === droppedTableName;
        const tgtMatch =
          frameNodeLabels.has(r.targetTableName) && r.sourceTableName === droppedTableName;
        return srcMatch || tgtMatch;
      });
      if (rel) {
        const connectedLabel = frameNodeLabels.has(rel.sourceTableName)
          ? rel.sourceTableName
          : rel.targetTableName;
        sourceVar = varByLabel[connectedLabel] ?? "n";
        relAlias = rel.alias;
      } else {
        // No known relationship — fall back to first MATCH variable
        const nodeVarMatch = frame.query.match(/\bMATCH\s*\(\s*(\w+)/i);
        sourceVar = nodeVarMatch?.[1] ?? "n";
      }

      const suffix = compoundLabel.replace(/[^a-zA-Z0-9]/g, "").slice(0, 8);
      const relVar = `r${suffix}`;
      const targetVar = `m${suffix}`;
      const relTypeStr = relAlias ? `[${relVar}:${relAlias}]` : `[${relVar}]`;
      const trimmed = frame.query.replace(/\s+LIMIT\s+\d+\s*$/i, "").trim();
      const returnMatches = [...trimmed.matchAll(/\bRETURN\b/gi)];
      const lastReturn = returnMatches.pop();
      let newQuery: string;
      if (!lastReturn || lastReturn.index === undefined) {
        newQuery = `${trimmed}\nOPTIONAL MATCH (${sourceVar})-${relTypeStr}-(${targetVar}:${compoundLabel})\nRETURN ${sourceVar}, ${relVar}, ${targetVar}`;
      } else {
        const beforeReturn = trimmed.slice(0, lastReturn.index).trimEnd();
        const returnClause = trimmed.slice(lastReturn.index + 6).trim();
        newQuery = `${beforeReturn}\nOPTIONAL MATCH (${sourceVar})-${relTypeStr}-(${targetVar}:${compoundLabel})\nRETURN ${returnClause}, ${relVar}, ${targetVar}`;
      }
      rerunFrame(frameId, newQuery);
    },
    [rerunFrame, adminRels],
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
      const updated = await fetchRelationships();
      setAdminRels(updated);
    },
    [adminRels],
  );

  // Build pkMap: compound label (e.g. "SalesAnalytics:Orders") → pk_columns
  const SYSTEM_DOMAINS = new Set(["meta", "ops"]);
  const visibleNodeLabels =
    checkedDomains.size === 0
      ? schemaNodeLabels
      : schemaNodeLabels.filter(
          (n) => !n.domainId || checkedDomains.has(n.domainId) || SYSTEM_DOMAINS.has(n.domainId),
        );

  // Falls back to idColumn (heuristically resolved) when no user-designated PKs
  const pkMap: Record<string, string[]> = {};
  for (const node of visibleNodeLabels) {
    const compoundLabel = node.domainLabel
      ? `${node.domainLabel}:${node.tableLabel}`
      : node.tableLabel;
    pkMap[compoundLabel] =
      node.pkColumns.length > 0 ? node.pkColumns : node.idColumn ? [node.idColumn] : [];
  }

  const cypherSchema: CypherSchema = {
    labels: visibleNodeLabels
      .flatMap((n) =>
        n.domainLabel
          ? [`${n.domainLabel}:${n.tableLabel}`, n.domainLabel, n.tableLabel]
          : [n.tableLabel],
      )
      .filter((v, i, a) => a.indexOf(v) === i),
    relationshipTypes: schemaRels.map((r) => r.type),
    propertyKeys: [
      ...new Set(visibleNodeLabels.flatMap((n) => [...n.properties, ...n.nativeFilterColumns])),
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
      const node = schemaNodeLabels.find((n) => {
        const cl = n.domainLabel ? `${n.domainLabel}:${n.tableLabel}` : n.tableLabel;
        return cl === compoundLabel || n.domainLabel === compoundLabel;
      });
      if (node && node.nativeFilterColumns.length > 0) {
        setNfModal({
          label: node.tableLabel,
          compoundLabel,
          filterColumns: node.nativeFilterColumns,
        });
      } else {
        runQuery(`MATCH (n:${compoundLabel}) RETURN n LIMIT 25`);
      }
    },
    [runQuery, schemaNodeLabels],
  );

  const handleRelClick = useCallback(
    (type: string) => {
      runQuery(`MATCH ()-[r:${type}]->() RETURN r LIMIT 25`);
    },
    [runQuery],
  );

  const handleNfConfirm = useCallback(
    (params: Record<string, string>) => {
      if (!nfModal) return;
      const { compoundLabel, filterColumns } = nfModal;
      const whereClauses = filterColumns
        .filter((col) => params[col] !== "")
        .map(
          (col) => `n._nf_${col} = '${params[col].replace(/\\/g, "\\\\").replace(/'/g, "\\'")}'`,
        );
      const whereStr = whereClauses.length > 0 ? ` WHERE ${whereClauses.join(" AND ")}` : "";
      setNfModal(null);
      runQuery(`MATCH (n:${compoundLabel})${whereStr} RETURN n LIMIT 25`);
    },
    [nfModal, runQuery],
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
      <Sidebar
        schemaNodeLabels={visibleNodeLabels}
        schemaRels={schemaRels}
        schemaLoading={schemaLoading}
        history={history}
        colorOverrides={colorOverrides}
        sizeOverrides={sizeOverrides}
        labelProperty={labelProperty}
        relLineOverrides={relLineOverrides}
        onHistorySelect={handleHistorySelect}
        onLabelClick={handleLabelClick}
        onDomainClick={handleDomainClick}
        onRelClick={handleRelClick}
        onColorChange={handleColorChange}
        onSizeChange={handleSizeChange}
        onLabelPropertyChange={handleLabelPropertyChange}
        onRelLineChange={handleRelLineChange}
        width={sidebarWidth}
        onWidthChange={setSidebarWidth}
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
          {frames.map((frame) => (
            <GraphFrame
              key={frame.id}
              frame={frame}
              onClose={closeFrame}
              onRerun={rerunFrame}
              onTableDrop={onTableDrop}
              colorOverrides={colorOverrides}
              sizeOverrides={sizeOverrides}
              labelProperty={labelProperty}
              relLineOverrides={relLineOverrides}
              onColorChange={handleColorChange}
              pkMap={pkMap}
              relationships={adminRels}
              schemaRels={schemaRels}
              autoImpute={autoImpute}
              onSaveEdgeAlias={handleSaveEdgeAlias}
            />
          ))}
        </div>
      </div>
    </div>
  );
}
