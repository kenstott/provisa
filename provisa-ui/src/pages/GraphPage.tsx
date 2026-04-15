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
import CodeMirror from "@uiw/react-codemirror";
import { cypherLanguage } from "@neo4j-cypher/codemirror";
import { oneDark } from "@codemirror/theme-one-dark";
import { EditorView, keymap } from "@codemirror/view";
import { Prec } from "@codemirror/state";
import {
  GraphFrame,
  labelColor,
  PALETTE,
  extractElements,
  type FrameData,
  type RelLineOverride,
} from "./GraphFrame";
import { fetchRelationships, upsertRelationship } from "../api/admin";
import type { Relationship } from "../types/admin";
import "./GraphPage.css";

// ── localStorage-backed state ─────────────────────────────────────────────────
function useLocalStorage<T>(key: string, initial: T): [T, (v: T | ((prev: T) => T)) => void] {
  const [value, setValue] = useState<T>(() => {
    try {
      const raw = localStorage.getItem(key);
      return raw !== null ? (JSON.parse(raw) as T) : initial;
    } catch {
      return initial;
    }
  });

  const set = useCallback((v: T | ((prev: T) => T)) => {
    setValue((prev) => {
      const next = typeof v === "function" ? (v as (p: T) => T)(prev) : v;
      try { localStorage.setItem(key, JSON.stringify(next)); } catch { /* quota */ }
      return next;
    });
  }, [key]);

  return [value, set];
}

interface SchemaNodeLabel {
  domainLabel: string | null;
  tableLabel: string;
  properties: string[];
  pkColumns: string[];
  idColumn: string | null;
}
interface SchemaRel {
  type: string;
  source: string;
  target: string;
}

// ── Context menu ──────────────────────────────────────────────────────────────
interface ContextMenuState {
  x: number;
  y: number;
  compoundLabel: string;
  tableLabel: string;
  properties: string[];
}

interface RelContextMenuState {
  x: number;
  y: number;
  type: string;
}

interface NodeContextMenuProps {
  menu: ContextMenuState;
  colorOverrides: Record<string, string>;
  sizeOverrides: Record<string, number>;
  labelProperty: Record<string, string>;
  onColorChange: (label: string, color: string) => void;
  onSizeChange: (label: string, size: number) => void;
  onLabelPropertyChange: (label: string, prop: string) => void;
  onClose: () => void;
}

function NodeContextMenu({ menu, colorOverrides, sizeOverrides, labelProperty, onColorChange, onSizeChange, onLabelPropertyChange, onClose }: NodeContextMenuProps) {
  const ref = useRef<HTMLDivElement>(null);
  const currentColor = colorOverrides[menu.compoundLabel] ?? labelColor(menu.compoundLabel);
  const currentSize = sizeOverrides[menu.compoundLabel] ?? 44;
  const currentProp = labelProperty[menu.compoundLabel] ?? "";

  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) onClose();
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [onClose]);

  return (
    <div
      ref={ref}
      className="node-ctx-menu"
      style={{ left: menu.x, top: menu.y }}
      onMouseDown={(e) => e.stopPropagation()}
    >
      <div className="node-ctx-title">{menu.tableLabel}</div>
      <div className="node-ctx-section-label">Color</div>
      <div className="node-ctx-palette">
        {PALETTE.map((c) => (
          <button
            key={c}
            className={`node-ctx-swatch${currentColor === c ? " active" : ""}`}
            style={{ background: c }}
            onClick={() => { onColorChange(menu.compoundLabel, c); onClose(); }}
          />
        ))}
      </div>
      <div className="node-ctx-section-label">Size</div>
      <div className="node-ctx-size-row">
        <input
          type="range"
          min={20}
          max={120}
          value={currentSize}
          onChange={(e) => onSizeChange(menu.compoundLabel, Number(e.target.value))}
        />
        <span>{currentSize}px</span>
      </div>
      {menu.properties.length > 0 && (
        <>
          <div className="node-ctx-section-label">Label by</div>
          <select
            className="node-ctx-select"
            value={currentProp}
            onChange={(e) => { onLabelPropertyChange(menu.compoundLabel, e.target.value); onClose(); }}
          >
            <option value="">default</option>
            {menu.properties.map((p) => (
              <option key={p} value={p}>{p}</option>
            ))}
          </select>
        </>
      )}
    </div>
  );
}

// ── Relationship context menu ─────────────────────────────────────────────────
interface RelContextMenuProps {
  menu: RelContextMenuState;
  relLineOverrides: Record<string, RelLineOverride>;
  onRelLineChange: (type: string, override: RelLineOverride) => void;
  onClose: () => void;
}

const LINE_STYLES: Array<RelLineOverride["style"]> = ["solid", "dashed", "dotted"];
const LINE_STYLE_LABELS: Record<RelLineOverride["style"], string> = { solid: "—", dashed: "╌", dotted: "···" };

function RelContextMenu({ menu, relLineOverrides, onRelLineChange, onClose }: RelContextMenuProps) {
  const ref = useRef<HTMLDivElement>(null);
  const current = relLineOverrides[menu.type] ?? { width: 1.5, style: "solid" as const };

  useEffect(() => {
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) onClose();
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [onClose]);

  return (
    <div ref={ref} className="node-ctx-menu" style={{ left: menu.x, top: menu.y }}
         onMouseDown={(e) => e.stopPropagation()}>
      <div className="node-ctx-title">{menu.type}</div>
      <div className="node-ctx-section-label">Line Style</div>
      <div className="rel-ctx-style-row">
        {LINE_STYLES.map((s) => (
          <button
            key={s}
            className={`rel-ctx-style-btn${current.style === s ? " active" : ""}`}
            onClick={() => onRelLineChange(menu.type, { ...current, style: s })}
            title={s}
          >
            {LINE_STYLE_LABELS[s]}
          </button>
        ))}
      </div>
      <div className="node-ctx-section-label">Line Width</div>
      <div className="node-ctx-size-row">
        <input
          type="range"
          min={0.5}
          max={8}
          step={0.5}
          value={current.width}
          onChange={(e) => onRelLineChange(menu.type, { ...current, width: Number(e.target.value) })}
        />
        <span>{current.width}px</span>
      </div>
    </div>
  );
}

// ── Sidebar: schema summary ───────────────────────────────────────────────────
interface SidebarProps {
  schemaNodeLabels: SchemaNodeLabel[];
  schemaRels: SchemaRel[];
  schemaLoading: boolean;
  history: string[];
  colorOverrides: Record<string, string>;
  sizeOverrides: Record<string, number>;
  labelProperty: Record<string, string>;
  relLineOverrides: Record<string, RelLineOverride>;
  onHistorySelect: (q: string) => void;
  onLabelClick: (label: string) => void;
  onRelClick: (type: string) => void;
  onColorChange: (label: string, color: string) => void;
  onSizeChange: (label: string, size: number) => void;
  onLabelPropertyChange: (label: string, prop: string) => void;
  onRelLineChange: (type: string, override: RelLineOverride) => void;
  width: number;
  onWidthChange: (w: number) => void;
}

function Sidebar({ schemaNodeLabels, schemaRels, schemaLoading, history, colorOverrides, sizeOverrides, labelProperty, relLineOverrides, onHistorySelect, onLabelClick, onRelClick, onColorChange, onSizeChange, onLabelPropertyChange, onRelLineChange, width, onWidthChange }: SidebarProps) {
  const [section, setSection] = useState<"db" | "history">("db");
  const [contextMenu, setContextMenu] = useState<ContextMenuState | null>(null);
  const [relContextMenu, setRelContextMenu] = useState<RelContextMenuState | null>(null);
  const dragging = useRef(false);

  const onMouseDown = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    dragging.current = true;
    const startX = e.clientX;
    const startW = width;
    const onMove = (ev: MouseEvent) => {
      if (!dragging.current) return;
      const next = Math.max(160, Math.min(480, startW + ev.clientX - startX));
      onWidthChange(next);
    };
    const onUp = () => { dragging.current = false; window.removeEventListener("mousemove", onMove); window.removeEventListener("mouseup", onUp); };
    window.addEventListener("mousemove", onMove);
    window.addEventListener("mouseup", onUp);
  }, [width, onWidthChange]);

  const handleNodeRightClick = useCallback((e: React.MouseEvent, node: SchemaNodeLabel) => {
    e.preventDefault();
    const compoundLabel = node.domainLabel ? `${node.domainLabel}:${node.tableLabel}` : node.tableLabel;
    setContextMenu({ x: e.clientX, y: e.clientY, compoundLabel, tableLabel: node.tableLabel, properties: node.properties });
  }, []);

  const handleRelRightClick = useCallback((e: React.MouseEvent, type: string) => {
    e.preventDefault();
    setRelContextMenu({ x: e.clientX, y: e.clientY, type });
  }, []);

  const domainLabels = schemaLoading ? [] : [...new Set(
    schemaNodeLabels.filter(n => n.domainLabel).map(n => n.domainLabel!)
  )].sort();

  return (
    <aside className="graph-sidebar" style={{ width }}>
      <div className="graph-sidebar-tabs">
        <button
          className={`graph-sidebar-tab ${section === "db" ? "active" : ""}`}
          onClick={() => setSection("db")}
          title="Database"
        >
          ◉
        </button>
        <button
          className={`graph-sidebar-tab ${section === "history" ? "active" : ""}`}
          onClick={() => setSection("history")}
          title="History"
        >
          ⏱
        </button>
      </div>

      <div className="graph-sidebar-body">
        {section === "db" && (
          <>
            {domainLabels.length > 0 && (
              <div className="graph-schema-section">
                <div className="graph-schema-heading">Domain Labels</div>
                <div className="graph-label-list">
                  {domainLabels.map((lbl) => (
                    <div key={lbl} className="graph-label-item">
                      <span
                        className="graph-label-pill"
                        style={{ background: colorOverrides[lbl] ?? labelColor(lbl) }}
                        onClick={() => onLabelClick(lbl)}
                        title={`MATCH (n:${lbl}) RETURN n LIMIT 25`}
                      >
                        {lbl}
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            )}
            <div className="graph-schema-section">
              <div className="graph-schema-heading">Node Labels</div>
              {schemaLoading ? (
                <div className="graph-schema-empty">Loading…</div>
              ) : schemaNodeLabels.length === 0 ? (
                <div className="graph-schema-empty">No labels found</div>
              ) : (
                <div className="graph-label-list">
                  {schemaNodeLabels.map((node) => {
                    const compoundLabel = node.domainLabel
                      ? `${node.domainLabel}:${node.tableLabel}`
                      : node.tableLabel;
                    const color = colorOverrides[compoundLabel] ?? labelColor(compoundLabel);
                    return (
                      <div key={compoundLabel} className="graph-label-item">
                        <span
                          className="graph-label-pill"
                          style={{ background: color }}
                          onClick={() => onLabelClick(compoundLabel)}
                          onContextMenu={(e) => handleNodeRightClick(e, node)}
                          title={`MATCH (n:${compoundLabel}) RETURN n LIMIT 25`}
                        >
                          {node.tableLabel}
                        </span>
                      </div>
                    );
                  })}
                </div>
              )}
            </div>

            <div className="graph-schema-section">
              <div className="graph-schema-heading">Relationship Types</div>
              {schemaLoading ? (
                <div className="graph-schema-empty">Loading…</div>
              ) : schemaRels.length === 0 ? (
                <div className="graph-schema-empty">No relationship types found</div>
              ) : (
                <div className="graph-rel-list">
                  {schemaRels.map(({ type }) => {
                    const ov = relLineOverrides[type];
                    return (
                      <div
                        key={type}
                        className="graph-rel-item graph-rel-item--clickable"
                        title={`MATCH ()-[r:${type}]->() RETURN r LIMIT 25`}
                        onClick={() => onRelClick(type)}
                        onContextMenu={(e) => handleRelRightClick(e, type)}
                      >
                        <span className="graph-rel-arrow" style={ov ? { borderBottomWidth: ov.width, borderBottomStyle: ov.style === "solid" ? "solid" : ov.style === "dashed" ? "dashed" : "dotted" } : {}}>–</span>
                        <span className="graph-rel-type">{type}</span>
                      </div>
                    );
                  })}
                </div>
              )}
            </div>
          </>
        )}

        {section === "history" && (
          <div className="graph-schema-section">
            <div className="graph-schema-heading">History</div>
            {history.length === 0 ? (
              <div className="graph-schema-empty">No history yet</div>
            ) : (
              <div className="graph-history-list">
                {history.map((q, i) => (
                  <div
                    key={i}
                    className="graph-history-item"
                    onClick={() => onHistorySelect(q)}
                    title={q}
                  >
                    {q}
                  </div>
                ))}
              </div>
            )}
          </div>
        )}
      </div>

      <div className="graph-sidebar-resizer" onMouseDown={onMouseDown} />

      {contextMenu && (
        <NodeContextMenu
          menu={contextMenu}
          colorOverrides={colorOverrides}
          sizeOverrides={sizeOverrides}
          labelProperty={labelProperty}
          onColorChange={onColorChange}
          onSizeChange={onSizeChange}
          onLabelPropertyChange={onLabelPropertyChange}
          onClose={() => setContextMenu(null)}
        />
      )}
      {relContextMenu && (
        <RelContextMenu
          menu={relContextMenu}
          relLineOverrides={relLineOverrides}
          onRelLineChange={onRelLineChange}
          onClose={() => setRelContextMenu(null)}
        />
      )}
    </aside>
  );
}

// ── Query bar ─────────────────────────────────────────────────────────────────
interface QueryBarProps {
  onRun: (query: string) => void;
  initialQuery?: string;
}

function QueryBar({ onRun, initialQuery }: QueryBarProps) {
  const [query, setQuery] = useState(initialQuery ?? "MATCH (n) RETURN n LIMIT 25");

  return (
    <div className="graph-query-bar">
      <div className="graph-query-prompt">$</div>
      <div className="graph-query-editor-wrap">
        <CodeMirror
          className="graph-query-input"
          value={query}
          theme={oneDark}
          extensions={[
            cypherLanguage(),
            EditorView.lineWrapping,
            Prec.highest(keymap.of([{
              key: "Mod-Enter",
              run: () => { onRun(query.trim()); return true; },
            }, {
              key: "Enter",
              run: () => { onRun(query.trim()); return true; },
            }])),
          ]}
          onChange={(val) => setQuery(val)}
          basicSetup={{ lineNumbers: false, foldGutter: false, highlightActiveLine: false }}
          placeholder="MATCH (n) RETURN n LIMIT 25"
        />
        <button
          className="gf-copy-query-btn"
          title="Copy query"
          onClick={() => navigator.clipboard.writeText(query)}
        >⎘</button>
      </div>
      <button
        className="graph-run-btn"
        onClick={() => onRun(query.trim())}
        title="Run query (⌘↵)"
      >
        ▶
      </button>
    </div>
  );
}

// ── Main page ─────────────────────────────────────────────────────────────────
export function GraphPage() {
  const [frames, setFrames] = useState<FrameData[]>([]);
  const [history, setHistory] = useState<string[]>([]);
  const [historyQuery, setHistoryQuery] = useState<string | null>(null);
  const [schemaNodeLabels, setSchemaNodeLabels] = useState<SchemaNodeLabel[]>([]);
  const [schemaRels, setSchemaRels] = useState<SchemaRel[]>([]);
  const [schemaLoading, setSchemaLoading] = useState(true);
  const [sidebarWidth, setSidebarWidth] = useState(240);
  const [colorOverrides, setColorOverrides] = useLocalStorage<Record<string, string>>("provisa.graph.colorOverrides", {});
  const [sizeOverrides, setSizeOverrides] = useLocalStorage<Record<string, number>>("provisa.graph.sizeOverrides", {});
  const [labelProperty, setLabelProperty] = useLocalStorage<Record<string, string>>("provisa.graph.labelProperty", {});
  const [relLineOverrides, setRelLineOverrides] = useLocalStorage<Record<string, RelLineOverride>>("provisa.graph.relLineOverrides", {});
  const [adminRels, setAdminRels] = useState<Relationship[]>([]);
  const frameIdRef = useRef(0);

  // Fetch admin relationships for edge alias editing
  useEffect(() => {
    fetchRelationships().then(setAdminRels).catch(() => {});
  }, []);

  // Fetch schema on mount via dedicated graph-schema endpoint
  useEffect(() => {
    fetch("/data/graph-schema")
      .then((r) => r.json())
      .then((data) => {
        const nodeLabels: SchemaNodeLabel[] = (data.node_labels ?? []).map(
          (n: { label: string; domain_label: string | null; table_label: string; properties: string[]; pk_columns: string[]; id_column?: string }) => ({
            domainLabel: n.domain_label ?? null,
            tableLabel: n.table_label,
            properties: n.properties ?? [],
            pkColumns: n.pk_columns ?? [],
            idColumn: n.id_column ?? null,
          })
        );
        const rels: SchemaRel[] = (data.relationship_types ?? []).map((r: { type: string; source: string; target: string }) => ({
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
        setSchemaNodeLabels(uniqueNodeLabels);
        setSchemaRels(rels.sort((a, b) => a.type.localeCompare(b.type)));
      })
      .catch(() => {})
      .finally(() => setSchemaLoading(false));
  }, []);

  const runQuery = useCallback(async (query: string) => {
    if (!query) return;
    const id = String(++frameIdRef.current);
    const start = Date.now();
    setFrames((f) => [
      { id, query, status: "loading", nodes: new Map(), edges: new Map(), rows: [], columns: [] },
      ...f,
    ]);
    setHistory((h) => [query, ...h.filter((q) => q !== query).slice(0, 49)]);
    try {
      const res = await fetch("/data/cypher", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ query, params: {} }),
      });
      const elapsed = Date.now() - start;
      if (!res.ok) {
        const text = await res.text();
        let msg: string;
        try { msg = (JSON.parse(text) as { error?: string }).error ?? text; } catch { msg = text; }
        setFrames((f) => f.map((fr) => fr.id === id ? { ...fr, status: "error", error: msg } : fr));
        return;
      }
      const data = await res.json();
      const rows: Record<string, unknown>[] = data.rows ?? [];
      const columns: string[] = data.columns ?? [];
      const { nodes, edges } = extractElements(rows);
      setFrames((f) => f.map((fr) => fr.id === id ? { ...fr, status: "done", nodes, edges, rows, columns, elapsed } : fr));
    } catch (err) {
      setFrames((f) => f.map((fr) => fr.id === id ? { ...fr, status: "error", error: String(err) } : fr));
    }
  }, []);

  const closeFrame = useCallback((id: string) => {
    setFrames((f) => f.filter((fr) => fr.id !== id));
  }, []);

  const rerunFrame = useCallback(async (id: string, query: string) => {
    if (!query) return;
    const start = Date.now();
    setFrames((f) => f.map((fr) =>
      fr.id === id
        ? { ...fr, query, status: "loading", nodes: new Map(), edges: new Map(), rows: [], columns: [], elapsed: undefined, error: undefined }
        : fr
    ));
    try {
      const res = await fetch("/data/cypher", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ query, params: {} }),
      });
      const elapsed = Date.now() - start;
      if (!res.ok) {
        const text = await res.text();
        let msg: string;
        try { msg = (JSON.parse(text) as { error?: string }).error ?? text; } catch { msg = text; }
        setFrames((f) => f.map((fr) => fr.id === id ? { ...fr, status: "error", error: msg } : fr));
        return;
      }
      const data = await res.json();
      const rows: Record<string, unknown>[] = data.rows ?? [];
      const columns: string[] = data.columns ?? [];
      const { nodes, edges } = extractElements(rows);
      setFrames((f) => f.map((fr) => fr.id === id ? { ...fr, status: "done", nodes, edges, rows, columns, elapsed } : fr));
    } catch (err) {
      setFrames((f) => f.map((fr) => fr.id === id ? { ...fr, status: "error", error: String(err) } : fr));
    }
  }, []);

  const handleColorChange = useCallback((label: string, color: string) => {
    setColorOverrides((prev) => ({ ...prev, [label]: color }));
  }, []);

  const handleSizeChange = useCallback((label: string, size: number) => {
    setSizeOverrides((prev) => ({ ...prev, [label]: size }));
  }, []);

  const handleLabelPropertyChange = useCallback((label: string, prop: string) => {
    setLabelProperty((prev) => ({ ...prev, [label]: prop }));
  }, []);

  const handleRelLineChange = useCallback((type: string, override: RelLineOverride) => {
    setRelLineOverrides((prev) => ({ ...prev, [type]: override }));
  }, []);

  const handleSaveEdgeAlias = useCallback(async (relId: number, cqlAlias: string, gqlAlias: string) => {
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
  }, [adminRels]);

  // Build pkMap: compound label (e.g. "SalesAnalytics:Orders") → pk_columns
  // Falls back to idColumn (heuristically resolved) when no user-designated PKs
  const pkMap: Record<string, string[]> = {};
  for (const node of schemaNodeLabels) {
    const compoundLabel = node.domainLabel ? `${node.domainLabel}:${node.tableLabel}` : node.tableLabel;
    pkMap[compoundLabel] = node.pkColumns.length > 0
      ? node.pkColumns
      : (node.idColumn ? [node.idColumn] : []);
  }

  const handleHistorySelect = useCallback((q: string) => setHistoryQuery(q), []);
  const handleLabelClick = useCallback((label: string) => {
    runQuery(`MATCH (n:${label}) RETURN n LIMIT 25`);
  }, [runQuery]);

  const handleRelClick = useCallback((type: string) => {
    runQuery(`MATCH ()-[r:${type}]->() RETURN r LIMIT 25`);
  }, [runQuery]);

  return (
    <div className="graph-page">
      <Sidebar
        schemaNodeLabels={schemaNodeLabels}
        schemaRels={schemaRels}
        schemaLoading={schemaLoading}
        history={history}
        colorOverrides={colorOverrides}
        sizeOverrides={sizeOverrides}
        labelProperty={labelProperty}
        relLineOverrides={relLineOverrides}
        onHistorySelect={handleHistorySelect}
        onLabelClick={handleLabelClick}
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
          initialQuery={historyQuery ?? undefined}
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
              colorOverrides={colorOverrides}
              sizeOverrides={sizeOverrides}
              labelProperty={labelProperty}
              relLineOverrides={relLineOverrides}
              onColorChange={handleColorChange}
              pkMap={pkMap}
              relationships={adminRels}
              onSaveEdgeAlias={handleSaveEdgeAlias}
            />
          ))}
        </div>
      </div>
    </div>
  );
}
