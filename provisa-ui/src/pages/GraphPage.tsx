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
import CodeMirror from "@uiw/react-codemirror";
import * as _neo4jCypherMod from "@neo4j-cypher/codemirror";
import "@neo4j-cypher/codemirror/css/cypher-codemirror.css";
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const { getCypherLanguageExtensions, useAutocompleteExtensions, cypherLinter } = _neo4jCypherMod as any;
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
  type GNode,
  type GEdge,
} from "./GraphFrame";
import { fetchRelationships, upsertRelationship } from "../api/admin";
import { useAuth } from "../context/AuthContext";
import type { Relationship } from "../types/admin";
import "./GraphPage.css";
import { CopySymbolButton } from "../components/CopyButton";

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
  domainId: string | null;
  tableLabel: string;
  properties: string[];
  pkColumns: string[];
  idColumn: string | null;
  nativeFilterColumns: string[];
  scl1: number | null;
  scl2: number | null;
  scl3: number | null;
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

// ── Native filter modal ───────────────────────────────────────────────────────
interface NativeFilterModalProps {
  label: string;
  filterColumns: string[];
  onConfirm: (params: Record<string, string>) => void;
  onCancel: () => void;
}

function NativeFilterModal({ label, filterColumns, onConfirm, onCancel }: NativeFilterModalProps) {
  const [values, setValues] = useState<Record<string, string>>(() =>
    Object.fromEntries(filterColumns.map((c) => [c, ""]))
  );

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    onConfirm(values);
  };

  return (
    <div className="nf-modal-backdrop" onClick={onCancel}>
      <div className="nf-modal" onClick={(e) => e.stopPropagation()}>
        <div className="nf-modal-title">Parameters for {label}</div>
        <form onSubmit={handleSubmit}>
          {filterColumns.map((col) => (
            <div key={col} className="nf-modal-field">
              <label className="nf-modal-label">{col}</label>
              <input
                className="nf-modal-input"
                value={values[col]}
                onChange={(e) => setValues((v) => ({ ...v, [col]: e.target.value }))}
                placeholder={col}
                autoFocus={filterColumns[0] === col}
              />
            </div>
          ))}
          <div className="nf-modal-actions">
            <button type="button" className="nf-modal-cancel" onClick={onCancel}>Cancel</button>
            <button type="submit" className="nf-modal-run">Run</button>
          </div>
        </form>
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
  onDomainClick: (domainId: string) => void;
  onRelClick: (type: string) => void;
  onColorChange: (label: string, color: string) => void;
  onSizeChange: (label: string, size: number) => void;
  onLabelPropertyChange: (label: string, prop: string) => void;
  onRelLineChange: (type: string, override: RelLineOverride) => void;
  width: number;
  onWidthChange: (w: number) => void;
}

function Sidebar({ schemaNodeLabels, schemaRels, schemaLoading, history, colorOverrides, sizeOverrides, labelProperty, relLineOverrides, onHistorySelect, onLabelClick, onDomainClick, onRelClick, onColorChange, onSizeChange, onLabelPropertyChange, onRelLineChange, width, onWidthChange }: SidebarProps) {
  const [section, setSection] = useState<"db" | "history">("db");
  const [contextMenu, setContextMenu] = useState<ContextMenuState | null>(null);
  const [relContextMenu, setRelContextMenu] = useState<RelContextMenuState | null>(null);
  const [nodeLabelsCollapsed, setNodeLabelsCollapsed] = useState(false);
  const [relTypesCollapsed, setRelTypesCollapsed] = useState(false);
  const [nodeLabelsPage, setNodeLabelsPage] = useState(0);
  const [relTypesPage, setRelTypesPage] = useState(0);
  const SCHEMA_PAGE_SIZE = 50;
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
            {!schemaLoading && (() => {
              const domainLabels = [...new Set(schemaNodeLabels.map(n => n.domainLabel).filter(Boolean) as string[])].sort();
              return domainLabels.length > 0 ? (
                <div className="graph-schema-section">
                  <div className="graph-schema-heading">Domain Labels</div>
                  <div className="graph-label-list">
                    {domainLabels.map((lbl) => {
                      const color = colorOverrides[lbl] ?? labelColor(lbl);
                      return (
                        <div key={lbl} className="graph-label-item">
                          <span
                            className="graph-label-pill"
                            style={{ background: color }}
                            onClick={() => onDomainClick(lbl)}
                            onContextMenu={(e) => { e.preventDefault(); setContextMenu({ x: e.clientX, y: e.clientY, compoundLabel: lbl, tableLabel: lbl, properties: [] }); }}
                            title={`MATCH (n:${lbl}) RETURN n LIMIT 25`}
                          >
                            {lbl}
                          </span>
                        </div>
                      );
                    })}
                  </div>
                </div>
              ) : null;
            })()}
            <div className="graph-schema-section">
              <div className="graph-schema-heading graph-schema-heading--collapsible" onClick={() => setNodeLabelsCollapsed(c => !c)}>
                Node Labels
                <span className={`graph-schema-chevron ${nodeLabelsCollapsed ? "graph-schema-chevron--collapsed" : ""}`}>▾</span>
              </div>
              {!nodeLabelsCollapsed && (schemaLoading ? (
                <div className="graph-schema-empty">Loading…</div>
              ) : schemaNodeLabels.length === 0 ? (
                <div className="graph-schema-empty">No labels found</div>
              ) : (
                <div className="graph-label-list">
                  {(() => {
                    const sorted = [...schemaNodeLabels].sort((a, b) => a.tableLabel.localeCompare(b.tableLabel));
                    const totalPages = Math.max(1, Math.ceil(sorted.length / SCHEMA_PAGE_SIZE));
                    const paged = sorted.slice(nodeLabelsPage * SCHEMA_PAGE_SIZE, (nodeLabelsPage + 1) * SCHEMA_PAGE_SIZE);
                    return paged.map((node) => {
                    const compoundLabel = node.domainLabel
                      ? `${node.domainLabel}:${node.tableLabel}`
                      : node.tableLabel;
                    const color = colorOverrides[compoundLabel] ?? labelColor(compoundLabel);
                    return (
                      <div key={compoundLabel} className="graph-label-item">
                        <span
                          className="graph-label-pill"
                          style={{ background: color }}
                          draggable
                          onDragStart={(e) => e.dataTransfer.setData("text/x-provisa-label", compoundLabel)}
                          onClick={() => onLabelClick(compoundLabel)}
                          onContextMenu={(e) => handleNodeRightClick(e, node)}
                          title={`MATCH (n:${compoundLabel}) RETURN n LIMIT 25`}
                        >
                          {node.tableLabel}
                        </span>
                      </div>
                    );
                    });
                  })()}
                  {(() => {
                    const sorted = [...schemaNodeLabels].sort((a, b) => a.tableLabel.localeCompare(b.tableLabel));
                    const totalPages = Math.max(1, Math.ceil(sorted.length / SCHEMA_PAGE_SIZE));
                    if (totalPages === 1) return null;
                    return (
                      <div style={{ display: "flex", gap: "0.25rem", padding: "0.4rem 0", fontSize: "0.65rem", color: "var(--text-muted)", justifyContent: "center" }}>
                        <button onClick={() => setNodeLabelsPage(0)} disabled={nodeLabelsPage === 0} style={{ background: "none", border: "none", cursor: nodeLabelsPage > 0 ? "pointer" : "default", padding: "0.1rem 0.2rem", color: "var(--text-muted)", opacity: nodeLabelsPage > 0 ? 1 : 0.4 }}>«</button>
                        <button onClick={() => setNodeLabelsPage(p => p - 1)} disabled={nodeLabelsPage === 0} style={{ background: "none", border: "none", cursor: nodeLabelsPage > 0 ? "pointer" : "default", padding: "0.1rem 0.2rem", color: "var(--text-muted)", opacity: nodeLabelsPage > 0 ? 1 : 0.4 }}>‹</button>
                        <span>{nodeLabelsPage + 1}/{totalPages}</span>
                        <button onClick={() => setNodeLabelsPage(p => p + 1)} disabled={nodeLabelsPage >= totalPages - 1} style={{ background: "none", border: "none", cursor: nodeLabelsPage < totalPages - 1 ? "pointer" : "default", padding: "0.1rem 0.2rem", color: "var(--text-muted)", opacity: nodeLabelsPage < totalPages - 1 ? 1 : 0.4 }}>›</button>
                        <button onClick={() => setNodeLabelsPage(totalPages - 1)} disabled={nodeLabelsPage >= totalPages - 1} style={{ background: "none", border: "none", cursor: nodeLabelsPage < totalPages - 1 ? "pointer" : "default", padding: "0.1rem 0.2rem", color: "var(--text-muted)", opacity: nodeLabelsPage < totalPages - 1 ? 1 : 0.4 }}>»</button>
                      </div>
                    );
                  })()}
                </div>
              ))}
            </div>

            <div className="graph-schema-section">
              <div className="graph-schema-heading graph-schema-heading--collapsible" onClick={() => setRelTypesCollapsed(c => !c)}>
                Relationship Types
                <span className={`graph-schema-chevron ${relTypesCollapsed ? "graph-schema-chevron--collapsed" : ""}`}>▾</span>
              </div>
              {!relTypesCollapsed && (schemaLoading ? (
                <div className="graph-schema-empty">Loading…</div>
              ) : schemaRels.length === 0 ? (
                <div className="graph-schema-empty">No relationship types found</div>
              ) : (
                <div className="graph-rel-list">
                  {(() => {
                    const uniqueRels = [...new Map(schemaRels.map(r => [r.type, r])).values()];
                    const totalPages = Math.max(1, Math.ceil(uniqueRels.length / SCHEMA_PAGE_SIZE));
                    const paged = uniqueRels.slice(relTypesPage * SCHEMA_PAGE_SIZE, (relTypesPage + 1) * SCHEMA_PAGE_SIZE);
                    return paged.map(({ type }) => {
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
                    });
                  })()}
                  {(() => {
                    const uniqueRels = [...new Map(schemaRels.map(r => [r.type, r])).values()];
                    const totalPages = Math.max(1, Math.ceil(uniqueRels.length / SCHEMA_PAGE_SIZE));
                    if (totalPages === 1) return null;
                    return (
                      <div style={{ display: "flex", gap: "0.25rem", padding: "0.4rem 0", fontSize: "0.65rem", color: "var(--text-muted)", justifyContent: "center" }}>
                        <button onClick={() => setRelTypesPage(0)} disabled={relTypesPage === 0} style={{ background: "none", border: "none", cursor: relTypesPage > 0 ? "pointer" : "default", padding: "0.1rem 0.2rem", color: "var(--text-muted)", opacity: relTypesPage > 0 ? 1 : 0.4 }}>«</button>
                        <button onClick={() => setRelTypesPage(p => p - 1)} disabled={relTypesPage === 0} style={{ background: "none", border: "none", cursor: relTypesPage > 0 ? "pointer" : "default", padding: "0.1rem 0.2rem", color: "var(--text-muted)", opacity: relTypesPage > 0 ? 1 : 0.4 }}>‹</button>
                        <span>{relTypesPage + 1}/{totalPages}</span>
                        <button onClick={() => setRelTypesPage(p => p + 1)} disabled={relTypesPage >= totalPages - 1} style={{ background: "none", border: "none", cursor: relTypesPage < totalPages - 1 ? "pointer" : "default", padding: "0.1rem 0.2rem", color: "var(--text-muted)", opacity: relTypesPage < totalPages - 1 ? 1 : 0.4 }}>›</button>
                        <button onClick={() => setRelTypesPage(totalPages - 1)} disabled={relTypesPage >= totalPages - 1} style={{ background: "none", border: "none", cursor: relTypesPage < totalPages - 1 ? "pointer" : "default", padding: "0.1rem 0.2rem", color: "var(--text-muted)", opacity: relTypesPage < totalPages - 1 ? 1 : 0.4 }}>»</button>
                      </div>
                    );
                  })()}
                </div>
              ))}
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

// ── Module-level state (survives SPA navigation, persisted to localStorage) ───
const _LS_KEY = "provisa.graph.state";
const _DEFAULT_QUERY = "MATCH (n) RETURN n LIMIT 25";

interface _SerializedFrame {
  id: string; query: string; status: "done" | "error";
  nodes: [string, GNode][];
  edges: [string, GEdge][];
  rows: Record<string, unknown>[]; columns: string[];
  error?: string; elapsed?: number;
}

function _serializeFrame(f: FrameData): _SerializedFrame {
  return {
    id: f.id, query: f.query,
    status: f.status === "loading" ? "error" : f.status,
    error: f.status === "loading" ? "Session interrupted" : f.error,
    nodes: [...f.nodes.entries()],
    edges: [...f.edges.entries()],
    rows: f.rows, columns: f.columns, elapsed: f.elapsed,
  };
}

function _deserializeFrame(s: _SerializedFrame): FrameData {
  return { ...s, nodes: new Map(s.nodes), edges: new Map(s.edges) };
}

function _saveGraphState(state: typeof _graphState): void {
  try {
    localStorage.setItem(_LS_KEY, JSON.stringify({
      frames: state.frames.map(_serializeFrame),
      history: state.history,
      currentQuery: state.currentQuery,
    }));
  } catch { /* quota */ }
}

function _loadGraphState(): { frames: FrameData[]; history: string[]; currentQuery: string } {
  try {
    const raw = localStorage.getItem(_LS_KEY);
    if (!raw) return { frames: [], history: [], currentQuery: _DEFAULT_QUERY };
    const s = JSON.parse(raw) as { frames?: _SerializedFrame[]; history?: string[]; currentQuery?: string };
    return {
      frames: (s.frames ?? []).map(_deserializeFrame),
      history: s.history ?? [],
      currentQuery: s.currentQuery ?? _DEFAULT_QUERY,
    };
  } catch {
    return { frames: [], history: [], currentQuery: _DEFAULT_QUERY };
  }
}

const _graphState = _loadGraphState();

// ── Query bar ─────────────────────────────────────────────────────────────────
interface CypherSchema {
  labels: string[];
  relationshipTypes: string[];
  propertyKeys: string[];
}

interface QueryBarProps {
  onRun: (query: string) => void;
  initialQuery?: string;
  onQueryChange: (q: string) => void;
  cypherSchema?: CypherSchema;
  autoImpute: boolean;
  onToggleAutoImpute: () => void;
}

// Polyfill: @neo4j-cypher/codemirror 1.x calls view.newContentVersion() which doesn't exist on current @codemirror/view
if (!(EditorView.prototype as any).newContentVersion) {
  let _ver = 0;
  (EditorView.prototype as any).newContentVersion = function () { return ++_ver; };
}

// eslint-disable-next-line @typescript-eslint/no-explicit-any
const _cypherLangExts = getCypherLanguageExtensions({ cypherLanguage: true } as any);

function QueryBar({ onRun, initialQuery, onQueryChange, cypherSchema, autoImpute, onToggleAutoImpute }: QueryBarProps) {
  const [query, setQuery] = useState(initialQuery ?? "MATCH (n) RETURN n LIMIT 25");
  const viewRef = useRef<EditorView | null>(null);

  useEffect(() => {
    if (!cypherSchema || !viewRef.current) return;
    try {
      // editorSupportField is included by getCypherLanguageExtensions
      // eslint-disable-next-line @typescript-eslint/no-require-imports
      // @ts-ignore
      const { editorSupportField } = require("@neo4j-cypher/codemirror/lib/cypher-state-definitions") as { editorSupportField: import("@codemirror/state").StateField<{ setSchema: (s: CypherSchema) => void }> };
      const editorSupport = viewRef.current.state.field(editorSupportField, false);
      if (editorSupport) editorSupport.setSchema(cypherSchema);
    } catch (_) { /* subpath not resolved */ }
  }, [cypherSchema]);

  const handleChange = (val: string) => {
    setQuery(val);
    onQueryChange(val);
  };

  return (
    <div className="graph-query-bar">
      <div className="graph-query-prompt">$</div>
      <div className="graph-query-editor-wrap">
        <CodeMirror
          className="graph-query-input"
          value={query}
          theme={oneDark}
          extensions={[
            ..._cypherLangExts,
            cypherLinter({ showErrors: false }),
            useAutocompleteExtensions,
            EditorView.lineWrapping,
            Prec.highest(keymap.of([{
              key: "Mod-Enter",
              run: () => { onRun(query.trim()); return true; },
            }, {
              key: "Enter",
              run: () => { onRun(query.trim()); return true; },
            }])),
          ]}
          onCreateEditor={(view) => { viewRef.current = view; }}
          onChange={handleChange}
          basicSetup={{ lineNumbers: false, foldGutter: false, highlightActiveLine: false, completionKeymap: false }}
          placeholder="MATCH (n) RETURN n LIMIT 25"
        />
        <CopySymbolButton text={query} className="gf-copy-query-btn" title="Copy query" />
      </div>
      <button
        className={`gf-icon-btn${autoImpute ? " gf-icon-btn--on" : ""}`}
        onClick={onToggleAutoImpute}
        title={autoImpute ? "Auto-impute relationships ON — click to disable" : "Auto-impute relationships between visible nodes"}
        style={{ marginRight: 4 }}
      >⊕</button>
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
  const { role } = useAuth();
  const { checkedDomains } = useDomainFilter();
  const [frames, setFrames] = useState<FrameData[]>(_graphState.frames);
  const [history, setHistory] = useState<string[]>(_graphState.history);
  const [historyQuery, setHistoryQuery] = useState<string | null>(null);
  const [schemaNodeLabels, setSchemaNodeLabels] = useState<SchemaNodeLabel[]>([]);
  const [schemaRels, setSchemaRels] = useState<SchemaRel[]>([]);
  const [schemaLoading, setSchemaLoading] = useState(true);
  const [sidebarWidth, setSidebarWidth] = useState(240);
  const [colorOverrides, setColorOverrides] = useLocalStorage<Record<string, string>>("provisa.graph.colorOverrides", {});
  const [sizeOverrides, setSizeOverrides] = useLocalStorage<Record<string, number>>("provisa.graph.sizeOverrides", {});
  const [labelProperty, setLabelProperty] = useLocalStorage<Record<string, string>>("provisa.graph.labelProperty", {});
  const [autoImpute, setAutoImpute] = useLocalStorage<boolean>("provisa.graph.autoImpute", false);
  const [relLineOverrides, setRelLineOverrides] = useLocalStorage<Record<string, RelLineOverride>>("provisa.graph.relLineOverrides", {});
  const [adminRels, setAdminRels] = useState<Relationship[]>([]);
  const [nfModal, setNfModal] = useState<{ label: string; compoundLabel: string; filterColumns: string[] } | null>(null);
  const frameIdRef = useRef(_graphState.frames.reduce((max, f) => Math.max(max, parseInt(f.id) || 0), 0));
  const clusterMapRef = useRef<Record<string, { scl1: number | null; scl2: number | null; scl3: number | null }>>({});

  // Fetch admin relationships for edge alias editing
  useEffect(() => {
    fetchRelationships().then(setAdminRels).catch(() => {});
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
          (n: { label: string; domain_label: string | null; domain_id: string | null; table_label: string; properties: string[]; pk_columns: string[]; id_column?: string; native_filter_columns?: string[]; scl1?: number | null; scl2?: number | null; scl3?: number | null }) => ({
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
          })
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
        const newClusterMap: Record<string, { scl1: number | null; scl2: number | null; scl3: number | null }> = {};
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
  }, [role?.id]);

  const runQuery = useCallback(async (query: string) => {
    if (!query) return;
    const id = String(++frameIdRef.current);
    const start = Date.now();
    setFrames((f) => {
      const next = [{ id, query, status: "loading" as const, nodes: new Map(), edges: new Map(), rows: [], columns: [] }, ...f];
      _graphState.frames = next;
      _saveGraphState(_graphState);
      return next;
    });
    setHistory((h) => {
      const next = [query, ...h.filter((q) => q !== query).slice(0, 49)];
      _graphState.history = next;
      _saveGraphState(_graphState);
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
        try { msg = (JSON.parse(text) as { error?: string }).error ?? text; } catch { msg = text; }
        setFrames((f) => { const next = f.map((fr) => fr.id === id ? { ...fr, status: "error" as const, error: msg } : fr); _graphState.frames = next; _saveGraphState(_graphState); return next; });
        return;
      }
      const data = await res.json();
      const rows: Record<string, unknown>[] = data.rows ?? [];
      const columns: string[] = data.columns ?? [];
      const { nodes, edges } = extractElements(rows);
      nodes.forEach((node) => {
        const clusters = clusterMapRef.current[node.label];
        if (clusters) Object.assign(node.properties, { scl1: clusters.scl1, scl2: clusters.scl2, scl3: clusters.scl3 });
      });
      setFrames((f) => { const next = f.map((fr) => fr.id === id ? { ...fr, status: "done" as const, nodes, edges, rows, columns, elapsed } : fr); _graphState.frames = next; _saveGraphState(_graphState); return next; });
    } catch (err) {
      setFrames((f) => { const next = f.map((fr) => fr.id === id ? { ...fr, status: "error" as const, error: String(err) } : fr); _graphState.frames = next; _saveGraphState(_graphState); return next; });
    }
  }, [role]);

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
    setFrames((f) => { const next = f.filter((fr) => fr.id !== id); _graphState.frames = next; _saveGraphState(_graphState); return next; });
  }, []);

  const rerunFrame = useCallback(async (id: string, query: string) => {
    if (!query) return;
    const start = Date.now();
    setFrames((f) => { const next = f.map((fr) => fr.id === id ? { ...fr, query, status: "loading" as const, nodes: new Map(), edges: new Map(), rows: [], columns: [], elapsed: undefined, error: undefined } : fr); _graphState.frames = next; _saveGraphState(_graphState); return next; });
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
        try { msg = (JSON.parse(text) as { error?: string }).error ?? text; } catch { msg = text; }
        setFrames((f) => { const next = f.map((fr) => fr.id === id ? { ...fr, status: "error" as const, error: msg } : fr); _graphState.frames = next; _saveGraphState(_graphState); return next; });
        return;
      }
      const data = await res.json();
      const rows: Record<string, unknown>[] = data.rows ?? [];
      const columns: string[] = data.columns ?? [];
      const { nodes, edges } = extractElements(rows);
      nodes.forEach((node) => {
        const clusters = clusterMapRef.current[node.label];
        if (clusters) Object.assign(node.properties, { scl1: clusters.scl1, scl2: clusters.scl2, scl3: clusters.scl3 });
      });
      setFrames((f) => { const next = f.map((fr) => fr.id === id ? { ...fr, status: "done" as const, nodes, edges, rows, columns, elapsed } : fr); _graphState.frames = next; _saveGraphState(_graphState); return next; });
    } catch (err) {
      setFrames((f) => { const next = f.map((fr) => fr.id === id ? { ...fr, status: "error" as const, error: String(err) } : fr); _graphState.frames = next; _saveGraphState(_graphState); return next; });
    }
  }, [role]);

  const framesRef = useRef(frames);
  framesRef.current = frames;

  const onTableDrop = useCallback((frameId: string, compoundLabel: string) => {
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
      labels.split(":").forEach((l) => { varByLabel[l] = varName; });
    }

    // Find a rel where one side is a frame node and the other is the dropped table
    let sourceVar = "n";
    let relAlias: string | null = null;
    const rel = adminRels.find((r) => {
      const srcMatch = frameNodeLabels.has(r.sourceTableName) && r.targetTableName === droppedTableName;
      const tgtMatch = frameNodeLabels.has(r.targetTableName) && r.sourceTableName === droppedTableName;
      return srcMatch || tgtMatch;
    });
    if (rel) {
      const connectedLabel = frameNodeLabels.has(rel.sourceTableName) ? rel.sourceTableName : rel.targetTableName;
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
  }, [rerunFrame, adminRels]);

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
  const SYSTEM_DOMAINS = new Set(["meta", "ops"]);
  const visibleNodeLabels = checkedDomains.size === 0
    ? schemaNodeLabels
    : schemaNodeLabels.filter((n) => !n.domainId || checkedDomains.has(n.domainId) || SYSTEM_DOMAINS.has(n.domainId));

  // Falls back to idColumn (heuristically resolved) when no user-designated PKs
  const pkMap: Record<string, string[]> = {};
  for (const node of visibleNodeLabels) {
    const compoundLabel = node.domainLabel ? `${node.domainLabel}:${node.tableLabel}` : node.tableLabel;
    pkMap[compoundLabel] = node.pkColumns.length > 0
      ? node.pkColumns
      : (node.idColumn ? [node.idColumn] : []);
  }

  const cypherSchema: CypherSchema = {
    labels: visibleNodeLabels.flatMap((n) =>
      n.domainLabel ? [`${n.domainLabel}:${n.tableLabel}`, n.domainLabel, n.tableLabel] : [n.tableLabel]
    ).filter((v, i, a) => a.indexOf(v) === i),
    relationshipTypes: schemaRels.map((r) => r.type),
    propertyKeys: [...new Set(visibleNodeLabels.flatMap((n) => [...n.properties, ...n.nativeFilterColumns]))],
  };

  const handleHistorySelect = useCallback((q: string) => setHistoryQuery(q), []);
  const handleDomainClick = useCallback((domainId: string) => {
    runQuery(`MATCH (n:${domainId}) RETURN n LIMIT 25`);
  }, [runQuery]);
  const handleLabelClick = useCallback((compoundLabel: string) => {
    const node = schemaNodeLabels.find((n) => {
      const cl = n.domainLabel ? `${n.domainLabel}:${n.tableLabel}` : n.tableLabel;
      return cl === compoundLabel || n.domainLabel === compoundLabel;
    });
    if (node && node.nativeFilterColumns.length > 0) {
      setNfModal({ label: node.tableLabel, compoundLabel, filterColumns: node.nativeFilterColumns });
    } else {
      runQuery(`MATCH (n:${compoundLabel}) RETURN n LIMIT 25`);
    }
  }, [runQuery, schemaNodeLabels]);

  const handleRelClick = useCallback((type: string) => {
    runQuery(`MATCH ()-[r:${type}]->() RETURN r LIMIT 25`);
  }, [runQuery]);

  const handleNfConfirm = useCallback((params: Record<string, string>) => {
    if (!nfModal) return;
    const { compoundLabel, filterColumns } = nfModal;
    const whereClauses = filterColumns
      .filter((col) => params[col] !== "")
      .map((col) => `n._nf_${col} = '${params[col].replace(/\\/g, "\\\\").replace(/'/g, "\\'")}'`);
    const whereStr = whereClauses.length > 0 ? ` WHERE ${whereClauses.join(" AND ")}` : "";
    setNfModal(null);
    runQuery(`MATCH (n:${compoundLabel})${whereStr} RETURN n LIMIT 25`);
  }, [nfModal, runQuery]);

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
          initialQuery={historyQuery ?? _graphState.currentQuery}
          onQueryChange={(q) => { _graphState.currentQuery = q; _saveGraphState(_graphState); }}
          cypherSchema={schemaLoading ? undefined : cypherSchema}
          autoImpute={autoImpute}
          onToggleAutoImpute={() => setAutoImpute(v => !v)}
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
