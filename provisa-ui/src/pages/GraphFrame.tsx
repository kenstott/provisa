// Copyright (c) 2026 Kenneth Stott
// Canary: a3f7e2d1-8c4b-4a9f-b5e6-2d1c7f8a3e4b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useRef, useEffect, useState, useCallback } from "react";
import { createPortal } from "react-dom";
import cytoscape from "cytoscape";
import type { Core, NodeSingular } from "cytoscape";

// ── Palette ───────────────────────────────────────────────────────────────────
export const PALETTE = [
  "#6366f1","#22c55e","#f59e0b","#ec4899",
  "#14b8a6","#f97316","#8b5cf6","#06b6d4",
  "#d946ef","#10b981",
];

export function labelColor(label: string): string {
  let h = 0;
  for (let i = 0; i < label.length; i++) h = (h * 31 + label.charCodeAt(i)) & 0xffff;
  return PALETTE[h % PALETTE.length];
}

// ── Wire types ────────────────────────────────────────────────────────────────
export interface GNode {
  id: string;
  label: string;
  properties: Record<string, unknown>;
}
export interface GEdge {
  id: string;
  type: string;
  startNode: GNode;
  endNode: GNode;
  properties: Record<string, unknown>;
}

export function isNode(v: unknown): v is GNode {
  if (typeof v !== "object" || v === null) return false;
  const o = v as Record<string, unknown>;
  return "label" in o && "properties" in o && !("startNode" in o);
}
export function isEdge(v: unknown): v is GEdge {
  if (typeof v !== "object" || v === null) return false;
  const o = v as Record<string, unknown>;
  return "type" in o && "startNode" in o && "endNode" in o;
}

export function extractElements(rows: unknown[]): { nodes: Map<string, GNode>; edges: Map<string, GEdge> } {
  const nodes = new Map<string, GNode>();
  const edges = new Map<string, GEdge>();
  function walk(v: unknown) {
    if (v === null || v === undefined) return;
    if (isEdge(v)) {
      walk(v.startNode);
      walk(v.endNode);
      edges.set(v.id, v);
    } else if (isNode(v)) {
      nodes.set(v.id, v);
    } else if (Array.isArray(v)) {
      v.forEach(walk);
    } else if (typeof v === "object") {
      Object.values(v as Record<string, unknown>).forEach(walk);
    }
  }
  rows.forEach(walk);
  return { nodes, edges };
}

// ── Frame data ────────────────────────────────────────────────────────────────
export interface FrameData {
  id: string;
  query: string;
  status: "loading" | "done" | "error";
  nodes: Map<string, GNode>;
  edges: Map<string, GEdge>;
  rows: Record<string, unknown>[];
  columns: string[];
  error?: string;
  elapsed?: number;
}

// ── Inspector panel ───────────────────────────────────────────────────────────
interface InspectorProps {
  selected: { kind: "node"; data: GNode } | { kind: "edge"; data: GEdge } | null;
  colorOverrides: Record<string, string>;
  onColorChange: (label: string, color: string) => void;
  onClose: () => void;
  width: number;
  onResizeStart: (e: React.MouseEvent) => void;
}

function Inspector({ selected, colorOverrides, onColorChange, onClose, width, onResizeStart }: InspectorProps) {
  const [inspView, setInspView] = useState<"details" | "table" | "json">("details");
  const [showPalette, setShowPalette] = useState(false);
  const [hovered, setHovered] = useState(false);

  if (!selected) return null;

  const viewSel = hovered && (
    <div className="gf-insp-viewsel">
      {(["details", "table", "json"] as const).map((v) => (
        <button key={v} className={`gf-insp-viewbtn ${inspView === v ? "active" : ""}`}
                onClick={() => setInspView(v)} title={v}>
          {v === "details" ? "⊡" : v === "table" ? "⊞" : "{}"}
        </button>
      ))}
    </div>
  );

  const isN = selected.kind === "node";
  const label = isN ? selected.data.label : (selected.data as GEdge).type;
  const color = colorOverrides[label] ?? labelColor(label);
  const props = selected.data.properties;
  const allFields: Record<string, unknown> = {
    id: selected.data.id,
    ...(isN ? { label: (selected.data as GNode).label } : { type: (selected.data as GEdge).type }),
    ...props,
  };

  return (
    <div className="gf-inspector" style={{ width }}
         onMouseEnter={() => setHovered(true)}
         onMouseLeave={() => { setHovered(false); setShowPalette(false); }}>
      <div className="gf-inspector-resize-handle" onMouseDown={onResizeStart} />
      <button className="gf-insp-close" onClick={onClose} title="Close">✕</button>
      {viewSel}
      <div style={{ position: "relative", alignSelf: "flex-start" }}>
        <div className="gf-inspector-badge" style={{ background: color, cursor: "pointer" }}
             title="Click to change color" onClick={() => setShowPalette((p) => !p)}>
          {label}
        </div>
        {showPalette && (
          <div className="gf-color-palette">
            {PALETTE.map((c) => (
              <button key={c} className="gf-color-swatch"
                      style={{ background: c, outline: color === c ? "2px solid #fff" : "none" }}
                      onClick={() => { onColorChange(label, c); setShowPalette(false); }} />
            ))}
          </div>
        )}
      </div>
      <div className="gf-inspector-kind">{isN ? "Node" : "Relationship"}</div>
      <div className="gf-inspector-id">id: {selected.data.id}</div>
      {!isN && (
        <div className="gf-inspector-endpoints">
          <span style={{ color: colorOverrides[(selected.data as GEdge).startNode.label] ?? labelColor((selected.data as GEdge).startNode.label) }}>
            {(selected.data as GEdge).startNode.label}
          </span>
          {" → "}
          <span style={{ color: colorOverrides[(selected.data as GEdge).endNode.label] ?? labelColor((selected.data as GEdge).endNode.label) }}>
            {(selected.data as GEdge).endNode.label}
          </span>
        </div>
      )}
      {inspView === "details" && (
        Object.keys(props).length === 0 ? (
          <div className="gf-inspector-no-props">No properties</div>
        ) : (
          <table className="gf-inspector-table">
            <tbody>
              {Object.entries(props).map(([k, v]) => (
                <tr key={k}>
                  <td className="gf-prop-key">{k}</td>
                  <td className="gf-prop-val">{String(v)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )
      )}
      {inspView === "table" && (
        <table className="gf-inspector-table">
          <tbody>
            {Object.entries(allFields).map(([k, v]) => (
              <tr key={k}>
                <td className="gf-prop-key">{k}</td>
                <td className="gf-prop-val">{String(v)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      {inspView === "json" && (
        <pre className="gf-inspector-json">{JSON.stringify(selected.data, null, 2)}</pre>
      )}
    </div>
  );
}

// ── Graph canvas ──────────────────────────────────────────────────────────────
interface CanvasProps {
  nodes: Map<string, GNode>;
  edges: Map<string, GEdge>;
  onSelect: (item: { kind: "node"; data: GNode } | { kind: "edge"; data: GEdge } | null) => void;
  colorOverrides: Record<string, string>;
}

function GraphCanvas({ nodes, edges, onSelect, colorOverrides }: CanvasProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const cyRef = useRef<Core | null>(null);
  const colorOverridesRef = useRef(colorOverrides);
  colorOverridesRef.current = colorOverrides;

  const fitView = useCallback(() => cyRef.current?.fit(undefined, 40), []);

  useEffect(() => {
    if (!containerRef.current) return;
    const els: cytoscape.ElementDefinition[] = [];
    nodes.forEach((n) => {
      els.push({ group: "nodes", data: { id: n.id, label: n.label, _node: n } });
    });
    edges.forEach((e) => {
      if (nodes.has(e.startNode.id) && nodes.has(e.endNode.id)) {
        els.push({
          group: "edges",
          data: { id: e.id, source: e.startNode.id, target: e.endNode.id, label: e.type, _edge: e },
        });
      }
    });

    const cy = cytoscape({
      container: containerRef.current,
      elements: els,
      style: [
        {
          selector: "node",
          style: {
            "background-color": (ele: NodeSingular) => {
              const lbl = ele.data("label") as string;
              return colorOverridesRef.current[lbl] ?? labelColor(lbl);
            },
            "label": (ele: NodeSingular) => {
              const n = ele.data("_node") as GNode | undefined;
              if (!n) return String(ele.data("label") ?? "");
              return n.label || String(n.properties["name"] ?? n.properties["title"] ?? n.id);
            },
            "color": "#fff",
            "font-size": 10,
            "text-valign": "center",
            "text-halign": "center",
            "width": 44,
            "height": 44,
            "text-wrap": "ellipsis",
            "text-max-width": "36px",
            "border-width": 2,
            "border-color": "rgba(255,255,255,0.15)",
          },
        },
        {
          selector: "node:selected",
          style: {
            "border-width": 4,
            "border-color": "#fff",
            "overlay-color": "#fff",
            "overlay-opacity": 0.08,
          },
        },
        {
          selector: "edge",
          style: {
            "line-color": "#3a3d4e",
            "target-arrow-color": "#3a3d4e",
            "target-arrow-shape": "triangle",
            "curve-style": "bezier",
            "label": "data(label)",
            "font-size": 8,
            "color": "#6b6f82",
            "text-background-color": "#0f1117",
            "text-background-opacity": 0.85,
            "text-background-padding": "2px",
            "width": 1.5,
          },
        },
        {
          selector: "edge:selected",
          style: {
            "line-color": "#6366f1",
            "target-arrow-color": "#6366f1",
            "width": 2.5,
          },
        },
      ],
      layout: {
        name: els.length > 0 ? "cose" : "null",
        animate: false,
        nodeRepulsion: () => 10000,
        idealEdgeLength: () => 80,
        gravity: 0.3,
        numIter: 1000,
        initialTemp: 200,
        coolingFactor: 0.95,
        minTemp: 1,
      } as cytoscape.LayoutOptions,
      minZoom: 0.05,
      maxZoom: 8,
    });

    cy.on("tap", "node", (evt) => {
      onSelect({ kind: "node", data: evt.target.data("_node") as GNode });
    });
    cy.on("tap", "edge", (evt) => {
      onSelect({ kind: "edge", data: evt.target.data("_edge") as GEdge });
    });
    cy.on("tap", (evt) => {
      if (evt.target === cy) onSelect(null);
    });

    cyRef.current = cy;
    return () => {
      cyRef.current = null;
      cy.destroy();
    };
  }, [nodes, edges]); // eslint-disable-line react-hooks/exhaustive-deps

  // Update node colors when colorOverrides changes without rebuilding the graph
  useEffect(() => {
    const cy = cyRef.current;
    if (!cy) return;
    cy.nodes().forEach((node) => {
      const lbl = node.data("label") as string;
      node.style("background-color", colorOverridesRef.current[lbl] ?? labelColor(lbl));
    });
  }, [colorOverrides]);

  return (
    <div className="gf-canvas-wrap">
      <div ref={containerRef} className="gf-canvas" />
      <div className="gf-canvas-controls">
        <button className="gf-ctrl-btn" onClick={() => cyRef.current?.zoom(cyRef.current.zoom() * 1.3)} title="Zoom in">+</button>
        <button className="gf-ctrl-btn" onClick={() => cyRef.current?.zoom(cyRef.current.zoom() * 0.77)} title="Zoom out">−</button>
        <button className="gf-ctrl-btn" onClick={fitView} title="Fit to screen">⤢</button>
      </div>
    </div>
  );
}

// ── Table view ────────────────────────────────────────────────────────────────
function cellText(v: unknown): string {
  if (v === null || v === undefined) return "";
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}

function TableView({ columns, rows }: { columns: string[]; rows: Record<string, unknown>[] }) {
  if (rows.length === 0) return <div className="gf-table-empty">No rows</div>;
  return (
    <div className="gf-table-wrap">
      <table className="gf-table">
        <thead>
          <tr>{columns.map((c) => <th key={c}>{c}</th>)}</tr>
        </thead>
        <tbody>
          {rows.map((r, i) => (
            <tr key={i}>
              {columns.map((c) => (
                <td key={c}>{cellText(r[c])}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

// ── Frame component ───────────────────────────────────────────────────────────
interface GraphFrameProps {
  frame: FrameData;
  onClose: (id: string) => void;
  onRerun: (id: string, query: string) => void;
}

export function GraphFrame({ frame, onClose, onRerun }: GraphFrameProps) {
  const [view, setView] = useState<"graph" | "table" | "json">("graph");
  const [selected, setSelected] = useState<{ kind: "node"; data: GNode } | { kind: "edge"; data: GEdge } | null>(null);
  const [collapsed, setCollapsed] = useState(false);
  const [expanded, setExpanded] = useState(false);
  const [colorOverrides, setColorOverrides] = useState<Record<string, string>>({});
  const [inspectorWidth, setInspectorWidth] = useState(260);
  const [editQuery, setEditQuery] = useState(frame.query);
  const inspWidthRef = useRef(inspectorWidth);
  inspWidthRef.current = inspectorWidth;

  const handleColorChange = (label: string, color: string) =>
    setColorOverrides((prev) => ({ ...prev, [label]: color }));

  const handleResizeStart = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    const startX = e.clientX;
    const startW = inspWidthRef.current;
    const onMove = (me: MouseEvent) => {
      setInspectorWidth(Math.max(160, Math.min(480, startW + (startX - me.clientX))));
    };
    const onUp = () => {
      window.removeEventListener("mousemove", onMove);
      window.removeEventListener("mouseup", onUp);
    };
    window.addEventListener("mousemove", onMove);
    window.addEventListener("mouseup", onUp);
  }, []);

  const hasGraph = frame.nodes.size > 0 || frame.edges.size > 0;
  const activeView: "graph" | "table" | "json" = hasGraph ? view : (view === "json" ? "json" : "table");

  const renderHeader = (isModal: boolean) => (
    <div className="gf-header">
      <input
        className="gf-header-query-input"
        value={editQuery}
        onChange={(e) => setEditQuery(e.target.value)}
        onKeyDown={(e) => {
          if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); onRerun(frame.id, editQuery.trim()); }
        }}
        spellCheck={false}
      />
      <div className="gf-header-meta">
        {frame.status === "loading" && <span className="gf-loading">Running…</span>}
        {frame.status === "done" && (
          <span className="gf-meta-text">
            {frame.nodes.size} nodes · {frame.edges.size} rels
            {frame.elapsed !== undefined && ` · ${frame.elapsed}ms`}
          </span>
        )}
        {frame.status === "error" && <span className="gf-meta-error">Error</span>}
      </div>
      <div className="gf-header-actions">
        <button className="gf-run-inline-btn" onClick={() => onRerun(frame.id, editQuery.trim())} title="Run">▶</button>
        {hasGraph && (
          <button className={`gf-view-btn ${activeView === "graph" ? "active" : ""}`}
                  onClick={() => setView("graph")} title="Graph">⬡</button>
        )}
        <button className={`gf-view-btn ${activeView === "table" ? "active" : ""}`}
                onClick={() => setView("table")} title="Table">⊞</button>
        <button className={`gf-view-btn ${activeView === "json" ? "active" : ""}`}
                onClick={() => setView("json")} title="JSON">{"{}"}</button>
        {!isModal && (
          <button className="gf-icon-btn" onClick={() => setExpanded(true)} title="Expand">⤢</button>
        )}
        {isModal && (
          <button className="gf-icon-btn" onClick={() => setExpanded(false)} title="Exit full screen">⤡</button>
        )}
        {!isModal && (
          <button className="gf-icon-btn" onClick={() => setCollapsed((c) => !c)} title={collapsed ? "Expand" : "Collapse"}>
            {collapsed ? "▼" : "▲"}
          </button>
        )}
        <button className="gf-icon-btn" onClick={() => onClose(frame.id)} title="Close">✕</button>
      </div>
    </div>
  );

  const frameBody = (
    <div className="gf-body">
      {frame.status === "error" && <div className="gf-error">{frame.error}</div>}
      {frame.status !== "error" && activeView === "graph" && (
        <div className="gf-graph-area">
          <GraphCanvas nodes={frame.nodes} edges={frame.edges} onSelect={setSelected} colorOverrides={colorOverrides} />
          <Inspector selected={selected} colorOverrides={colorOverrides} onColorChange={handleColorChange}
                     onClose={() => setSelected(null)}
                     width={inspectorWidth} onResizeStart={handleResizeStart} />
        </div>
      )}
      {frame.status !== "error" && activeView === "table" && (
        <TableView columns={frame.columns} rows={frame.rows} />
      )}
      {frame.status !== "error" && activeView === "json" && (
        <pre className="gf-json-view">{JSON.stringify(frame.rows, null, 2)}</pre>
      )}
    </div>
  );

  return (
    <>
      <div className="gf-frame">
        {renderHeader(false)}
        {!collapsed && !expanded && frameBody}
      </div>
      {expanded && createPortal(
        <div className="gf-modal-overlay" onClick={() => setExpanded(false)}>
          <div className="gf-modal-frame" onClick={(e) => e.stopPropagation()}>
            {renderHeader(true)}
            {frameBody}
          </div>
        </div>,
        document.body
      )}
    </>
  );
}
