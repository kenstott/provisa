// Copyright (c) 2026 Kenneth Stott
// Canary: e5692b82-a5ec-4d86-9cd1-51afff8d3874
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/* eslint-disable react-hooks/refs --
   A prev-value ref gates the documented render-phase setState that resyncs the
   alias inputs when the selected edge changes; this is intentional. */

import { useRef, useState, useMemo, useCallback } from "react";
import type { Relationship } from "../../types/admin";
import { PALETTE, labelColor, getStableNodeId } from "./graph-model";
import type { GNode, GEdge, GraphStats } from "./graph-model";
import CodeMirror from "@uiw/react-codemirror";
import { json as jsonLang } from "@codemirror/lang-json";
import { oneDark } from "@codemirror/theme-one-dark";

interface InspectorProps {
  selected: { kind: "node"; data: GNode } | { kind: "edge"; data: GEdge } | null;
  graphStats?: GraphStats;
  colorOverrides: Record<string, string>;
  onColorChange: (label: string, color: string) => void;
  onClose: () => void;
  width: number;
  onResizeStart: (e: React.MouseEvent) => void;
  relationships?: Relationship[];
  onSaveEdgeAlias?: (relId: number, cqlAlias: string, gqlAlias: string) => Promise<void>;
  pkMap: Record<string, string[]>;
}

function CopyIcon() {
  return (
    <svg viewBox="0 0 16 16" width="12" height="12" fill="currentColor">
      <path d="M4 4h7v1H4zM4 6h7v1H4zM4 8h5v1H4z" opacity="0.6"/>
      <rect x="1" y="2" width="9" height="11" rx="1" fill="none" stroke="currentColor" strokeWidth="1.2"/>
      <rect x="5" y="5" width="9" height="9" rx="1" fill="#111318" stroke="currentColor" strokeWidth="1.2"/>
      <path d="M7 7h5v1H7zM7 9h5v1H7zM7 11h3v1H7z"/>
    </svg>
  );
}

export function Inspector({
  selected,
  graphStats,
  colorOverrides,
  onColorChange,
  onClose,
  width,
  onResizeStart,
  relationships,
  onSaveEdgeAlias,
  pkMap,
}: InspectorProps) {
  const [inspView, setInspView] = useState<"details" | "json">("details");
  const [showPalette, setShowPalette] = useState(false);
  const [edgeCql, setEdgeCql] = useState("");
  const [edgeGql, setEdgeGql] = useState("");
  const [savingAlias, setSavingAlias] = useState(false);
  const [copiedKey, setCopiedKey] = useState<string | null>(null);

  const matchedRel = useMemo(() => {
    if (!selected || selected.kind !== "edge" || !relationships) return null;
    const edgeType = (selected.data as GEdge).type;
    return relationships.find((r) => (r.alias ?? r.computedCypherAlias) === edgeType) ?? null;
  }, [selected, relationships]);

  const prevRelId = useRef<number | null>(null);
  if (matchedRel && matchedRel.id !== prevRelId.current) {
    prevRelId.current = matchedRel.id;
    setEdgeCql(matchedRel.alias ?? "");
    setEdgeGql(matchedRel.graphqlAlias ?? "");
  }
  if (!matchedRel && prevRelId.current !== null) {
    prevRelId.current = null;
    setEdgeCql("");
    setEdgeGql("");
  }

  const handleCopy = useCallback((key: string, value: string) => {
    navigator.clipboard.writeText(value).catch(() => {});
    setCopiedKey(key);
    setTimeout(() => setCopiedKey(null), 1200);
  }, []);

  if (!selected) {
    return (
      <div className="gf-inspector" style={{ width, flexShrink: 0 }}>
        <div className="gf-inspector-resize-handle" onMouseDown={onResizeStart} />
        <div className="gf-insp-header">
          <span className="gf-insp-header-title">Node properties</span>
          <div className="gf-insp-header-actions">
            <button className="gf-insp-close" onClick={onClose} title="Hide panel">›</button>
          </div>
        </div>
        <div className="gf-inspector-hint">Click a node or edge to inspect its properties.</div>
      </div>
    );
  }

  const isN = selected.kind === "node";
  const label = isN ? selected.data.label : (selected.data as GEdge).type;
  const color = colorOverrides[label] ?? labelColor(label);
  const props = selected.data.properties;

  const nodeLabel = isN ? (selected.data as GNode).label : "";
  const colonIdx = nodeLabel.indexOf(":");
  const typeName = colonIdx > 0 ? nodeLabel.slice(colonIdx + 1) : nodeLabel;

  const stableId = isN ? getStableNodeId(selected.data as GNode, pkMap) : null;

  const pkCols = isN ? (pkMap[(selected.data as GNode).label] ?? []) : [];
  const idColName = pkCols[0] ?? null;
  const pkEntry: Record<string, unknown> =
    isN && idColName && !(idColName in props) ? { [idColName]: (selected.data as GNode).id } : {};

  const HIDDEN_PROPS = new Set(["l1Cluster", "l2Cluster", "l3Cluster", "scl1", "scl2", "scl3", "deg_in", "deg_out", "deg_total"]);

  const idRow: [string, unknown][] = stableId ? [["<id>", stableId]] : [];

  const propRows: [string, unknown][] = isN
    ? [
        ...idRow,
        ...Object.entries(props).filter(([k]) => !HIDDEN_PROPS.has(k)).sort(([a], [b]) => a.localeCompare(b)),
        ...Object.entries(pkEntry),
      ]
    : (() => {
        const e = selected.data as GEdge;
        return [
          ["<id>", e.identity],
          ["start", e.start],
          ["end", e.end],
          ...Object.entries(props).sort(([a], [b]) => a.localeCompare(b)),
        ] as [string, unknown][];
      })();

  const headerLabel = isN ? "Node properties" : "Relationship properties";
  const chipLabel = isN ? (typeName || label) : label;

  return (
    <div className="gf-inspector" style={{ width }}>
      <div className="gf-inspector-resize-handle" onMouseDown={onResizeStart} />
      <div className="gf-insp-header">
        <span className="gf-insp-header-title">{headerLabel}</span>
        <div className="gf-insp-header-actions">
          <button
            className={`gf-insp-viewbtn ${inspView === "details" ? "active" : ""}`}
            onClick={() => setInspView("details")}
            title="Details"
          >
            ⊡
          </button>
          <button
            className={`gf-insp-viewbtn ${inspView === "json" ? "active" : ""}`}
            onClick={() => setInspView("json")}
            title="JSON"
          >
            {}
          </button>
          <button className="gf-insp-close" onClick={onClose} title="Close">
            ✕
          </button>
        </div>
      </div>

      <div className="gf-insp-chip-row">
        <div style={{ position: "relative" }}>
          <div
            className="gf-inspector-badge"
            style={{ background: color, cursor: "pointer" }}
            title="Click to change color"
            onClick={() => setShowPalette((p) => !p)}
          >
            {chipLabel}
          </div>
          {showPalette && (
            <div className="gf-color-palette">
              {PALETTE.map((c) => (
                <button
                  key={c}
                  className="gf-color-swatch"
                  style={{ background: c, outline: color === c ? "2px solid #fff" : "none" }}
                  onClick={() => {
                    onColorChange(label, c);
                    setShowPalette(false);
                  }}
                />
              ))}
            </div>
          )}
        </div>
      </div>

      {!isN && (
        <>
          <div className="gf-inspector-endpoints">
            <span
              style={{
                color:
                  colorOverrides[(selected.data as GEdge).startNode.label] ??
                  labelColor((selected.data as GEdge).startNode.label),
              }}
            >
              {(selected.data as GEdge).startNode.label || (selected.data as GEdge).start}
            </span>
            {" → "}
            <span
              style={{
                color:
                  colorOverrides[(selected.data as GEdge).endNode.label] ??
                  labelColor((selected.data as GEdge).endNode.label),
              }}
            >
              {(selected.data as GEdge).endNode.label || (selected.data as GEdge).end}
            </span>
          </div>
          {matchedRel && onSaveEdgeAlias && (
            <div className="gf-insp-alias-form">
              <label className="gf-insp-alias-label">
                CQL Alias (UPPER_SNAKE)
                <input
                  value={edgeCql}
                  onChange={(e) => setEdgeCql(e.target.value)}
                  placeholder={matchedRel.computedCypherAlias ?? (selected.data as GEdge).type}
                  style={{ fontSize: "0.8rem", padding: "0.2rem 0.4rem" }}
                />
              </label>
              <label className="gf-insp-alias-label">
                GQL Alias (camelCase)
                <input
                  value={edgeGql}
                  onChange={(e) => setEdgeGql(e.target.value)}
                  placeholder={matchedRel.graphqlAlias ?? ""}
                  style={{ fontSize: "0.8rem", padding: "0.2rem 0.4rem" }}
                />
              </label>
              <button
                style={{ fontSize: "0.75rem", padding: "0.2rem 0.5rem", alignSelf: "flex-end" }}
                disabled={savingAlias}
                onClick={async () => {
                  setSavingAlias(true);
                  await onSaveEdgeAlias(matchedRel.id, edgeCql, edgeGql);
                  setSavingAlias(false);
                }}
              >
                {savingAlias ? "Saving…" : "Save"}
              </button>
            </div>
          )}
        </>
      )}

      {inspView === "details" && (
        <div className="gf-insp-props-section">
          {isN && graphStats && (
            <>
              <div className="gf-insp-section-label">Graph stats</div>
              <table className="gf-inspector-table">
                <tbody>
                  {(Object.entries(graphStats) as [string, string | number][])
                    .sort(([a], [b]) => a.localeCompare(b))
                    .map(([k, v]) => {
                      const vs = String(v);
                      return (
                        <tr key={k} className="gf-prop-row">
                          <td className="gf-prop-key">{k}</td>
                          <td className="gf-prop-val">{vs}</td>
                          <td className="gf-prop-copy-cell">
                            <button
                              className={`gf-prop-copy${copiedKey === `stats:${k}` ? " copied" : ""}`}
                              title="Copy value"
                              onClick={() => handleCopy(`stats:${k}`, vs)}
                            >
                              <CopyIcon />
                            </button>
                          </td>
                        </tr>
                      );
                    })}
                </tbody>
              </table>
            </>
          )}
          <table className="gf-inspector-table">
            <tbody>
              {propRows.map(([k, v]) => {
                const vs = v === null || v === undefined ? "" : typeof v === "object" ? JSON.stringify(v) : String(v);
                return (
                  <tr key={k} className="gf-prop-row">
                    <td className="gf-prop-key">{k}</td>
                    <td className="gf-prop-val">{vs}</td>
                    <td className="gf-prop-copy-cell">
                      <button
                        className={`gf-prop-copy${copiedKey === k ? " copied" : ""}`}
                        title="Copy value"
                        onClick={() => handleCopy(k, vs)}
                      >
                        <CopyIcon />
                      </button>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      )}

      {inspView === "json" && (
        <CodeMirror
          value={JSON.stringify(selected.data, null, 2)}
          extensions={[jsonLang()]}
          theme={oneDark}
          basicSetup={{ foldGutter: true, lineNumbers: false, highlightActiveLine: false }}
          readOnly
          style={{ fontSize: 12, flex: 1, overflow: "auto" }}
        />
      )}
    </div>
  );
}
