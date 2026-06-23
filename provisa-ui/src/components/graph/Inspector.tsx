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

import { useRef, useState, useMemo } from "react";
import type { Relationship } from "../../types/admin";
import { PALETTE, labelColor, getStableNodeId } from "./graph-model";
import type { GNode, GEdge } from "./graph-model";
import CodeMirror from "@uiw/react-codemirror";
import { json as jsonLang } from "@codemirror/lang-json";
import { oneDark } from "@codemirror/theme-one-dark";

interface InspectorProps {
  selected: { kind: "node"; data: GNode } | { kind: "edge"; data: GEdge } | null;
  colorOverrides: Record<string, string>;
  onColorChange: (label: string, color: string) => void;
  onClose: () => void;
  width: number;
  onResizeStart: (e: React.MouseEvent) => void;
  relationships?: Relationship[];
  onSaveEdgeAlias?: (relId: number, cqlAlias: string, gqlAlias: string) => Promise<void>;
  pkMap: Record<string, string[]>;
}

export function Inspector({
  selected,
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
  const [hovered, setHovered] = useState(false);
  const [edgeCql, setEdgeCql] = useState("");
  const [edgeGql, setEdgeGql] = useState("");
  const [savingAlias, setSavingAlias] = useState(false);

  // Find matching relationship when an edge is selected
  const matchedRel = useMemo(() => {
    if (!selected || selected.kind !== "edge" || !relationships) return null;
    const edgeType = (selected.data as GEdge).type;
    return relationships.find((r) => (r.alias ?? r.computedCypherAlias) === edgeType) ?? null;
  }, [selected, relationships]);

  // Sync alias inputs when selection changes — React's documented "adjust state
  // while rendering on prop change" pattern: a prev-value ref gates a render-phase
  // setState so the inputs reset synchronously without an extra effect pass.
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

  if (!selected) return null;

  const viewSel = hovered && (
    <div className="gf-insp-viewsel">
      {(["details", "json"] as const).map((v) => (
        <button
          key={v}
          className={`gf-insp-viewbtn ${inspView === v ? "active" : ""}`}
          onClick={() => setInspView(v)}
          title={v}
        >
          {v === "details" ? "⊡" : "{}"}
        </button>
      ))}
    </div>
  );

  const isN = selected.kind === "node";
  const label = isN ? selected.data.label : (selected.data as GEdge).type;
  const color = colorOverrides[label] ?? labelColor(label);
  const props = selected.data.properties;

  const nodeLabel = isN ? (selected.data as GNode).label : "";
  const colonIdx = nodeLabel.indexOf(":");
  const domain = colonIdx > 0 ? nodeLabel.slice(0, colonIdx) : null;
  const typeName = colonIdx > 0 ? nodeLabel.slice(colonIdx + 1) : nodeLabel;

  const stableId = isN ? getStableNodeId(selected.data as GNode, pkMap) : null;

  const pkCols = isN ? (pkMap[(selected.data as GNode).label] ?? []) : [];
  const idColName = pkCols[0] ?? null;
  const pkEntry: Record<string, unknown> =
    isN && idColName && !(idColName in props) ? { [idColName]: (selected.data as GNode).id } : {};

  const HIDDEN_PROPS = new Set(["l1Cluster", "l2Cluster", "l3Cluster", "scl1", "scl2", "scl3"]);
  const allFields: Record<string, unknown> = isN
    ? {
        ...(domain ? { domain } : {}),
        label: typeName || nodeLabel,
        ...pkEntry,
        ...Object.fromEntries(Object.entries(props).filter(([k]) => !HIDDEN_PROPS.has(k))),
      }
    : (() => {
        const e = selected.data as GEdge;
        const srcLabel = e.startNode.label;
        const srcColon = srcLabel.indexOf(":");
        const edgeDomain = srcColon > 0 ? srcLabel.slice(0, srcColon) : srcLabel || null;
        return {
          ...(edgeDomain ? { domain: edgeDomain } : {}),
          identity: e.identity,
          start: e.start,
          end: e.end,
          type: e.type,
          ...props,
        };
      })();

  return (
    <div
      className="gf-inspector"
      style={{ width }}
      onMouseEnter={() => setHovered(true)}
      onMouseLeave={() => {
        setHovered(false);
        setShowPalette(false);
      }}
    >
      <div className="gf-inspector-resize-handle" onMouseDown={onResizeStart} />
      <button className="gf-insp-close" onClick={onClose} title="Close">
        ✕
      </button>
      {viewSel}
      <div style={{ position: "relative", alignSelf: "flex-start" }}>
        <div
          className="gf-inspector-badge"
          style={{ background: color, cursor: "pointer" }}
          title="Click to change color"
          onClick={() => setShowPalette((p) => !p)}
        >
          {isN ? typeName || label : label}
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
      <div className="gf-inspector-kind">{isN ? "Node" : "Relationship"}</div>
      <div className="gf-inspector-id">
        &lt;id&gt;: {isN ? stableId : (selected.data as GEdge).identity}
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
            <div
              style={{
                padding: "0.5rem 0",
                display: "flex",
                flexDirection: "column",
                gap: "0.4rem",
                borderTop: "1px solid var(--border)",
                marginTop: "0.25rem",
              }}
            >
              <label
                style={{
                  fontSize: "0.75rem",
                  color: "var(--text-muted)",
                  display: "flex",
                  flexDirection: "column",
                  gap: "0.2rem",
                }}
              >
                CQL Alias (UPPER_SNAKE)
                <input
                  value={edgeCql}
                  onChange={(e) => setEdgeCql(e.target.value)}
                  placeholder={matchedRel.computedCypherAlias ?? (selected.data as GEdge).type}
                  style={{ fontSize: "0.8rem", padding: "0.2rem 0.4rem" }}
                />
              </label>
              <label
                style={{
                  fontSize: "0.75rem",
                  color: "var(--text-muted)",
                  display: "flex",
                  flexDirection: "column",
                  gap: "0.2rem",
                }}
              >
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
        <table className="gf-inspector-table">
          <tbody>
            {Object.entries(allFields).sort(([a], [b]) => a.localeCompare(b)).map(([k, v]) => (
              <tr key={k}>
                <td className="gf-prop-key">{k}</td>
                <td className="gf-prop-val">
                  {v === null || v === undefined
                    ? ""
                    : typeof v === "object"
                      ? JSON.stringify(v)
                      : String(v)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
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
