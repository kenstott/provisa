// Copyright (c) 2026 Kenneth Stott
// Canary: 77407c55-2cd3-4160-92e1-86e317fd31b2
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState, useCallback, useRef } from "react";
import { labelColor } from "./graph-model";
import type { RelLineOverride } from "./graph-model";
import type { SchemaNodeLabel, SchemaRel } from "./graph-schema-types";
import {
  NodeContextMenu,
  RelContextMenu,
  type ContextMenuState,
  type RelContextMenuState,
} from "./graph-context-menus";

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

export function Sidebar({
  schemaNodeLabels,
  schemaRels,
  schemaLoading,
  history,
  colorOverrides,
  sizeOverrides,
  labelProperty,
  relLineOverrides,
  onHistorySelect,
  onLabelClick,
  onDomainClick,
  onRelClick,
  onColorChange,
  onSizeChange,
  onLabelPropertyChange,
  onRelLineChange,
  width,
  onWidthChange,
}: SidebarProps) {
  const [section, setSection] = useState<"db" | "history">("db");
  const [contextMenu, setContextMenu] = useState<ContextMenuState | null>(null);
  const [relContextMenu, setRelContextMenu] = useState<RelContextMenuState | null>(null);
  const [nodeLabelsCollapsed, setNodeLabelsCollapsed] = useState(false);
  const [relTypesCollapsed, setRelTypesCollapsed] = useState(false);
  const [nodeLabelsPage, setNodeLabelsPage] = useState(0);
  const [relTypesPage, setRelTypesPage] = useState(0);
  const SCHEMA_PAGE_SIZE = 50;
  const dragging = useRef(false);

  const onMouseDown = useCallback(
    (e: React.MouseEvent) => {
      e.preventDefault();
      dragging.current = true;
      const startX = e.clientX;
      const startW = width;
      const onMove = (ev: MouseEvent) => {
        if (!dragging.current) return;
        const next = Math.max(160, Math.min(480, startW + ev.clientX - startX));
        onWidthChange(next);
      };
      const onUp = () => {
        dragging.current = false;
        window.removeEventListener("mousemove", onMove);
        window.removeEventListener("mouseup", onUp);
      };
      window.addEventListener("mousemove", onMove);
      window.addEventListener("mouseup", onUp);
    },
    [width, onWidthChange],
  );

  const handleNodeRightClick = useCallback((e: React.MouseEvent, node: SchemaNodeLabel) => {
    e.preventDefault();
    const compoundLabel = node.domainLabel
      ? `${node.domainLabel}:${node.tableLabel}`
      : node.tableLabel;
    setContextMenu({
      x: e.clientX,
      y: e.clientY,
      compoundLabel,
      tableLabel: node.tableLabel,
      properties: node.properties,
    });
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
            {!schemaLoading &&
              (() => {
                const domainLabels = [
                  ...new Set(
                    schemaNodeLabels.map((n) => n.domainLabel).filter(Boolean) as string[],
                  ),
                ].sort();
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
                              onContextMenu={(e) => {
                                e.preventDefault();
                                setContextMenu({
                                  x: e.clientX,
                                  y: e.clientY,
                                  compoundLabel: lbl,
                                  tableLabel: lbl,
                                  properties: [],
                                });
                              }}
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
              <div
                className="graph-schema-heading graph-schema-heading--collapsible"
                onClick={() => setNodeLabelsCollapsed((c) => !c)}
              >
                Node Labels
                <span
                  className={`graph-schema-chevron ${nodeLabelsCollapsed ? "graph-schema-chevron--collapsed" : ""}`}
                >
                  ▾
                </span>
              </div>
              {!nodeLabelsCollapsed &&
                (schemaLoading ? (
                  <div className="graph-schema-empty">Loading…</div>
                ) : schemaNodeLabels.length === 0 ? (
                  <div className="graph-schema-empty">No labels found</div>
                ) : (
                  <div className="graph-label-list">
                    {(() => {
                      const sorted = [...schemaNodeLabels].sort((a, b) =>
                        a.tableLabel.localeCompare(b.tableLabel),
                      );
                      const paged = sorted.slice(
                        nodeLabelsPage * SCHEMA_PAGE_SIZE,
                        (nodeLabelsPage + 1) * SCHEMA_PAGE_SIZE,
                      );
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
                              onDragStart={(e) =>
                                e.dataTransfer.setData("text/x-provisa-label", compoundLabel)
                              }
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
                      const sorted = [...schemaNodeLabels].sort((a, b) =>
                        a.tableLabel.localeCompare(b.tableLabel),
                      );
                      const totalPages = Math.max(1, Math.ceil(sorted.length / SCHEMA_PAGE_SIZE));
                      if (totalPages === 1) return null;
                      return (
                        <div
                          style={{
                            display: "flex",
                            gap: "0.25rem",
                            padding: "0.4rem 0",
                            fontSize: "0.65rem",
                            color: "var(--text-muted)",
                            justifyContent: "center",
                          }}
                        >
                          <button
                            onClick={() => setNodeLabelsPage(0)}
                            disabled={nodeLabelsPage === 0}
                            style={{
                              background: "none",
                              border: "none",
                              cursor: nodeLabelsPage > 0 ? "pointer" : "default",
                              padding: "0.1rem 0.2rem",
                              color: "var(--text-muted)",
                              opacity: nodeLabelsPage > 0 ? 1 : 0.4,
                            }}
                          >
                            «
                          </button>
                          <button
                            onClick={() => setNodeLabelsPage((p) => p - 1)}
                            disabled={nodeLabelsPage === 0}
                            style={{
                              background: "none",
                              border: "none",
                              cursor: nodeLabelsPage > 0 ? "pointer" : "default",
                              padding: "0.1rem 0.2rem",
                              color: "var(--text-muted)",
                              opacity: nodeLabelsPage > 0 ? 1 : 0.4,
                            }}
                          >
                            ‹
                          </button>
                          <span>
                            {nodeLabelsPage + 1}/{totalPages}
                          </span>
                          <button
                            onClick={() => setNodeLabelsPage((p) => p + 1)}
                            disabled={nodeLabelsPage >= totalPages - 1}
                            style={{
                              background: "none",
                              border: "none",
                              cursor: nodeLabelsPage < totalPages - 1 ? "pointer" : "default",
                              padding: "0.1rem 0.2rem",
                              color: "var(--text-muted)",
                              opacity: nodeLabelsPage < totalPages - 1 ? 1 : 0.4,
                            }}
                          >
                            ›
                          </button>
                          <button
                            onClick={() => setNodeLabelsPage(totalPages - 1)}
                            disabled={nodeLabelsPage >= totalPages - 1}
                            style={{
                              background: "none",
                              border: "none",
                              cursor: nodeLabelsPage < totalPages - 1 ? "pointer" : "default",
                              padding: "0.1rem 0.2rem",
                              color: "var(--text-muted)",
                              opacity: nodeLabelsPage < totalPages - 1 ? 1 : 0.4,
                            }}
                          >
                            »
                          </button>
                        </div>
                      );
                    })()}
                  </div>
                ))}
            </div>

            <div className="graph-schema-section">
              <div
                className="graph-schema-heading graph-schema-heading--collapsible"
                onClick={() => setRelTypesCollapsed((c) => !c)}
              >
                Relationship Types
                <span
                  className={`graph-schema-chevron ${relTypesCollapsed ? "graph-schema-chevron--collapsed" : ""}`}
                >
                  ▾
                </span>
              </div>
              {!relTypesCollapsed &&
                (schemaLoading ? (
                  <div className="graph-schema-empty">Loading…</div>
                ) : schemaRels.length === 0 ? (
                  <div className="graph-schema-empty">No relationship types found</div>
                ) : (
                  <div className="graph-rel-list">
                    {(() => {
                      const uniqueRels = [...new Map(schemaRels.map((r) => [r.type, r])).values()];
                      const paged = uniqueRels.slice(
                        relTypesPage * SCHEMA_PAGE_SIZE,
                        (relTypesPage + 1) * SCHEMA_PAGE_SIZE,
                      );
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
                            <span
                              className="graph-rel-arrow"
                              style={
                                ov
                                  ? {
                                      borderBottomWidth: ov.width,
                                      borderBottomStyle:
                                        ov.style === "solid"
                                          ? "solid"
                                          : ov.style === "dashed"
                                            ? "dashed"
                                            : "dotted",
                                    }
                                  : {}
                              }
                            >
                              –
                            </span>
                            <span className="graph-rel-type">{type}</span>
                          </div>
                        );
                      });
                    })()}
                    {(() => {
                      const uniqueRels = [...new Map(schemaRels.map((r) => [r.type, r])).values()];
                      const totalPages = Math.max(
                        1,
                        Math.ceil(uniqueRels.length / SCHEMA_PAGE_SIZE),
                      );
                      if (totalPages === 1) return null;
                      return (
                        <div
                          style={{
                            display: "flex",
                            gap: "0.25rem",
                            padding: "0.4rem 0",
                            fontSize: "0.65rem",
                            color: "var(--text-muted)",
                            justifyContent: "center",
                          }}
                        >
                          <button
                            onClick={() => setRelTypesPage(0)}
                            disabled={relTypesPage === 0}
                            style={{
                              background: "none",
                              border: "none",
                              cursor: relTypesPage > 0 ? "pointer" : "default",
                              padding: "0.1rem 0.2rem",
                              color: "var(--text-muted)",
                              opacity: relTypesPage > 0 ? 1 : 0.4,
                            }}
                          >
                            «
                          </button>
                          <button
                            onClick={() => setRelTypesPage((p) => p - 1)}
                            disabled={relTypesPage === 0}
                            style={{
                              background: "none",
                              border: "none",
                              cursor: relTypesPage > 0 ? "pointer" : "default",
                              padding: "0.1rem 0.2rem",
                              color: "var(--text-muted)",
                              opacity: relTypesPage > 0 ? 1 : 0.4,
                            }}
                          >
                            ‹
                          </button>
                          <span>
                            {relTypesPage + 1}/{totalPages}
                          </span>
                          <button
                            onClick={() => setRelTypesPage((p) => p + 1)}
                            disabled={relTypesPage >= totalPages - 1}
                            style={{
                              background: "none",
                              border: "none",
                              cursor: relTypesPage < totalPages - 1 ? "pointer" : "default",
                              padding: "0.1rem 0.2rem",
                              color: "var(--text-muted)",
                              opacity: relTypesPage < totalPages - 1 ? 1 : 0.4,
                            }}
                          >
                            ›
                          </button>
                          <button
                            onClick={() => setRelTypesPage(totalPages - 1)}
                            disabled={relTypesPage >= totalPages - 1}
                            style={{
                              background: "none",
                              border: "none",
                              cursor: relTypesPage < totalPages - 1 ? "pointer" : "default",
                              padding: "0.1rem 0.2rem",
                              color: "var(--text-muted)",
                              opacity: relTypesPage < totalPages - 1 ? 1 : 0.4,
                            }}
                          >
                            »
                          </button>
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
