// Copyright (c) 2026 Kenneth Stott
// Canary: a3d9e2f1-7b4c-4a8e-9d5f-2c1b6e3a7f8d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useRef, useEffect, useState, useCallback } from "react";
import { createPortal } from "react-dom";
import { X, Download, ChevronDown, ChevronRight, Layers } from "lucide-react";
import cytoscape from "cytoscape";
import fcoseRaw from "cytoscape-fcose";
import cytoscapeSvgRaw from "cytoscape-svg";
import { buildErdElements, buildTableLabel } from "./erd-model";
import type { ColumnDetail, ErdNodeDomain, ErdNodeTable } from "./erd-model";
import type { RegisteredTable, Relationship, Domain, TableColumn } from "../../types/admin";
import { labelColor, darkenColor } from "../graph/graph-model";
import { downloadBlob } from "../graph/graph-export";
import type { CyInstance, CyEvent } from "../graph/cytoscape-types";

// ── cytoscape plugin registration ────────────────────────────────────────────
type CyExt = Parameters<typeof cytoscape.use>[0];
type CyExtModule = { default?: CyExt } | CyExt;
const _interop = (m: CyExtModule): CyExt => (m as { default?: CyExt }).default ?? (m as CyExt);
try { cytoscape.use(_interop(fcoseRaw as CyExtModule)); } catch { /* already registered */ }
try { cytoscape.use(_interop(cytoscapeSvgRaw as CyExtModule)); } catch { /* already registered */ }

// ── stylesheet ────────────────────────────────────────────────────────────────
function buildErdStylesheet() {
  return [
    {
      selector: "node",
      style: { "text-wrap": "wrap", "font-family": "monospace" },
    },
    {
      selector: ".erd-domain",
      style: {
        "background-color": (ele: { data(k: string): unknown }) =>
          labelColor(ele.data("domainId") as string) + "22",
        "background-opacity": 1,
        "border-color": (ele: { data(k: string): unknown }) =>
          labelColor(ele.data("domainId") as string),
        "border-width": 2,
        "border-style": "solid",
        label: (ele: { data(k: string): unknown }) => ele.data("label") as string,
        "text-valign": "top",
        "text-halign": "center",
        color: "#e2e8f0",
        "font-size": 13,
        "font-weight": "bold",
        padding: "24px",
        "compound-sizing-wrt-labels": "include",
      },
    },
    {
      selector: ".erd-table",
      style: {
        shape: "rectangle",
        "background-color": "#1e293b",
        "border-color": (ele: { data(k: string): unknown }) =>
          darkenColor(labelColor(ele.data("domainId") as string), 1.2),
        "border-width": 1,
        label: (ele: { data(k: string): unknown }) => ele.data("displayLabel") as string,
        "text-valign": "center",
        "text-halign": "center",
        color: "#e2e8f0",
        "font-size": 10,
        "text-wrap": "wrap",
        width: 170,
        height: (ele: { data(k: string): unknown }) =>
          Math.max(32, ((ele.data("lineCount") as number) ?? 1) * 14 + 10),
      },
    },
    {
      selector: ".erd-table:selected",
      style: { "border-color": "#60a5fa", "border-width": 2 },
    },
    {
      selector: ".erd-rel",
      style: {
        "curve-style": "bezier",
        "line-color": "#475569",
        width: 1.5,
        "target-arrow-color": "#475569",
        "target-arrow-shape": "triangle",
        "source-arrow-color": "#475569",
        "source-arrow-shape": (ele: { data(k: string): unknown }) =>
          (ele.data("cardinality") as string) === "many_to_many" ||
          (ele.data("cardinality") as string) === "many_to_one"
            ? "triangle"
            : "none",
        label: (ele: { data(k: string): unknown }) => ele.data("label") as string,
        "font-size": 9,
        color: "#94a3b8",
        "text-rotation": "autorotate",
        "text-background-color": "#0f172a",
        "text-background-opacity": 0.75,
        "text-background-padding": "2px",
      },
    },
    {
      // proxy edges (collapsed-domain → table/domain) rendered dashed
      selector: ".erd-rel--proxy",
      style: {
        "line-style": "dashed",
        "line-dash-pattern": [6, 3],
        "line-color": "#334155",
        "target-arrow-color": "#334155",
        "source-arrow-color": "#334155",
        color: "#475569",
      },
    },
  ];
}

// ── small toolbar-button helper ───────────────────────────────────────────────
function TBtn({
  onClick, title, active, children,
}: {
  onClick: () => void;
  title?: string;
  active?: boolean;
  children: React.ReactNode;
}) {
  return (
    <button
      onClick={onClick}
      title={title}
      style={{
        padding: "2px 8px",
        fontSize: 11,
        background: active ? "#334155" : "transparent",
        color: active ? "#e2e8f0" : "#64748b",
        border: "1px solid #334155",
        borderRadius: 4,
        cursor: "pointer",
        display: "flex",
        alignItems: "center",
        gap: 4,
      }}
    >
      {children}
    </button>
  );
}

// ── types ─────────────────────────────────────────────────────────────────────
interface TooltipState {
  visible: boolean;
  x: number;
  y: number;
  title: string;
  body: string;
}

interface ErdModalProps {
  tables: RegisteredTable[];
  relationships: Relationship[];
  domains: Domain[];
  activeDomain: string | null;
  onClose: () => void;
}

// ── component ─────────────────────────────────────────────────────────────────
export function ErdModal({ tables, relationships, domains, activeDomain, onClose }: ErdModalProps) {
  const containerRef = useRef<HTMLDivElement>(null);
  const cyRef = useRef<CyInstance | null>(null);
  const domainPickerRef = useRef<HTMLDivElement>(null);

  const [columnDetail, setColumnDetail] = useState<ColumnDetail>("key");
  const [collapsedDomains, setCollapsedDomains] = useState<Set<string>>(new Set());
  const [hiddenDomains, setHiddenDomains] = useState<Set<string>>(new Set());
  const [showDomainPicker, setShowDomainPicker] = useState(false);
  const [tooltip, setTooltip] = useState<TooltipState>({
    visible: false, x: 0, y: 0, title: "", body: "",
  });

  // All domain IDs present in the scoped table list (before hiding).
  const allDomainIds = [
    ...new Set(
      (activeDomain ? tables.filter((t) => t.domainId === activeDomain) : tables).map(
        (t) => t.domainId,
      ),
    ),
  ];

  // Close picker on outside click.
  useEffect(() => {
    if (!showDomainPicker) return;
    const handler = (e: MouseEvent) => {
      if (domainPickerRef.current && !domainPickerRef.current.contains(e.target as Node)) {
        setShowDomainPicker(false);
      }
    };
    document.addEventListener("mousedown", handler);
    return () => document.removeEventListener("mousedown", handler);
  }, [showDomainPicker]);

  // ── initialise / rebuild on structural changes ────────────────────────────
  useEffect(() => {
    if (!containerRef.current) return;
    const elements = buildErdElements(
      tables, relationships, domains,
      collapsedDomains, hiddenDomains, columnDetail, activeDomain,
    );
    const allEls = [...elements.nodes, ...elements.edges] as unknown[];

    const cy = cytoscape({
      container: containerRef.current,
      elements: allEls as Parameters<typeof cytoscape>[0]["elements"],
      style: buildErdStylesheet() as unknown as Parameters<typeof cytoscape>[0]["style"],
      layout: {
        name: "fcose",
        animate: false,
        nodeSeparation: 60,
        idealEdgeLength: () => 180,
        nodeRepulsion: () => 12000,
        packComponents: true,
        tile: true,
        tilingPaddingVertical: 20,
        tilingPaddingHorizontal: 20,
      } as Parameters<typeof cytoscape>[0]["layout"],
    }) as unknown as CyInstance;

    cyRef.current = cy;

    cy.on("tap", ".erd-domain", (evt: CyEvent) => {
      const domainId = evt.target.data("domainId") as string;
      setCollapsedDomains((prev) => {
        const next = new Set(prev);
        if (next.has(domainId)) next.delete(domainId); else next.add(domainId);
        return next;
      });
    });

    cy.on("mouseover", "node", (evt: CyEvent) => {
      const type = evt.target.data("type") as string;
      let title = "";
      let body = "";
      if (type === "domain") {
        title = evt.target.data("label") as string;
        body = (evt.target.data("description") as string) || "";
      } else if (type === "table") {
        title = evt.target.data("tableName") as string;
        body = (evt.target.data("description") as string) || "";
      }
      if (title) {
        const pos = evt.renderedPosition ?? evt.position;
        const rect = containerRef.current?.getBoundingClientRect();
        setTooltip({
          visible: true,
          x: (rect?.left ?? 0) + pos.x + 12,
          y: (rect?.top ?? 0) + pos.y + 12,
          title,
          body,
        });
      }
    });

    cy.on("mouseout", "node", () => setTooltip((t) => ({ ...t, visible: false })));

    return () => { cy.destroy(); cyRef.current = null; };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [tables, relationships, domains, collapsedDomains, hiddenDomains, activeDomain]);

  // ── label-only update when columnDetail changes (no re-layout) ──────────
  useEffect(() => {
    const cy = cyRef.current;
    if (!cy) return;
    cy.batch(() => {
      cy.$(".erd-table").forEach((n) => {
        const cols = n.data("columns") as TableColumn[];
        const name = n.data("tableName") as string;
        const { label, lineCount } = buildTableLabel(name, cols, columnDetail);
        n.data("displayLabel", label);
        n.data("lineCount", lineCount);
      });
    });
    cy.style(buildErdStylesheet() as unknown as Parameters<CyInstance["style"]>[0]);
  }, [columnDetail]);

  // ── exports ───────────────────────────────────────────────────────────────
  const exportSvg = useCallback(() => {
    const cy = cyRef.current;
    if (!cy) return;
    downloadBlob(new Blob([cy.svg({ full: true, bg: "#0f172a" }) as string], { type: "image/svg+xml" }), "erd.svg");
  }, []);

  const exportPng = useCallback(() => {
    const cy = cyRef.current;
    if (!cy) return;
    downloadBlob(cy.png({ output: "blob", full: true, bg: "#0f172a" }) as unknown as Blob, "erd.png");
  }, []);

  const exportJson = useCallback(() => {
    const cy = cyRef.current;
    if (!cy) return;
    downloadBlob(
      new Blob([JSON.stringify((cy as unknown as { json(): unknown }).json(), null, 2)], { type: "application/json" }),
      "erd.json",
    );
  }, []);

  // ── collapse all / expand all (visible domains only) ─────────────────────
  const visibleDomainIds = allDomainIds.filter((id) => !hiddenDomains.has(id));
  const allCollapsed = visibleDomainIds.length > 0 && visibleDomainIds.every((id) => collapsedDomains.has(id));
  const toggleAll = () =>
    setCollapsedDomains(allCollapsed ? new Set() : new Set(visibleDomainIds));

  const toggleHidden = (domainId: string) =>
    setHiddenDomains((prev) => {
      const next = new Set(prev);
      if (next.has(domainId)) next.delete(domainId); else next.add(domainId);
      return next;
    });

  const hiddenCount = hiddenDomains.size;

  return createPortal(
    <div
      className="modal-overlay"
      onClick={(e) => { if (e.target === e.currentTarget) onClose(); }}
    >
      <div
        className="modal modal--erd"
        style={{
          width: "92vw", height: "88vh", maxWidth: "92vw",
          display: "flex", flexDirection: "column",
          background: "#0f172a", padding: 0, overflow: "hidden",
        }}
        onClick={(e) => e.stopPropagation()}
      >
        {/* ── header ── */}
        <div style={{
          display: "flex", alignItems: "center", gap: "0.5rem",
          padding: "0.6rem 0.75rem", borderBottom: "1px solid #1e293b",
          flexShrink: 0, flexWrap: "wrap",
        }}>
          <span style={{ fontWeight: 600, color: "#e2e8f0", marginRight: "0.25rem" }}>
            Entity Relationship Diagram
          </span>

          {/* domain picker */}
          <div ref={domainPickerRef} style={{ position: "relative" }}>
            <TBtn
              onClick={() => setShowDomainPicker((v) => !v)}
              active={showDomainPicker || hiddenCount > 0}
              title="Show / hide domains"
            >
              <Layers size={11} />
              Domains
              {hiddenCount > 0 && (
                <span style={{
                  background: "#ef4444", color: "#fff",
                  borderRadius: 8, padding: "0 4px", fontSize: 10, lineHeight: "14px",
                }}>
                  -{hiddenCount}
                </span>
              )}
            </TBtn>
            {showDomainPicker && (
              <div style={{
                position: "absolute", top: "calc(100% + 4px)", left: 0,
                background: "#1e293b", border: "1px solid #334155",
                borderRadius: 6, padding: "6px 0", zIndex: 100,
                minWidth: 180, boxShadow: "0 4px 12px rgba(0,0,0,0.4)",
              }}>
                {allDomainIds.map((id) => (
                  <label
                    key={id}
                    style={{
                      display: "flex", alignItems: "center", gap: 8,
                      padding: "4px 12px", cursor: "pointer", fontSize: 12,
                      color: hiddenDomains.has(id) ? "#475569" : "#e2e8f0",
                    }}
                  >
                    <input
                      type="checkbox"
                      checked={!hiddenDomains.has(id)}
                      onChange={() => toggleHidden(id)}
                      style={{ accentColor: labelColor(id) }}
                    />
                    <span
                      style={{
                        display: "inline-block", width: 8, height: 8,
                        borderRadius: "50%", background: labelColor(id), flexShrink: 0,
                      }}
                    />
                    {id}
                  </label>
                ))}
              </div>
            )}
          </div>

          {/* column detail toggle */}
          <div style={{ display: "flex", gap: "2px" }}>
            {(["all", "key", "none"] as ColumnDetail[]).map((d) => (
              <TBtn key={d} onClick={() => setColumnDetail(d)} active={columnDetail === d}>
                {d === "all" ? "All cols" : d === "key" ? "Keys" : "No cols"}
              </TBtn>
            ))}
          </div>

          {/* collapse/expand all */}
          <TBtn onClick={toggleAll} title={allCollapsed ? "Expand all domains" : "Collapse all domains"}>
            {allCollapsed ? <ChevronRight size={12} /> : <ChevronDown size={12} />}
            {allCollapsed ? "Expand all" : "Collapse all"}
          </TBtn>

          <div style={{ flex: 1 }} />

          {/* exports */}
          <TBtn onClick={exportSvg} title="Download SVG"><Download size={11} /> SVG</TBtn>
          <TBtn onClick={exportPng} title="Download PNG"><Download size={11} /> PNG</TBtn>
          <TBtn onClick={exportJson} title="Download JSON"><Download size={11} /> JSON</TBtn>

          <button
            className="modal-close"
            onClick={onClose}
            style={{ color: "#64748b", marginLeft: "0.25rem" }}
          >
            <X size={16} />
          </button>
        </div>

        {/* ── hint bar ── */}
        {allDomainIds.length > 0 && (
          <div style={{
            fontSize: 10, color: "#475569",
            padding: "3px 12px", borderBottom: "1px solid #1e293b", flexShrink: 0,
          }}>
            Click a domain group to collapse / expand · dashed lines connect collapsed domains
          </div>
        )}

        {/* ── canvas ── */}
        <div ref={containerRef} style={{ flex: 1, background: "#0f172a" }} />
      </div>

      {/* ── tooltip ── */}
      {tooltip.visible && (
        <div style={{
          position: "fixed", left: tooltip.x, top: tooltip.y,
          background: "#1e293b", border: "1px solid #334155",
          borderRadius: 6, padding: "6px 10px", fontSize: 11,
          color: "#e2e8f0", maxWidth: 260, pointerEvents: "none",
          zIndex: 2000, boxShadow: "0 4px 12px rgba(0,0,0,0.4)",
        }}>
          <div style={{ fontWeight: 600, marginBottom: tooltip.body ? 4 : 0 }}>{tooltip.title}</div>
          {tooltip.body && <div style={{ color: "#94a3b8", lineHeight: 1.4 }}>{tooltip.body}</div>}
        </div>
      )}
    </div>,
    document.body,
  );
}

export type { ErdNodeDomain, ErdNodeTable };
