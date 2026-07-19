// Copyright (c) 2026 Kenneth Stott
// Canary: 77407c55-2cd3-4160-92e1-86e317fd31b2
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState, useCallback, useRef, useEffect } from "react";
import { useTranslation } from "react-i18next";
import { Tooltip, TextInput } from "@mantine/core";
import { labelColor } from "./graph-model";
import { DatabaseIcon, HistoryIcon, StarIcon, ExportIcon } from "./GraphIcons";
import type { RelLineOverride } from "./graph-model";
import type { SchemaNodeLabel, SchemaRel } from "./graph-schema-types";
import type { Favorite } from "./graph-persistence";
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
  sizeByProperty: Record<string, string>;
  sizeMultiplier: Record<string, number>;
  relLineOverrides: Record<string, RelLineOverride>;
  onHistorySelect: (q: string) => void;
  onLabelClick: (label: string) => void;
  onDomainClick: (domainId: string) => void;
  onRelClick: (type: string) => void;
  onColorChange: (label: string, color: string) => void;
  onSizeChange: (label: string, size: number) => void;
  onLabelPropertyChange: (label: string, prop: string) => void;
  onSizeByPropertyChange: (label: string, prop: string) => void;
  onSizeMultiplierChange: (label: string, multiplier: number) => void;
  onRelLineChange: (type: string, override: RelLineOverride) => void;
  numericPropsByLabel: Record<string, string[]>;
  onNeo4jExport?: () => void;
  width: number;
  onWidthChange: (w: number) => void;
  highlightedLabel?: string | null;
  favorites?: Favorite[];
  onFavoriteSelect?: (query: string) => void;
  onFavoriteRun?: (query: string) => void;
  onFavoriteRename?: (id: string, name: string) => void;
  onFavoriteDelete?: (id: string) => void;
  propertyKeys?: string[];
  onPropertyKeyClick?: (key: string) => void;
  totalNodeCount?: number | null;
  totalRelCount?: number | null;
  labelCounts?: Record<string, number>;
}

const NUMERIC_TYPES = new Set(["int", "integer", "bigint", "float", "double", "decimal", "numeric", "real", "number"]);
const isNumericType = (t: string) => {
  const lower = t.toLowerCase();
  for (const nt of NUMERIC_TYPES) if (lower === nt || lower.startsWith(nt + "(") || lower.startsWith(nt + " ")) return true;
  return false;
};

export function Sidebar({
  schemaNodeLabels,
  schemaRels,
  schemaLoading,
  history,
  colorOverrides,
  sizeOverrides,
  labelProperty,
  sizeByProperty,
  sizeMultiplier,
  relLineOverrides,
  onHistorySelect,
  onLabelClick,
  onDomainClick,
  onRelClick,
  onColorChange,
  onSizeChange,
  onLabelPropertyChange,
  onSizeByPropertyChange,
  onSizeMultiplierChange,
  onRelLineChange,
  numericPropsByLabel,
  onNeo4jExport,
  width,
  onWidthChange,
  highlightedLabel,
  favorites = [],
  onFavoriteSelect,
  onFavoriteRun,
  onFavoriteRename,
  onFavoriteDelete,
  propertyKeys = [],
  onPropertyKeyClick,
  totalNodeCount = null,
  totalRelCount = null,
  labelCounts = {},
}: SidebarProps) {
  const { t } = useTranslation();
  const [section, setSection] = useState<"db" | "history" | "favorites">("db");
  const [contextMenu, setContextMenu] = useState<ContextMenuState | null>(null);
  const [relContextMenu, setRelContextMenu] = useState<RelContextMenuState | null>(null);

  const readCollapsed = (key: string, def: boolean): boolean => {
    try { const v = localStorage.getItem(key); return v === null ? def : v === "1"; } catch { return def; }
  };
  const writeCollapsed = (key: string, v: boolean) => { try { localStorage.setItem(key, v ? "1" : "0"); } catch { /* ignore */ } };

  const [nodeLabelsCollapsed, setNodeLabelsCollapsedRaw] = useState(() => readCollapsed("graph-sidebar:nodeLabels:collapsed", false));
  const [relTypesCollapsed, setRelTypesCollapsedRaw] = useState(() => readCollapsed("graph-sidebar:relTypes:collapsed", false));
  const [propKeysCollapsed, setPropKeysCollapsedRaw] = useState(() => readCollapsed("graph-sidebar:propKeys:collapsed", false));

  const setNodeLabelsCollapsed = (updater: boolean | ((prev: boolean) => boolean)) => {
    setNodeLabelsCollapsedRaw((prev) => { const next = typeof updater === "function" ? updater(prev) : updater; writeCollapsed("graph-sidebar:nodeLabels:collapsed", next); return next; });
  };
  const setRelTypesCollapsed = (updater: boolean | ((prev: boolean) => boolean)) => {
    setRelTypesCollapsedRaw((prev) => { const next = typeof updater === "function" ? updater(prev) : updater; writeCollapsed("graph-sidebar:relTypes:collapsed", next); return next; });
  };
  const setPropKeysCollapsed = (updater: boolean | ((prev: boolean) => boolean)) => {
    setPropKeysCollapsedRaw((prev) => { const next = typeof updater === "function" ? updater(prev) : updater; writeCollapsed("graph-sidebar:propKeys:collapsed", next); return next; });
  };
  const [nodeLabelsPage, setNodeLabelsPage] = useState(0);
  const [relTypesPage, setRelTypesPage] = useState(0);
  const SCHEMA_PAGE_SIZE = 50;
  const dragging = useRef(false);
  const [renamingId, setRenamingId] = useState<string | null>(null);
  const [renameValue, setRenameValue] = useState("");
  const renameInputRef = useRef<HTMLInputElement>(null);

  useEffect(() => {
    if (renamingId && renameInputRef.current) renameInputRef.current.select();
  }, [renamingId]);

  const commitRename = useCallback(() => {
    if (renamingId && renameValue.trim()) onFavoriteRename?.(renamingId, renameValue.trim());
    setRenamingId(null);
  }, [renamingId, renameValue, onFavoriteRename]);

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
    const numericFromSchema = node.nativeFilterColumns
      .filter(c => isNumericType(c.type))
      .map(c => c.name);
    const numericFromPropertyTypes = Object.entries(node.propertyTypes)
      .filter(([, t]) => isNumericType(t))
      .map(([k]) => k);
    const numericFromData = numericPropsByLabel[node.tableLabel] ?? [];
    const numericProperties = [...new Set([...numericFromSchema, ...numericFromPropertyTypes, ...numericFromData, "degIn", "degOut", "degTotal", "degreeCentrality"])];
    setContextMenu({
      x: e.clientX,
      y: e.clientY,
      compoundLabel,
      tableLabel: node.tableLabel,
      properties: node.properties,
      numericProperties,
    });
  }, [numericPropsByLabel]);

  const handleRelRightClick = useCallback((e: React.MouseEvent, type: string) => {
    e.preventDefault();
    setRelContextMenu({ x: e.clientX, y: e.clientY, type });
  }, []);

  return (
    <aside className="graph-sidebar" style={{ width }}>
      <div className="graph-sidebar-tabs" role="tablist">
        <Tooltip label={t("graphSidebar.tabDatabase")} withinPortal transitionProps={{ duration: 0 }}>
          <button
            type="button"
            className={`graph-sidebar-tab ${section === "db" ? "active" : ""}`}
            onClick={() => setSection("db")}
            aria-label={t("graphSidebar.tabDatabase")}
            aria-selected={section === "db"}
            role="tab"
            data-testid="graph-sidebar-tab-db"
          >
            <DatabaseIcon size={15} />
          </button>
        </Tooltip>
        <Tooltip label={t("graphSidebar.tabHistory")} withinPortal transitionProps={{ duration: 0 }}>
          <button
            type="button"
            className={`graph-sidebar-tab ${section === "history" ? "active" : ""}`}
            onClick={() => setSection("history")}
            aria-label={t("graphSidebar.tabHistory")}
            aria-selected={section === "history"}
            role="tab"
            data-testid="graph-sidebar-tab-history"
          >
            <HistoryIcon size={15} />
          </button>
        </Tooltip>
        <Tooltip label={t("graphSidebar.tabFavorites")} withinPortal transitionProps={{ duration: 0 }}>
          <button
            type="button"
            className={`graph-sidebar-tab ${section === "favorites" ? "active" : ""}`}
            onClick={() => setSection("favorites")}
            aria-label={t("graphSidebar.tabFavorites")}
            aria-selected={section === "favorites"}
            role="tab"
            data-testid="graph-sidebar-tab-favorites"
          >
            <StarIcon size={15} />
          </button>
        </Tooltip>
        {onNeo4jExport && (
          <Tooltip label={t("graphSidebar.tabExport")} withinPortal transitionProps={{ duration: 0 }}>
            <button
              type="button"
              className="graph-sidebar-tab"
              onClick={onNeo4jExport}
              aria-label={t("graphSidebar.tabExport")}
              data-testid="graph-sidebar-tab-export"
            >
              <ExportIcon size={15} />
            </button>
          </Tooltip>
        )}
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
                    <div className="graph-schema-heading">{t("graphSidebar.domainLabels")}</div>
                    <div className="graph-label-list">
                      {domainLabels.map((lbl) => {
                        const color = colorOverrides[lbl] ?? labelColor(lbl);
                        return (
                          <div key={lbl} className="graph-label-item">
                            <button
                              type="button"
                              className={`graph-label-pill${highlightedLabel === lbl ? " graph-label-pill--highlight" : ""}`}
                              style={{ background: color }}
                              draggable
                              onDragStart={(e) =>
                                e.dataTransfer.setData("text/x-provisa-domain", lbl)
                              }
                              onClick={() => onDomainClick(lbl)}
                              onContextMenu={(e) => {
                                e.preventDefault();
                                const domainNumeric = [
                                  ...new Set([
                                    ...schemaNodeLabels
                                      .filter((n) => n.domainLabel === lbl)
                                      .flatMap((n) => numericPropsByLabel[n.tableLabel] ?? []),
                                    "degIn",
                                    "degOut",
                                    "degTotal",
                                    "degreeCentrality",
                                  ]),
                                ];
                                setContextMenu({
                                  x: e.clientX,
                                  y: e.clientY,
                                  compoundLabel: lbl,
                                  tableLabel: lbl,
                                  properties: [],
                                  numericProperties: domainNumeric,
                                });
                              }}
                              title={`MATCH (n:${lbl}) RETURN n LIMIT 25`}
                            >
                              {lbl}
                            </button>
                          </div>
                        );
                      })}
                    </div>
                  </div>
                ) : null;
              })()}
            <div className="graph-schema-section">
              <button
                type="button"
                className="graph-schema-heading graph-schema-heading--collapsible"
                onClick={() => setNodeLabelsCollapsed((c) => !c)}
                aria-expanded={!nodeLabelsCollapsed}
                data-testid="graph-sidebar-node-labels-toggle"
              >
                {t("graphSidebar.nodeLabels")}
                <span
                  className={`graph-schema-chevron ${nodeLabelsCollapsed ? "graph-schema-chevron--collapsed" : ""}`}
                  aria-hidden="true"
                >
                  ▾
                </span>
              </button>
              {!nodeLabelsCollapsed &&
                (schemaLoading ? (
                  <div className="graph-schema-empty">{t("graphSidebar.loading")}</div>
                ) : schemaNodeLabels.length === 0 ? (
                  <div className="graph-schema-empty">{t("graphSidebar.noLabelsFound")}</div>
                ) : (
                  <div className="graph-label-list">
                    <div className="graph-label-item">
                      <button
                        type="button"
                        className="graph-label-pill graph-label-pill--all"
                        onClick={() => onLabelClick("*")}
                        title={t("graphSidebar.matchAllNodes")}
                      >
                        *({totalNodeCount !== null ? totalNodeCount.toLocaleString() : schemaNodeLabels.length})
                      </button>
                    </div>
                    {(() => {
                      const sorted = [...schemaNodeLabels]
                        .map((n) => ({
                          node: n,
                          compoundLabel: n.domainLabel ? `${n.domainLabel}:${n.tableLabel}` : n.tableLabel,
                        }))
                        .sort((a, b) => a.node.tableLabel.localeCompare(b.node.tableLabel));
                      const paged = sorted.slice(
                        nodeLabelsPage * SCHEMA_PAGE_SIZE,
                        (nodeLabelsPage + 1) * SCHEMA_PAGE_SIZE,
                      );
                      return paged.map(({ node, compoundLabel }) => {
                        const color = colorOverrides[compoundLabel] ?? labelColor(compoundLabel);
                        return (
                          <div key={compoundLabel} className="graph-label-item">
                            <button
                              type="button"
                              className={`graph-label-pill${highlightedLabel === compoundLabel ? " graph-label-pill--highlight" : ""}`}
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
                              {labelCounts[compoundLabel] !== undefined && (
                                <span style={{ opacity: 0.7, fontSize: "0.7em", marginLeft: "0.25em" }}>
                                  ({labelCounts[compoundLabel].toLocaleString()})
                                </span>
                              )}
                            </button>
                          </div>
                        );
                      });
                    })()}
                    {(() => {
                      const sorted = [...schemaNodeLabels]
                        .sort((a, b) => a.tableLabel.localeCompare(b.tableLabel));
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
                            type="button"
                            onClick={() => setNodeLabelsPage(0)}
                            disabled={nodeLabelsPage === 0}
                            aria-label={t("graphSidebar.firstPage")}
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
                            type="button"
                            onClick={() => setNodeLabelsPage((p) => p - 1)}
                            disabled={nodeLabelsPage === 0}
                            aria-label={t("graphSidebar.previousPage")}
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
                            {t("graphSidebar.pageIndicator", { page: nodeLabelsPage + 1, total: totalPages })}
                          </span>
                          <button
                            type="button"
                            onClick={() => setNodeLabelsPage((p) => p + 1)}
                            disabled={nodeLabelsPage >= totalPages - 1}
                            aria-label={t("graphSidebar.nextPage")}
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
                            type="button"
                            onClick={() => setNodeLabelsPage(totalPages - 1)}
                            disabled={nodeLabelsPage >= totalPages - 1}
                            aria-label={t("graphSidebar.lastPage")}
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
              <button
                type="button"
                className="graph-schema-heading graph-schema-heading--collapsible"
                onClick={() => setRelTypesCollapsed((c) => !c)}
                aria-expanded={!relTypesCollapsed}
                data-testid="graph-sidebar-rel-types-toggle"
              >
                {t("graphSidebar.relationshipTypes")}
                <span
                  className={`graph-schema-chevron ${relTypesCollapsed ? "graph-schema-chevron--collapsed" : ""}`}
                  aria-hidden="true"
                >
                  ▾
                </span>
              </button>
              {!relTypesCollapsed &&
                (schemaLoading ? (
                  <div className="graph-schema-empty">{t("graphSidebar.loading")}</div>
                ) : schemaRels.length === 0 ? (
                  <div className="graph-schema-empty">{t("graphSidebar.noRelTypesFound")}</div>
                ) : (
                  <div className="graph-label-list">
                    {(() => {
                      const uniqueRels = [...new Map(schemaRels.map((r) => [r.type, r])).values()];
                      const paged = uniqueRels.slice(
                        relTypesPage * SCHEMA_PAGE_SIZE,
                        (relTypesPage + 1) * SCHEMA_PAGE_SIZE,
                      );
                      return [
                        <div key="__all__" className="graph-label-item">
                          <button
                            type="button"
                            className="graph-label-pill graph-label-pill--all"
                            onClick={() => onRelClick("*")}
                            title={t("graphSidebar.matchAllRels")}
                          >
                            *({totalRelCount !== null ? totalRelCount.toLocaleString() : uniqueRels.length})
                          </button>
                        </div>,
                        ...paged.map(({ type }) => (
                          <div key={type} className="graph-label-item">
                            <button
                              type="button"
                              className="graph-rel-badge"
                              title={`MATCH ()-[r:${type}]->() RETURN r LIMIT 25`}
                              onClick={() => onRelClick(type)}
                              onContextMenu={(e) => handleRelRightClick(e, type)}
                            >
                              {type}
                            </button>
                          </div>
                        )),
                      ];
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
                            type="button"
                            onClick={() => setRelTypesPage(0)}
                            disabled={relTypesPage === 0}
                            aria-label={t("graphSidebar.firstPage")}
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
                            type="button"
                            onClick={() => setRelTypesPage((p) => p - 1)}
                            disabled={relTypesPage === 0}
                            aria-label={t("graphSidebar.previousPage")}
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
                            {t("graphSidebar.pageIndicator", { page: relTypesPage + 1, total: totalPages })}
                          </span>
                          <button
                            type="button"
                            onClick={() => setRelTypesPage((p) => p + 1)}
                            disabled={relTypesPage >= totalPages - 1}
                            aria-label={t("graphSidebar.nextPage")}
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
                            type="button"
                            onClick={() => setRelTypesPage(totalPages - 1)}
                            disabled={relTypesPage >= totalPages - 1}
                            aria-label={t("graphSidebar.lastPage")}
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

            {propertyKeys.length > 0 && (
              <div className="graph-schema-section">
                <button
                  type="button"
                  className="graph-schema-heading graph-schema-heading--collapsible"
                  onClick={() => setPropKeysCollapsed((c) => !c)}
                  aria-expanded={!propKeysCollapsed}
                  data-testid="graph-sidebar-prop-keys-toggle"
                >
                  {t("graphSidebar.propertyKeys")}
                  <span
                    className={`graph-schema-chevron ${propKeysCollapsed ? "graph-schema-chevron--collapsed" : ""}`}
                    aria-hidden="true"
                  >
                    ▾
                  </span>
                </button>
                {!propKeysCollapsed && (
                  <div className="graph-prop-key-list">
                    {[...propertyKeys].sort().map((k) => (
                      <button
                        key={k}
                        type="button"
                        className={`graph-prop-key-tag${onPropertyKeyClick ? " graph-prop-key-tag--clickable" : ""}`}
                        onClick={() => onPropertyKeyClick?.(k)}
                        title={onPropertyKeyClick ? `MATCH (n)\nWHERE n.${k} IS NOT NULL\nRETURN DISTINCT "node" AS entity, n.${k} AS ${k}\nLIMIT 25\nUNION ALL\nMATCH ()-[r]-()\nWHERE r.${k} IS NOT NULL\nRETURN DISTINCT "relationship" AS entity, r.${k} AS ${k}\nLIMIT 25` : undefined}
                      >
                        {k}
                      </button>
                    ))}
                  </div>
                )}
              </div>
            )}
          </>
        )}

        {section === "history" && (
          <div className="graph-schema-section">
            <div className="graph-schema-heading">{t("graphSidebar.history")}</div>
            {history.length === 0 ? (
              <div className="graph-schema-empty">{t("graphSidebar.noHistory")}</div>
            ) : (
              <div className="graph-history-list">
                {history.map((q, i) => (
                  <button
                    key={i}
                    type="button"
                    className="graph-history-item"
                    onClick={() => onHistorySelect(q)}
                    title={q}
                  >
                    {q}
                  </button>
                ))}
              </div>
            )}
          </div>
        )}

        {section === "favorites" && (
          <div className="graph-schema-section">
            <div className="graph-schema-heading">{t("graphSidebar.favorites")}</div>
            {favorites.length === 0 ? (
              <div className="graph-schema-empty">{t("graphSidebar.noFavorites")}</div>
            ) : (
              <div className="graph-history-list">
                {[...favorites].sort((a, b) => b.ts - a.ts).map((fav) => (
                  <div key={fav.id} className="graph-fav-item">
                    {renamingId === fav.id ? (
                      <TextInput
                        ref={renameInputRef}
                        aria-label={t("graphSidebar.renameFavoriteInput")}
                        classNames={{ input: "graph-fav-rename-input" }}
                        value={renameValue}
                        onChange={(e) => setRenameValue(e.target.value)}
                        onKeyDown={(e) => {
                          if (e.key === "Enter") commitRename();
                          if (e.key === "Escape") setRenamingId(null);
                        }}
                        onBlur={commitRename}
                      />
                    ) : (
                      <>
                        <button
                          type="button"
                          className="graph-fav-run"
                          title={t("graphSidebar.run")}
                          aria-label={t("graphSidebar.run")}
                          onClick={(e) => { e.stopPropagation(); onFavoriteRun?.(fav.query); }}
                        >
                          ▶
                        </button>
                        <button
                          type="button"
                          className="graph-fav-label"
                          onClick={() => onFavoriteSelect?.(fav.query)}
                          title={fav.query}
                        >
                          {fav.label}
                        </button>
                        <button
                          type="button"
                          className="graph-fav-rename-btn"
                          title={t("graphSidebar.rename")}
                          aria-label={t("graphSidebar.rename")}
                          onClick={(e) => { e.stopPropagation(); setRenameValue(fav.label); setRenamingId(fav.id); }}
                        >
                          ✎
                        </button>
                        <button
                          type="button"
                          className="graph-fav-del"
                          title={t("graphSidebar.remove")}
                          aria-label={t("graphSidebar.remove")}
                          onClick={() => onFavoriteDelete?.(fav.id)}
                        >
                          ✕
                        </button>
                      </>
                    )}
                  </div>
                ))}
              </div>
            )}
          </div>
        )}
      </div>

      <div
        className="graph-sidebar-resizer"
        onMouseDown={onMouseDown}
        role="separator"
        aria-orientation="vertical"
        aria-label={t("graphSidebar.resizeSidebar")}
        tabIndex={0}
      />

      {contextMenu && (
        <NodeContextMenu
          menu={contextMenu}
          colorOverrides={colorOverrides}
          sizeOverrides={sizeOverrides}
          labelProperty={labelProperty}
          sizeByProperty={sizeByProperty}
          sizeMultiplier={sizeMultiplier}
          onColorChange={onColorChange}
          onSizeChange={onSizeChange}
          onLabelPropertyChange={onLabelPropertyChange}
          onSizeByPropertyChange={onSizeByPropertyChange}
          onSizeMultiplierChange={onSizeMultiplierChange}
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
