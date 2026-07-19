// Copyright (c) 2026 Kenneth Stott
// Canary: c3bc1ca0-9ecf-4cce-8fe5-d998ed968e53
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/* eslint-disable react-hooks/refs --
   Latest-value refs mirror props/state for stable event-handler closures, and
   prev-value refs gate documented render-phase setState on prop change. These
   ref reads/writes during render are intentional throughout this module. */

import { useRef, useEffect, useState, useCallback, useMemo } from "react";
import { ActionIcon, Loader, Menu, Select } from "@mantine/core";
import { useTranslation } from "react-i18next";
import type { Relationship } from "../../types/admin";
import { extractElements, injectExclusion } from "./graph-model";
import type { GNode, GEdge, GraphStats, RelLineOverride, FrameData } from "./graph-model";
import CodeMirror from "@uiw/react-codemirror";
import * as _neo4jCypherMod from "@neo4j-cypher/codemirror";
import "@neo4j-cypher/codemirror/css/cypher-codemirror.css";
const { getCypherLanguageExtensions: _getGFCypherExts, cypherLinter: _gfCypherLinter } =
  _neo4jCypherMod as unknown as {
    getCypherLanguageExtensions: (opts: { cypherLanguage: boolean }) => Extension[];
    cypherLinter: (...args: unknown[]) => Extension;
  };
const _gfCypherLangExts = _getGFCypherExts({ cypherLanguage: true });
import { json as jsonLang } from "@codemirror/lang-json";
import { oneDark } from "@codemirror/theme-one-dark";
import { EditorView, keymap } from "@codemirror/view";
import { Prec } from "@codemirror/state";
import type { Extension } from "@codemirror/state";
import { createPortal } from "react-dom";
import { CopySymbolButton } from "../../components/CopyButton";
import type { CyInstance } from "./cytoscape-types";
import type { ClusterLevel } from "./graph-clusters";
import { downloadBlob, compositeGraphDownload, downloadGraphSvg, toCSV } from "./graph-export";
import { GraphCanvas } from "./GraphCanvas";
import { GraphStatsPanel } from "./GraphStatsModal";
import { Inspector } from "./Inspector";
import { TableView, JsonCopyButton } from "./TableView";
import { useLocalStorage } from "./graph-persistence";
import { useOverlayNavigation } from "./frame/use-overlay-navigation";
import { GraphViewIcon, TableViewIcon, JsonViewIcon, StatsViewIcon, CodeViewIcon, ExpandModalIcon, CollapseModalIcon, CollapseQueryIcon, ExpandQueryIcon, PushPinIcon } from "./GraphIcons";

// ── Frame component ───────────────────────────────────────────────────────────
interface GraphFrameProps {
  frame: FrameData;
  onClose: (id: string) => void;
  onRerun: (id: string, query: string) => void;
  onTableDrop?: (frameId: string, compoundLabel: string) => void;
  onDomainDrop?: (frameId: string, domainLabel: string) => void;
  colorOverrides: Record<string, string>;
  sizeOverrides: Record<string, number>;
  labelProperty: Record<string, string>;
  sizeByProperty: Record<string, string>;
  sizeMultiplier: Record<string, number>;
  relLineOverrides: Record<string, RelLineOverride>;
  onColorChange: (label: string, color: string) => void;
  pkMap: Record<string, string[]>;
  labelToTableLabel: Record<string, string>;
  relationships?: Relationship[];
  autoImpute?: boolean;
  onSaveEdgeAlias?: (relId: number, cqlAlias: string, gqlAlias: string) => Promise<void>;
  onSelectedLabelChange?: (label: string | null) => void;
  onAddFavorite?: (query: string) => void;
  isFavorited?: (query: string) => boolean;
  onPin?: (id: string) => void;
  onEffectiveDataChange?: (
    frameId: string,
    nodes: Map<string, GNode>,
    edges: Map<string, GEdge>,
  ) => void;
}

export function GraphFrame({
  frame,
  onClose,
  onRerun,
  onTableDrop,
  onDomainDrop,
  colorOverrides,
  sizeOverrides,
  labelProperty,
  sizeByProperty,
  sizeMultiplier,
  relLineOverrides,
  onColorChange,
  pkMap,
  labelToTableLabel,
  relationships,
  autoImpute: autoImputeProp = false,
  onSaveEdgeAlias,
  onSelectedLabelChange,
  onAddFavorite,
  isFavorited,
  onPin,
  onEffectiveDataChange,
}: GraphFrameProps) {
  const { t } = useTranslation();
  const [view, setView] = useState<"graph" | "table" | "json" | "graphstats" | "code">("graph");
  const [selected, setSelectedRaw] = useState<
    { kind: "node"; data: GNode; graphStats?: GraphStats } | { kind: "edge"; data: GEdge } | null
  >(null);
  const [inspectorVisible, setInspectorVisible] = useState(true);
  const setSelected = useCallback(
    (s: { kind: "node"; data: GNode; graphStats?: GraphStats } | { kind: "edge"; data: GEdge } | null) => {
      setSelectedRaw(s);
      if (s !== null) setInspectorVisible(true);
      onSelectedLabelChange?.(s?.kind === "node" ? s.data.label : null);
    },
    [onSelectedLabelChange],
  );
  const [collapsed, setCollapsed] = useState(false);
  const [expanded, setExpanded] = useState(false);
  const [modalHeaderHeight, setModalHeaderHeight] = useState(44);
  const [inspectorWidth, setInspectorWidth] = useState(260);
  const [graphAreaHeight, setGraphAreaHeight] = useState(460);
  const [editQuery, setEditQuery] = useState(frame.query);
  const editQueryRef = useRef(editQuery);
  editQueryRef.current = editQuery;
  const [queryFocused, setQueryFocused] = useState(false);
  const editorViewRef = useRef<import("@codemirror/view").EditorView | null>(null);
  const pendingFocusRef = useRef(false);
  /* eslint-disable react-hooks/set-state-in-effect -- sync the editable query buffer to the frame.query prop when the frame is re-run or replaced externally */
  useEffect(() => {
    setEditQuery(frame.query);
  }, [frame.query]);
  /* eslint-enable react-hooks/set-state-in-effect */
  const [overlayData, setOverlayData] = useState<
    Map<string, { nodes: Map<string, GNode>; edges: Map<string, GEdge> }>
  >(new Map());
  useEffect(() => {
    if (typeof window !== "undefined")
      (window as unknown as Record<string, unknown>).__overlayData = overlayData;
  }, [overlayData]);
  const [autoImpute, setAutoImpute] = useState(autoImputeProp);
  const [dragOver, setDragOver] = useState(false);
  const handleRerun = useCallback(
    (id: string, query: string) => {
      setOverlayData(new Map());
      onRerun(id, query);
    },
    [onRerun],
  );
  const [showDlMenu, setShowDlMenu] = useState(false);
  const [clusterLevel, setClusterLevel] = useLocalStorage<ClusterLevel>(
    `provisa.graph.clusterLevel.${frame.id}`,
    "none",
  );
  const [tableWrap, setTableWrap] = useState(false);
  const [tableColWidths, setTableColWidths] = useState<number[]>(() =>
    frame.columns.map(() => 180),
  );
  const prevColumnsKey = useRef(frame.columns.join(","));
  if (frame.columns.join(",") !== prevColumnsKey.current) {
    prevColumnsKey.current = frame.columns.join(",");
    setTableColWidths(frame.columns.map(() => 180));
  }
  const canvasCyRef = useRef<CyInstance | null>(null);
  const canvasHullSvgRef = useRef<SVGSVGElement | null>(null);
  const inspWidthRef = useRef(inspectorWidth);
  inspWidthRef.current = inspectorWidth;
  const graphAreaHeightRef = useRef(graphAreaHeight);
  graphAreaHeightRef.current = graphAreaHeight;

  const {
    handleToggleChildren,
    handleToggleChildrenCircular,
    handleToggleChildrenBatch,
    handleToggleParents,
    handleToggleParentsCircular,
    handleToggleParentsBatch,
  } = useOverlayNavigation({
    overlayData,
    setOverlayData,
    frameNodes: frame.nodes,
    relationships,
  });

  const handleExcludeNode = useCallback(
    (nodeKeys: string[]) => {
      // Chain exclusions across all selected nodes, then only update query text.
      // Nodes are already removed from canvas by the caller — no relayout or overlay reset needed.
      let currentQuery = editQueryRef.current;
      for (const nodeKey of nodeKeys) {
        const gNode = frame.nodes.get(nodeKey);
        if (!gNode) continue;
        const nodeId = String(gNode.id);
        const pkCols = pkMap[gNode.label] ?? [];
        const pkCol = pkCols[0] ?? null;
        const pkValue = pkCol ? gNode.properties[pkCol] : undefined;
        const newQuery = injectExclusion(
          currentQuery,
          gNode.tableLabel,
          nodeId,
          pkCol,
          pkValue,
          relationships,
        );
        if (newQuery) currentQuery = newQuery;
      }
      if (currentQuery !== editQueryRef.current) {
        setEditQuery(currentQuery);
      }
    },
    [frame.nodes, pkMap, relationships],
  );

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

  const handleFrameResizeStart = useCallback((e: React.MouseEvent) => {
    e.preventDefault();
    const startY = e.clientY;
    const startH = graphAreaHeightRef.current;
    const onMove = (me: MouseEvent) => {
      setGraphAreaHeight(Math.max(150, Math.min(1200, startH + (me.clientY - startY))));
    };
    const onUp = () => {
      window.removeEventListener("mousemove", onMove);
      window.removeEventListener("mouseup", onUp);
    };
    window.addEventListener("mousemove", onMove);
    window.addEventListener("mouseup", onUp);
  }, []);

  const overlayNodes = useMemo(() => {
    if (overlayData.size === 0) return new Map<string, GNode>();
    const m = new Map<string, GNode>();
    for (const d of overlayData.values())
      d.nodes.forEach((n, k) => {
        if (!frame.nodes.has(k)) m.set(k, n);
      });
    return m;
  }, [frame.nodes, overlayData]);

  const overlayEdges = useMemo(() => {
    if (overlayData.size === 0) return new Map<string, GEdge>();
    // Dedup against frame edges by both identity key and endpoint+type fingerprint
    const frameFingerprints = new Set<string>();
    frame.edges.forEach((e) => {
      frameFingerprints.add(
        `${e.startNode.label}:${e.startNode.id}→${e.endNode.label}:${e.endNode.id}:${e.type}`,
      );
      // Also store reversed fingerprint so backward-traversal frame edges match canonical imputed edges
      frameFingerprints.add(
        `${e.endNode.label}:${e.endNode.id}→${e.startNode.label}:${e.startNode.id}:${e.type}`,
      );
    });
    const m = new Map<string, GEdge>();
    for (const d of overlayData.values()) {
      d.edges.forEach((e, k) => {
        if (frame.edges.has(k)) return;
        const fp = `${e.startNode.label}:${e.startNode.id}→${e.endNode.label}:${e.endNode.id}:${e.type}`;
        if (frameFingerprints.has(fp)) return;
        m.set(k, e);
      });
    }
    return m;
  }, [frame.edges, overlayData]);

  const augmentedNodes = useMemo(() => {
    const degIn = new Map<string, number>();
    const degOut = new Map<string, number>();
    const allEdges = overlayEdges.size > 0 ? new Map([...frame.edges, ...overlayEdges]) : frame.edges;
    allEdges.forEach((e) => {
      const srcKey = `${e.startNode.label}:${e.startNode.id}`;
      const tgtKey = `${e.endNode.label}:${e.endNode.id}`;
      degOut.set(srcKey, (degOut.get(srcKey) ?? 0) + 1);
      degIn.set(tgtKey, (degIn.get(tgtKey) ?? 0) + 1);
    });
    const totalNodes = frame.nodes.size;
    const result = new Map<string, GNode>();
    frame.nodes.forEach((n, k) => {
      const i = degIn.get(k) ?? 0;
      const o = degOut.get(k) ?? 0;
      const deg = i + o;
      const degreeCentrality = totalNodes > 1 ? parseFloat((deg / (totalNodes - 1)).toFixed(4)) : 0;
      result.set(k, { ...n, properties: { ...n.properties, degIn: i, degOut: o, degTotal: deg, degreeCentrality } });
    });
    return result;
  }, [frame.nodes, frame.edges, overlayEdges]);

  useEffect(() => {
    if (!onEffectiveDataChange) return;
    const allNodes =
      overlayNodes.size > 0 ? new Map([...augmentedNodes, ...overlayNodes]) : augmentedNodes;
    const allEdges =
      overlayEdges.size > 0 ? new Map([...frame.edges, ...overlayEdges]) : frame.edges;
    onEffectiveDataChange(frame.id, allNodes, allEdges);
  }, [frame.id, augmentedNodes, frame.edges, overlayNodes, overlayEdges, onEffectiveDataChange]);

  const overviewData = useMemo(() => {
    const allNodes = overlayNodes.size > 0 ? new Map([...augmentedNodes, ...overlayNodes]) : augmentedNodes;
    const allEdges = overlayEdges.size > 0 ? new Map([...frame.edges, ...overlayEdges]) : frame.edges;
    const labelCounts = new Map<string, number>();
    allNodes.forEach((n) => {
      labelCounts.set(n.label, (labelCounts.get(n.label) ?? 0) + 1);
    });
    const typeCounts = new Map<string, number>();
    allEdges.forEach((e) => typeCounts.set(e.type, (typeCounts.get(e.type) ?? 0) + 1));
    return {
      nodesByLabel: [...labelCounts.entries()].sort((a, b) => b[1] - a[1]),
      edgesByType: [...typeCounts.entries()].sort((a, b) => b[1] - a[1]),
      nodeCount: allNodes.size,
      edgeCount: allEdges.size,
    };
  }, [augmentedNodes, overlayNodes, frame.edges, overlayEdges]);

  const showingChildrenNatural = useMemo(
    () =>
      new Set(
        Array.from(overlayData.keys())
          .filter((k) => k.endsWith(":children"))
          .map((k) => k.slice(0, -":children".length)),
      ),
    [overlayData],
  );
  const showingChildrenCircular = useMemo(
    () =>
      new Set(
        Array.from(overlayData.keys())
          .filter((k) => k.endsWith(":children:circular"))
          .map((k) => k.slice(0, -":children:circular".length)),
      ),
    [overlayData],
  );
  const showingParents = useMemo(
    () =>
      new Set(
        Array.from(overlayData.keys())
          .filter((k) => k.endsWith(":parents"))
          .map((k) => k.slice(0, -":parents".length)),
      ),
    [overlayData],
  );
  const showingParentsCircular = useMemo(
    () =>
      new Set(
        Array.from(overlayData.keys())
          .filter((k) => k.endsWith(":parents:circular"))
          .map((k) => k.slice(0, -":parents:circular".length)),
      ),
    [overlayData],
  );

  // When autoImpute is turned off, clear its overlay
  useEffect(() => {
    if (!autoImpute) {
      /* eslint-disable-next-line react-hooks/set-state-in-effect --
         clear the imputed-relationships overlay synchronously when the user
         turns autoImpute off */
      setOverlayData((prev) => {
        const next = new Map(prev);
        next.delete("__remaining_rels");
        return next;
      });
    }
  }, [autoImpute]);

  // Run imputation whenever the frame result changes or autoImpute toggles on
  useEffect(() => {
    if (!autoImpute || frame.status !== "done" || frame.nodes.size === 0) return;
    let cancelled = false;
    const nodeList = [...frame.nodes.values()].map((n) => ({ label: n.label, id: n.id }));
    (async () => {
      const res = await fetch("/data/impute-relationships", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ nodes: nodeList }),
      });
      if (cancelled) return;
      if (!res.ok) {
        const text = await res.text().catch(() => "");
        let err: unknown;
        try { err = JSON.parse(text); } catch { err = text; }
        console.error("impute-relationships failed (HTTP", res.status, "):", err);
        return;
      }
      const data = await res.json();
      const rows: Record<string, unknown>[] = data.rows ?? [];
      const result = extractElements(rows);
      if (!cancelled && (result.nodes.size > 0 || result.edges.size > 0)) {
        setOverlayData((prev) => new Map(prev).set("__remaining_rels", result));
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [autoImpute, frame.status, frame.nodes]);

  const hasGraph = frame.nodes.size > 0 || frame.edges.size > 0;

  // Properties available for grouping: virtual schema_L1/L2/L3 (mapped to scl1/scl2/scl3)
  // followed by any scalar property with more than one distinct value.
  const groupableAttrs = useMemo(() => {
    if (augmentedNodes.size === 0) return [];
    const SKIP = new Set(["scl1", "scl2", "scl3", "l1Cluster", "l2Cluster", "l3Cluster", "degIn", "degOut", "degTotal"]);
    const schemaVirtuals: string[] = [];
    for (const [virtName, prop] of [
      ["schema_L1", "scl1"],
      ["schema_L2", "scl2"],
      ["schema_L3", "scl3"],
    ] as const) {
      const vals = new Set<string>();
      augmentedNodes.forEach((n) => {
        const v = n.properties[prop];
        if (v !== null && v !== undefined) vals.add(String(v));
      });
      if (vals.size > 1) schemaVirtuals.push(virtName);
    }
    const degreeVirtuals: string[] = [];
    for (const key of ["degIn", "degOut", "degTotal"] as const) {
      const vals = new Set<string>();
      augmentedNodes.forEach((n) => {
        const v = n.properties[key];
        if (v !== null && v !== undefined) vals.add(String(v));
      });
      if (vals.size > 1) degreeVirtuals.push(key);
    }
    const counts = new Map<string, Set<string>>();
    augmentedNodes.forEach((n) => {
      Object.entries(n.properties).forEach(([k, v]) => {
        if (SKIP.has(k) || v === null || v === undefined) return;
        if (typeof v === "object") return;
        if (!counts.has(k)) counts.set(k, new Set());
        counts.get(k)!.add(String(v));
      });
    });
    const regularAttrs = [...counts.entries()]
      .filter(([, vals]) => vals.size > 1)
      .map(([k]) => k);
    return ["domain", ...schemaVirtuals, ...degreeVirtuals, ...regularAttrs].sort((a, b) => a.localeCompare(b));
  }, [augmentedNodes]);
  const activeView: "graph" | "table" | "json" | "graphstats" | "code" = hasGraph
    ? view
    : view === "json"
      ? "json"
      : view === "graphstats"
        ? "graphstats"
        : view === "code"
          ? "code"
          : "table";

  const renderHeader = (isModal: boolean) => (
    <div className="gf-header">
      <div className="gf-query-editor-wrap">
        {!queryFocused && (
          <div
            className="gf-header-query-collapsed"
            onClick={() => {
              setQueryFocused(true);
              pendingFocusRef.current = true;
            }}
            title={editQuery}
          >
            {editQuery.replace(/\s*\n\s*/g, " ")}
          </div>
        )}
        {queryFocused && (
          <CodeMirror
            className="gf-header-query-input"
            value={editQuery}
            theme={oneDark}
            minHeight="2.8em"
            extensions={[
              ..._gfCypherLangExts,
              _gfCypherLinter({ showErrors: false }),
              EditorView.lineWrapping,
              Prec.highest(
                keymap.of([
                  {
                    key: "Enter",
                    run: () => {
                      handleRerun(frame.id, editQuery.trim());
                      return true;
                    },
                  },
                ]),
              ),
            ]}
            onChange={(val) => setEditQuery(val)}
            onCreateEditor={(view) => {
              editorViewRef.current = view;
              if (pendingFocusRef.current) {
                pendingFocusRef.current = false;
                view.focus();
              }
            }}
            onUpdate={(vu) => {
              if (vu.docChanged) vu.view.requestMeasure();
              if (vu.focusChanged && !vu.view.hasFocus) setQueryFocused(false);
            }}
            basicSetup={{ lineNumbers: false, foldGutter: false, highlightActiveLine: false }}
          />
        )}
        <CopySymbolButton text={editQuery} className="gf-copy-query-btn" title={t("graphFrame.copyQuery")} />
      </div>
      <div className="gf-header-right">
        <div className="gf-header-top">
          <div className="gf-header-meta">
            {frame.status === "loading" && (
              <span className="gf-loading">{t("graphFrame.running")}</span>
            )}
            {frame.status === "done" && (
              <span className="gf-meta-text">
                {frame.elapsed !== undefined
                  ? t("graphFrame.metaCountElapsed", {
                      nodes: frame.nodes.size,
                      edges: frame.edges.size,
                      elapsed: frame.elapsed,
                    })
                  : t("graphFrame.metaCount", {
                      nodes: frame.nodes.size,
                      edges: frame.edges.size,
                    })}
              </span>
            )}
            {frame.status === "error" && (
              <span className="gf-meta-error">{t("graphFrame.error")}</span>
            )}
          </div>
          {!isModal && (
            <ActionIcon
              variant="subtle"
              className="gf-icon-btn"
              onClick={() => setExpanded(true)}
              title={t("graphFrame.expand")}
              aria-label={t("graphFrame.expand")}
              data-testid="graph-frame-expand-btn"
            >
              <ExpandModalIcon size={14} />
            </ActionIcon>
          )}
          {isModal && (
            <ActionIcon
              variant="subtle"
              className="gf-icon-btn"
              onClick={() => setExpanded(false)}
              title={t("graphFrame.exitFullScreen")}
              aria-label={t("graphFrame.exitFullScreen")}
              data-testid="graph-frame-exit-fullscreen-btn"
            >
              <CollapseModalIcon size={14} />
            </ActionIcon>
          )}
          {!isModal && (
            <ActionIcon
              variant="subtle"
              className="gf-icon-btn"
              onClick={() => setCollapsed((c) => !c)}
              title={collapsed ? t("graphFrame.expand") : t("graphFrame.collapse")}
              aria-label={collapsed ? t("graphFrame.expand") : t("graphFrame.collapse")}
              data-testid="graph-frame-collapse-btn"
            >
              {collapsed ? <ExpandQueryIcon size={14} /> : <CollapseQueryIcon size={14} />}
            </ActionIcon>
          )}
          {!isModal && onPin && (
            <ActionIcon
              variant="subtle"
              className={`gf-icon-btn${frame.pinned ? " gf-icon-btn--on" : ""}`}
              onClick={() => onPin(frame.id)}
              title={frame.pinned ? t("graphFrame.unpinHint") : t("graphFrame.pinToTop")}
              aria-label={frame.pinned ? t("graphFrame.unpinHint") : t("graphFrame.pinToTop")}
              aria-current={frame.pinned ? "true" : undefined}
              data-testid="graph-frame-pin-btn"
            >
              <PushPinIcon size={14} />
            </ActionIcon>
          )}
          {!isModal && (
            <ActionIcon
              variant="subtle"
              className="gf-icon-btn"
              onClick={() => onClose(frame.id)}
              title={t("graphFrame.close")}
              aria-label={t("graphFrame.close")}
              data-testid="graph-frame-close-btn"
            >
              <span aria-hidden="true">✕</span>
            </ActionIcon>
          )}
        </div>
        <div className="gf-header-actions">
        <ActionIcon
          variant="subtle"
          className="gf-run-inline-btn"
          onClick={() => handleRerun(frame.id, editQuery.trim())}
          title={t("graphFrame.run")}
          aria-label={t("graphFrame.run")}
          data-testid="graph-frame-run-btn"
        >
          <span aria-hidden="true">▶</span>
        </ActionIcon>
        {onAddFavorite && (
          <ActionIcon
            variant="subtle"
            className={`gf-icon-btn${isFavorited?.(editQuery.trim()) ? " gf-icon-btn--on" : ""}`}
            title={isFavorited?.(editQuery.trim()) ? t("graphFrame.removeFromFavorites") : t("graphFrame.addToFavorites")}
            aria-label={isFavorited?.(editQuery.trim()) ? t("graphFrame.removeFromFavorites") : t("graphFrame.addToFavorites")}
            aria-current={isFavorited?.(editQuery.trim()) ? "true" : undefined}
            onClick={() => onAddFavorite(editQuery.trim())}
            data-testid="graph-frame-favorite-btn"
          >
            <span aria-hidden="true">★</span>
          </ActionIcon>
        )}
        {hasGraph && frame.status === "done" && (
          <ActionIcon
            variant="subtle"
            className={`gf-run-inline-btn${autoImpute ? " gf-run-inline-btn--on" : ""}`}
            onClick={() => setAutoImpute((v) => !v)}
            title={autoImpute ? t("graphFrame.autoImputeOn") : t("graphFrame.autoImputeOff")}
            aria-label={autoImpute ? t("graphFrame.autoImputeOn") : t("graphFrame.autoImputeOff")}
            aria-current={autoImpute ? "true" : undefined}
            data-testid="graph-frame-auto-impute-btn"
          >
            <span aria-hidden="true">⊕</span>
          </ActionIcon>
        )}
        {hasGraph && frame.status === "done" && groupableAttrs.length > 0 && (
          <Select
            aria-label={t("graphFrame.groupTooltip")}
            title={t("graphFrame.groupTooltip")}
            className={`gf-attr-select${groupableAttrs.includes(clusterLevel) ? " gf-icon-btn--on" : ""}`}
            size="xs"
            placeholder={t("graphFrame.groupPlaceholder")}
            value={groupableAttrs.includes(clusterLevel) ? clusterLevel : null}
            onChange={(val) => setClusterLevel((val ?? "none") as ClusterLevel)}
            data={groupableAttrs.map((a) => ({ value: a, label: a }))}
            clearable
            comboboxProps={{ withinPortal: true }}
            data-testid="graph-frame-group-select"
          />
        )}
        {activeView === "table" && frame.rows.length > 0 && (
          <ActionIcon
            variant="subtle"
            className={`gf-icon-btn${tableWrap ? " gf-icon-btn--on" : ""}`}
            title={t("graphFrame.wrapCellText")}
            aria-label={t("graphFrame.wrapCellText")}
            aria-current={tableWrap ? "true" : undefined}
            onClick={() => setTableWrap((v) => !v)}
            data-testid="graph-frame-table-wrap-btn"
          >
            <span aria-hidden="true">⇌</span>
          </ActionIcon>
        )}
        {frame.status === "done" && (frame.rows.length > 0 || hasGraph) && (
          <div className="gf-dl-wrap">
            <Menu
              position="bottom-end"
              withinPortal
              transitionProps={{ duration: 0 }}
              opened={showDlMenu}
              onChange={setShowDlMenu}
            >
              <Menu.Target>
                <ActionIcon
                  variant="subtle"
                  className="gf-icon-btn"
                  title={t("graphFrame.download")}
                  aria-label={t("graphFrame.download")}
                  onClick={() => setShowDlMenu((v) => !v)}
                  data-testid="graph-frame-download-btn"
                >
                  <svg
                    width="14"
                    height="14"
                    viewBox="0 0 16 16"
                    fill="currentColor"
                    xmlns="http://www.w3.org/2000/svg"
                    aria-hidden="true"
                  >
                    <path d="M8 10.5L4.5 7h2V2h3v5h2L8 10.5z" />
                    <rect x="2" y="12" width="12" height="1.5" rx="0.75" />
                  </svg>
                </ActionIcon>
              </Menu.Target>
              <Menu.Dropdown className="gf-dl-menu">
                {frame.rows.length > 0 && (
                  <Menu.Item
                    className="gf-dl-item"
                    onClick={() => {
                      const json = JSON.stringify(frame.rows, null, 2);
                      downloadBlob(new Blob([json], { type: "application/json" }), "result.json");
                      setShowDlMenu(false);
                    }}
                  >
                    {t("graphFrame.downloadJson")}
                  </Menu.Item>
                )}
                {frame.rows.length > 0 && (
                  <Menu.Item
                    className="gf-dl-item"
                    onClick={() => {
                      const csv = toCSV(frame.columns, frame.rows);
                      downloadBlob(new Blob([csv], { type: "text/csv" }), "result.csv");
                      setShowDlMenu(false);
                    }}
                  >
                    {t("graphFrame.downloadCsv")}
                  </Menu.Item>
                )}
                {hasGraph && activeView === "graph" && (
                  <Menu.Item
                    className="gf-dl-item"
                    onClick={() => {
                      const cy = canvasCyRef.current;
                      if (!cy) return;
                      compositeGraphDownload(cy, canvasHullSvgRef.current, "graph.png", "png");
                      setShowDlMenu(false);
                    }}
                  >
                    {t("graphFrame.downloadPng")}
                  </Menu.Item>
                )}
                {hasGraph && activeView === "graph" && (
                  <Menu.Item
                    className="gf-dl-item"
                    onClick={() => {
                      const cy = canvasCyRef.current;
                      if (!cy) return;
                      compositeGraphDownload(cy, canvasHullSvgRef.current, "graph.jpg", "jpg");
                      setShowDlMenu(false);
                    }}
                  >
                    {t("graphFrame.downloadJpg")}
                  </Menu.Item>
                )}
                {hasGraph && activeView === "graph" && (
                  <Menu.Item
                    className="gf-dl-item"
                    onClick={() => {
                      const cy = canvasCyRef.current;
                      if (!cy) return;
                      downloadGraphSvg(cy, canvasHullSvgRef.current);
                      setShowDlMenu(false);
                    }}
                  >
                    {t("graphFrame.downloadSvg")}
                  </Menu.Item>
                )}
              </Menu.Dropdown>
            </Menu>
          </div>
        )}
        </div>
      </div>
    </div>
  );

  const frameBody = (
    <div className="gf-body" style={expanded ? undefined : { height: graphAreaHeight }}>
      <div className="gf-view-bar">
        {(
          [
            { key: "graph" as const, label: t("graphFrame.viewGraph"), Icon: GraphViewIcon, show: hasGraph, onClick: () => setView("graph") },
            { key: "table" as const, label: t("graphFrame.viewTable"), Icon: TableViewIcon, show: true, onClick: () => setView("table") },
            { key: "json" as const, label: t("graphFrame.viewJson"), Icon: JsonViewIcon, show: true, onClick: () => setView("json") },
            { key: "graphstats" as const, label: t("graphFrame.viewGraphStats"), Icon: StatsViewIcon, show: hasGraph, onClick: () => setView(activeView === "graphstats" ? "graph" : "graphstats") },
            { key: "code" as const, label: t("graphFrame.viewCode"), Icon: CodeViewIcon, show: true, onClick: () => setView("code") },
          ]
        ).map(({ key, label, Icon, show, onClick }) =>
          show && (
            <ActionIcon
              key={key}
              variant="subtle"
              className={`gf-view-bar-btn ${activeView === key ? "active" : ""}`}
              onClick={onClick}
              title={label}
              aria-label={label}
              aria-current={activeView === key ? "true" : undefined}
              data-testid={`graph-frame-view-${key}-btn`}
            >
              <Icon size={15} />
            </ActionIcon>
          ),
        )}
      </div>
      <div className="gf-body-content">
      {frame.status === "loading" && (
        <div
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            padding: "2rem",
            gap: "0.5rem",
            color: "var(--text-muted)",
            fontSize: "0.85rem",
          }}
        >
          <Loader size="xs" />
          {t("graphFrame.running")}
        </div>
      )}
      {frame.status === "error" && (
        <div className="gf-error gf-error--copyable">
          <span className="gf-error-text">{frame.error}</span>
          <CopySymbolButton
            text={frame.error ?? ""}
            className="gf-error-copy-btn"
            title={t("graphFrame.copyErrorTitle")}
          />
        </div>
      )}
      {frame.status !== "error" && hasGraph && (
        <div
          className="gf-graph-area"
          style={{ flexBasis: graphAreaHeight, display: activeView === "graph" ? undefined : "none" }}
        >
          <GraphCanvas
            nodes={augmentedNodes}
            edges={frame.edges}
            overlayNodes={overlayNodes}
            overlayEdges={overlayEdges}
            onSelect={setSelected}
            colorOverrides={colorOverrides}
            sizeOverrides={sizeOverrides}
            labelProperty={labelProperty}
            sizeByProperty={sizeByProperty}
            sizeMultiplier={sizeMultiplier}
            relLineOverrides={relLineOverrides}
            onExcludeNode={handleExcludeNode}
            pkMap={pkMap}
            labelToTableLabel={labelToTableLabel}
            relationships={relationships ?? []}
            showingChildrenNatural={showingChildrenNatural}
            onToggleChildren={handleToggleChildren}
            onToggleChildrenBatch={handleToggleChildrenBatch}
            showingChildrenCircular={showingChildrenCircular}
            onToggleChildrenCircular={handleToggleChildrenCircular}
            showingParents={showingParents}
            onToggleParents={handleToggleParents}
            onToggleParentsBatch={handleToggleParentsBatch}
            showingParentsCircular={showingParentsCircular}
            onToggleParentsCircular={handleToggleParentsCircular}
            onCyReady={(cy) => {
              canvasCyRef.current = cy;
              // eslint-disable-next-line react-hooks/immutability -- intentional debug-only global handle set from a cytoscape ready callback, not during render
              if (typeof window !== "undefined") (window as unknown as Record<string, unknown>).__cy = cy;
            }}
            clusterLevel={clusterLevel}
            hullSvgRef={canvasHullSvgRef}
            isExpanded={expanded}
          />
          {!inspectorVisible && (
            <ActionIcon
              variant="subtle"
              className="gf-insp-show-btn"
              onClick={() => setInspectorVisible(true)}
              title={t("graphFrame.showPropertiesPanel")}
              aria-label={t("graphFrame.showPropertiesPanel")}
              data-testid="graph-frame-show-inspector-btn"
            >
              <span aria-hidden="true">‹</span>
            </ActionIcon>
          )}
          {inspectorVisible && (
            <Inspector
              selected={selected}
              graphStats={selected?.kind === "node" ? selected.graphStats : undefined}
              overviewData={overviewData}
              colorOverrides={colorOverrides}
              onColorChange={onColorChange}
              onClose={() => setInspectorVisible(false)}
              width={inspectorWidth}
              onResizeStart={handleResizeStart}
              relationships={relationships}
              onSaveEdgeAlias={onSaveEdgeAlias}
              pkMap={pkMap}
            />
          )}
        </div>
      )}
      {frame.status !== "error" && activeView === "table" && (
        <TableView
          columns={frame.columns}
          rows={frame.rows}
          wrap={tableWrap}
          colWidths={tableColWidths}
          setColWidths={setTableColWidths}
        />
      )}
      {frame.status !== "error" &&
        activeView === "json" &&
        (() => {
          const jsonStr = JSON.stringify(frame.rows, null, 2);
          return (
            <div className="gf-json-wrap">
              <CodeMirror
                className="gf-json-view"
                value={jsonStr}
                theme={oneDark}
                height={`${graphAreaHeight}px`}
                readOnly
                basicSetup={{ foldGutter: true, lineNumbers: true }}
                extensions={[jsonLang(), EditorView.lineWrapping]}
              />
              <JsonCopyButton text={jsonStr} />
            </div>
          );
        })()}
      {frame.status !== "error" && activeView === "graphstats" && hasGraph && (
        <GraphStatsPanel
          nodes={augmentedNodes}
          edges={overlayEdges.size > 0 ? new Map([...frame.edges, ...overlayEdges]) : frame.edges}
          queryStats={frame.queryStats}
        />
      )}
      {frame.status !== "error" && activeView === "code" && (() => {
        type SourceEntry = { field?: string; source?: string; strategy?: string; elapsed_ms?: number; rows?: number; physical_sql?: string; cache_hit?: boolean };
        const stats = frame.queryStats as { total_elapsed_ms?: number; sources?: SourceEntry[]; mermaid?: string } | undefined;
        const sqlQueries = (stats?.sources ?? [])
          .filter((s) => s.physical_sql)
          .map((s) => ({
            field: s.field,
            source: s.source,
            strategy: s.strategy,
            elapsed_ms: s.elapsed_ms,
            rows: s.rows,
            ...(s.cache_hit ? { cache_hit: true } : {}),
            sql: s.physical_sql,
          }));
        const codeData: Record<string, unknown> = {
          query: frame.query,
          summary: {
            nodes: frame.nodes.size,
            relationships: frame.edges.size,
            rows: frame.rows.length,
            columns: frame.columns,
            ...(frame.elapsed !== undefined ? { elapsed_ms: frame.elapsed } : {}),
          },
          ...(sqlQueries.length > 0 ? { sql: sqlQueries } : {}),
          ...(stats ? { stats: { total_elapsed_ms: stats.total_elapsed_ms, sources: (stats.sources ?? []).map(({ physical_sql: _, ...rest }) => rest) } } : {}),
        };
        const jsonStr = JSON.stringify(codeData, null, 2);
        return (
          <div className="gf-json-wrap">
            <CodeMirror
              className="gf-json-view"
              value={jsonStr}
              theme={oneDark}
              height={`${graphAreaHeight}px`}
              readOnly
              basicSetup={{ foldGutter: true, lineNumbers: true }}
              extensions={[jsonLang(), EditorView.lineWrapping]}
            />
            <JsonCopyButton text={jsonStr} />
          </div>
        );
      })()}
      </div>
    </div>
  );

  return (
    <>
      <div
        className={`gf-frame${expanded ? " gf-expanded" : ""}${dragOver ? " gf-frame--drag-over" : ""}`}
        style={expanded ? { top: `calc(2.5vh + ${modalHeaderHeight}px)`, height: `calc(95vh - ${modalHeaderHeight}px)` } : undefined}
        onDragOver={(e) => {
          if (
            e.dataTransfer.types.includes("text/x-provisa-label") ||
            e.dataTransfer.types.includes("text/x-provisa-domain")
          ) {
            e.preventDefault();
            setDragOver(true);
          }
        }}
        onDragLeave={() => setDragOver(false)}
        onDrop={(e) => {
          setDragOver(false);
          const label = e.dataTransfer.getData("text/x-provisa-label");
          if (label && onTableDrop) {
            e.preventDefault();
            onTableDrop(frame.id, label);
            return;
          }
          const domain = e.dataTransfer.getData("text/x-provisa-domain");
          if (domain && onDomainDrop) {
            e.preventDefault();
            onDomainDrop(frame.id, domain);
          }
        }}
      >
        {renderHeader(false)}
        {!collapsed && frameBody}
        {!collapsed && !expanded && (
          <div className="gf-frame-resize-handle" onMouseDown={handleFrameResizeStart} />
        )}
      </div>
      {expanded &&
        createPortal(
          <div
            className="gf-modal-overlay"
            onClick={() => setExpanded(false)}
            onKeyDown={(e) => {
              if (e.key === "Escape") setExpanded(false);
            }}
          >
            <div
              className="gf-modal-frame"
              role="dialog"
              aria-modal="true"
              aria-label={t("graphFrame.expandedDialogLabel")}
              onClick={(e) => e.stopPropagation()}
              ref={(el) => {
                if (!el) return;
                const ro = new ResizeObserver(() => setModalHeaderHeight(el.offsetHeight));
                ro.observe(el);
                return () => ro.disconnect();
              }}
            >
              {renderHeader(true)}
            </div>
          </div>,
          document.body,
        )}
    </>
  );
}
