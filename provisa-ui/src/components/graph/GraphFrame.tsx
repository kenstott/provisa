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
import { tableLabel as dbTableLabel } from "../../naming";
import { useLocalStorage } from "./graph-persistence";

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
  onEffectiveDataChange,
}: GraphFrameProps) {
  const [view, setView] = useState<"graph" | "table" | "json" | "graphstats">("graph");
  const [selected, setSelectedRaw] = useState<
    { kind: "node"; data: GNode; graphStats?: GraphStats } | { kind: "edge"; data: GEdge } | null
  >(null);
  const setSelected = useCallback(
    (s: { kind: "node"; data: GNode; graphStats?: GraphStats } | { kind: "edge"; data: GEdge } | null) => {
      setSelectedRaw(s);
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
  /* eslint-disable react-hooks/set-state-in-effect -- sync the editable query buffer to the frame.query prop when the frame is re-run or replaced externally */
  useEffect(() => {
    setEditQuery(frame.query);
  }, [frame.query]);
  /* eslint-enable react-hooks/set-state-in-effect */
  const [overlayData, setOverlayData] = useState<
    Map<string, { nodes: Map<string, GNode>; edges: Map<string, GEdge> }>
  >(new Map());
  if (typeof window !== "undefined") (window as unknown as Record<string, unknown>).__overlayData = overlayData;
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
  const [dlMenuPos, setDlMenuPos] = useState<{ top: number; right: number } | null>(null);
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

  const _resolveNodeForKey = useCallback(
    (nodeKey: string): GNode | undefined => {
      let gNode: GNode | undefined = frame.nodes.get(nodeKey);
      if (!gNode) {
        for (const d of overlayData.values()) {
          gNode = d.nodes.get(nodeKey);
          if (gNode) break;
        }
      }
      return gNode;
    },
    [frame.nodes, overlayData],
  );

  const _fetchNeighbors = useCallback(
    async (
      cypherQuery: string,
    ): Promise<{ nodes: Map<string, GNode>; edges: Map<string, GEdge> } | null> => {
      const res = await fetch("/data/cypher", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ query: cypherQuery, params: {} }),
      });
      if (!res.ok) {
        const text = await res.text().catch(() => "");
        let err: unknown;
        try {
          err = JSON.parse(text);
        } catch {
          err = text;
        }
        console.error("show neighbors query failed (HTTP", res.status, "):", err);
        return null;
      }
      const data = await res.json();
      const rows: Record<string, unknown>[] = data.rows ?? [];
      return extractElements(rows);
    },
    [],
  );

  type MergedOverlay = { nodes: Map<string, GNode>; edges: Map<string, GEdge> };

  const _fetchChildrenForNode = useCallback(
    async (nodeKey: string): Promise<MergedOverlay | null> => {
      const gNode = _resolveNodeForKey(nodeKey);
      if (!gNode) return null;
      const tableLabel = gNode.tableLabel;
      const pkCols = pkMap[gNode.label] ?? [];
      const pkCol = pkCols[0] ?? null;
      const pkValue = gNode.properties[pkCol ?? ""] ?? gNode.id;
      const pkLit =
        pkValue === null || pkValue === undefined
          ? null
          : isNaN(Number(pkValue))
            ? `'${String(pkValue).replace(/'/g, "\\'")}'`
            : String(pkValue);
      if (!pkLit || !pkCol) return null;
      const rels = (relationships ?? []).filter((r) => dbTableLabel(r.sourceTableName) === tableLabel);
      const myPkKey = pkCols.join(",");
      const siblingSourceRels =
        rels.length === 0
          ? (() => {
              const siblingLabels = Object.entries(pkMap)
                .filter(
                  ([lbl, cols]) =>
                    cols.join(",") === myPkKey && lbl !== gNode.label,
                )
                .map(([lbl]) => labelToTableLabel[lbl] ?? lbl);
              return (relationships ?? []).filter((r) =>
                siblingLabels.includes(dbTableLabel(r.sourceTableName)),
              );
            })()
          : null;
      const effectiveRels = rels.length > 0 ? rels : (siblingSourceRels ?? []);
      const effectiveLabel =
        rels.length > 0
          ? tableLabel
          : (siblingSourceRels && siblingSourceRels.length > 0
              ? dbTableLabel(siblingSourceRels[0].sourceTableName)
              : tableLabel);
      if (effectiveRels.length === 0) return null;
      const merged: MergedOverlay = { nodes: new Map(), edges: new Map() };
      await Promise.all(
        effectiveRels.map(async (r) => {
          const relType = (r.alias ?? r.computedCypherAlias ?? "").toUpperCase();
          const q = `MATCH (n:${effectiveLabel})-[r:${relType}]->(child) WHERE n.${pkCol} = ${pkLit} RETURN n, r, child`;
          const result = await _fetchNeighbors(q);
          if (result) {
            result.nodes.forEach((n, k) => merged.nodes.set(k, n));
            result.edges.forEach((e, k) => merged.edges.set(k, e));
          }
        }),
      );
      if (siblingSourceRels && siblingSourceRels.length > 0) {
        const sibNodeKey = [...merged.nodes.keys()].find((k) => {
          const n = merged.nodes.get(k)!;
          return String(n.id) === String(pkValue) && n.label !== gNode.label;
        });
        if (sibNodeKey) {
          merged.nodes.delete(sibNodeKey);
          merged.edges.forEach((edge) => {
            if (`${edge.startNode.label}:${edge.startNode.id}` === sibNodeKey)
              edge.startNode = gNode;
          });
        }
      }
      return merged.nodes.size > 0 || merged.edges.size > 0 ? merged : null;
    },
    [pkMap, relationships, _resolveNodeForKey, _fetchNeighbors],
  );

  const _fetchParentsForNode = useCallback(
    async (nodeKey: string): Promise<MergedOverlay | null> => {
      const gNode = _resolveNodeForKey(nodeKey);
      if (!gNode) return null;
      const tableLabel = gNode.tableLabel;
      const pkCols = pkMap[gNode.label] ?? [];
      const pkCol = pkCols[0] ?? null;
      const pkValue = gNode.properties[pkCol ?? ""] ?? gNode.id;
      const pkLit =
        pkValue === null || pkValue === undefined
          ? null
          : isNaN(Number(pkValue))
            ? `'${String(pkValue).replace(/'/g, "\\'")}'`
            : String(pkValue);
      if (!pkLit || !pkCol) return null;
      const rels = (relationships ?? []).filter((r) => dbTableLabel(r.targetTableName) === tableLabel);
      const myPkKey = pkCols.join(",");
      const siblingTargetRels =
        rels.length === 0
          ? (() => {
              const siblingLabels = Object.entries(pkMap)
                .filter(
                  ([lbl, cols]) =>
                    cols.join(",") === myPkKey && lbl !== gNode.label,
                )
                .map(([lbl]) => labelToTableLabel[lbl] ?? lbl);
              return (relationships ?? []).filter((r) =>
                siblingLabels.includes(dbTableLabel(r.targetTableName)),
              );
            })()
          : null;
      const effectiveRels = rels.length > 0 ? rels : (siblingTargetRels ?? []);
      const effectiveLabel =
        rels.length > 0
          ? tableLabel
          : (siblingTargetRels && siblingTargetRels.length > 0
              ? dbTableLabel(siblingTargetRels[0].targetTableName)
              : tableLabel);
      if (effectiveRels.length === 0) return null;
      const merged: MergedOverlay = { nodes: new Map(), edges: new Map() };
      await Promise.all(
        effectiveRels.map(async (r) => {
          const relType = (r.alias ?? r.computedCypherAlias ?? "").toUpperCase();
          const q = `MATCH (parent)-[r:${relType}]->(n:${effectiveLabel}) WHERE n.${pkCol} = ${pkLit} RETURN n, r, parent`;
          const result = await _fetchNeighbors(q);
          if (result) {
            result.nodes.forEach((n, k) => merged.nodes.set(k, n));
            result.edges.forEach((e, k) => merged.edges.set(k, e));
          }
        }),
      );
      if (siblingTargetRels && siblingTargetRels.length > 0) {
        const sibNodeKey = [...merged.nodes.keys()].find((k) => {
          const n = merged.nodes.get(k)!;
          return String(n.id) === String(pkValue) && n.label !== gNode.label;
        });
        if (sibNodeKey) {
          merged.nodes.delete(sibNodeKey);
          merged.edges.forEach((edge) => {
            if (`${edge.endNode.label}:${edge.endNode.id}` === sibNodeKey) edge.endNode = gNode;
          });
        }
      }
      return merged.nodes.size > 0 || merged.edges.size > 0 ? merged : null;
    },
    [pkMap, relationships, _resolveNodeForKey, _fetchNeighbors],
  );

  const handleToggleChildren = useCallback(
    async (nodeKey: string) => {
      const overlayKey = `${nodeKey}:children`;
      if (overlayData.has(overlayKey)) {
        setOverlayData((prev) => {
          const next = new Map(prev);
          next.delete(overlayKey);
          return next;
        });
        return;
      }
      const merged = await _fetchChildrenForNode(nodeKey);
      if (merged) setOverlayData((prev) => new Map(prev).set(overlayKey, merged));
    },
    [overlayData, _fetchChildrenForNode],
  );

  const handleToggleChildrenCircular = useCallback(
    async (nodeKey: string) => {
      const overlayKey = `${nodeKey}:children:circular`;
      if (overlayData.has(overlayKey)) {
        setOverlayData((prev) => {
          const next = new Map(prev);
          next.delete(overlayKey);
          return next;
        });
        return;
      }
      const merged = await _fetchChildrenForNode(nodeKey);
      if (merged) setOverlayData((prev) => new Map(prev).set(overlayKey, merged));
    },
    [overlayData, _fetchChildrenForNode],
  );

  const handleToggleChildrenBatch = useCallback(
    async (nodeKeys: string[], circular = false) => {
      const suffix = circular ? ":children:circular" : ":children";
      const toRemove = nodeKeys.filter((id) => overlayData.has(`${id}${suffix}`));
      const toAdd = nodeKeys.filter((id) => !overlayData.has(`${id}${suffix}`));
      if (toAdd.length === 0) {
        setOverlayData((prev) => {
          const next = new Map(prev);
          toRemove.forEach((id) => next.delete(`${id}${suffix}`));
          return next;
        });
        return;
      }
      // All nodes fetched in parallel off-screen; single setOverlayData call renders them all at once.
      const results = await Promise.all(toAdd.map((id) => _fetchChildrenForNode(id)));
      setOverlayData((prev) => {
        const next = new Map(prev);
        toAdd.forEach((id, i) => {
          if (results[i]) next.set(`${id}${suffix}`, results[i]!);
        });
        return next;
      });
    },
    [overlayData, _fetchChildrenForNode],
  );

  const handleToggleParents = useCallback(
    async (nodeKey: string) => {
      const overlayKey = `${nodeKey}:parents`;
      if (overlayData.has(overlayKey)) {
        setOverlayData((prev) => {
          const next = new Map(prev);
          next.delete(overlayKey);
          return next;
        });
        return;
      }
      const merged = await _fetchParentsForNode(nodeKey);
      if (merged) setOverlayData((prev) => new Map(prev).set(overlayKey, merged));
    },
    [overlayData, _fetchParentsForNode],
  );

  const handleToggleParentsCircular = useCallback(
    async (nodeKey: string) => {
      const overlayKey = `${nodeKey}:parents:circular`;
      if (overlayData.has(overlayKey)) {
        setOverlayData((prev) => {
          const next = new Map(prev);
          next.delete(overlayKey);
          return next;
        });
        return;
      }
      const merged = await _fetchParentsForNode(nodeKey);
      if (merged) setOverlayData((prev) => new Map(prev).set(overlayKey, merged));
    },
    [overlayData, _fetchParentsForNode],
  );

  const handleToggleParentsBatch = useCallback(
    async (nodeKeys: string[], circular = false) => {
      const suffix = circular ? ":parents:circular" : ":parents";
      const toRemove = nodeKeys.filter((id) => overlayData.has(`${id}${suffix}`));
      const toAdd = nodeKeys.filter((id) => !overlayData.has(`${id}${suffix}`));
      if (toAdd.length === 0) {
        setOverlayData((prev) => {
          const next = new Map(prev);
          toRemove.forEach((id) => next.delete(`${id}${suffix}`));
          return next;
        });
        return;
      }
      const results = await Promise.all(toAdd.map((id) => _fetchParentsForNode(id)));
      setOverlayData((prev) => {
        const next = new Map(prev);
        toAdd.forEach((id, i) => {
          if (results[i]) next.set(`${id}${suffix}`, results[i]!);
        });
        return next;
      });
    },
    [overlayData, _fetchParentsForNode],
  );

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
      const degree_centrality = totalNodes > 1 ? parseFloat((deg / (totalNodes - 1)).toFixed(4)) : 0;
      result.set(k, { ...n, properties: { ...n.properties, deg_in: i, deg_out: o, deg_total: deg, degree_centrality } });
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
    const SKIP = new Set(["scl1", "scl2", "scl3", "l1Cluster", "l2Cluster", "l3Cluster", "deg_in", "deg_out", "deg_total"]);
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
    for (const key of ["deg_in", "deg_out", "deg_total"] as const) {
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
  const activeView: "graph" | "table" | "json" | "graphstats" = hasGraph
    ? view
    : view === "json"
      ? "json"
      : view === "graphstats"
        ? "graphstats"
        : "table";

  const renderHeader = (isModal: boolean) => (
    <div className="gf-header">
      <div className="gf-query-editor-wrap">
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
          onUpdate={(vu) => { if (vu.docChanged) vu.view.requestMeasure(); }}
          basicSetup={{ lineNumbers: false, foldGutter: false, highlightActiveLine: false }}
        />
        <CopySymbolButton text={editQuery} className="gf-copy-query-btn" title="Copy query" />
      </div>
      <div className="gf-header-right">
        <div className="gf-header-top">
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
          {!isModal && (
            <button className="gf-icon-btn" onClick={() => setExpanded(true)} title="Expand">
              ⤢
            </button>
          )}
          {isModal && (
            <button
              className="gf-icon-btn"
              onClick={() => setExpanded(false)}
              title="Exit full screen"
            >
              ⤡
            </button>
          )}
          {!isModal && (
            <button
              className="gf-icon-btn"
              onClick={() => setCollapsed((c) => !c)}
              title={collapsed ? "Expand" : "Collapse"}
            >
              {collapsed ? "▼" : "▲"}
            </button>
          )}
          {!isModal && (
            <button className="gf-icon-btn" onClick={() => onClose(frame.id)} title="Close">
              ✕
            </button>
          )}
        </div>
        <div className="gf-header-actions">
        <button
          className="gf-run-inline-btn"
          onClick={() => handleRerun(frame.id, editQuery.trim())}
          title="Run"
        >
          ▶
        </button>
        {hasGraph && frame.status === "done" && (
          <button
            className={`gf-icon-btn${autoImpute ? " gf-icon-btn--on" : ""}`}
            onClick={() => setAutoImpute((v) => !v)}
            title={
              autoImpute
                ? "Auto-impute relationships ON — click to disable"
                : "Auto-impute relationships between visible nodes"
            }
          >
            ⊕
          </button>
        )}
        {hasGraph && frame.status === "done" && groupableAttrs.length > 0 && (
          <select
            className={`gf-attr-select${groupableAttrs.includes(clusterLevel) ? " gf-icon-btn--on" : ""}`}
            value={groupableAttrs.includes(clusterLevel) ? clusterLevel : ""}
            onChange={(e) => setClusterLevel(e.target.value || "none")}
            title="Group nodes by attribute (double-click a hull to collapse; double-click collapsed node to expand)"
          >
            <option value="">⬡ group</option>
            {groupableAttrs.map((a) => (
              <option key={a} value={a}>
                {a}
              </option>
            ))}
          </select>
        )}
        {hasGraph && (
          <button
            className={`gf-view-btn ${activeView === "graph" ? "active" : ""}`}
            onClick={() => setView("graph")}
            title="Graph"
          >
            ✦
          </button>
        )}
        <button
          className={`gf-view-btn ${activeView === "table" ? "active" : ""}`}
          onClick={() => setView("table")}
          title="Table"
        >
          ⊞
        </button>
        <button
          className={`gf-view-btn ${activeView === "json" ? "active" : ""}`}
          onClick={() => setView("json")}
          title="JSON"
        >
          {"{}"}
        </button>
        {hasGraph && (
          <button
            className={`gf-view-btn ${activeView === "graphstats" ? "active" : ""}`}
            onClick={() => setView(activeView === "graphstats" ? "graph" : "graphstats")}
            title="Graph statistics"
          >
            ∑
          </button>
        )}
        {activeView === "table" && frame.rows.length > 0 && (
          <button
            className={`gf-icon-btn${tableWrap ? " gf-icon-btn--on" : ""}`}
            title="Wrap cell text"
            onClick={() => setTableWrap((v) => !v)}
          >
            ⇌
          </button>
        )}
        {frame.status === "done" && (frame.rows.length > 0 || hasGraph) && (
          <div className="gf-dl-wrap">
            <button
              className="gf-icon-btn"
              title="Download"
              onClick={(e) => {
                const rect = e.currentTarget.getBoundingClientRect();
                setDlMenuPos({ top: rect.bottom + 4, right: window.innerWidth - rect.right });
                setShowDlMenu((v) => !v);
              }}
            >
              <svg
                width="14"
                height="14"
                viewBox="0 0 16 16"
                fill="currentColor"
                xmlns="http://www.w3.org/2000/svg"
              >
                <path d="M8 10.5L4.5 7h2V2h3v5h2L8 10.5z" />
                <rect x="2" y="12" width="12" height="1.5" rx="0.75" />
              </svg>
            </button>
            {showDlMenu && dlMenuPos && createPortal(
              <div
                className="gf-dl-menu"
                style={{ position: "fixed", top: dlMenuPos.top, right: dlMenuPos.right, left: "unset" }}
                onMouseLeave={() => setShowDlMenu(false)}
              >
                {frame.rows.length > 0 && (
                  <button
                    className="gf-dl-item"
                    onClick={() => {
                      const json = JSON.stringify(frame.rows, null, 2);
                      downloadBlob(new Blob([json], { type: "application/json" }), "result.json");
                      setShowDlMenu(false);
                    }}
                  >
                    JSON
                  </button>
                )}
                {frame.rows.length > 0 && (
                  <button
                    className="gf-dl-item"
                    onClick={() => {
                      const csv = toCSV(frame.columns, frame.rows);
                      downloadBlob(new Blob([csv], { type: "text/csv" }), "result.csv");
                      setShowDlMenu(false);
                    }}
                  >
                    CSV
                  </button>
                )}
                {hasGraph && activeView === "graph" && (
                  <button
                    className="gf-dl-item"
                    onClick={() => {
                      const cy = canvasCyRef.current;
                      if (!cy) return;
                      compositeGraphDownload(cy, canvasHullSvgRef.current, "graph.png", "png");
                      setShowDlMenu(false);
                    }}
                  >
                    PNG
                  </button>
                )}
                {hasGraph && activeView === "graph" && (
                  <button
                    className="gf-dl-item"
                    onClick={() => {
                      const cy = canvasCyRef.current;
                      if (!cy) return;
                      compositeGraphDownload(cy, canvasHullSvgRef.current, "graph.jpg", "jpg");
                      setShowDlMenu(false);
                    }}
                  >
                    JPG
                  </button>
                )}
                {hasGraph && activeView === "graph" && (
                  <button
                    className="gf-dl-item"
                    onClick={() => {
                      const cy = canvasCyRef.current;
                      if (!cy) return;
                      downloadGraphSvg(cy, canvasHullSvgRef.current);
                      setShowDlMenu(false);
                    }}
                  >
                    SVG
                  </button>
                )}
              </div>,
              document.body,
            )}
          </div>
        )}
        </div>
      </div>
    </div>
  );

  const frameBody = (
    <div className="gf-body">
      {frame.status === "error" && (
        <div className="gf-error gf-error--copyable">
          <span className="gf-error-text">{frame.error}</span>
          <CopySymbolButton
            text={frame.error ?? ""}
            className="gf-error-copy-btn"
            title="Copy error"
          />
        </div>
      )}
      {frame.status !== "error" && hasGraph && (
        <div
          className="gf-graph-area"
          style={{ height: graphAreaHeight, display: activeView === "graph" ? undefined : "none" }}
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
              if (typeof window !== "undefined") (window as unknown as Record<string, unknown>).__cy = cy;
            }}
            clusterLevel={clusterLevel}
            hullSvgRef={canvasHullSvgRef}
          />
          <Inspector
            selected={selected}
            graphStats={selected?.kind === "node" ? selected.graphStats : undefined}
            colorOverrides={colorOverrides}
            onColorChange={onColorChange}
            onClose={() => setSelected(null)}
            width={inspectorWidth}
            onResizeStart={handleResizeStart}
            relationships={relationships}
            onSaveEdgeAlias={onSaveEdgeAlias}
            pkMap={pkMap}
          />
        </div>
      )}
      {frame.status !== "error" && activeView === "table" && (
        <TableView
          columns={frame.columns}
          rows={frame.rows}
          wrap={tableWrap}
          height={graphAreaHeight}
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
          height={graphAreaHeight}
        />
      )}
    </div>
  );

  return (
    <>
      <div
        className={`gf-frame${expanded ? " gf-expanded" : ""}${dragOver ? " gf-frame--drag-over" : ""}`}
        style={expanded ? { top: `calc(5vh + ${modalHeaderHeight}px)`, height: `calc(90vh - ${modalHeaderHeight}px)` } : undefined}
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
          <div className="gf-modal-overlay" onClick={() => setExpanded(false)}>
            <div
              className="gf-modal-frame"
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
