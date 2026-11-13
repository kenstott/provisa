// Copyright (c) 2026 Kenneth Stott
// Canary: 11557514-6a05-4a67-ae76-7227c880597f
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useCallback, useEffect, useLayoutEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { DEFAULT_THEME, Menu, useComputedColorScheme } from "@mantine/core";
import { Check, Copy, Download, MoveHorizontal } from "lucide-react";
import { theme } from "../theme/theme";
import { Tip } from "./Tip";
import { downloadBlob } from "./graph/graph-export";
import { FitScreenIcon, ZoomInIcon, ZoomOutIcon } from "./graph/GraphIcons";
import {
  CanvasButton,
  CanvasControls,
  CONTROL_ICON,
  FullscreenButton,
} from "./provisa-graph/CanvasControls";
import { graphTokens, type GraphTokens } from "./provisa-graph/tokens";
import { FIT_PADDING } from "./provisa-graph/viewport";
import {
  centredOn,
  CHART_GAP,
  HINT_BAND,
  fitWhole,
  fitWidth,
  openingView,
  scrollView,
  zoomAt,
  type Drawing,
  type Size,
  type View,
} from "./mermaidFit";
import "./provisa-graph/provisa-graph.css";
import "./MermaidDiagram.css";

let _mermaidDagSeq = 0;

const ZOOM_STEP = 1.3;
/** How far a wheel step (or a pinch) zooms, per pixel of its delta: the one graph's. */
const WHEEL_ZOOM = 0.002;
/** A press that moves less than this is a click, not a drag. */
const DRAG_SLOP = 3;
/** A canvas sized by its diagram is never shorter or taller than these. */
const AUTO_MIN_HEIGHT = 140;
const AUTO_MAX_HEIGHT = 480;

/** The Mermaid theme drawn from the one graph's tokens, so a plan reads like every other diagram
 * in either colour scheme. Labels are SVG text (not HTML), so the image can be downloaded. */
function mermaidConfig(tokens: GraphTokens) {
  return {
    startOnLoad: false,
    // suppressErrorRendering: a failed render throws and removes its scratch element instead
    // of drawing Mermaid's error graphic on the page body.
    suppressErrorRendering: true,
    theme: "base" as const,
    themeVariables: {
      background: tokens.canvas,
      primaryColor: tokens.nodeFill,
      primaryBorderColor: tokens.nodeBorder,
      primaryTextColor: tokens.text,
      secondaryColor: tokens.nodeFill,
      secondaryBorderColor: tokens.nodeBorder,
      secondaryTextColor: tokens.text,
      tertiaryColor: tokens.canvas,
      tertiaryBorderColor: tokens.nodeBorder,
      tertiaryTextColor: tokens.text,
      lineColor: tokens.edge,
      textColor: tokens.text,
      mainBkg: tokens.nodeFill,
      nodeBorder: tokens.nodeBorder,
      edgeLabelBackground: tokens.canvas,
      clusterBkg: tokens.canvas,
      clusterBorder: tokens.nodeBorder,
      titleColor: tokens.text,
      fontFamily: theme.fontFamily ?? DEFAULT_THEME.fontFamily,
      fontSize: "14px",
    },
    flowchart: { htmlLabels: false },
  };
}

/** Mermaid names a node's element `<diagram id>-flowchart-<node id>-<n>`. */
const NODE_ID = /-flowchart-(.+)-\d+$/;

/** Where Mermaid placed a node: the translate of its group. */
function translateOf(g: Element): { x: number; y: number } | null {
  const m = /translate\(\s*(-?[\d.]+)[\s,]+(-?[\d.]+)\s*\)/.exec(g.getAttribute("transform") ?? "");
  return m ? { x: Number(m[1]), y: Number(m[2]) } : null;
}

/** The charts drawn, stacked top to bottom at their natural size: their size together and where
 * each node is, in drawing px. */
function measure(box: HTMLElement): Drawing {
  const nodes = new Map<string, { x: number; y: number }>();
  let width = 0;
  let top = 0;
  box.querySelectorAll<SVGSVGElement>(":scope > svg").forEach((svg, i) => {
    const vb = (svg.getAttribute("viewBox") ?? "").trim().split(/\s+/).map(Number);
    if (vb.length !== 4 || !(vb[2] > 0) || !(vb[3] > 0)) {
      throw new Error("Mermaid SVG has no viewBox size");
    }
    const [vx, vy, vw, vh] = vb;
    if (i > 0) top += CHART_GAP;
    svg.setAttribute("width", String(vw));
    svg.setAttribute("height", String(vh));
    svg.style.maxWidth = "none";
    svg.style.marginTop = i > 0 ? `${CHART_GAP}px` : "0";
    svg.querySelectorAll("g.node").forEach((g) => {
      const key = NODE_ID.exec(g.id)?.[1];
      const at = translateOf(g);
      if (key && at && !nodes.has(key)) nodes.set(key, { x: at.x - vx, y: at.y - vy + top });
    });
    width = Math.max(width, vw);
    top += vh;
  });
  return { width, height: top, nodes };
}

/** The charts as one image: each placed where it is drawn, on the canvas colour. */
function imageSvg(box: HTMLElement, drawing: Drawing, bg: string): string {
  const pad = FIT_PADDING;
  const w = drawing.width + 2 * pad;
  const h = drawing.height + 2 * pad;
  const ns = "http://www.w3.org/2000/svg";
  const out = document.createElementNS(ns, "svg");
  out.setAttribute("xmlns", ns);
  out.setAttribute("width", String(w));
  out.setAttribute("height", String(h));
  out.setAttribute("viewBox", `0 0 ${w} ${h}`);
  const rect = document.createElementNS(ns, "rect");
  rect.setAttribute("width", String(w));
  rect.setAttribute("height", String(h));
  rect.setAttribute("fill", bg);
  out.appendChild(rect);
  let top = pad;
  box.querySelectorAll<SVGSVGElement>(":scope > svg").forEach((svg) => {
    const copy = svg.cloneNode(true) as SVGSVGElement;
    copy.removeAttribute("style");
    copy
      .querySelectorAll(".plan-node-selected")
      .forEach((g) => g.classList.remove("plan-node-selected"));
    copy.setAttribute("x", String(pad));
    copy.setAttribute("y", String(top));
    out.appendChild(copy);
    top += Number(svg.getAttribute("height")) + CHART_GAP;
  });
  return new XMLSerializer().serializeToString(out);
}

function downloadRaster(svgText: string, mime: string, name: string): void {
  const url = URL.createObjectURL(new Blob([svgText], { type: "image/svg+xml" }));
  const img = new Image();
  img.onload = () => {
    const canvas = document.createElement("canvas");
    canvas.width = img.width;
    canvas.height = img.height;
    canvas.getContext("2d")!.drawImage(img, 0, 0);
    URL.revokeObjectURL(url);
    canvas.toBlob((b) => b && downloadBlob(b, name), mime, 0.92);
  };
  img.src = url;
}

export interface MermaidDiagramProps {
  /** One Mermaid flowchart, or several joined by a blank line (a batch's statements). */
  chart: string;
  /** The node marked (a Mermaid node id): brought into view when it changes. */
  selected?: string | null;
  /** A node picked; its Mermaid node id. Nodes are clickable only when given. */
  onNodeClick?: (key: string) => void;
  /** The canvas's CSS height; by default as tall as the opening view needs, within limits. */
  height?: string;
  /** The file name of a download, without its extension. */
  exportName?: string;
}

/** The query plan and query stats diagram (provisa_stats.mermaid, the analyze plan), drawn by
 * Mermaid in the canvas every diagram shares: the hover controls in its corner (zoom, fit the whole
 * image, fit the width, full screen, download, copy), drag to pan, scroll to move, Ctrl/Cmd-scroll
 * to zoom. It opens at the readable zoom with the plan's root in view (REQ-2239).
 *
 * Every chart is parsed before any is drawn. A chart Mermaid cannot read is reported in the
 * diagram's own place, with Mermaid's message, instead of Mermaid's error graphic (which it
 * appends to the page body, outside this component). */
export function MermaidDiagram({
  chart,
  selected = null,
  onNodeClick,
  height,
  exportName = "query-plan",
}: MermaidDiagramProps) {
  const { t } = useTranslation();
  const isDark = useComputedColorScheme("dark") === "dark";
  const tokens = graphTokens(isDark);
  const stageRef = useRef<HTMLDivElement>(null);
  const drawRef = useRef<HTMLDivElement>(null);
  const idPrefixRef = useRef(`mermaid-dag-${++_mermaidDagSeq}`);
  // Keyed by the chart it belongs to, so a new chart never shows the last one's error.
  const [failure, setFailure] = useState<{ chart: string; message: string } | null>(null);
  const error = failure?.chart === chart ? failure.message : null;
  const [drawing, setDrawing] = useState<Drawing | null>(null);
  const [view, setView] = useState<View | null>(null);
  const [autoHeight, setAutoHeight] = useState<number | null>(null);
  // The canvas's size as last laid out: what a selection is brought into view within.
  const [size, setSize] = useState<Size | null>(null);
  const viewRef = useRef<View | null>(null);
  const drawingRef = useRef<Drawing | null>(null);
  useLayoutEffect(() => {
    viewRef.current = view;
    drawingRef.current = drawing;
  });

  const stageSize = useCallback((): Size => {
    const el = stageRef.current;
    return el ? { width: el.clientWidth, height: el.clientHeight } : { width: 0, height: 0 };
  }, []);

  // The opening view, once the chart is drawn and the canvas has a size: the readable zoom, the
  // root in view. A canvas sized by its diagram takes the height that view needs.
  const open = useCallback(
    (d: Drawing) => {
      const at = stageSize();
      if (at.width <= 0) return;
      let room = at;
      if (height === undefined) {
        const first = openingView(d, { width: at.width, height: Number.POSITIVE_INFINITY });
        const want = Math.round(d.height * first.zoom + HINT_BAND + FIT_PADDING);
        const h = Math.min(AUTO_MAX_HEIGHT, Math.max(AUTO_MIN_HEIGHT, want));
        setAutoHeight(h);
        room = { width: at.width, height: h };
      }
      if (room.height <= 0) return;
      setSize(room);
      setView(openingView(d, room));
    },
    [height, stageSize],
  );

  useEffect(() => {
    let cancelled = false;
    const charts = chart.split(/\n\n(?=flowchart)/).filter(Boolean);
    const ids = charts.map((_, i) => `${idPrefixRef.current}-${i}`);
    import("mermaid").then(async (m) => {
      if (cancelled || !drawRef.current) return;
      const mermaid = m.default;
      // The tokens follow the colour scheme: a change of scheme draws the chart again.
      mermaid.initialize(mermaidConfig(graphTokens(isDark)));
      try {
        for (const c of charts) await mermaid.parse(c);
        const svgs = await Promise.all(
          charts.map((c, i) => mermaid.render(ids[i], c).then(({ svg }) => svg)),
        );
        if (cancelled || !drawRef.current) return;
        drawRef.current.innerHTML = svgs.join("");
        const d = measure(drawRef.current);
        setView(null);
        setDrawing(d);
        open(d);
      } catch (e) {
        if (cancelled) return;
        if (drawRef.current) drawRef.current.innerHTML = "";
        setDrawing(null);
        setFailure({ chart, message: e instanceof Error ? e.message : String(e) });
      }
    });
    return () => {
      cancelled = true;
    };
  }, [chart, isDark, open]);

  // A canvas laid out later (a dialog opening, a tab shown) opens when it has its size; one
  // resized (full screen) keeps its view and brings a selection into view within its new size.
  useEffect(() => {
    const el = stageRef.current;
    if (!el) return;
    const ro = new ResizeObserver(() => {
      const d = drawingRef.current;
      if (!d) return;
      if (viewRef.current) setSize(stageSize());
      else open(d);
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, [open, stageSize]);

  // The selected node marked.
  useEffect(() => {
    const box = drawRef.current;
    if (!box || !drawing) return;
    box.querySelectorAll("g.node").forEach((g) => {
      const key = NODE_ID.exec(g.id)?.[1];
      g.classList.toggle("plan-node-selected", key !== undefined && key === selected);
      g.classList.toggle("plan-node-pickable", !!onNodeClick);
    });
  }, [drawing, selected, onNodeClick]);

  // A node selected from outside (a row of the operator list) is brought into view: only a change
  // of selection moves the view; the reader's own moves are kept.
  // The node last picked in the diagram itself: picked where the reader is looking, it moves no view.
  const [pickedHere, setPickedHere] = useState<string | null>(null);
  const [lastSelected, setLastSelected] = useState<string | null>(selected);
  if (selected !== lastSelected && drawing && view && size) {
    setLastSelected(selected);
    setPickedHere(null);
    const at = selected && selected !== pickedHere ? drawing.nodes.get(selected) : undefined;
    if (at) {
      const next = centredOn(view, at, size);
      if (next !== view) setView(next);
    }
  }

  // Scroll moves the drawing; Ctrl/Cmd-scroll (and a pinch) zooms about the pointer.
  useEffect(() => {
    const el = stageRef.current;
    if (!el || !drawing) return;
    const onWheel = (e: WheelEvent) => {
      const v = viewRef.current;
      if (!v) return;
      const size = stageSize();
      if (e.ctrlKey || e.metaKey) {
        e.preventDefault();
        const r = el.getBoundingClientRect();
        setView(
          zoomAt(v, Math.exp(-e.deltaY * WHEEL_ZOOM), {
            x: e.clientX - r.left,
            y: e.clientY - r.top,
          }),
        );
        return;
      }
      const moved = scrollView(v, drawing, size, e.deltaX, e.deltaY);
      // A drawing that fits its canvas does not move, and the wheel scrolls the page.
      if (moved !== v) {
        e.preventDefault();
        setView(moved);
      }
    };
    el.addEventListener("wheel", onWheel, { passive: false });
    return () => el.removeEventListener("wheel", onWheel);
  }, [drawing, stageSize]);

  // Drag to pan; a press that does not move is a click (on a node, a pick).
  const dragged = useRef(false);
  const onPointerDown = (e: React.PointerEvent) => {
    if (e.button !== 0 || !viewRef.current) return;
    if ((e.target as Element).closest(".pg-controls")) return;
    const start = { x: e.clientX, y: e.clientY, view: viewRef.current };
    dragged.current = false;
    const move = (ev: PointerEvent) => {
      const dx = ev.clientX - start.x;
      const dy = ev.clientY - start.y;
      if (!dragged.current && Math.hypot(dx, dy) < DRAG_SLOP) return;
      dragged.current = true;
      setView({ ...start.view, x: start.view.x + dx, y: start.view.y + dy });
    };
    const up = () => {
      window.removeEventListener("pointermove", move);
      window.removeEventListener("pointerup", up);
    };
    window.addEventListener("pointermove", move);
    window.addEventListener("pointerup", up);
  };
  const onClick = (e: React.MouseEvent) => {
    if (dragged.current) {
      dragged.current = false;
      return;
    }
    if (!onNodeClick) return;
    const g = (e.target as Element).closest("g.node");
    const key = g ? NODE_ID.exec(g.id)?.[1] : undefined;
    if (!key) return;
    // Picked where the reader is looking: the view stays as it is.
    setPickedHere(key);
    onNodeClick(key);
  };

  const apply = (f: (v: View) => View) => {
    if (viewRef.current) setView(f(viewRef.current));
  };
  const zoomBy = (factor: number) => {
    const s = stageSize();
    apply((v) => zoomAt(v, factor, { x: s.width / 2, y: s.height / 2 }));
  };
  const exportImage = (kind: "svg" | "png" | "jpeg") => {
    const box = drawRef.current;
    const d = drawingRef.current;
    if (!box || !d) return;
    const svg = imageSvg(box, d, graphTokens(isDark).canvas);
    if (kind === "svg") {
      downloadBlob(new Blob([svg], { type: "image/svg+xml" }), `${exportName}.svg`);
    } else if (kind === "png") {
      downloadRaster(svg, "image/png", `${exportName}.png`);
    } else {
      downloadRaster(svg, "image/jpeg", `${exportName}.jpg`);
    }
  };
  const exports = [
    { key: "svg", label: "SVG", hint: t("erdModal.downloadSvg") },
    { key: "png", label: "PNG", hint: t("erdModal.downloadPng") },
    { key: "jpeg", label: "JPEG", hint: t("erdModal.downloadJpeg") },
  ] as const;
  const [copied, setCopied] = useState(false);
  const copyChart = () => {
    void navigator.clipboard.writeText(chart).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    });
  };

  const sized = height !== undefined;
  return (
    <div
      className={sized ? "stats-mermaid stats-mermaid--sized" : "stats-mermaid"}
      style={sized ? { height } : undefined}
      data-testid="plan-diagram"
    >
      {error !== null && (
        <div className="stats-mermaid-error" role="alert" data-testid="mermaid-error">
          <div className="stats-mermaid-error-title">{t("mermaidDiagram.unavailable")}</div>
          <pre className="stats-mermaid-error-message">{error}</pre>
        </div>
      )}
      <div
        ref={stageRef}
        className="pg-stage plan-stage"
        hidden={error !== null}
        style={{
          height: sized ? undefined : `${autoHeight ?? AUTO_MIN_HEIGHT}px`,
          ["--pg-canvas" as string]: tokens.canvas,
          ["--plan-accent" as string]: tokens.accent,
        }}
        onPointerDown={onPointerDown}
        onClick={onClick}
        aria-label={t("mermaidDiagram.canvasLabel")}
        role="figure"
      >
        <div
          ref={drawRef}
          className="plan-drawing"
          data-testid="plan-diagram-view"
          data-zoom={view?.zoom}
          data-pan-x={view?.x}
          data-pan-y={view?.y}
          style={{
            transform: view ? `translate(${view.x}px, ${view.y}px) scale(${view.zoom})` : undefined,
            visibility: view ? "visible" : "hidden",
          }}
        />
        {drawing && (
          <div className="pg-corner">
            <div className="pg-hint">{t("provisaGraph.hint")}</div>
          </div>
        )}
        {drawing && (
          <CanvasControls testId="plan-diagram">
            <CanvasButton
              label={t("canvasControls.zoomIn")}
              icon={<ZoomInIcon size={CONTROL_ICON} />}
              onClick={() => zoomBy(ZOOM_STEP)}
              testId="plan-diagram-zoom-in"
            />
            <CanvasButton
              label={t("canvasControls.zoomOut")}
              icon={<ZoomOutIcon size={CONTROL_ICON} />}
              onClick={() => zoomBy(1 / ZOOM_STEP)}
              testId="plan-diagram-zoom-out"
            />
            <CanvasButton
              label={t("erdModal.fitAll")}
              icon={<FitScreenIcon size={CONTROL_ICON} />}
              onClick={() => apply(() => fitWhole(drawing, stageSize()))}
              testId="plan-diagram-fit-all"
            />
            <CanvasButton
              label={t("mermaidDiagram.fitWidth")}
              icon={<MoveHorizontal size={CONTROL_ICON} />}
              onClick={() => apply((v) => fitWidth(v, drawing, stageSize()))}
              testId="plan-diagram-fit-width"
            />
            <FullscreenButton stageRef={stageRef} testId="plan-diagram" />
            <Menu position="right-end" withinPortal={false} transitionProps={{ duration: 0 }}>
              <Menu.Target>
                <CanvasButton
                  label={t("erdModal.download")}
                  icon={<Download size={CONTROL_ICON} />}
                  testId="plan-diagram-download"
                />
              </Menu.Target>
              <Menu.Dropdown data-testid="plan-diagram-download-menu">
                {exports.map(({ key, label, hint }) => (
                  <Tip key={key} text={hint} position="right" inPlace>
                    <Menu.Item
                      onClick={() => exportImage(key)}
                      data-testid={`plan-diagram-export-${key}`}
                    >
                      {label}
                    </Menu.Item>
                  </Tip>
                ))}
              </Menu.Dropdown>
            </Menu>
            <CanvasButton
              label={copied ? t("graph3d.mermaidCopied") : t("graph3d.copyMermaid")}
              icon={copied ? <Check size={CONTROL_ICON} /> : <Copy size={CONTROL_ICON} />}
              onClick={copyChart}
              testId="plan-diagram-copy"
            />
            {copied && (
              <span className="pg-copied" role="status">
                {t("graph3d.mermaidCopied")}
              </span>
            )}
          </CanvasControls>
        )}
      </div>
    </div>
  );
}
