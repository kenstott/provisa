// Copyright (c) 2026 Kenneth Stott
// Canary: 9c4f1a62-7e3b-4d85-a2c0-6b8e3f1d7a49
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2174 (amended 2026-10-28; 2026-11-07): the graph's 2D view, the lane diagram (laneDiagram.ts)
// drawn by cytoscape and laid out by ELK's layered algorithm in fixed lane partitions, products to
// terms left to right; the term lane is one compound laid out top-down by its rank edges, its other
// links arcs drawn over it once placed. A term link is dashed and named by its type. A click picks a node (the detail panel), a hover
// shows its card, a "+N columns" node expands in place; the diagram copies as Mermaid text. A
// card holds the node's facts with links to their pages, and stays while the pointer is on it.

import { useCallback, useEffect, useLayoutEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { Button, useComputedColorScheme, type FloatingPosition } from "@mantine/core";
import { Check, Copy } from "lucide-react";
import cytoscape, { type EventObject } from "cytoscape";
import elkRaw from "cytoscape-elk";
import type { CyLayoutOptions } from "../graph/cytoscape-types";
import { cyCanvas } from "../graph/cyCanvas";
import type { CameraCommand } from "../../pages/tables/profile3d/Stage3D";
import type { GraphHover } from "./graphScene";
import { GraphLegend } from "./GraphStage";
import { hoverBody } from "./hoverBody";
import { MoreFacts } from "./nodeFacts";
import { TIP_MAX_WIDTH, TipSurface } from "../Tip";
import {
  LANES,
  connectedTo,
  laneMermaid,
  placedLaneOrder,
  stackTerms,
  tipPlacement,
  type DrawnGraph,
  type Lane,
  type LaneDiagram,
} from "./laneDiagram";
import { nodeLook } from "./nodeLook";

type CyExt = Parameters<typeof cytoscape.use>[0];
const elk = (elkRaw as { default?: CyExt }).default ?? (elkRaw as CyExt);
// cytoscape.use throws when an extension is registered twice (TermDag and LineageDag register
// the same ELK layout): a registered "elk" layout is the one wanted, so it is used as is.
const registered = (cytoscape as unknown as (type: string, name: string) => unknown)(
  "layout",
  "elk",
);
if (!registered) cytoscape.use(elk);

type CyCore = ReturnType<typeof cytoscape> & { resize(): void };

const FIT_PAD = 24;
// The legend's offset from the stage's foot (profile3d.css .p3d-legend) and a gap above it.
const LEGEND_GAP = 16;
// How long a card stays once the pointer leaves its node: time to reach the card's links.
const TIP_LINGER_MS = 300;
const NODE_H = 26;
const TERM_LANE = "lane:term";
// A node is as wide as its name: ELK lays it out by this size, before cytoscape draws any text.
const nodeWidth = (label: string) => Math.max(56, Math.ceil(label.length * 6.6) + 22);

const SHAPE: Record<Lane, string> = {
  term: "round-rectangle",
  column: "rectangle",
  table: "barrel",
  product: "round-hexagon",
};

/** What is hovered and the box the card sits beside on the stage: the node, or the point on a
 * line where the pointer is. */
interface TipAt {
  h: GraphHover;
  x: number;
  y: number;
  width: number;
  height: number;
}

/** The card's padding and border around its content (the tip surface's). */
const TIP_FRAME = 24;

/** The nodes and lines cytoscape has drawn, on the page: what a card should stay off. */
function drawnGraph(cy: CyCore, stage: DOMRect): DrawnGraph {
  const drawn: DrawnGraph = { boxes: [], lines: [] };
  const at = (p: cytoscape.Position) => ({ x: stage.left + p.x, y: stage.top + p.y });
  cy.elements().forEach((e) => {
    if (e.isNode()) {
      const n = e as unknown as cytoscape.NodeSingular;
      const kind = n.data("kind");
      if (kind !== "node" && kind !== "more") return;
      const b = n.renderedBoundingBox({});
      drawn.boxes.push({
        left: stage.left + b.x1,
        top: stage.top + b.y1,
        right: stage.left + b.x2,
        bottom: stage.top + b.y2,
      });
    } else {
      const k = e as unknown as cytoscape.EdgeSingular;
      drawn.lines.push([at(k.source().renderedPosition()), at(k.target().renderedPosition())]);
    }
  });
  return drawn;
}

/** A --p3d-* token's value on ``el`` (profile3d.css sets one per colour scheme). */
const token = (el: HTMLElement, name: string) =>
  getComputedStyle(el).getPropertyValue(`--p3d-${name}`).trim();

export function LaneStage({
  diagram,
  selected,
  setSelected,
  hover,
  setHover,
  onMore,
  command,
  testId,
  bare = false,
}: {
  diagram: LaneDiagram;
  selected: string | null;
  setSelected: (id: string) => void;
  hover: GraphHover | null;
  setHover: (h: GraphHover | null) => void;
  /** A "+N columns" node was clicked: its table's columns are drawn one by one. */
  onMore: (table: string) => void;
  command: CameraCommand | null;
  testId: string;
  /** A glance only (a card): no copy control or hint. */
  bare?: boolean;
}) {
  const { t } = useTranslation();
  const scheme = useComputedColorScheme("dark");
  const wrapRef = useRef<HTMLDivElement | null>(null);
  const boxRef = useRef<HTMLDivElement | null>(null);
  const legendRef = useRef<HTMLDivElement | null>(null);
  // The diagram is laid out above the legend, never under it: the canvas ends where the legend
  // begins (graph3d.css, --g3d-legend-h), and its resize refits the diagram.
  useLayoutEffect(() => {
    const legend = legendRef.current;
    const wrap = wrapRef.current;
    if (!legend || !wrap) return;
    const clear = () =>
      wrap.style.setProperty("--g3d-legend-h", `${legend.offsetHeight + LEGEND_GAP}px`);
    clear();
    const observer = new ResizeObserver(clear);
    observer.observe(legend);
    return () => observer.disconnect();
  }, []);
  const cyRef = useRef<CyCore | null>(null);
  const [tip, setTip] = useState<TipAt | null>(null);
  const [tipEl, setTipEl] = useState<HTMLDivElement | null>(null);
  const [tipSide, setTipSide] = useState<FloatingPosition>("right-start");
  const [copied, setCopied] = useState(false);
  // The cytoscape effect binds these once per diagram; the ref holds the latest callbacks.
  const latest = useRef({ setSelected, setHover, onMore });
  useEffect(() => {
    latest.current = { setSelected, setHover, onMore };
  });
  // The pending hide of the card, cancelled while the pointer is on the card.
  const linger = useRef<ReturnType<typeof setTimeout> | null>(null);
  const keepTip = useCallback(() => {
    if (linger.current !== null) clearTimeout(linger.current);
    linger.current = null;
  }, []);
  const dropTip = useCallback(() => {
    if (linger.current !== null) clearTimeout(linger.current);
    linger.current = null;
    setTip(null);
    latest.current.setHover(null);
  }, []);
  const laneTitle = useCallback((l: Lane) => t(`graph3d.lane_${l}`), [t]);

  useEffect(() => {
    const box = boxRef.current as HTMLDivElement;
    const wrap = wrapRef.current as HTMLDivElement;
    // Text and lines in the stage's own text colour, as its legend and hint are drawn.
    const ink = getComputedStyle(wrap).color;
    const line = token(wrap, "grid");
    const hl = token(wrap, "hl");
    const g = diagram.graph;
    const hasTerms = g.nodes.some((n) => diagram.lane.get(n.id) === "term");
    // ELK answers after this view may have moved on (a new diagram): the core draws into a canvas
    // of its own, which outlives the view while a layout is pending (cyCanvas).
    const canvas = cyCanvas(box);
    const cy = cytoscape({
      container: canvas.container,
      elements: [
        ...(hasTerms
          ? [{ group: "nodes" as const, data: { id: TERM_LANE, kind: "laneBox" } }]
          : []),
        ...g.nodes.map((n) => {
          const lane = diagram.lane.get(n.id) as Lane;
          return {
            group: "nodes" as const,
            data: {
              id: n.id,
              label: n.label,
              lane,
              kind: "node",
              ...(n.status !== undefined ? { status: n.status } : {}),
              shape: SHAPE[lane],
              fill: token(wrap, nodeLook(n).colour),
              w: nodeWidth(n.label),
              ...(lane === "term" ? { parent: TERM_LANE } : {}),
            },
          };
        }),
        ...diagram.more.map((m) => {
          const label = t("graph3d.moreColumns", { count: m.count });
          return {
            group: "nodes" as const,
            data: {
              id: m.id,
              label,
              lane: "column",
              kind: "more",
              table: m.table,
              w: nodeWidth(label),
            },
          };
        }),
        // A line between lanes runs left to right, from the product side; inside the term lane
        // from the broader term down.
        ...diagram.links.map((k) => {
          const inTerms = diagram.lane.get(k.from) === "term" && diagram.lane.get(k.to) === "term";
          return {
            group: "edges" as const,
            data: {
              id: k.id,
              source: inTerms ? k.from : k.to,
              target: inTerms ? k.to : k.from,
              ...(inTerms ? { term: "yes", rank: "yes", label: k.label ?? "" } : {}),
            },
          };
        }),
      ],
      layout: { name: "preset" },
      style: [
        {
          selector: 'node[kind = "node"], node[kind = "more"]',
          style: {
            label: "data(label)",
            "font-size": "11px",
            "text-valign": "center",
            "text-halign": "center",
            width: "data(w)",
            height: NODE_H,
            shape: "data(shape)",
            "background-color": "data(fill)",
            color: "#1a1b1e",
            "border-width": 1,
            "border-color": ink,
            cursor: "pointer",
          },
        },
        {
          selector: 'node[kind = "more"]',
          style: {
            shape: "rectangle",
            "background-opacity": 0,
            "border-style": "dashed",
            color: ink,
          },
        },
        { selector: 'node[status = "draft"]', style: { "font-style": "italic" } },
        { selector: 'node[status = "deprecated"]', style: { opacity: 0.55 } },
        { selector: 'node[status = "retired"]', style: { opacity: 0.35 } },
        {
          selector: 'node[kind = "laneBox"]',
          style: {
            "background-opacity": 0,
            "border-width": 0,
            padding: "8px",
            events: "no",
          },
        },
        {
          selector: 'node[kind = "header"]',
          style: {
            label: "data(label)",
            "font-size": "12px",
            "font-weight": "bold",
            color: ink,
            "background-opacity": 0,
            "border-width": 0,
            width: 1,
            height: 1,
            "text-valign": "center",
            events: "no",
          },
        },
        {
          selector: "edge",
          style: {
            width: 1.2,
            "line-color": line,
            "target-arrow-color": line,
            "target-arrow-shape": "triangle",
            "arrow-scale": 0.8,
            "curve-style": "bezier",
          },
        },
        {
          // A term link: dashed and named by its type, as the 3D view draws it.
          selector: 'edge[term = "yes"]',
          style: {
            "line-style": "dashed",
            label: "data(label)",
            "font-size": "9px",
            color: ink,
            "text-rotation": "autorotate",
            "text-background-color": token(wrap, "canvas"),
            "text-background-opacity": 0.8,
          },
        },
        {
          // A rank link is laid out broader term first (above); its arrow points the link's own
          // way, from the narrower term to the broader.
          selector: 'edge[rank = "yes"]',
          style: {
            "target-arrow-shape": "none",
            "source-arrow-shape": "triangle",
            "source-arrow-color": line,
          },
        },
        {
          selector: 'edge[arc = "yes"]',
          style: {
            "curve-style": "unbundled-bezier",
            "control-point-distances": 36,
            "control-point-weights": 0.5,
          },
        },
        { selector: ".picked", style: { "border-width": 3, "border-color": hl } },
        { selector: "edge.lit", style: { "line-color": hl, "target-arrow-color": hl, width: 2.2 } },
        { selector: ".faded", style: { opacity: 0.2 } },
      ],
      minZoom: 0.1,
      // Names are legible at zoom 1; a small diagram is enlarged only a little to fit.
      maxZoom: 1.6,
      boxSelectionEnabled: false,
      autoungrabify: true,
    } as unknown as Parameters<typeof cytoscape>[0]) as CyCore;
    cyRef.current = cy;

    cy.on("tap", 'node[kind = "node"]', (evt: EventObject) =>
      latest.current.setSelected(evt.target.id()),
    );
    cy.on("tap", 'node[kind = "more"]', (evt: EventObject) =>
      latest.current.onMore(evt.target.data("table") as string),
    );
    const enter = (h: GraphHover) => (evt: EventObject) => {
      // A node's card sits beside the node; a line's beside the pointer on it.
      const b = h.kind === "node" ? (evt.target as unknown as cytoscape.NodeSingular).renderedBoundingBox({}) : null;
      const at = evt.renderedPosition as cytoscape.Position;
      keepTip();
      setTip(
        b
          ? { h, x: b.x1, y: b.y1, width: b.x2 - b.x1, height: b.y2 - b.y1 }
          : { h, x: at.x, y: at.y, width: 0, height: 0 },
      );
      latest.current.setHover(h);
    };
    cy.on("mouseover", 'node[kind = "node"], node[kind = "more"]', (evt: EventObject) =>
      enter({ kind: "node", id: evt.target.id() })(evt),
    );
    cy.on("mouseover", "edge", (evt: EventObject) => {
      // A "+N columns" node's line is no edge of the graph: it has no card.
      if (g.edges.some((e) => e.id === evt.target.id()))
        enter({ kind: "edge", id: evt.target.id() })(evt);
    });
    cy.on("mouseout", 'node[kind = "node"], node[kind = "more"], edge', () => {
      keepTip();
      linger.current = setTimeout(dropTip, TIP_LINGER_MS);
    });

    // Each lane is an ELK partition, placed strictly left to right; inside the term lane its
    // rank edges set the levels top-down.
    const partition = (l: Lane) => String(LANES.indexOf(l));
    const elkLayout = (ordered: boolean) =>
      cy.layout({
        name: "elk",
        fit: true,
        padding: FIT_PAD,
        nodeLayoutOptions: (n: cytoscape.NodeSingular) => {
          if (n.id() === TERM_LANE)
            return {
              "elk.partitioning.partition": partition("term"),
              "elk.algorithm": "layered",
              "elk.direction": "DOWN",
              "elk.aspectRatio": "0.4",
              "elk.layered.spacing.nodeNodeBetweenLayers": "36",
              "elk.spacing.nodeNode": "14",
            };
          if (n.data("lane") === "term") return {};
          return { "elk.partitioning.partition": partition(n.data("lane") as Lane) };
        },
        elk: {
          algorithm: "layered",
          "elk.direction": "RIGHT",
          "elk.partitioning.activate": "true",
          // One graph, so every lane lines up across all of it.
          "elk.separateConnectedComponents": "false",
          // A card is narrow: its lanes sit closer.
          "elk.layered.spacing.nodeNodeBetweenLayers": bare ? "40" : "90",
          "elk.spacing.nodeNode": "10",
          "elk.layered.nodePlacement.strategy": "NETWORK_SIMPLEX",
          // The second pass keeps each lane in the order the first pass's terms ask for.
          ...(ordered ? { "elk.layered.crossingMinimization.strategy": "INTERACTIVE" } : {}),
        },
      } as CyLayoutOptions);

    // The term lane is laid out on its own (top-down), so the outer lanes cannot see the lines
    // from it. A second pass orders them by it (placedLaneOrder), each lane spread over the
    // height of the longest.
    const reorder = () => {
      const [terms, ...lanes] = placedLaneOrder(diagram, (id) => cy.getElementById(id).position());
      const span = Math.max(...lanes.map((row) => row.length));
      // Only the order within each lane counts (ELK's interactive crossing minimisation).
      for (const row of lanes)
        row.forEach((id, i) => {
          const n = cy.getElementById(id);
          n.position({ x: n.position("x"), y: ((i + 0.5) * span * NODE_H * 2) / row.length });
        });
      return terms;
    };

    // The term links that are not rank edges, laid over the placed term lane: they move no node.
    const drawArcs = () =>
      cy.add(
        diagram.arcs.map((k) => ({
          group: "edges" as const,
          data: {
            id: k.id,
            source: k.from,
            target: k.to,
            term: "yes",
            arc: "yes",
            label: k.label ?? "",
          },
        })),
      );
    const labelLanes = () => {
      const placed = cy.nodes('[kind = "node"], [kind = "more"]');
      if (placed.empty()) return;
      const top = placed.boundingBox({}).y1 - 26;
      for (const l of LANES) {
        const inLane = placed.filter((n) => n.data("lane") === l);
        if (inLane.empty()) continue;
        const bb = inLane.boundingBox({});
        cy.add({
          group: "nodes",
          data: { id: `header:${l}`, kind: "header", label: laneTitle(l) },
          position: { x: (bb.x1 + bb.x2) / 2, y: top },
        });
      }
      cy.fit(undefined, FIT_PAD);
    };
    // ELK lays out asynchronously: each lane's name goes above it, over its middle, once placed.
    canvas.run(elkLayout(false), () => {
      const termOrder = reorder();
      canvas.run(elkLayout(true), () => {
        const moved = stackTerms(
          diagram,
          termOrder,
          (id) => cy.getElementById(id).position(),
          (id) => cy.getElementById(id).data("w") as number,
        );
        cy.batch(() => moved.forEach((p, id) => cy.getElementById(id).position(p)));
        drawArcs();
        labelLanes();
      });
    });

    const observer = new ResizeObserver(() => {
      cy.resize();
      cy.fit(undefined, FIT_PAD);
    });
    observer.observe(box);
    return () => {
      observer.disconnect();
      cyRef.current = null;
      canvas.end(cy);
    };
  }, [diagram, scheme, t, laneTitle, bare, keepTip, dropTip]);
  // A card's pending hide ends with the view.
  useEffect(() => keepTip, [keepTip]);

  // The card goes where it covers least of the drawing, once its size is known.
  useLayoutEffect(() => {
    const cy = cyRef.current;
    const box = boxRef.current;
    if (!tip || !tipEl || !cy || !box) return;
    const stage = box.getBoundingClientRect();
    setTipSide(
      tipPlacement(
        {
          left: stage.left + tip.x,
          top: stage.top + tip.y,
          right: stage.left + tip.x + tip.width,
          bottom: stage.top + tip.y + tip.height,
        },
        {
          width: Math.max(tipEl.offsetWidth, TIP_MAX_WIDTH),
          height: tipEl.offsetHeight + TIP_FRAME,
        },
        drawnGraph(cy, stage),
        { width: window.innerWidth, height: window.innerHeight },
      ),
    );
  }, [tip, tipEl]);

  // What is focused -- the node picked or hovered -- is lit with everything connected to it, and
  // the rest fades; a line hovered lights itself and its two ends.
  useEffect(() => {
    const cy = cyRef.current;
    if (!cy) return;
    const lit = new Set<string>();
    const near = new Set<string>();
    const focus = hover?.kind === "node" ? hover.id : hover ? null : selected;
    if (focus !== null) {
      const c = connectedTo(diagram, focus);
      for (const id of c.links) lit.add(id);
      for (const id of c.links) near.add(id);
      for (const id of c.nodes) near.add(id);
    } else if (hover) {
      for (const k of [...diagram.links, ...diagram.arcs])
        if (k.id === hover.id) {
          lit.add(k.id);
          near.add(k.id).add(k.from).add(k.to);
        }
    }
    const faded = focus !== null || hover !== null;
    cy.batch(() =>
      cy.elements().forEach((ele) => {
        const id = ele.id();
        const drawn = ele.data("kind") !== "laneBox" && ele.data("kind") !== "header";
        ele.removeClass("picked").removeClass("lit").removeClass("faded");
        if (id === selected) ele.addClass("picked");
        if (lit.has(id)) ele.addClass("lit");
        if (faded && drawn && !near.has(id)) ele.addClass("faded");
      }),
    );
  }, [diagram, scheme, selected, hover]);

  useEffect(() => {
    const cy = cyRef.current;
    if (!cy || command == null) return;
    if (command.kind === "reset" || command.target == null) cy.fit(undefined, FIT_PAD);
    else {
      const at = cy.getElementById(command.target);
      if (at.empty()) return;
      cy.zoom(Math.max(cy.zoom(), 1.2));
      cy.center(cy.elements().filter((e) => e.id() === command.target));
    }
  }, [command]);

  const copy = () => {
    const text = laneMermaid(diagram, laneTitle, (count) => t("graph3d.moreColumns", { count }));
    void navigator.clipboard.writeText(text).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    });
  };

  const more = tip ? diagram.more.find((m) => m.id === tip.h.id) : undefined;
  const body = !tip ? null : more ? (
    <>
      <b>{t("graph3d.moreColumns", { count: more.count })}</b>
      <MoreFacts columns={more.columns} testId={`${testId}-tip-facts`} />
    </>
  ) : (
    hoverBody(diagram.graph, tip.h, t, { data: diagram.data, testId: `${testId}-tip-facts` })
  );
  return (
    <div
      className="p3d-stage g3d-lanes"
      ref={wrapRef}
      data-testid={testId}
      data-ready
      data-scheme={scheme}
    >
      <div className="g3d-lanes-canvas" ref={boxRef} aria-label={t("graph3d.lanesLabel")} />
      {tip && body && (
        // Through the application's one tooltip, drawn at the end of the page: nothing of the
        // stage clips it, and the legend stays behind it.
        <TipSurface
          opened
          card
          position={tipSide}
          content={
            <div
              ref={setTipEl}
              className="g3d-tip-live"
              onMouseEnter={keepTip}
              onMouseLeave={dropTip}
              data-testid={`${testId}-tip`}
            >
              {body}
            </div>
          }
        >
          <span
            className="g3d-tip-anchor"
            style={{ left: tip.x, top: tip.y, width: tip.width, height: tip.height }}
          />
        </TipSurface>
      )}
      {!bare && (
        <>
          <div className="p3d-export">
            <Button
              size="compact-xs"
              variant="default"
              leftSection={copied ? <Check size={12} /> : <Copy size={12} />}
              onClick={copy}
              data-testid={`${testId}-mermaid`}
            >
              {copied ? t("graph3d.mermaidCopied") : t("graph3d.copyMermaid")}
            </Button>
          </div>
          <div className="p3d-hint">{t("profile3d.hintFlat")}</div>
        </>
      )}
      <div className="p3d-legend" ref={legendRef}>
        <GraphLegend graph={diagram.graph} />
      </div>
    </div>
  );
}
