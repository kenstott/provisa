// Copyright (c) 2026 Kenneth Stott
// Canary: 9c4f1a62-7e3b-4d85-a2c0-6b8e3f1d7a49
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2174 (amended 2026-10-28): the graph's 2D view, the lane diagram (laneDiagram.ts) drawn by
// cytoscape and laid out by ELK's layered algorithm in fixed lane partitions, left to right; the
// term lane is one compound laid out top-down. A click picks a node (the detail panel), a hover
// shows its card, a "+N columns" node expands in place; the diagram copies as Mermaid text. A
// card holds the node's facts with links to their pages, and stays while the pointer is on it.

import { useCallback, useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { Button, useComputedColorScheme } from "@mantine/core";
import { Check, Copy } from "lucide-react";
import cytoscape, { type EventObject } from "cytoscape";
import elkRaw from "cytoscape-elk";
import type { CyLayoutOptions } from "../graph/cytoscape-types";
import type { CameraCommand } from "../../pages/tables/profile3d/Stage3D";
import type { GraphHover } from "./graphScene";
import { GraphLegend } from "./GraphStage";
import { hoverBody } from "./hoverBody";
import { MoreFacts } from "./nodeFacts";
import { LANES, laneMermaid, type Lane, type LaneDiagram } from "./laneDiagram";
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

/** A card beside the pointer: below and right of it, or -- past the stage's middle -- on the
 * other side, so a card never runs off the stage. */
interface TipAt {
  h: GraphHover;
  x: number;
  y: number;
  left: boolean;
  up: boolean;
}

const tipPlace = (at: TipAt) =>
  `translate(${at.x + (at.left ? -14 : 14)}px, ${at.y + (at.up ? -14 : 14)}px) ` +
  `translate(${at.left ? "-100%" : "0"}, ${at.up ? "-100%" : "0"})`;

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
  const cyRef = useRef<CyCore | null>(null);
  const [tip, setTip] = useState<TipAt | null>(null);
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
    const cy = cytoscape({
      container: box,
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
        ...diagram.links.map((k) => ({
          group: "edges" as const,
          data: { id: k.id, source: k.from, target: k.to },
        })),
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
      // A pointer event always carries where it happened on the canvas.
      const at = evt.renderedPosition as cytoscape.Position;
      keepTip();
      setTip({
        h,
        x: at.x,
        y: at.y,
        left: at.x > wrap.clientWidth / 2,
        up: at.y > wrap.clientHeight / 2,
      });
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
    // from it. A second pass orders them by it: each table at the mean height of the terms its
    // columns are bound to, its columns together beside it in their terms' order (its "+N
    // columns" last), each product at the mean of its tables.
    const reorder = () => {
      const y = (id: string) => cy.getElementById(id).position("y");
      const laneOf = (id: string) => diagram.lane.get(id);
      const avg = (xs: number[]) => xs.reduce((sum, v) => sum + v, 0) / xs.length;
      const terms = new Map<string, number[]>();
      const tableOf = new Map<string, string>();
      const tablesOf = new Map<string, string[]>();
      for (const k of diagram.links) {
        if (laneOf(k.from) === "term" && laneOf(k.to) !== "term")
          terms.set(k.to, [...(terms.get(k.to) ?? []), y(k.from)]);
        if (laneOf(k.to) === "table" && laneOf(k.from) !== "term") tableOf.set(k.from, k.to);
        if (laneOf(k.to) === "product") tablesOf.set(k.to, [...(tablesOf.get(k.to) ?? []), k.from]);
      }
      const own = (id: string) => {
        const ys = terms.get(id);
        return ys ? avg(ys) : y(id);
      };
      const inLane = (l: Lane) => g.nodes.filter((n) => laneOf(n.id) === l).map((n) => n.id);
      const tableKey = new Map<string, number>();
      for (const t of inLane("table")) {
        const ys = [...tableOf].filter(([c, at]) => at === t && terms.has(c)).map(([c]) => own(c));
        ys.push(...(terms.get(t) ?? []));
        tableKey.set(t, ys.length === 0 ? y(t) : avg(ys));
      }
      const more = new Set(diagram.more.map((m) => m.id));
      const key = (id: string): [number, number] => {
        const t = tableOf.get(id);
        const inner = more.has(id) ? Infinity : own(id);
        return t ? [tableKey.get(t) as number, inner] : [inner, 0];
      };
      const columns = [...inLane("column"), ...more].sort((p, q) => {
        const [a1, a2] = key(p);
        const [b1, b2] = key(q);
        return a1 - b1 || a2 - b2;
      });
      const tables = inLane("table").sort(
        (p, q) => (tableKey.get(p) as number) - (tableKey.get(q) as number),
      );
      const place = new Map<string, number>();
      columns.forEach((id, i) => place.set(id, i));
      tables.forEach((id, i) => place.set(id, (i * columns.length) / Math.max(1, tables.length)));
      for (const p of inLane("product")) {
        const ts = tablesOf.get(p) ?? [];
        place.set(p, ts.length === 0 ? y(p) : avg(ts.map((t) => place.get(t) as number)));
      }
      // Only the order within each lane counts (ELK's interactive crossing minimisation).
      for (const [id, at] of place) {
        const n = cy.getElementById(id);
        n.position({ x: n.position("x"), y: at * NODE_H * 2 });
      }
    };

    // ELK lays out asynchronously: each lane's name goes above it, over its middle, once placed.
    // ELK answers after this view may have moved on (a new diagram): a cytoscape torn down while
    // it lays out is detached at once and destroyed when the layout ends.
    let running = true;
    let gone = false;
    const first = elkLayout(false);
    first.one("layoutstop", () => {
      if (gone) {
        running = false;
        cy.destroy();
        return;
      }
      reorder();
      const second = elkLayout(true);
      second.one("layoutstop", () => {
        running = false;
        if (gone) {
          cy.destroy();
          return;
        }
        labelLanes();
      });
      second.run();
    });
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
    first.run();

    const observer = new ResizeObserver(() => {
      cy.resize();
      cy.fit(undefined, FIT_PAD);
    });
    observer.observe(box);
    return () => {
      observer.disconnect();
      cyRef.current = null;
      if (!running) cy.destroy();
      else {
        gone = true;
        (cy as unknown as { unmount: () => void }).unmount();
      }
    };
  }, [diagram, scheme, t, laneTitle, bare, keepTip, dropTip]);
  // A card's pending hide ends with the view.
  useEffect(() => keepTip, [keepTip]);

  // What is picked or hovered is lit, with its lines; the rest fades while something is hovered.
  useEffect(() => {
    const cy = cyRef.current;
    if (!cy) return;
    const lit = new Set<string>();
    const near = new Set<string>();
    if (hover) {
      for (const k of diagram.links)
        if (hover.kind === "node" ? k.from === hover.id || k.to === hover.id : k.id === hover.id) {
          lit.add(k.id);
          near.add(k.id).add(k.from).add(k.to);
        }
      near.add(hover.id);
    }
    cy.batch(() =>
      cy.elements().forEach((ele) => {
        const id = ele.id();
        const drawn = ele.data("kind") !== "laneBox" && ele.data("kind") !== "header";
        ele.removeClass("picked").removeClass("lit").removeClass("faded");
        if (id === selected) ele.addClass("picked");
        if (lit.has(id)) ele.addClass("lit");
        if (hover && drawn && !near.has(id)) ele.addClass("faded");
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
        <div
          className="p3d-tip g3d-tip-live"
          style={{ transform: tipPlace(tip) }}
          onMouseEnter={keepTip}
          onMouseLeave={dropTip}
          data-testid={`${testId}-tip`}
        >
          {body}
        </div>
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
      <div className="p3d-legend">
        <GraphLegend graph={diagram.graph} />
      </div>
    </div>
  );
}
