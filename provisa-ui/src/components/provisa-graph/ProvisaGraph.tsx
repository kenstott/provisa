// Copyright (c) 2026 Kenneth Stott
// Canary: 2b7e5d9a-6f13-4c48-a9d2-0e4c8b3f7a61
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2239: the one 2D graph component. A view hands it nodes, edges, groups, each node's type and
// a layout preset; it draws them in the one visual language (SPEC.md) and owns everything else:
// the library's setup, the stylesheet, the presets, the controls, the legend, hover and selection,
// focus and filter marks, saved places, group collapse and resize, and export.

import {
  useCallback,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
  type MutableRefObject,
  type ReactNode,
} from "react";
import { useTranslation } from "react-i18next";
import { Group, Loader, Text, useComputedColorScheme, type FloatingPosition } from "@mantine/core";
import type { CyInstance } from "../graph/cytoscape-types";
import { TIP_MAX_WIDTH, TipSurface } from "../Tip";
import { arrangeLayered, nodesWithEdges, placeIsolatedGrid } from "./arrange";
import {
  cytoscape,
  type Core,
  type EdgeSingular,
  type EventObject,
  type NodeSingular,
} from "./core";
import { cyCanvas, type CyCanvas } from "./cyCanvas";
import { PROVISA_ELK } from "./elkLayout";
import { exportJpeg, exportJson, exportPng, exportSvg } from "./exportImage";
import { GraphControls, type GraphCopy } from "./GraphControls";
import { GraphLegend } from "./GraphLegend";
import { GroupResize } from "./GroupResize";
import { runLanes, type LanesOptions } from "./lanes";
import {
  markFilter,
  markFocused,
  markHover,
  nearOf,
  type GraphFilter,
  type Neighbourhood,
} from "./marks";
import { graphElements, typesDrawn, type Detail, type GraphSpec } from "./model";
import { readPlaces, standPlaces, type GraphPlaces, type Pt, type StandPlaces } from "./places";
import { ERD_ELK_LAYOUT } from "./presets";
import { graphStylesheet } from "./stylesheet";
import { applyTaxiLanes, clearTaxiLanes } from "./taxi";
import { tipPlacement, type DrawnGraph } from "./tipPlacement";
import { graphTokens, type NodeType } from "./tokens";
import { FIT_PADDING, fitClear, scrollBy, showFocus, zoomAbout } from "./viewport";

/** How far a wheel step (or a pinch) zooms, per pixel of its delta. */
const WHEEL_ZOOM = 0.002;
import "./provisa-graph.css";

export type LayoutPreset = "layered" | "lanes" | "force";

/** The reader's choices of a layered graph (the ERD's options). */
export interface LayeredOptions {
  detail: Detail;
  routing: "bezier" | "taxi";
  gridSnap: number;
  showOrphans: boolean;
}

/** What is hovered: a node, or a line. */
export interface GraphHover {
  kind: "node" | "edge";
  id: string;
}

/** A camera move asked of the graph: fit it all, or centre one node. */
export interface GraphCommand {
  kind: "reset" | "focus";
  target?: string | null;
}

export interface ProvisaGraphProps {
  graph: GraphSpec;
  preset: LayoutPreset;
  /** The lanes preset's lanes. */
  lanes?: LanesOptions;
  testId: string;
  ariaLabel?: string;
  /** The file name of a download, without its extension. */
  exportName: string;
  /** The graph as data: the download menu's JSON. */
  exportData?: () => unknown;
  /** Text the controls copy (a Mermaid flowchart of the graph). */
  copy?: GraphCopy;
  /** A glance (a card): no controls, legend or hint. */
  bare?: boolean;
  /** The layered preset's options. */
  options?: LayeredOptions;
  /** The groups drawn collapsed; a click on a group toggles it. */
  collapsed?: ReadonlySet<string>;
  onCollapsedChange?: (next: Set<string>) => void;
  /** The node picked (its detail open). */
  selected?: string | null;
  onNodeClick?: (id: string, data: (key: string) => unknown) => void;
  onEdgeClick?: (id: string, data: (key: string) => unknown) => void;
  /** A fold node clicked: what it stands for is to be drawn. */
  onMore?: (id: string, data: (key: string) => unknown) => void;
  /** What is hovered, when the view keeps it; otherwise the graph does. */
  hover?: GraphHover | null;
  onHover?: (h: GraphHover | null) => void;
  /** What hovering or picking a node brings forward; its own lines and their ends if not given. */
  neighbourhood?: (id: string) => Neighbourhood;
  /** Whether hovering a line shows a card (a fold node's line has none). */
  edgeHasCard?: (id: string) => boolean;
  /** A hovered thing's card; the node's name and description if not given. */
  hoverCard?: (h: GraphHover) => ReactNode;
  /** The node the graph is about (a term's graph's term): the graph opens, and Reset view comes
   * back, with it in view (showFocus), the zoom the opening fit's. Null: it opens as fitted. */
  opensOn?: string | null;
  /** The elements a focus names: marked and fitted into view. */
  focused?: string[];
  /** The focused elements the graph holds, after each draw. */
  onFocused?: (held: string[]) => void;
  /** The view's filter: matches marked, the rest dimmed. */
  filter?: GraphFilter | null;
  command?: GraphCommand | null;
  /** Saved places, stood after the automatic layout. */
  places?: StandPlaces | null;
  /** Set to a reader of the graph's places, for a save; it reads null before the canvas is. */
  placesRef?: MutableRefObject<(() => GraphPlaces | null) | null>;
  /** The reader edited the arrangement: moved, resized or collapsed something. */
  onArrange?: () => void;
  /** The view's header row (its menus), above the canvas. */
  header?: ReactNode;
  /** Drawn over the canvas, top left. */
  overlay?: ReactNode;
  /** One line of help under the canvas. */
  hint?: string;
}

/** How long a card stays once the pointer leaves its node: time to reach the card's links. */
const TIP_LINGER_MS = 300;
/** The card's padding and border around its content (the tip surface's). */
const TIP_FRAME = 24;

/** Where a card sits beside what is hovered, on the stage. */
interface TipAt {
  h: GraphHover;
  x: number;
  y: number;
  width: number;
  height: number;
  /** What the default card says: the thing's name and description. */
  name: string;
  description: string;
}

/** The nodes and lines drawn, on the page: what a card should stay off. */
function drawnGraph(cy: Core, stage: DOMRect): DrawnGraph {
  const drawn: DrawnGraph = { boxes: [], lines: [] };
  const at = (p: { x: number; y: number }) => ({ x: stage.left + p.x, y: stage.top + p.y });
  cy.elements().forEach((e) => {
    if (e.isNode()) {
      const n = e as unknown as NodeSingular;
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
      const k = e as unknown as EdgeSingular;
      drawn.lines.push([at(k.source().renderedPosition()), at(k.target().renderedPosition())]);
    }
  });
  return drawn;
}

const NO_IDS: string[] = [];
const NO_GROUPS: ReadonlySet<string> = new Set();

export function ProvisaGraph({
  graph,
  preset,
  lanes,
  testId,
  ariaLabel,
  exportName,
  exportData,
  copy,
  bare = false,
  options,
  collapsed = NO_GROUPS,
  onCollapsedChange,
  selected = null,
  onNodeClick,
  onEdgeClick,
  onMore,
  hover: hoverProp,
  onHover,
  neighbourhood,
  edgeHasCard,
  hoverCard,
  opensOn = null,
  focused = NO_IDS,
  onFocused,
  filter = null,
  command = null,
  places = null,
  placesRef,
  onArrange,
  header,
  overlay,
  hint,
}: ProvisaGraphProps) {
  const { t } = useTranslation();
  const isDark = useComputedColorScheme("dark") === "dark";
  const tokens = graphTokens(isDark);
  const stageRef = useRef<HTMLDivElement | null>(null);
  const boxRef = useRef<HTMLDivElement | null>(null);
  const cyRef = useRef<Core | null>(null);
  const [drawnAt, setDrawnAt] = useState(0);
  // The box the core draws in attaches after this component's first commit: a callback ref
  // re-runs the drawing once it has.
  const [boxReady, setBoxReady] = useState(false);
  const setBox = useCallback((el: HTMLDivElement | null) => {
    boxRef.current = el;
    setBoxReady(!!el);
  }, []);

  const detail = options?.detail ?? "all";
  const typeName = useCallback((type: NodeType) => t(`provisaGraph.type.${type}`), [t]);
  const elements = useMemo(() => graphElements(graph, typeName, detail), [graph, typeName, detail]);

  // Read when an event comes, so the drawing is not rebuilt when a handler changes.
  const latest = useRef({
    onNodeClick,
    onEdgeClick,
    onMore,
    onHover,
    onArrange,
    onCollapsedChange,
    collapsed,
    options,
    places,
    opensOn,
    focused,
    onFocused,
    filter,
  });
  useEffect(() => {
    latest.current = {
      onNodeClick,
      onEdgeClick,
      onMore,
      onHover,
      onArrange,
      onCollapsedChange,
      collapsed,
      options,
      places,
      opensOn,
      focused,
      onFocused,
      filter,
    };
  });

  // ── hover ────────────────────────────────────────────────────────────────────────────────
  const [ownHover, setOwnHover] = useState<GraphHover | null>(null);
  const hover = hoverProp === undefined ? ownHover : hoverProp;
  const setHover = useCallback((h: GraphHover | null) => {
    setOwnHover(h);
    latest.current.onHover?.(h);
  }, []);
  const [tip, setTip] = useState<TipAt | null>(null);
  const [tipEl, setTipEl] = useState<HTMLDivElement | null>(null);
  const [tipSide, setTipSide] = useState<FloatingPosition>("right-start");
  const linger = useRef<ReturnType<typeof setTimeout> | null>(null);
  const keepTip = useCallback(() => {
    if (linger.current !== null) clearTimeout(linger.current);
    linger.current = null;
  }, []);
  const dropTip = useCallback(() => {
    keepTip();
    setTip(null);
    setHover(null);
  }, [keepTip, setHover]);
  // A card's pending hide ends with the graph.
  useEffect(() => keepTip, [keepTip]);

  // ── the layered preset's state ───────────────────────────────────────────────────────────
  // Nodes the reader moved, by id: they stand where they were put over any later layout.
  const movedRef = useRef<Map<string, Pt>>(new Map());
  useEffect(() => {
    movedRef.current = new Map();
  }, [places]);
  const isolatedRef = useRef<Set<string>>(new Set());
  const [hoveredGroup, setHoveredGroup] = useState<string | null>(null);

  // Whether the drawing carries a focus or a filter now: one never set (a view that names none)
  // is never cleared.
  const marked = useRef({ focus: false, filter: false });
  /** Marks the focus, or clears the one marked before. */
  const applyFocus = useCallback((cy: Core, ids: string[]) => {
    if (ids.length === 0 && !marked.current.focus) return;
    marked.current.focus = ids.length > 0;
    latest.current.onFocused?.(markFocused(cy as unknown as CyInstance, ids));
  }, []);
  /** Marks the filter, or clears the one marked before. */
  const applyFilter = useCallback((cy: Core, filter: GraphFilter | null) => {
    if (filter === null && !marked.current.filter) return;
    marked.current.filter = filter !== null;
    markFilter(cy as unknown as CyInstance, filter);
  }, []);
  /** The marks a draw starts without: the focus, the filter. */
  const applyMarks = useCallback(
    (cy: Core) => {
      marked.current = { focus: false, filter: false };
      applyFocus(cy, latest.current.focused);
      applyFilter(cy, latest.current.filter);
    },
    [applyFocus, applyFilter],
  );

  // ── laying out ───────────────────────────────────────────────────────────────────────────
  // REQ-2239 (A GRAPH BEING LAID OUT SAYS SO): from the moment a layout starts until every node
  // has its place and the opening fit has run, the core's div is held hidden -- the frame of
  // unplaced nodes, all at one point, is never painted -- and the canvas says it is laying the
  // graph out. Every preset passes through here: lanes, ELK (with saved places stood after it) and
  // fCoSE.
  const [layingOut, setLayingOut] = useState(true);
  /** The core once its nodes have their places: what the controls, downloads, the camera and a
   * save act on. Null while a layout runs. */
  const placedRef = useRef<Core | null>(null);
  /** A layout that failed: thrown while rendering, to the page's error boundary. */
  const [failed, setFailed] = useState<Error | null>(null);

  // ── the drawing ──────────────────────────────────────────────────────────────────────────
  useEffect(() => {
    const box = boxRef.current;
    if (!boxReady || !box) return;
    // ELK answers after this view may have moved on: the core draws into a canvas of its own,
    // which outlives the view while a layout is pending (cyCanvas).
    const canvas = cyCanvas(box);
    // Hidden before the core is made in it: nothing is drawn until the layout has placed it.
    canvas.container.style.visibility = "hidden";
    placedRef.current = null;
    setLayingOut(true);
    let live = true;
    const fail = (error: unknown) => {
      if (!live) return;
      setLayingOut(false);
      setFailed(error instanceof Error ? error : new Error(String(error)));
    };
    /** The canvas, its layouts' failures said: one that throws as it starts, that fails after it
     * started (ELK's rejected answer), or whose placement throws once it answers, ends the laying
     * out and goes to the page's error path. */
    const laying: CyCanvas = {
      container: canvas.container,
      end: canvas.end,
      run(layout, then) {
        try {
          canvas.run(
            layout,
            () => {
              try {
                then?.();
              } catch (error) {
                fail(error);
              }
            },
            fail,
          );
        } catch (error) {
          fail(error);
        }
      },
    };
    const cy = cytoscape({
      container: canvas.container,
      elements: elements as unknown as Parameters<typeof cytoscape>[0]["elements"],
      style: graphStylesheet(tokens) as unknown as Parameters<typeof cytoscape>[0]["style"],
      layout: { name: "preset" },
      minZoom: 0.1,
      maxZoom: preset === "lanes" ? 1.6 : 4,
      boxSelectionEnabled: false,
      autoungrabify: preset === "lanes",
      // The wheel scrolls (below); a pinch, or the wheel with Ctrl or Cmd held, zooms.
      userZoomingEnabled: false,
    } as unknown as Parameters<typeof cytoscape>[0]) as Core;
    cyRef.current = cy;
    const c = cy as unknown as CyInstance;
    // A diagram taller or wider than its canvas scrolls under the wheel (across with Shift, or a
    // trackpad's sideways swipe), at a zoom its labels can be read at; at its end the wheel
    // scrolls the page.
    canvas.container.addEventListener(
      "wheel",
      (ev: WheelEvent) => {
        if (ev.ctrlKey || ev.metaKey) {
          const r = canvas.container.getBoundingClientRect();
          zoomAbout(c, Math.exp(-ev.deltaY * WHEEL_ZOOM), {
            x: ev.clientX - r.left,
            y: ev.clientY - r.top,
          });
          ev.preventDefault();
        } else {
          // Shift turns a mouse wheel's scroll across, as the browser does for a page.
          const across = ev.shiftKey && ev.deltaX === 0;
          if (scrollBy(c, across ? ev.deltaY : ev.deltaX, across ? 0 : ev.deltaY))
            ev.preventDefault();
        }
      },
      { passive: false },
    );

    cy.on("tap", 'node[kind = "node"]', (evt: EventObject) =>
      latest.current.onNodeClick?.(evt.target.id(), (k) => evt.target.data(k)),
    );
    cy.on("tap", 'node[kind = "more"]', (evt: EventObject) =>
      latest.current.onMore?.(evt.target.id(), (k) => evt.target.data(k)),
    );
    cy.on("tap", "edge", (evt: EventObject) =>
      latest.current.onEdgeClick?.(evt.target.id(), (k) => evt.target.data(k)),
    );
    cy.on("tap", 'node[kind = "group"]', (evt: EventObject) => {
      const change = latest.current.onCollapsedChange;
      if (!change) return;
      const id = evt.target.id() as string;
      const next = new Set(latest.current.collapsed);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      latest.current.onArrange?.();
      change(next);
    });
    /** What the default card says of the thing under the pointer. */
    const said = (evt: EventObject) => {
      const data = (k: string) => (evt.target as { data?: (k: string) => unknown }).data?.(k);
      return {
        name: String(data("name") ?? data("label") ?? ""),
        description: String(data("description") ?? ""),
      };
    };
    const enter = (h: GraphHover) => (evt: EventObject) => {
      // A node's card sits beside the node; a line's beside the pointer on it.
      const b =
        h.kind === "node" ? (evt.target as unknown as NodeSingular).renderedBoundingBox({}) : null;
      const at = evt.renderedPosition as { x: number; y: number };
      keepTip();
      setTip(
        b
          ? { h, x: b.x1, y: b.y1, width: b.x2 - b.x1, height: b.y2 - b.y1, ...said(evt) }
          : { h, x: at.x, y: at.y, width: 0, height: 0, ...said(evt) },
      );
      setHover(h);
    };
    cy.on("mouseover", 'node[kind = "node"], node[kind = "more"]', (evt: EventObject) =>
      enter({ kind: "node", id: evt.target.id() })(evt),
    );
    cy.on("mouseover", "edge", (evt: EventObject) => {
      if (edgeHasCard && !edgeHasCard(evt.target.id())) return;
      enter({ kind: "edge", id: evt.target.id() })(evt);
    });
    cy.on("mouseout", 'node[kind = "node"], node[kind = "more"], edge', () => {
      keepTip();
      linger.current = setTimeout(dropTip, TIP_LINGER_MS);
    });
    // A group's card names it; hovering one lights nothing, and offers its resize handles.
    cy.on("mouseover", 'node[kind = "group"]', (evt: EventObject) => {
      const at = evt.renderedPosition as { x: number; y: number };
      keepTip();
      setTip({
        h: { kind: "node", id: evt.target.id() },
        x: at.x,
        y: at.y,
        width: 0,
        height: 0,
        ...said(evt),
      });
      setHoveredGroup(evt.target.id());
    });
    cy.on("mouseout", 'node[kind = "group"]', () => {
      keepTip();
      linger.current = setTimeout(() => setTip(null), TIP_LINGER_MS);
    });

    // The margin the opening fit leaves: the lanes' runLanes fits with 24.
    const padding = preset === "lanes" ? 24 : FIT_PADDING;
    // Every node placed and the opening fit run: the drawing is shown.
    const finish = () => {
      // Each preset's opening fit has run (fitClear): the node the graph is about is brought in.
      showFocus(cy as unknown as CyInstance, latest.current.opensOn, padding);
      applyMarks(cy);
      placedRef.current = cy;
      canvas.container.style.visibility = "";
      setLayingOut(false);
      setDrawnAt((n) => n + 1);
    };

    // The layout starts once the frame saying so has been painted: a placement that takes a
    // while (a large lane diagram's is one long task) is seen to be under way, not a still canvas.
    const layOut = () => {
      if (preset === "lanes" && lanes) {
        runLanes(
          cy,
          laying,
          lanes,
          graph.edges.filter((e) => e.overlay),
          finish,
        );
      } else if (preset === "force") {
        laying.run(
          cy.layout({ name: "fcose", animate: false, padding: 24 } as Parameters<
            typeof cy.layout
          >[0]),
          () => {
            fitClear(cy as unknown as CyInstance);
            finish();
          },
        );
      } else {
        cy.on("dragfree", 'node[kind = "node"]', (evt: EventObject) => {
          latest.current.onArrange?.();
          const target = evt.target as unknown as {
            id(): string;
            position(): Pt;
            position(p: Pt): void;
          };
          const raw = target.position();
          const g = latest.current.options?.gridSnap ?? 0;
          const at = g > 0 ? { x: Math.round(raw.x / g) * g, y: Math.round(raw.y / g) * g } : raw;
          if (g > 0) target.position(at);
          movedRef.current.set(target.id(), at);
          // A move can swap a line's dominant axis, which decides which way its lanes spread.
          if (latest.current.options?.routing === "taxi") applyTaxiLanes(c);
        });
        // Nodes with no lines are hidden while ELK places the rest; the arrangement after it puts
        // them in a grid under their group.
        const connected = nodesWithEdges(c);
        cy.nodes('[kind = "node"]').forEach((n) => {
          if (!connected.has(n.id())) n.style("display", "none");
        });
        // ELK through Provisa's own adapter, which says a failure after the layout started.
        const elk = { ...ERD_ELK_LAYOUT, name: PROVISA_ELK };
        laying.run(cy.layout(elk as Parameters<typeof cy.layout>[0]), () => {
          const opts = latest.current.options;
          const rect = box.getBoundingClientRect();
          const arranged = arrangeLayered(c, {
            gridSnap: opts?.gridSnap ?? 0,
            showOrphans: opts?.showOrphans ?? true,
            aspectRatio: rect.height > 0 ? rect.width / rect.height : null,
          });
          isolatedRef.current = arranged.isolatedIds;
          if (latest.current.places) standPlaces(c, latest.current.places, movedRef.current);
          fitClear(cy as unknown as CyInstance);
          const routing = opts?.routing ?? "bezier";
          cy.$(".pg-edge").style("curve-style", routing);
          if (routing === "taxi") applyTaxiLanes(c);
          finish();
        });
      }
    };
    let frame = requestAnimationFrame(() => {
      frame = requestAnimationFrame(layOut);
    });

    // The canvas narrows beside a detail panel and widens when it closes: the graph follows.
    const observer = new ResizeObserver(() => {
      if (box.clientWidth === 0) return;
      cy.resize();
      // A layout running fits the diagram when it ends.
      if (placedRef.current !== cy) return;
      fitClear(cy as unknown as CyInstance, padding);
      showFocus(cy as unknown as CyInstance, latest.current.opensOn, padding);
      if (preset !== "lanes") applyMarks(cy);
    });
    observer.observe(box);
    return () => {
      live = false;
      cancelAnimationFrame(frame);
      observer.disconnect();
      cyRef.current = null;
      placedRef.current = null;
      canvas.end(cy);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps -- the drawing is rebuilt when what it draws, or how, changes; handlers are read through ``latest`` and the options that move nothing are applied below
  }, [boxReady, elements, isDark, preset, lanes, places]);

  // ── the marks ────────────────────────────────────────────────────────────────────────────
  useEffect(() => {
    const cy = cyRef.current;
    if (cy) applyFocus(cy, focused);
  }, [focused, drawnAt, applyFocus]);
  useEffect(() => {
    const cy = cyRef.current;
    if (cy) applyFilter(cy, filter);
  }, [filter, drawnAt, applyFilter]);
  // What is hovered or picked is lit with what it brings forward, and the rest fades; a line
  // hovered lights itself and its two ends.
  useEffect(() => {
    const cy = cyRef.current;
    if (!cy) return;
    const c = cy as unknown as CyInstance;
    const near = (id: string) => (neighbourhood ? neighbourhood(id) : nearOf(c, id));
    const focus = hover?.kind === "node" ? hover.id : hover ? null : selected;
    let lit: Neighbourhood | null = null;
    if (focus !== null) lit = near(focus);
    else if (hover) {
      const e = cy.getElementById(hover.id);
      lit = {
        nodes: e.empty()
          ? new Set()
          : new Set([String(e.data("source")), String(e.data("target"))]),
        links: new Set([hover.id]),
      };
    }
    markHover(c, selected, lit);
  }, [selected, hover, neighbourhood, drawnAt]);

  // ── the layered options that move nothing ────────────────────────────────────────────────
  const showOrphans = options?.showOrphans ?? true;
  useEffect(() => {
    // A layout running arranges the orphans by the options when it ends.
    const cy = placedRef.current;
    if (!cy || preset !== "layered") return;
    const c = cy as unknown as CyInstance;
    const isolated = isolatedRef.current;
    if (showOrphans) {
      // Each group's extent from where its connected nodes stand now.
      const bboxes = new Map<string, { x1: number; x2: number; y2: number }>();
      cy.nodes('[kind = "group"]').forEach((group) => {
        let x1 = Infinity;
        let x2 = -Infinity;
        let y2 = -Infinity;
        let any = false;
        group.children().forEach((child) => {
          if (isolated.has(child.id())) return;
          const bb = child.boundingBox({});
          if (!bb.w) return;
          x1 = Math.min(x1, bb.x1);
          x2 = Math.max(x2, bb.x2);
          y2 = Math.max(y2, bb.y2);
          any = true;
        });
        if (any) bboxes.set(group.id(), { x1, x2, y2 });
      });
      placeIsolatedGrid(c, isolated, bboxes);
      isolated.forEach((id) => cy.getElementById(id).style("display", "element"));
    } else isolated.forEach((id) => cy.getElementById(id).style("display", "none"));
    fitClear(cy as unknown as CyInstance);
    showFocus(cy as unknown as CyInstance, latest.current.opensOn);
  }, [showOrphans, preset]);

  const gridSnap = options?.gridSnap ?? 0;
  useEffect(() => {
    const cy = placedRef.current;
    if (!cy || gridSnap === 0) return;
    cy.batch(() =>
      cy.nodes('[kind = "node"]').forEach((n) => {
        const p = n.position();
        n.position({
          x: Math.round(p.x / gridSnap) * gridSnap,
          y: Math.round(p.y / gridSnap) * gridSnap,
        });
      }),
    );
  }, [gridSnap]);

  const routing = options?.routing;
  useEffect(() => {
    const cy = placedRef.current;
    if (!cy || routing === undefined) return;
    const c = cy as unknown as CyInstance;
    cy.batch(() => {
      cy.$(".pg-edge").style("curve-style", routing);
      if (routing === "taxi") applyTaxiLanes(c);
      else clearTaxiLanes(c);
    });
  }, [routing]);

  // ── the camera ───────────────────────────────────────────────────────────────────────────
  useEffect(() => {
    const cy = placedRef.current;
    if (!cy || command === null) return;
    if (command.kind === "reset" || command.target == null) {
      fitClear(cy as unknown as CyInstance, 24);
      showFocus(cy as unknown as CyInstance, latest.current.opensOn, 24);
    } else {
      const at = cy.getElementById(command.target);
      if (at.empty()) return;
      cy.zoom(Math.max(cy.zoom(), 1.2));
      cy.center(cy.elements().filter((e) => e.id() === command.target));
    }
  }, [command]);

  // ── saved places ─────────────────────────────────────────────────────────────────────────
  useEffect(() => {
    if (!placesRef) return;
    placesRef.current = () =>
      placedRef.current === null ? null : readPlaces(placedRef.current as unknown as CyInstance);
  }, [placesRef]);

  // ── the card ─────────────────────────────────────────────────────────────────────────────
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

  const groupIds = useMemo(() => (graph.groups ?? []).map((g) => g.id), [graph]);
  /** A group's card is always its name: a view's card is for nodes and lines. */
  const isGroup = (id: string) => groupIds.includes(id);
  const drawnTypes = useMemo(() => typesDrawn(graph), [graph]);
  const body = !tip ? null : hoverCard && !isGroup(tip.h.id) ? (
    hoverCard(tip.h)
  ) : tip.name ? (
    <>
      <b>{tip.name}</b>
      {tip.description && <div className="pg-tip-body">{tip.description}</div>}
    </>
  ) : null;

  // A download is of the placed drawing: none while a layout runs.
  const exports = {
    svg: () =>
      placedRef.current &&
      exportSvg(placedRef.current as unknown as CyInstance, tokens.canvas, exportName),
    png: () =>
      placedRef.current &&
      exportPng(placedRef.current as unknown as CyInstance, tokens.canvas, exportName),
    jpeg: () =>
      placedRef.current &&
      exportJpeg(placedRef.current as unknown as CyInstance, tokens.canvas, exportName),
    ...(exportData ? { json: () => exportJson(exportData(), exportName) } : {}),
  };

  if (failed) throw failed;

  return (
    <div className="pg-root" data-testid={testId}>
      {header && <div className="pg-header">{header}</div>}
      <div
        ref={stageRef}
        className="pg-stage"
        style={{ ["--pg-canvas" as string]: tokens.canvas }}
        onMouseLeave={() => setHoveredGroup(null)}
      >
        <div
          ref={setBox}
          className="pg-canvas"
          aria-label={ariaLabel}
          data-scheme={isDark ? "dark" : "light"}
        />
        {layingOut && (
          // The graph views' loading indicator (ReadState's), saying what is under way.
          <div className="pg-laying-out" role="status" data-testid={`${testId}-laying-out`}>
            <Group gap="xs">
              <Loader size="sm" />
              <Text size="sm" c="dimmed">
                {t("provisaGraph.layingOut")}
              </Text>
            </Group>
          </div>
        )}
        {(overlay || (hint && !bare)) && (
          <div className="pg-corner">
            {overlay && <div className="pg-overlay">{overlay}</div>}
            {hint && !bare && <div className="pg-hint">{hint}</div>}
          </div>
        )}
        {preset === "layered" && onCollapsedChange && (
          <GroupResize
            cyRef={cyRef as MutableRefObject<CyInstance | null>}
            groupId={hoveredGroup}
            groupIds={groupIds}
            accent={tokens.accent}
            drawnAt={drawnAt}
            onLeave={() => setHoveredGroup(null)}
            onResized={(moved) => {
              moved.forEach((at, id) => movedRef.current.set(id, at));
              latest.current.onArrange?.();
            }}
          />
        )}
        {!bare && (
          <>
            <GraphControls
              cyRef={placedRef as MutableRefObject<CyInstance | null>}
              stageRef={stageRef}
              exports={exports}
              copy={copy}
              testId={testId}
            />
            <GraphLegend types={drawnTypes} typeName={typeName} />
          </>
        )}
        {tip && body && (
          // Through the application's one tooltip, drawn at the end of the page: nothing of the
          // stage clips it.
          <TipSurface
            opened
            card
            position={tipSide}
            content={
              <div
                ref={setTipEl}
                className="pg-tip"
                onMouseEnter={keepTip}
                onMouseLeave={dropTip}
                data-testid={`${testId}-tip`}
              >
                {body}
              </div>
            }
          >
            <span
              className="pg-tip-anchor"
              style={{ left: tip.x, top: tip.y, width: tip.width, height: tip.height }}
            />
          </TipSurface>
        )}
      </div>
    </div>
  );
}
