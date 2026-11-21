// Copyright (c) 2026 Kenneth Stott
// Canary: 4f6c2a83-9d15-4b7e-8a3c-e1d7b0f5c926
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2174 (amended 2026-10-28, ONE ONTOLOGY GRAPH; EVERY OTHER GRAPH IS A SNIPPET OF IT): the one
// graph's view -- the ontology as a 3D network or a 2D lane diagram, whose contents are layers the
// user turns on and off, filtered by domain, tag and a focus with its depth, under named presets
// that set the layers and the layout. A layer the data has nothing in is not offered. A snippet
// (a term's page, a product's panel) is the same view with only its 2D/3D and depth controls.
// (Amended 2026-11-07, WHAT THE GRAPH DRAWS:) the depth control is "Hops from <focus>", counted
// from the focus alone (amended 2026-11-12: each hop a step between terms, then All); Related tables draws the tables related to those drawn, one step, in 3D;
// a table's columns tied to no term drawn fold into one node; Reset view folds every table back.

import { useContext, useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { Anchor, Button, Chip, Group, SegmentedControl, Text } from "@mantine/core";
import { Link, useNavigate } from "react-router-dom";
import { SummaryMultiSelect } from "./SummaryMultiSelect";
import { AttractionSlider } from "../../pages/tables/profile3d/Attraction";
import { ATTRACTION_MAX, ATTRACTION_MIN } from "../../pages/tables/profile3d/attractionState";
import { Tip } from "../Tip";
import { ResetSettingsButton } from "../../pages/tables/profile3d/ResetSettingsButton";
import {
  chosenNow,
  selectionToSave,
  useGraphSettings,
  type GraphKey,
} from "../../pages/tables/profile3d/graphSettings";
import { matchesSearch, searchItems, type SearchGroups } from "../../lib/searchItems";
import { ClampedDetail } from "../../pages/data-products/ClampedDetail";
import type { CameraCommand } from "../../pages/tables/profile3d/Stage3D";
import { ExpandButton } from "../../pages/tables/profile3d/Expandable3D";
import {
  NO_FILTERS,
  availableLayers,
  edgeClass,
  edgesOf,
  filterChoices,
  foldUnbound,
  chosenLayout,
  layoutGraph,
  openingLayout,
  savedLayout,
  visibleGraph,
  type GraphData,
  type GraphFilters,
  type GraphEdge,
  type GraphNode,
  type GraphScope,
  type LayerId,
  type Preset,
  type PresetId,
  type QueryScope,
  TERM_STATUSES,
  type TermStatus,
} from "./graphModel";
import type { GraphHover } from "./graphScene";
import { GraphStage, type StageState } from "./GraphStage";
import { LaneStage } from "./LaneStage";
import { laneDiagram } from "./laneDiagram";
import { DimensionToggle } from "./DimensionToggle";
import { LayoutToggle } from "./LayoutToggle";
import { kindKey } from "./nodeLook";
import { OpenAsQuery } from "./OpenAsQuery";
import { erdHref, nodeHref } from "./nodeHref";
import { LayersMenu, TermsMenu } from "./OntologyMenus";
import { NodeFacts } from "./nodeFacts";
import { BreakableName } from "../list/BreakableName";
import { TourRunningContext } from "../../tour/tourRunning";
import "../../pages/tables/profile3d/profile3d.css";
import "./graph3d.css";

/** The statuses a term can be filtered by (REQ-2130); a live term is always drawn. */
const STATUSES: TermStatus[] = ["draft", "deprecated", "retired"];

/** Layers toggled beside the filters rather than in the Layers row: the Ontology View's Columns,
 * and Related tables (REQ-2174, amended 2026-11-07). */
const BESIDE_FILTERS: ReadonlySet<LayerId> = new Set(["columns", "related"]);

/** Layers the 2D view never draws: it is products, tables, columns and terms alone (REQ-2174,
 * amended 2026-11-07), so neither is read for it. */
const NOT_IN_2D: ReadonlySet<LayerId> = new Set(["related", "lineage"]);

/** The layers a term drawn brings with it, whatever the hop (REQ-2174, amended 2026-11-12): read
 * while chosen though the opening read, nearer the focus, found nothing in them. */
const GROUNDING: ReadonlySet<LayerId> = new Set(["columns", "data"]);

/** The depths a focus can be read to, in steps between terms; Infinity is All, the connected part
 * to its end (REQ-2174, amended 2026-11-12). */
const DEPTHS = [1, 2, 3, 4, Infinity];

/** A depth as the control and the remembered scope hold it: All as "all". */
const depthValue = (d: number) => (d === Infinity ? "all" : String(d));
const depthOf = (v: string) => (v === "all" ? Infinity : Number(v));

export function GraphExplorer({
  data,
  scope,
  preset: initialPreset,
  focus: initialFocus = null,
  home = null,
  onRest,
  selected: initialSelected = initialFocus,
  depth: initialDepth,
  layers: openingLayers,
  snippet = false,
  opensFlat = false,
  onExpand,
  search,
  settingsKey,
  exportName,
  query,
  within = null,
  testId = "graph3d",
}: {
  data: GraphData;
  scope: GraphScope;
  preset: PresetId;
  focus?: string | null;
  /** The product this view is the graph of: focused on it by the Focus filter, it offers the
   * rest of the model on asking, and Set Defaults comes back to it. */
  home?: string | null;
  /** Told whether the rest of the model is being shown, the page reading it then. */
  onRest?: (rest: boolean) => void;
  /** The node selected when the view opens, the focus by default: the whole ontology a snippet
   * expands to opens with the snippet's focus selected (REQ-2174), drawing everything. */
  selected?: string | null;
  /** How many hops from a focus or a search's matches are drawn (and read). */
  depth: number;
  /** The layers it opens with where they are not its preset's (a product's snippet draws the
   * Data layer's product). */
  layers?: LayerId[];
  /** A glance (a term's page, a product's panel): its 2D/3D and depth controls and its expand
   * button, no filters or detail panel. */
  snippet?: boolean;
  /** Drawn in 2D until the reader has chosen otherwise (a term's graph). */
  opensFlat?: boolean;
  /** Drawn in place: its expand button opens it at 95% of the viewport. */
  onExpand?: () => void;
  /** The page's search, focusing the graph in place of its own focus field: the terms and
   * products it matches (searchItems' rule) and their neighbourhoods; empty, the whole graph. */
  search?: string;
  /** Which graph's settings these are (graphSettings.ts): kept across reloads, per graph. */
  settingsKey: GraphKey;
  /** What the graph's exports are named after; the preset drawn is added ("ontology-taxonomy"). */
  exportName: string;
  /** REQ-2136: the layers and the domain, edge type and status filters run in the view's query.
   * The page reads ``data`` for the scope ``onScope`` reports; ``all`` (every layer, no filter)
   * is what the controls choose from, since ``data`` holds only what the filters keep. */
  query?: {
    all: GraphData;
    onScope: (s: QueryScope) => void;
    text: string | null;
    /** The focus the query itself states (a product's graph, read for that product): no
     * neighbourhood beyond the query's own. */
    own?: string;
    /** REQ-2143: said in place of the graph when the query would read more than the budget. */
    notice?: string | null;
    /** REQ-2143: ``data`` is the overview -- the domains with their counts -- and picking a domain
     * narrows the graph to it. */
    overview?: boolean;
    /** Told the tables expanded: the page reads a table's columns beyond those it read. */
    onExpanded?: (tables: ReadonlySet<string>) => void;
  };
  /** REQ-1723: the navbar's checked domains (useViewDomains), null for no narrowing. The graph
   * holds only what is in them; its own domain filter chooses among them, narrowing further and
   * never past them. */
  within?: ReadonlySet<string> | null;
  testId?: string;
}) {
  const { t } = useTranslation();
  const presetOf = (p: PresetId): Preset => {
    const found = scope.presets[p];
    if (!found) throw new Error(`No ${p} preset in this graph`);
    return found;
  };
  const every = query?.all ?? data;
  const offered = useMemo(() => availableLayers(every, scope), [every, scope]);
  // The domain filter offers the navbar's domains alone (REQ-1723).
  const choices = useMemo(() => {
    const c = filterChoices(every);
    return within ? { ...c, domains: c.domains.filter((d) => within.has(d)) } : c;
  }, [every, within]);
  const edgeTypes = useMemo(
    () =>
      [
        ...new Set(
          every.edges.filter((e) => scope.typedKinds.includes(e.kind)).map((e) => e.label ?? ""),
        ),
      ]
        .filter(Boolean)
        .sort(),
    [every, scope],
  );
  const hasStatus = useMemo(() => every.nodes.some((n) => n.status !== undefined), [every]);
  const hasTerms = useMemo(() => every.nodes.some((n) => n.type === "term"), [every]);
  // Everything the reader sets here is remembered for this graph (graphSettings.ts): the preset,
  // layers, toggles and filters. The focus is the page's (its search, or the product it opened on)
  // and the camera is where the reader left it, so neither is kept.
  const [saved, save, resetSettings] = useGraphSettings(settingsKey, {
    preset: initialPreset as string,
    layers: (openingLayers ?? presetOf(initialPreset).layers) as string[],
    // Terms left with no relationship under the chosen edge types and domains: drawn until unchecked.
    orphans: true as boolean,
    rotate: false as boolean,
    attraction: 1 as number,
    // The floor grid is not drawn until the reader turns Grid on (REQ-2239, amended 2026-11-13).
    grid: false as boolean,
    // REQ-2174: drawn in 2D, laid out flat through a face-on camera.
    flat: opensFlat as boolean,
    // REQ-2174: auto opens in the preset's own layout, else Force; else the reader's Force or Layered.
    layout: "auto" as string,
    // REQ-2174 (ONE ONTOLOGY GRAPH): whether lateral term edges (related to, synonym of...) are drawn.
    lateral: true as boolean,
    // null: every domain, every edge type (or the preset's own), so a new one is drawn too.
    domains: null as string[] | null,
    edgeTypes: null as string[] | null,
    hiddenStatuses: scope.hiddenStatuses as string[],
    tags: [] as string[],
    sensitivities: [] as string[],
  });
  const preset: PresetId = scope.presetOrder.includes(saved.preset as PresetId)
    ? (saved.preset as PresetId)
    : initialPreset;
  const presetOwn = scope.presets[preset]?.edgeTypes;
  const presetTypes = useMemo(() => presetOwn ?? edgeTypes, [presetOwn, edgeTypes]);
  const savedLayers = saved.layers as string[];
  // A tour shows the 2D graph, whatever the reader keeps for this graph, and leaves that choice
  // as it was: a change made during a tour holds for the tour alone.
  const touring = useContext(TourRunningContext);
  const [tourFlat, setTourFlat] = useState(true);
  const flat = touring ? tourFlat : (saved.flat as boolean);
  // A layer the graph always draws is on whatever was remembered (an old "ontology off" too).
  // REQ-2174 (2D SHOWS ONLY 2D CONTROLS): in 2D the lane diagram is the only layout and the
  // Layers control is not shown, so the preset's own layers are drawn.
  const presetLayers = openingLayers ?? presetOf(preset).layers;
  const layers = useMemo(
    () =>
      new Set([
        ...scope.implicitLayers,
        ...(flat
          ? presetLayers
          : savedLayers.filter((l): l is LayerId => scope.layerOrder.includes(l as LayerId))),
      ]),
    [savedLayers, scope, flat, presetLayers],
  );
  const orphans = saved.orphans as boolean;
  const rotate = saved.rotate as boolean;
  const attraction = Math.min(ATTRACTION_MAX, Math.max(ATTRACTION_MIN, saved.attraction as number));
  const grid = saved.grid as boolean;
  const [focus, setFocus] = useState<string[]>(initialFocus ? [initialFocus] : []);
  const [depth, setDepth] = useState(initialDepth);
  const lateral = saved.lateral as boolean;
  // The neighbourhood a focus draws, to the depth chosen.
  const reachScope = useMemo(
    () => ({ ...scope, neighbourhood: { ...scope.neighbourhood, depth } }),
    [scope, depth],
  );
  const savedDomains = saved.domains as string[] | null;
  const savedTypes = saved.edgeTypes as string[] | null;
  const savedStatuses = saved.hiddenStatuses as string[];
  const savedTags = saved.tags as string[];
  const savedSensitivities = saved.sensitivities as string[];
  const filters: GraphFilters = useMemo(
    () => ({
      ...NO_FILTERS,
      focus,
      domains: chosenNow(savedDomains, choices.domains),
      edgeTypes: savedTypes === null ? presetTypes : chosenNow(savedTypes, edgeTypes),
      hiddenStatuses: savedStatuses.filter((st): st is TermStatus =>
        TERM_STATUSES.includes(st as TermStatus),
      ),
      tags: chosenNow(savedTags, choices.tags),
      sensitivities: chosenNow(savedSensitivities, choices.sensitivities),
    }),
    [
      focus,
      savedDomains,
      savedTypes,
      savedStatuses,
      savedTags,
      savedSensitivities,
      choices,
      presetTypes,
      edgeTypes,
    ],
  );
  const setFilters = (change: (f: GraphFilters) => GraphFilters) => {
    const next = change(filters);
    if (next.focus !== filters.focus) setFocus(next.focus);
    if (next.domains !== filters.domains)
      save("domains", selectionToSave(next.domains, choices.domains));
    if (next.edgeTypes !== filters.edgeTypes)
      save(
        "edgeTypes",
        presetTypes.length === next.edgeTypes.length &&
          presetTypes.every((x) => next.edgeTypes.includes(x))
          ? null
          : [...next.edgeTypes],
      );
    if (next.hiddenStatuses !== filters.hiddenStatuses)
      save("hiddenStatuses", [...next.hiddenStatuses]);
    if (next.tags !== filters.tags) save("tags", [...next.tags]);
    if (next.sensitivities !== filters.sensitivities)
      save("sensitivities", [...next.sensitivities]);
  };
  const setOrphans = (on: boolean) => save("orphans", on);
  const setRotate = (on: boolean) => save("rotate", on);
  const setAttraction = (v: number) => save("attraction", v);
  const setGrid = (on: boolean) => save("grid", on);
  const setFlat = (on: boolean) => (touring ? setTourFlat(on) : save("flat", on));
  const [expanded, setExpanded] = useState<Set<string>>(new Set());
  const toggleExpanded = (id: string) =>
    setExpanded((prev) => {
      const next = new Set(prev);
      if (!next.delete(id)) next.add(id);
      return next;
    });
  const onExpanded = query?.onExpanded;
  useEffect(() => onExpanded?.(expanded), [onExpanded, expanded]);
  const [frame, setFrame] = useState(0);
  // A product's graph shows the rest of the model whenever its focus is anything but the product.
  const showsRest = home !== null && !(focus.length === 1 && focus[0] === home);
  useEffect(() => onRest?.(showsRest), [onRest, showsRest]);
  // What the control brings in is read after it flips: the camera fits a product's graph whenever
  // what it read has changed (a column opened in place changes no read, and leaves the camera be).
  const stageFrame = home === null ? frame : `${frame}:${data.nodes.map((n) => n.id).join()}`;
  const [picked, setSelected] = useState<string | null>(initialSelected);
  const [hover, setHover] = useState<GraphHover | null>(null);
  const [command, setCommand] = useState<CameraCommand | null>(null);
  const move = (kind: CameraCommand["kind"], target: string | null) =>
    setCommand((c) => ({ kind, target, seq: c == null ? 1 : c.seq + 1 }));

  // The Product map's Tables are the tables of the columns drawn: without Columns, there are none
  // to draw.
  const on = useMemo(
    () =>
      new Set(
        [...layers].filter(
          (l) =>
            (offered.has(l) || GROUNDING.has(l)) &&
            (l !== "ownerTables" || layers.has("columns")) &&
            !(flat && NOT_IN_2D.has(l)),
        ),
      ),
    [layers, offered, flat],
  );
  // Every choice chosen is no narrowing at all: the graph is drawn as if unfiltered, nodes that
  // carry no domain included.
  const narrowed = useMemo(() => {
    const all = (chosen: string[], among: string[]) =>
      among.length > 0 && among.every((v) => chosen.includes(v)) ? [] : chosen;
    // Inside the navbar's domains: the filter's choice among them, or all of them.
    const chosen = all(filters.domains, choices.domains);
    return {
      domains: within && chosen.length === 0 ? [...within].sort() : chosen,
      edgeTypes: all(filters.edgeTypes, edgeTypes),
      tags: all(filters.tags, choices.tags),
      sensitivities: all(filters.sensitivities, choices.sensitivities),
    };
  }, [filters, choices, edgeTypes, within]);
  // The scope the query reads, reported whenever it changes (a key, so an equal one is not news).
  const onScope = query?.onScope;
  const own = query?.own ?? null;
  const scopeKey = JSON.stringify([
    [...on].sort(),
    narrowed.domains,
    narrowed.edgeTypes,
    filters.hiddenStatuses,
    orphans,
    narrowed.tags,
    narrowed.sensitivities,
    searchItems(search ?? ""),
    flat,
    depthValue(depth),
  ]);
  useEffect(() => {
    if (!onScope) return;
    const [l, domains, types, statuses, kept, tags, sensitivities, items, lanes, hops] = JSON.parse(
      scopeKey,
    ) as [
      LayerId[],
      string[],
      string[],
      TermStatus[],
      boolean,
      string[],
      string[],
      SearchGroups,
      boolean,
      string,
    ];
    onScope({
      layers: new Set(l),
      domains,
      edgeTypes: types,
      hiddenStatuses: statuses,
      orphans: kept,
      tags,
      sensitivities,
      search: items,
      lanes,
      focus: own,
      depth: depthOf(hops),
    });
  }, [onScope, scopeKey, own]);

  // REQ-2143: the page's search is the query's own (its root filter and its neighbourhood); a node
  // focused by the focus field (the product graph's) is a neighbourhood no query states yet.
  const focused = filters.focus.some((f) => f !== query?.own);
  // The one node the graph is focused on (not a search): what its hops are counted from.
  const focusNode = useMemo(() => {
    const id = search === undefined && filters.focus.length === 1 ? filters.focus[0] : null;
    return id === null ? null : (data.nodes.find((n) => n.id === id) ?? null);
  }, [search, filters.focus, data]);
  // What the drawing reads of ``query``: the page rebuilds the object on each render, and a drawing
  // redrawn on every render of the page loses its layout (the 2D lanes are laid out anew).
  const queried = query !== undefined;
  const queryOwn = query?.own;
  const shown = useMemo(() => {
    // The page's search, when it drives the focus: what it matches, by title or definition.
    const items = search === undefined ? null : searchItems(search);
    const matched =
      items === null || items.length === 0
        ? null
        : data.nodes
            .filter((n) => n.type === "term" || n.type === "product")
            .filter((n) => matchesSearch([n.label, n.definition], items))
            .map((n) => n.id);
    // A search that matches nothing draws nothing.
    if (items !== null && items.length > 0 && matched?.length === 0)
      return { nodes: [], edges: [] };
    // Under a query the domains, edge types and statuses are its own: ``data`` is what they keep.
    // Tags are judged here: a column or a table carries its own.
    // The focus the query itself states (a term's snippet's) is read as its neighbourhood: it
    // prunes nothing more here. A product's hops are counted here too (REQ-2174, amended
    // 2026-11-07), in 2D as in 3D, a step between terms (amended 2026-11-12).
    const own = (f: string) => f === queryOwn && !f.startsWith("p:");
    const narrowing = {
      ...filters,
      focus: matched ?? filters.focus.filter((f) => !own(f)),
      ...(queried
        ? { domains: [], edgeTypes: [], hiddenStatuses: [], tags: narrowed.tags, sensitivities: [] }
        : narrowed),
    };
    const all = visibleGraph(data, on, narrowing, expanded, reachScope);
    // Lateral term edges are drawn on asking; they never place a node.
    const g = lateral
      ? all
      : {
          ...all,
          edges: all.edges.filter((e) => e.kind !== "term_edge" || edgeClass(e) !== "lateral"),
        };
    // Under a query its Orphans is its own too.
    if (orphans || queried) return g;
    // An orphan has no term relationship drawn, and no column it IS while Columns is on; its
    // domain does not count.
    const related = (e: GraphEdge) => e.kind === "term_edge" || e.kind === "term_binding";
    const joined = new Set(g.edges.filter(related).flatMap((e) => [e.a, e.b]));
    const nodes = g.nodes.filter((n) => n.type !== "term" || joined.has(n.id));
    const kept = new Set(nodes.map((n) => n.id));
    // With an orphan go the edges that only placed it (its domain).
    return { nodes, edges: g.edges.filter((e) => kept.has(e.a) && kept.has(e.b)) };
  }, [
    data,
    on,
    filters,
    narrowed,
    queried,
    queryOwn,
    expanded,
    reachScope,
    orphans,
    search,
    lateral,
  ]);
  // REQ-2174 (amended 2026-11-07): in 3D a table's columns tied to no term drawn are one node.
  const folded = useMemo(
    () => foldUnbound(shown, expanded, (count) => t("graph3d.moreColumns", { count })),
    [shown, expanded, t],
  );
  const layout = chosenLayout(
    folded,
    savedLayout(saved.layout as string) ?? presetOf(preset).layout ?? null,
  );
  const layeredPossible = useMemo(() => openingLayout(folded).layeredPossible, [folded]);
  // REQ-2174: in 2D the graph is a lane diagram (laneDiagram.ts), laid out by ELK as it is drawn.
  const lanes = useMemo(
    () => (flat ? laneDiagram(shown, data, expanded) : null),
    [flat, shown, data, expanded],
  );
  const drawn = lanes?.graph ?? folded;
  const at = useMemo(
    () =>
      flat ? new Map<string, [number, number, number]>() : layoutGraph(folded, layout, attraction),
    [folded, layout, attraction, flat],
  );
  const focusable = useMemo(
    () =>
      data.nodes
        .filter((n) => n.type === "product" || n.type === "term")
        .map((n) => ({ value: n.id, label: n.label }))
        .sort((a, b) => a.label.localeCompare(b.label)),
    [data],
  );

  // A preset chooses its layers and its edge types afresh.
  const choosePreset = (p: PresetId) => {
    save("preset", p);
    save("layers", [...presetOf(p).layers]);
    save("edgeTypes", null);
    save("layout", "auto");
    setFrame((f) => f + 1);
  };
  const toggleLayer = (l: LayerId, on: boolean) =>
    save("layers", on ? [...layers, l] : [...layers].filter((x) => x !== l));

  const selected = drawn.nodes.some((n) => n.id === picked) ? picked : null;
  const navigate = useNavigate();
  // In the overview a domain picked is the domain the graph is narrowed to. A snippet has no
  // detail panel: a node clicked in it opens on its own page.
  const select = (id: string | null) => {
    // A table's folded columns open in place, as a lane's fold node does.
    const fold = id === null ? undefined : drawn.nodes.find((n) => n.id === id && n.folded);
    if (fold?.parent) {
      toggleExpanded(fold.parent);
      return;
    }
    const node = snippet && id !== null ? drawn.nodes.find((n) => n.id === id) : undefined;
    if (node) {
      navigate(nodeHref(node));
      return;
    }
    if (query?.overview && id !== null && id.startsWith("d:")) {
      setFilters((f) => ({ ...f, domains: [id.slice(2)] }));
      return;
    }
    setSelected(id);
  };
  // A snippet is a glance: it turns and draws no grid, whatever the saved settings say; a drag
  // stops the turning, as it does anywhere.
  const [glanceTurn, setGlanceTurn] = useState(true);
  const s: StageState = {
    graph: folded,
    at,
    frame: stageFrame,
    flat,
    layout,
    selected,
    setSelected: select,
    hover,
    setHover,
    rotate: snippet ? glanceTurn : rotate,
    setRotate: snippet ? setGlanceTurn : setRotate,
    grid: snippet ? false : grid,
  };

  // A focus or a search draws a neighbourhood, to the depth chosen: "Hops from <focus>".
  const hasFocus = filters.focus.length > 0 || searchItems(search ?? "").length > 0;
  const hopsLabel = focusNode
    ? t("graph3d.hopsFrom", { focus: focusNode.label })
    : t("graph3d.hopsFromMatches");
  const depthControl = hasFocus && (
    <Group gap={4} wrap="nowrap">
      <Text size="xs" c="dimmed" data-testid={`${testId}-depth-label`}>
        {hopsLabel}
      </Text>
      <SegmentedControl
        size="xs"
        value={depthValue(depth)}
        onChange={(v) => setDepth(depthOf(v))}
        data={DEPTHS.map((d) => ({
          value: depthValue(d),
          label: d === Infinity ? t("graph3d.hopsAll") : String(d),
        }))}
        aria-label={hopsLabel}
        data-testid={`${testId}-depth`}
      />
    </Group>
  );
  // REQ-2174 (amended 2026-11-07): the tables related to those drawn, one step; the 3D view's.
  const relatedControl = !flat && offered.has("related") && (
    <Chip
      size="xs"
      checked={layers.has("related")}
      onChange={(on) => toggleLayer("related", on)}
      data-testid={`${testId}-related`}
    >
      {t("graph3d.relatedTables")}
    </Chip>
  );
  const restControl = home !== null && (
    <Chip
      size="xs"
      checked={showsRest}
      onChange={(on) => setFocus(on ? [] : [home])}
      data-testid={`${testId}-rest`}
    >
      {t("graph3d.restOfModel")}
    </Chip>
  );
  // Reset view: the camera back to its resting view, and every table folded back.
  const resetView = () => {
    move("reset", null);
    setExpanded(new Set());
  };
  const stage = query?.notice ? (
    <div className="p3d-stage g3d-empty" data-testid={`${testId}-over-budget`}>
      <Text size="sm" c="dimmed">
        {query.notice}
      </Text>
    </div>
  ) : drawn.nodes.length === 0 ? (
    <div className="p3d-stage g3d-empty" data-testid={`${testId}-empty`}>
      <Text size="sm" c="dimmed">
        {t("graph3d.empty")}
      </Text>
    </div>
  ) : lanes ? (
    <LaneStage
      diagram={lanes}
      selected={selected}
      setSelected={select}
      hover={hover}
      setHover={setHover}
      onMore={toggleExpanded}
      command={command}
      opensOn={focusNode?.id ?? null}
      testId={`${testId}-lanes`}
      bare={snippet}
    />
  ) : (
    <GraphStage
      s={s}
      command={command}
      exportName={preset === exportName ? exportName : `${exportName}-${preset}`}
      testId={`${testId}-stage`}
      controls={{
        testId,
        onReset: resetView,
        rotate: { value: rotate, onChange: setRotate },
        grid: { value: grid, onChange: setGrid },
      }}
      bare={snippet}
    />
  );

  if (snippet)
    return (
      <div className="g3d-panel" data-testid={testId}>
        <Group justify="flex-end" gap="xs" className="g3d-panel-bar">
          {depthControl}
          {relatedControl}
          <DimensionToggle flat={flat} onChange={setFlat} testId={testId} />
          {onExpand && <ExpandButton onClick={onExpand} testId={`${testId}-expand`} />}
        </Group>
        <div className="p3d g3d-panel-view">{stage}</div>
      </div>
    );

  return (
    <div className={`p3d g3d${onExpand ? " g3d-inline" : " is-expanded"}`} data-testid={testId}>
      <Group className="p3d-toolbar" gap="xs" wrap="wrap">
        <SegmentedControl
          size="xs"
          value={preset}
          onChange={(v) => choosePreset(v as PresetId)}
          data={scope.presetOrder.map((p) => ({ value: p, label: t(`graph3d.preset_${p}`) }))}
          aria-label={t("graph3d.preset")}
          data-testid={`${testId}-preset`}
        />
        <DimensionToggle flat={flat} onChange={setFlat} testId={testId} />
        {depthControl}
        {restControl}
        {relatedControl}
        {/* A 2D graph is a lane diagram: it has one layout, no floor, never turns, and draws no
            lateral edge. */}
        {!flat && (
          <>
            <LayoutToggle
              layout={layout}
              layeredPossible={layeredPossible}
              onChange={(l) => save("layout", l)}
              testId={testId}
            />
            {/* REQ-2174 (THE TOGGLE IS CALLED ASSOCIATIONS): the term links that rank nothing.
                The hint is on a span round the chip: the chip's own input is hidden, so the
                pointer never enters it. */}
            <Tip text={t("graph3d.lateralHelp")}>
              <span>
                <Chip
                  size="xs"
                  checked={lateral}
                  onChange={(on) => save("lateral", on)}
                  data-testid={`${testId}-lateral`}
                >
                  {t("graph3d.lateral")}
                </Chip>
              </span>
            </Tip>
          </>
        )}
        {/* REQ-2239: in 3D, Reset view is in the canvas's controls (Stage3D). The lane diagram's
            stays here: it folds every table back as well as fitting, where its canvas's Fit fits
            the whole image only. */}
        {flat && (
          <Button size="xs" variant="default" onClick={resetView} data-testid={`${testId}-reset`}>
            {t("profile3d.resetView")}
          </Button>
        )}
        {!flat && layout === "force" && (
          <AttractionSlider value={attraction} onChange={setAttraction} testId={testId} />
        )}
        <ResetSettingsButton
          onReset={() => {
            resetSettings();
            if (home !== null) {
              setFocus([home]);
              setDepth(initialDepth);
            }
          }}
          testId={testId}
        />
        {query && <OpenAsQuery query={query.text} focused={focused} testId={testId} />}
        {onExpand && <ExpandButton onClick={onExpand} testId={`${testId}-expand`} />}
      </Group>
      {!scope.menus && !flat && (
        <Group className="p3d-toolbar" gap="xs" wrap="wrap" data-testid={`${testId}-layers`}>
          <Text size="xs" c="dimmed">
            {t("graph3d.layers")}
          </Text>
          {scope.layerOrder
            .filter((l) => offered.has(l) && !BESIDE_FILTERS.has(l))
            .map((l) => (
              <Chip
                key={l}
                size="xs"
                checked={layers.has(l)}
                onChange={(on) => toggleLayer(l, on)}
                data-testid={`${testId}-layer-${l}`}
              >
                {t(`graph3d.layer_${l}`)}
              </Chip>
            ))}
        </Group>
      )}
      <Group className="p3d-toolbar g3d-filters" gap="xs" wrap="wrap">
        {scope.menus && !flat && (
          <LayersMenu
            offered={scope.layerOrder.filter(
              (l) => offered.has(l) && !scope.implicitLayers.includes(l) && l !== "related",
            )}
            layers={layers}
            onChange={(chosen) => save("layers", [...scope.implicitLayers, ...chosen])}
            testId={testId}
          />
        )}
        {scope.menus && hasStatus && (
          <TermsMenu
            hidden={filters.hiddenStatuses}
            unconnected={scope.orphans && hasTerms ? orphans : null}
            onChange={(hidden, unconnected) => {
              setFilters((f) => ({ ...f, hiddenStatuses: hidden }));
              setOrphans(unconnected);
            }}
            testId={testId}
          />
        )}
        {!scope.menus && hasStatus && (
          <Group gap={4} data-testid={`${testId}-statuses`}>
            <Text size="xs" c="dimmed">
              {t("graph3d.show")}
            </Text>
            {STATUSES.map((st) => (
              <Chip
                key={st}
                size="xs"
                checked={!filters.hiddenStatuses.includes(st)}
                onChange={(on) =>
                  setFilters((f) => ({
                    ...f,
                    hiddenStatuses: on
                      ? f.hiddenStatuses.filter((x) => x !== st)
                      : [...f.hiddenStatuses, st],
                  }))
                }
                data-testid={`${testId}-status-${st}`}
              >
                {t(`graph3d.status_${st}`)}
              </Chip>
            ))}
          </Group>
        )}
        {edgeTypes.length > 0 && (
          <SummaryMultiSelect
            label={t("graph3d.filterEdgeType")}
            data={edgeTypes}
            value={filters.edgeTypes}
            onChange={(types) => setFilters((f) => ({ ...f, edgeTypes: types }))}
            allText={t("graph3d.allEdgeTypes")}
            someText={(count, total) => t("graph3d.someEdgeTypes", { count, total })}
            testId={`${testId}-filter-edge-type`}
          />
        )}
        <SummaryMultiSelect
          label={t("graph3d.filterDomain")}
          data={choices.domains}
          value={filters.domains}
          onChange={(domains) => setFilters((f) => ({ ...f, domains }))}
          allText={t("graph3d.allDomains")}
          someText={(count, total) => t("graph3d.someDomains", { count, total })}
          testId={`${testId}-filter-domain`}
        />
        {!scope.menus && !flat && offered.has("columns") && (
          <Chip
            size="xs"
            checked={layers.has("columns")}
            onChange={(on) => toggleLayer("columns", on)}
            data-testid={`${testId}-columns`}
          >
            {t("graph3d.columnsToggle")}
          </Chip>
        )}
        {!scope.menus && scope.orphans && hasTerms && (
          <Chip size="xs" checked={orphans} onChange={setOrphans} data-testid={`${testId}-orphans`}>
            {t("graph3d.orphans")}
          </Chip>
        )}
        {/* A filter with nothing to choose from is not offered. */}
        {choices.tags.length > 0 && (
          <SummaryMultiSelect
            label={t("graph3d.filterTag")}
            data={choices.tags}
            value={filters.tags}
            onChange={(tags) => setFilters((f) => ({ ...f, tags }))}
            allText={t("graph3d.allTags")}
            someText={(count, total) => t("graph3d.someTags", { count, total })}
            testId={`${testId}-filter-tag`}
          />
        )}
        {choices.sensitivities.length > 0 && (
          <SummaryMultiSelect
            label={t("graph3d.filterSensitivity")}
            data={choices.sensitivities}
            value={filters.sensitivities}
            onChange={(sensitivities) => setFilters((f) => ({ ...f, sensitivities }))}
            allText={t("graph3d.allSensitivities")}
            someText={(count, total) => t("graph3d.someSensitivities", { count, total })}
            testId={`${testId}-filter-sensitivity`}
          />
        )}
        {search === undefined && (
          <SummaryMultiSelect
            label={t("graph3d.filterFocus")}
            data={focusable}
            value={filters.focus}
            onChange={(focus) => setFilters((f) => ({ ...f, focus }))}
            allText={t("graph3d.allFocus")}
            someText={(count, total) => t("graph3d.someFocus", { count, total })}
            searchable
            testId={`${testId}-filter-focus`}
          />
        )}
      </Group>
      <div className="p3d-body">
        {stage}
        <GraphDetail
          graph={drawn}
          data={data}
          selected={selected}
          setHover={setHover}
          expanded={expanded}
          // In 2D a table's unbound columns fold into one node, whatever the scope.
          folds={scope.foldColumns || flat}
          toggleExpanded={toggleExpanded}
          onFocus={(id) => {
            setRotate(false);
            move("focus", id);
          }}
          testId={testId}
        />
      </div>
    </div>
  );
}

function GraphDetail({
  graph,
  data,
  selected,
  setHover,
  expanded,
  folds,
  toggleExpanded,
  onFocus,
  testId,
}: {
  graph: GraphData;
  data: GraphData;
  selected: string | null;
  setHover: (h: GraphHover | null) => void;
  expanded: ReadonlySet<string>;
  /** Whether a table's columns fold into it (and so can be shown on asking). */
  folds: boolean;
  toggleExpanded: (id: string) => void;
  onFocus: (id: string) => void;
  testId: string;
}) {
  const { t } = useTranslation();
  const node = graph.nodes.find((n) => n.id === selected);
  const name = (id: string) => graph.nodes.find((n) => n.id === id)?.label ?? id;
  const links = node ? edgesOf(graph, node.id) : [];
  // A table expands to its other columns, offered only when it has columns beyond those drawn:
  // those its graph read (folded into it, or one node of those tied to no term; REQ-2174, amended
  // 2026-11-07), or those its read counted (a term's graph reads only the columns its terms
  // reach). Expanded, it collapses them.
  const hasColumns = node?.type === "table" && (expanded.has(node.id) || hasMore(node));
  function hasMore(table: GraphNode): boolean {
    const of = (g: GraphData) => g.nodes.filter((n) => n.parent === table.id && !n.folded);
    if (graph.nodes.some((n) => n.folded && n.parent === table.id)) return true;
    const drawnColumns = of(graph).filter((n) => n.type === "column").length;
    const read = folds ? of(data).filter((n) => n.type === "column").length : 0;
    return Math.max(read, table.columns ?? 0) > drawnColumns;
  }
  // Each kind drawn, named as its look shows it, with how many.
  const counts = useMemo(() => {
    const by = new Map<string, { key: string; type: string; count: number }>();
    for (const n of graph.nodes) {
      // A table's folded columns are one node drawn, not one column.
      if (n.folded) continue;
      const key = kindKey(n);
      const seen = by.get(key);
      if (seen) seen.count += 1;
      else by.set(key, { key, type: n.type, count: 1 });
    }
    return [...by.values()].sort((a, b) => b.count - a.count);
  }, [graph]);
  return (
    <div className="p3d-detail">
      {node && (
        <section data-testid={`${testId}-detail`} aria-live="polite">
          <div className="p3d-col-head">
            {/* The thing itself, on its own page; Back returns to the graph. */}
            <h4>
              <Anchor component={Link} to={nodeHref(node)} data-testid={`${testId}-title-link`}>
                {node.label}
              </Anchor>
            </h4>
            <span className="p3d-kind">{t(kindKey(node))}</span>
          </div>
          {node.definition && (
            <ClampedDetail testId={`${testId}-definition`}>
              <Text size="xs" c="dimmed">
                {node.definition}
              </Text>
            </ClampedDetail>
          )}
          {/* REQ-2174: what the node is, each name a link to its page. */}
          <NodeFacts node={node} data={data} testId={`${testId}-facts`} />
          {node.unlinkedTables && (
            <Text size="xs" c="orange" data-testid={`${testId}-unlinked-tables`}>
              {t("graph3d.unlinkedTables", { tables: node.unlinkedTables.join(", ") })}
            </Text>
          )}
          <div className="p3d-list">
            {links.map((e) => {
              const other = e.a === node.id ? e.b : e.a;
              // An edge drawn joins two nodes drawn: each connection is a link to the thing it
              // joins, on that thing's page.
              const otherNode = graph.nodes.find((n) => n.id === other) as GraphNode;
              return (
                <Link
                  key={e.id}
                  to={nodeHref(otherNode)}
                  className="p3d-row p3d-row-compact"
                  onMouseEnter={() => setHover({ kind: "edge", id: e.id })}
                  onMouseLeave={() => setHover(null)}
                  data-testid={`${testId}-link-${e.id}`}
                >
                  <span className="p3d-row-name is-wrap">
                    {e.a === node.id ? "→" : "←"}
                    {"\u00a0"}
                    <BreakableName name={name(other)} />
                    <small>{e.label ?? t(`graph3d.edge_${e.kind}`)}</small>
                  </span>
                  {(e.paths ?? 1) > 1 && (
                    <span className="p3d-num">{t("graph3d.paths", { count: e.paths })}</span>
                  )}
                </Link>
              );
            })}
          </div>
          <Group gap="xs">
            <Button
              size="xs"
              variant="default"
              onClick={() => onFocus(node.id)}
              data-testid={`${testId}-zoom`}
            >
              {t("graph3d.zoomTo")}
            </Button>
            {node.type === "table" && (
              <Button
                size="xs"
                variant="default"
                component={Link}
                to={erdHref(node)}
                data-testid={`${testId}-open-in-erd`}
              >
                {t("graph3d.openInErd")}
              </Button>
            )}
            {hasColumns && (
              <Button
                size="xs"
                variant="default"
                onClick={() => toggleExpanded(node.id)}
                data-testid={`${testId}-columns`}
              >
                {expanded.has(node.id) ? t("graph3d.foldColumns") : t("graph3d.showColumns")}
              </Button>
            )}
          </Group>
        </section>
      )}
      <section>
        <h5 className="p3d-section-title">{t("graph3d.shown")}</h5>
        <div className="p3d-list">
          {counts.map(({ key, type, count }) => (
            <div key={key} className="p3d-row p3d-row-compact">
              <span className="p3d-row-name is-wrap">
                <i className={`p3d-dot g3d-sw-${type}`} />
                <BreakableName name={t(key)} />
              </span>
              <span className="p3d-num">{count}</span>
            </div>
          ))}
        </div>
      </section>
      <p className="p3d-note">{t(folds ? "graph3d.note" : "graph3d.noteUnfolded")}</p>
    </div>
  );
}
