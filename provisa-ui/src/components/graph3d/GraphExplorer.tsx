// Copyright (c) 2026 Kenneth Stott
// Canary: 4f6c2a83-9d15-4b7e-8a3c-e1d7b0f5c926
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2128: the full data product graph — one 3D network whose contents are layers the user turns
// on and off, filtered by domain, tag, sensitivity and a focus, under named presets that set the
// layers and the layout. A layer the data has nothing in (the role cannot read it) is not offered.

import { useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { Button, Chip, Group, SegmentedControl, Text } from "@mantine/core";
import { SummaryMultiSelect } from "./SummaryMultiSelect";
import { AttractionSlider } from "../../pages/tables/profile3d/Attraction";
import { ATTRACTION_MAX, ATTRACTION_MIN } from "../../pages/tables/profile3d/attractionState";
import { GridToggle } from "../../pages/tables/profile3d/GridToggle";
import { ResetSettingsButton } from "../../pages/tables/profile3d/ResetSettingsButton";
import {
  chosenNow,
  selectionToSave,
  useGraphSettings,
  type GraphKey,
} from "../../pages/tables/profile3d/graphSettings";
import { matchesSearch, searchItems } from "../../lib/searchItems";
import { ClampedDetail } from "../../pages/data-products/ClampedDetail";
import type { CameraCommand } from "../../pages/tables/profile3d/Stage3D";
import { ExpandButton } from "../../pages/tables/profile3d/Expandable3D";
import {
  NO_FILTERS,
  availableLayers,
  edgesOf,
  filterChoices,
  layoutGraph,
  visibleGraph,
  type GraphData,
  type GraphFilters,
  type GraphEdge,
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
import { kindKey } from "./nodeLook";
import { OpenAsQuery } from "./OpenAsQuery";
import { LayersMenu, TermsMenu } from "./OntologyMenus";
import "../../pages/tables/profile3d/profile3d.css";
import "./graph3d.css";

/** The statuses a term can be filtered by (REQ-2130); a live term is always drawn. */
const STATUSES: TermStatus[] = ["proposed", "deprecated", "retired"];

/** Layers toggled beside the filters rather than in the Layers row: the Ontology View's Columns,
 * and its Tables, which draws the tables of the columns drawn and so goes with Columns. */
const BESIDE_FILTERS: ReadonlySet<LayerId> = new Set(["columns", "ownerTables"]);

export function GraphExplorer({
  data,
  scope,
  preset: initialPreset,
  focus: initialFocus = null,
  zero,
  onExpand,
  search,
  settingsKey,
  exportName,
  query,
  testId = "graph3d",
}: {
  data: GraphData;
  scope: GraphScope;
  preset: PresetId;
  focus?: string | null;
  /** The member tables lineage lanes are drawn around (lane 0). */
  zero: string[];
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
  };
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
  const choices = useMemo(() => filterChoices(every), [every]);
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
    layers: presetOf(initialPreset).layers as string[],
    // Terms left with no relationship under the chosen edge types and domains: drawn until unchecked.
    orphans: true as boolean,
    rotate: false as boolean,
    attraction: 1 as number,
    grid: true as boolean,
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
  // A layer the graph always draws is on whatever was remembered (an old "ontology off" too).
  const layers = useMemo(
    () =>
      new Set([
        ...scope.implicitLayers,
        ...savedLayers.filter((l): l is LayerId => scope.layerOrder.includes(l as LayerId)),
      ]),
    [savedLayers, scope],
  );
  const orphans = saved.orphans as boolean;
  const rotate = saved.rotate as boolean;
  const attraction = Math.min(ATTRACTION_MAX, Math.max(ATTRACTION_MIN, saved.attraction as number));
  const grid = saved.grid as boolean;
  const [focus, setFocus] = useState<string[]>(initialFocus ? [initialFocus] : []);
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
  const [expanded, setExpanded] = useState<Set<string>>(new Set());
  const [frame, setFrame] = useState(0);
  const [picked, setSelected] = useState<string | null>(initialFocus);
  const [hover, setHover] = useState<GraphHover | null>(null);
  const [command, setCommand] = useState<CameraCommand | null>(null);
  const move = (kind: CameraCommand["kind"], target: string | null) =>
    setCommand((c) => ({ kind, target, seq: c == null ? 1 : c.seq + 1 }));

  // Tables are the tables of the columns drawn: without Columns, there are none to draw.
  const on = useMemo(
    () =>
      new Set(
        [...layers].filter((l) => offered.has(l) && (l !== "ownerTables" || layers.has("columns"))),
      ),
    [layers, offered],
  );
  // Every choice chosen is no narrowing at all: the graph is drawn as if unfiltered, nodes that
  // carry no domain included.
  const narrowed = useMemo(() => {
    const all = (chosen: string[], among: string[]) =>
      among.length > 0 && among.every((v) => chosen.includes(v)) ? [] : chosen;
    return {
      domains: all(filters.domains, choices.domains),
      edgeTypes: all(filters.edgeTypes, edgeTypes),
      tags: all(filters.tags, choices.tags),
      sensitivities: all(filters.sensitivities, choices.sensitivities),
    };
  }, [filters, choices, edgeTypes]);
  // The scope the query reads, reported whenever it changes (a key, so an equal one is not news).
  const onScope = query?.onScope;
  const scopeKey = JSON.stringify([
    [...on].sort(),
    narrowed.domains,
    narrowed.edgeTypes,
    filters.hiddenStatuses,
    orphans,
    narrowed.tags,
    narrowed.sensitivities,
    searchItems(search ?? ""),
  ]);
  useEffect(() => {
    if (!onScope) return;
    const [l, domains, types, statuses, kept, tags, sensitivities, items] = JSON.parse(
      scopeKey,
    ) as [LayerId[], string[], string[], TermStatus[], boolean, string[], string[], string[]];
    onScope({
      layers: new Set(l),
      domains,
      edgeTypes: types,
      hiddenStatuses: statuses,
      orphans: kept,
      tags,
      sensitivities,
      search: items,
    });
  }, [onScope, scopeKey]);

  // REQ-2143: the page's search is the query's own (its root filter and its neighbourhood); a node
  // focused by the focus field (the product graph's) is a neighbourhood no query states yet.
  const focused = filters.focus.some((f) => f !== query?.own);

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
    const narrowing = {
      ...filters,
      focus: matched ?? filters.focus,
      ...(query
        ? { domains: [], edgeTypes: [], hiddenStatuses: [], tags: [], sensitivities: [] }
        : narrowed),
    };
    const g = visibleGraph(data, on, narrowing, expanded, scope);
    // Under a query its Orphans is its own too.
    if (orphans || query) return g;
    // An orphan has no term relationship drawn, and no column it IS while Columns is on; its
    // domain does not count.
    const related = (e: GraphEdge) => e.kind === "term_edge" || e.kind === "term_binding";
    const joined = new Set(g.edges.filter(related).flatMap((e) => [e.a, e.b]));
    const nodes = g.nodes.filter((n) => n.type !== "term" || joined.has(n.id));
    const kept = new Set(nodes.map((n) => n.id));
    // With an orphan go the edges that only placed it (its domain).
    return { nodes, edges: g.edges.filter((e) => kept.has(e.a) && kept.has(e.b)) };
  }, [data, on, filters, narrowed, query, expanded, scope, orphans, search]);
  const layout = presetOf(preset).layout;
  const at = useMemo(
    () => layoutGraph(shown, layout, zero, attraction),
    [shown, layout, zero, attraction],
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
    setFrame((f) => f + 1);
  };
  const toggleLayer = (l: LayerId, on: boolean) =>
    save("layers", on ? [...layers, l] : [...layers].filter((x) => x !== l));

  const selected = shown.nodes.some((n) => n.id === picked) ? picked : null;
  // In the overview a domain picked is the domain the graph is narrowed to.
  const select = (id: string | null) => {
    if (query?.overview && id !== null && id.startsWith("d:")) {
      setFilters((f) => ({ ...f, domains: [id.slice(2)] }));
      return;
    }
    setSelected(id);
  };
  const s: StageState = {
    graph: shown,
    at,
    frame,
    frontal: layout !== "force",
    selected,
    setSelected: select,
    hover,
    setHover,
    rotate,
    setRotate,
    grid,
  };

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
        <Chip size="xs" checked={rotate} onChange={setRotate} data-testid={`${testId}-rotate`}>
          {t("profile3d.rotate")}
        </Chip>
        <GridToggle value={grid} onChange={setGrid} testId={testId} />
        <Button
          size="xs"
          variant="default"
          onClick={() => move("reset", null)}
          data-testid={`${testId}-reset`}
        >
          {t("profile3d.resetView")}
        </Button>
        {layout === "force" && (
          <AttractionSlider value={attraction} onChange={setAttraction} testId={testId} />
        )}
        <ResetSettingsButton onReset={resetSettings} testId={testId} />
        {query && <OpenAsQuery query={query.text} focused={focused} testId={testId} />}
        {onExpand && <ExpandButton onClick={onExpand} testId={`${testId}-expand`} />}
      </Group>
      {!scope.menus && (
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
        {scope.menus && (
          <LayersMenu
            offered={scope.layerOrder.filter(
              (l) => offered.has(l) && !scope.implicitLayers.includes(l),
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
        {!scope.menus && offered.has("columns") && (
          <Chip
            size="xs"
            checked={layers.has("columns")}
            onChange={(on) => toggleLayer("columns", on)}
            data-testid={`${testId}-columns`}
          >
            {t("graph3d.columnsToggle")}
          </Chip>
        )}
        {!scope.menus && offered.has("ownerTables") && (
          <Chip
            size="xs"
            checked={layers.has("ownerTables")}
            disabled={!layers.has("columns")}
            onChange={(on) => toggleLayer("ownerTables", on)}
            data-testid={`${testId}-tables`}
          >
            {t("graph3d.tablesToggle")}
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
        {query?.notice ? (
          <div className="p3d-stage g3d-empty" data-testid={`${testId}-over-budget`}>
            <Text size="sm" c="dimmed">
              {query.notice}
            </Text>
          </div>
        ) : shown.nodes.length === 0 ? (
          <div className="p3d-stage g3d-empty" data-testid={`${testId}-empty`}>
            <Text size="sm" c="dimmed">
              {t("graph3d.empty")}
            </Text>
          </div>
        ) : (
          <GraphStage
            s={s}
            command={command}
            exportName={preset === exportName ? exportName : `${exportName}-${preset}`}
            testId={`${testId}-stage`}
          />
        )}
        <GraphDetail
          graph={shown}
          data={data}
          selected={selected}
          setSelected={setSelected}
          setHover={setHover}
          expanded={expanded}
          folds={scope.foldColumns}
          toggleExpanded={(id) =>
            setExpanded((prev) => {
              const next = new Set(prev);
              if (!next.delete(id)) next.add(id);
              return next;
            })
          }
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
  setSelected,
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
  setSelected: (id: string) => void;
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
  const hasColumns =
    folds &&
    node?.type === "table" &&
    data.nodes.some((n) => n.type === "column" && n.parent === node.id);
  // Each kind drawn, named as its look shows it, with how many.
  const counts = useMemo(() => {
    const by = new Map<string, { key: string; type: string; count: number }>();
    for (const n of graph.nodes) {
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
            <h4>{node.label}</h4>
            <span className="p3d-kind">{t(kindKey(node))}</span>
          </div>
          {node.definition && (
            <ClampedDetail testId={`${testId}-definition`}>
              <Text size="xs" c="dimmed">
                {node.definition}
              </Text>
            </ClampedDetail>
          )}
          {node.identityGap && (
            <Text size="xs" c="orange" data-testid={`${testId}-identity-gap`}>
              {t("graph3d.identityGap", { tables: node.identityGap.join(", ") })}
            </Text>
          )}
          <div className="p3d-list">
            {links.map((e) => {
              const other = e.a === node.id ? e.b : e.a;
              return (
                <button
                  type="button"
                  key={e.id}
                  className="p3d-row p3d-row-link"
                  onClick={() => setSelected(other)}
                  onMouseEnter={() => setHover({ kind: "edge", id: e.id })}
                  onMouseLeave={() => setHover(null)}
                  data-testid={`${testId}-link-${e.id}`}
                >
                  <span className="p3d-row-name">
                    {e.a === node.id ? "→" : "←"} {name(other)}
                    <small>{e.label ?? t(`graph3d.edge_${e.kind}`)}</small>
                  </span>
                  {(e.paths ?? 1) > 1 && (
                    <span className="p3d-num">{t("graph3d.paths", { count: e.paths })}</span>
                  )}
                </button>
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
              <span className="p3d-row-name">
                <i className={`p3d-dot g3d-sw-${type}`} />
                {t(key)}
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
