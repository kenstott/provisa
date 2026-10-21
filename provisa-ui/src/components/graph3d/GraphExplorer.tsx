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

import { useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import { Button, Chip, Group, MultiSelect, SegmentedControl, Select, Text } from "@mantine/core";
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
  type GraphScope,
  type LayerId,
  type Preset,
  type PresetId,
  type TermStatus,
} from "./graphModel";
import type { GraphHover } from "./graphScene";
import { GraphStage, type StageState } from "./GraphStage";
import "../../pages/tables/profile3d/profile3d.css";
import "./graph3d.css";

/** The statuses a term can be filtered by (REQ-2130); a live term is always drawn. */
const STATUSES: TermStatus[] = ["proposed", "deprecated", "retired"];

export function GraphExplorer({
  data,
  scope,
  preset: initialPreset,
  focus: initialFocus = null,
  zero,
  onExpand,
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
  testId?: string;
}) {
  const { t } = useTranslation();
  const presetOf = (p: PresetId): Preset => {
    const found = scope.presets[p];
    if (!found) throw new Error(`No ${p} preset in this graph`);
    return found;
  };
  const offered = useMemo(() => availableLayers(data, scope), [data, scope]);
  const choices = useMemo(() => filterChoices(data), [data]);
  const edgeTypes = useMemo(
    () =>
      [...new Set(data.edges.filter((e) => e.kind === "term_edge").map((e) => e.label ?? ""))]
        .filter(Boolean)
        .sort(),
    [data],
  );
  const hasStatus = useMemo(() => data.nodes.some((n) => n.status !== undefined), [data]);
  const [preset, setPreset] = useState<PresetId>(initialPreset);
  const [layers, setLayers] = useState<Set<LayerId>>(new Set(presetOf(initialPreset).layers));
  const [filters, setFilters] = useState<GraphFilters>({
    ...NO_FILTERS,
    focus: initialFocus,
    edgeTypes: presetOf(initialPreset).edgeTypes ?? [],
    hiddenStatuses: scope.hiddenStatuses,
  });
  const [expanded, setExpanded] = useState<Set<string>>(new Set());
  const [frame, setFrame] = useState(0);
  const [picked, setSelected] = useState<string | null>(initialFocus);
  const [hover, setHover] = useState<GraphHover | null>(null);
  const [rotate, setRotate] = useState(false);
  const [command, setCommand] = useState<CameraCommand | null>(null);
  const move = (kind: CameraCommand["kind"], target: string | null) =>
    setCommand((c) => ({ kind, target, seq: c == null ? 1 : c.seq + 1 }));

  const shown = useMemo(() => {
    const on = new Set([...layers].filter((l) => offered.has(l)));
    return visibleGraph(data, on, filters, expanded, scope);
  }, [data, layers, offered, filters, expanded, scope]);
  const layout = presetOf(preset).layout;
  const at = useMemo(() => layoutGraph(shown, layout, zero), [shown, layout, zero]);
  const focusable = useMemo(
    () =>
      data.nodes
        .filter((n) => n.type === "product" || n.type === "term")
        .map((n) => ({ value: n.id, label: n.label }))
        .sort((a, b) => a.label.localeCompare(b.label)),
    [data],
  );

  const choosePreset = (p: PresetId) => {
    setPreset(p);
    setLayers(new Set(presetOf(p).layers));
    setFilters((f) => ({ ...f, edgeTypes: presetOf(p).edgeTypes ?? [] }));
    setFrame((f) => f + 1);
  };
  const toggleLayer = (l: LayerId, on: boolean) =>
    setLayers((prev) => {
      const next = new Set(prev);
      if (on) next.add(l);
      else next.delete(l);
      return next;
    });

  const selected = shown.nodes.some((n) => n.id === picked) ? picked : null;
  const s: StageState = {
    graph: shown,
    at,
    frame,
    selected,
    setSelected,
    hover,
    setHover,
    rotate,
    setRotate,
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
        <Button
          size="xs"
          variant="default"
          onClick={() => move("reset", null)}
          data-testid={`${testId}-reset`}
        >
          {t("profile3d.resetView")}
        </Button>
        {onExpand && <ExpandButton onClick={onExpand} testId={`${testId}-expand`} />}
      </Group>
      <Group className="p3d-toolbar" gap="xs" wrap="wrap" data-testid={`${testId}-layers`}>
        <Text size="xs" c="dimmed">
          {t("graph3d.layers")}
        </Text>
        {scope.layerOrder
          .filter((l) => offered.has(l))
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
      <Group className="p3d-toolbar g3d-filters" gap="xs" wrap="wrap">
        {hasStatus && (
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
          <MultiSelect
            size="xs"
            placeholder={t("graph3d.filterEdgeType")}
            aria-label={t("graph3d.filterEdgeType")}
            data={edgeTypes}
            value={filters.edgeTypes}
            onChange={(types) => setFilters((f) => ({ ...f, edgeTypes: types }))}
            clearable
            data-testid={`${testId}-filter-edge-type`}
          />
        )}
        <MultiSelect
          size="xs"
          placeholder={t("graph3d.filterDomain")}
          aria-label={t("graph3d.filterDomain")}
          data={choices.domains}
          value={filters.domains}
          onChange={(domains) => setFilters((f) => ({ ...f, domains }))}
          clearable
          data-testid={`${testId}-filter-domain`}
        />
        {/* A filter with nothing to choose from is not offered. */}
        {choices.tags.length > 0 && (
          <MultiSelect
            size="xs"
            placeholder={t("graph3d.filterTag")}
            aria-label={t("graph3d.filterTag")}
            data={choices.tags}
            value={filters.tags}
            onChange={(tags) => setFilters((f) => ({ ...f, tags }))}
            clearable
            data-testid={`${testId}-filter-tag`}
          />
        )}
        {choices.sensitivities.length > 0 && (
          <MultiSelect
            size="xs"
            placeholder={t("graph3d.filterSensitivity")}
            aria-label={t("graph3d.filterSensitivity")}
            data={choices.sensitivities}
            value={filters.sensitivities}
            onChange={(sensitivities) => setFilters((f) => ({ ...f, sensitivities }))}
            clearable
            data-testid={`${testId}-filter-sensitivity`}
          />
        )}
        <Select
          size="xs"
          placeholder={t("graph3d.filterFocus")}
          aria-label={t("graph3d.filterFocus")}
          data={focusable}
          value={filters.focus}
          onChange={(focus) => setFilters((f) => ({ ...f, focus }))}
          searchable
          clearable
          data-testid={`${testId}-filter-focus`}
        />
      </Group>
      <div className="p3d-body">
        {shown.nodes.length === 0 ? (
          <div className="p3d-stage g3d-empty" data-testid={`${testId}-empty`}>
            <Text size="sm" c="dimmed">
              {t("graph3d.empty")}
            </Text>
          </div>
        ) : (
          <GraphStage s={s} command={command} testId={`${testId}-stage`} />
        )}
        <GraphDetail
          graph={shown}
          data={data}
          selected={selected}
          setSelected={setSelected}
          setHover={setHover}
          expanded={expanded}
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
  toggleExpanded: (id: string) => void;
  onFocus: (id: string) => void;
  testId: string;
}) {
  const { t } = useTranslation();
  const node = graph.nodes.find((n) => n.id === selected);
  const name = (id: string) => graph.nodes.find((n) => n.id === id)?.label ?? id;
  const links = node ? edgesOf(graph, node.id) : [];
  const hasColumns =
    node?.type === "table" && data.nodes.some((n) => n.type === "column" && n.parent === node.id);
  const counts = useMemo(() => {
    const by = new Map<string, number>();
    for (const n of graph.nodes) by.set(n.type, (by.get(n.type) ?? 0) + 1);
    return [...by.entries()].sort((a, b) => b[1] - a[1]);
  }, [graph]);
  return (
    <div className="p3d-detail">
      {node && (
        <section data-testid={`${testId}-detail`} aria-live="polite">
          <div className="p3d-col-head">
            <h4>{node.label}</h4>
            <span className="p3d-kind">{t(`graph3d.type_${node.type}`)}</span>
          </div>
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
          {counts.map(([type, count]) => (
            <div key={type} className="p3d-row p3d-row-compact">
              <span className="p3d-row-name">
                <i className={`p3d-dot g3d-sw-${type}`} />
                {t(`graph3d.type_${type}`)}
              </span>
              <span className="p3d-num">{count}</span>
            </div>
          ))}
        </div>
      </section>
      <p className="p3d-note">{t("graph3d.note")}</p>
    </div>
  );
}
