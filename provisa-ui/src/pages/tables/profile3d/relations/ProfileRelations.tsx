// Copyright (c) 2026 Kenneth Stott
// Canary: 59e1b91f-1a5f-4440-a7db-f69142452b9f
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2124: the relationships map — a run's columns floating in 3D, pulled together by their
// correlations and their dependency network, beside the selected column's relationships listed.

import { useEffect, useMemo, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { Button, Chip, Group, SegmentedControl, Text } from "@mantine/core";
import type { ProfileRunResults } from "../../../../api/profiler";
import { ExpandButton, Expandable3D } from "../Expandable3D";
import { AttractionSlider } from "../Attraction";
import { ATTRACTION_MAX, ATTRACTION_MIN } from "../attractionState";
import { GridToggle } from "../GridToggle";
import { ResetSettingsButton } from "../ResetSettingsButton";
import { useGraphSettings } from "../graphSettings";
import { Stage3D, type CameraCommand } from "../Stage3D";
import { useLazyScene } from "../useLazyScene";
import {
  buildRelations,
  edgesOf,
  visibleEdges,
  type EdgeKind,
  type RelEdge,
  type RelationsModel,
} from "./relationsModel";
import type { RelHover, RelationsScene } from "./relationsScene";
import "../profile3d.css";

const STRENGTHS = ["0.1", "0.3", "0.5", "0.7"];

interface Shared {
  /** What the view's exports are named after. */
  exportName: string;
  model: RelationsModel;
  edges: RelEdge[];
  attraction: number;
  setAttraction: (v: number) => void;
  min: string;
  setMin: (v: string) => void;
  kinds: Set<EdgeKind>;
  toggleKind: (k: "correlation" | "dependence", on: boolean) => void;
  rotate: boolean;
  setRotate: (on: boolean) => void;
  grid: boolean;
  setGrid: (shown: boolean) => void;
  selected: string | null;
  setSelected: (id: string) => void;
  hover: RelHover | null;
  setHover: (h: RelHover | null) => void;
  resetSettings: () => void;
}

const KINDS: readonly EdgeKind[] = ["spearman", "correlation_ratio", "dependence"];

const edgeValue = (e: RelEdge) => e.value.toFixed(2);

function RelationsStage({ s, command }: { s: Shared; command: CameraCommand | null }) {
  const { t } = useTranslation();
  const [tip, setTip] = useState<{ h: RelHover; x: number; y: number } | null>(null);
  const latest = useRef(s);
  useEffect(() => {
    latest.current = s;
  });
  const { wrapRef, canvasRef, labelsRef, scene, error } = useLazyScene<RelationsScene>(
    (canvas, labels, theme) =>
      import("./relationsScene").then(({ RelationsScene }) => {
        const sc = new RelationsScene(
          canvas,
          labels,
          {
            onHover: (h, x, y) => {
              setTip(h ? { h, x, y } : null);
              latest.current.setHover(h);
            },
            onSelect: (h) => {
              if (h.kind === "node") latest.current.setSelected(h.id);
            },
            onUserMove: () => latest.current.setRotate(false),
          },
          theme,
        );
        sc.setGrid(latest.current.grid);
        sc.setData(latest.current.model, latest.current.edges, true);
        return sc;
      }),
  );
  // A new layout (the attraction moved) is drawn where the camera already is.
  const laid = useRef<RelationsModel | null>(null);
  useEffect(() => {
    if (!scene) return;
    if (laid.current === null) laid.current = latest.current.model;
    else if (laid.current !== s.model) {
      laid.current = s.model;
      scene.setData(s.model, latest.current.edges, false);
    }
  }, [scene, s.model]);
  const shown = useRef<RelEdge[] | null>(null);
  useEffect(() => {
    if (!scene) return;
    if (shown.current === null) shown.current = latest.current.edges;
    else if (shown.current !== s.edges) {
      shown.current = s.edges;
      scene.setEdges(s.edges);
    }
  }, [scene, s.edges]);
  useEffect(() => scene?.setSelected(s.selected), [scene, s.selected]);
  useEffect(() => scene?.setHover(s.hover), [scene, s.hover]);
  useEffect(() => scene?.setRotate(s.rotate), [scene, s.rotate]);
  useEffect(() => scene?.setGrid(s.grid), [scene, s.grid]);
  useEffect(() => {
    if (!scene || command == null) return;
    if (command.kind === "reset") scene.resetView();
    else if (command.target != null) scene.focus(command.target);
  }, [scene, command]);

  let body = null;
  if (tip?.h.kind === "node") {
    const node = s.model.nodes.find((n) => n.id === tip.h.id);
    if (node) {
      body = (
        <>
          <b>{node.id}</b>
          <span>{t(`profileRelations.family_${node.family}`)}</span>
          <span className="p3d-num">
            {t("profileRelations.tipLinks", { count: edgesOf(s.edges, node.id).length })}
          </span>
        </>
      );
    }
  } else if (tip?.h.kind === "edge") {
    const e = s.edges.find((x) => x.id === tip.h.id);
    if (e) {
      body = (
        <>
          <b>{e.kind === "dependence" ? `${e.a} → ${e.b}` : `${e.a} ↔ ${e.b}`}</b>
          <span>{t(`profileRelations.measure_${e.kind}`)}</span>
          <span className="p3d-num">{edgeValue(e)}</span>
        </>
      );
    }
  }

  return (
    <Stage3D
      wrapRef={wrapRef}
      canvasRef={canvasRef}
      labelsRef={labelsRef}
      ready={scene != null}
      error={error}
      canvasLabel={t("profileRelations.canvasLabel")}
      tip={tip && body ? { x: tip.x, y: tip.y, body } : null}
      exportable={scene}
      exportName={s.exportName}
      testId="relations-stage"
      legend={
        <>
          <span>
            <i className="p3d-sw p3d-sw-cool" />
            {t("profileRelations.legendPositive")}
          </span>
          <span>
            <i className="p3d-sw p3d-sw-warm" />
            {t("profileRelations.legendNegative")}
          </span>
          <span>
            <i className="p3d-sw p3d-sw-tail" />
            {t("profileRelations.legendRatio")}
          </span>
          <span>
            <i className="p3d-sw p3d-sw-hl" />
            {t("profileRelations.legendDependence")}
          </span>
        </>
      }
    />
  );
}

function RelationsDetail({ s, onFocus }: { s: Shared; onFocus: (id: string) => void }) {
  const { t } = useTranslation();
  const node = s.model.nodes.find((n) => n.id === s.selected);
  const links = node ? edgesOf(s.edges, node.id) : [];
  const ranked = useMemo(
    () =>
      s.model.nodes
        .map((n) => ({ id: n.id, family: n.family, links: edgesOf(s.edges, n.id).length }))
        .sort((a, b) => b.links - a.links || a.id.localeCompare(b.id)),
    [s.model.nodes, s.edges],
  );
  return (
    <div className="p3d-detail">
      {node && (
        <section data-testid="relations-detail" aria-live="polite">
          <div className="p3d-col-head">
            <h4>{node.id}</h4>
            <span className="p3d-kind">{t(`profileRelations.family_${node.family}`)}</span>
          </div>
          {links.length === 0 ? (
            <p className="p3d-note">{t("profileRelations.noLinks")}</p>
          ) : (
            <div className="p3d-list">
              {links.map((e) => {
                const other = e.a === node.id ? e.b : e.a;
                const arrow = e.kind !== "dependence" ? "↔" : e.b === node.id ? "←" : "→";
                return (
                  <button
                    type="button"
                    key={e.id}
                    className="p3d-row p3d-row-link"
                    onClick={() => s.setSelected(other)}
                    onMouseEnter={() => s.setHover({ kind: "edge", id: e.id })}
                    onMouseLeave={() => s.setHover(null)}
                    data-testid={`relations-link-${e.id}`}
                  >
                    <span className="p3d-row-name">
                      {arrow} {other}
                      <small>{t(`profileRelations.measure_${e.kind}`)}</small>
                    </span>
                    <span className="p3d-strength">
                      <span
                        className={`p3d-strength-bar p3d-edge-${e.kind === "spearman" ? (e.value >= 0 ? "pos" : "neg") : e.kind}`}
                        style={{ width: `${(e.strength * 100).toFixed(0)}%` }}
                      />
                    </span>
                    <span className="p3d-num">{edgeValue(e)}</span>
                  </button>
                );
              })}
            </div>
          )}
          <div>
            <Button
              size="xs"
              variant="default"
              onClick={() => onFocus(node.id)}
              data-testid="relations-focus"
            >
              {t("profile3d.zoomTo")}
            </Button>
          </div>
        </section>
      )}
      <section>
        <h5 className="p3d-section-title">{t("profileRelations.columns")}</h5>
        <div className="p3d-list">
          {ranked.map((n) => (
            <button
              type="button"
              key={n.id}
              className="p3d-row p3d-row-compact"
              aria-pressed={n.id === s.selected}
              onClick={() => s.setSelected(n.id)}
              onMouseEnter={() => s.setHover({ kind: "node", id: n.id })}
              onMouseLeave={() => s.setHover(null)}
              data-testid={`relations-row-${n.id}`}
            >
              <span className="p3d-row-name">
                <i className={`p3d-dot p3d-fam-${n.family}`} />
                {n.id}
              </span>
              <span className="p3d-num">{t("profileRelations.tipLinks", { count: n.links })}</span>
            </button>
          ))}
        </div>
      </section>
      <p className="p3d-note">{t("profileRelations.note")}</p>
    </div>
  );
}

function RelationsView({
  s,
  expanded,
  onExpand,
}: {
  s: Shared;
  expanded: boolean;
  onExpand?: () => void;
}) {
  const { t } = useTranslation();
  const [command, setCommand] = useState<CameraCommand | null>(null);
  const move = (kind: CameraCommand["kind"], target: string | null) =>
    setCommand((c) => ({ kind, target, seq: c == null ? 1 : c.seq + 1 }));
  return (
    <div
      className={`p3d${expanded ? " is-expanded" : ""}`}
      data-testid={expanded ? "relations-expanded" : "relations"}
    >
      <Group className="p3d-toolbar" gap="xs" wrap="wrap">
        <Text size="xs" c="dimmed">
          {t("profileRelations.minStrength")}
        </Text>
        <SegmentedControl
          size="xs"
          value={s.min}
          onChange={s.setMin}
          data={STRENGTHS.map((v) => ({ value: v, label: `≥ ${v}` }))}
          aria-label={t("profileRelations.minStrength")}
          data-testid="relations-min"
        />
        <Chip
          size="xs"
          checked={s.kinds.has("spearman")}
          onChange={(on) => s.toggleKind("correlation", on)}
          data-testid="relations-correlation"
        >
          {t("profileRelations.showCorrelation")}
        </Chip>
        <Chip
          size="xs"
          checked={s.kinds.has("dependence")}
          onChange={(on) => s.toggleKind("dependence", on)}
          data-testid="relations-dependence"
        >
          {t("profileRelations.showDependence")}
        </Chip>
        <AttractionSlider value={s.attraction} onChange={s.setAttraction} testId="relations" />
        <Chip size="xs" checked={s.rotate} onChange={s.setRotate} data-testid="relations-rotate">
          {t("profile3d.rotate")}
        </Chip>
        <GridToggle value={s.grid} onChange={s.setGrid} testId="relations" />
        <Button
          size="xs"
          variant="default"
          onClick={() => move("reset", null)}
          data-testid="relations-reset"
        >
          {t("profile3d.resetView")}
        </Button>
        <ResetSettingsButton onReset={s.resetSettings} testId="relations" />
        {onExpand && <ExpandButton onClick={onExpand} testId="relations-expand" />}
      </Group>
      <div className="p3d-body">
        <RelationsStage s={s} command={command} />
        <RelationsDetail
          s={s}
          onFocus={(id) => {
            s.setRotate(false);
            move("focus", id);
          }}
        />
      </div>
    </div>
  );
}

export function ProfileRelations({
  results,
  tableId,
  tableName,
}: {
  results: ProfileRunResults;
  tableId: number;
  tableName: string;
}) {
  const { t } = useTranslation();
  // Remembered for this table's relationship graph (graphSettings.ts).
  const [saved, save, resetSettings] = useGraphSettings(
    { kind: "profile-relations", subject: tableId },
    {
      attraction: 1 as number,
      min: "0.3",
      kinds: [...KINDS] as string[],
      rotate: true as boolean,
      grid: true as boolean,
    },
  );
  const attraction = Math.min(ATTRACTION_MAX, Math.max(ATTRACTION_MIN, saved.attraction as number));
  const setAttraction = (v: number) => save("attraction", v);
  const model = useMemo(() => buildRelations(results, attraction), [results, attraction]);
  const min = STRENGTHS.includes(saved.min as string) ? (saved.min as string) : "0.3";
  const setMin = (v: string) => save("min", v);
  const savedKinds = saved.kinds as string[];
  const kinds = useMemo(
    () => new Set(savedKinds.filter((k): k is EdgeKind => KINDS.includes(k as EdgeKind))),
    [savedKinds],
  );
  const rotate = saved.rotate as boolean;
  const setRotate = (on: boolean) => save("rotate", on);
  const grid = saved.grid as boolean;
  const setGrid = (on: boolean) => save("grid", on);
  const [picked, setSelected] = useState<string | null>(null);
  const [hover, setHover] = useState<RelHover | null>(null);
  const edges = useMemo(() => visibleEdges(model, Number(min), kinds), [model, min, kinds]);

  if (model.edges.length === 0) {
    return (
      <Text size="sm" c="dimmed" data-testid="relations-empty">
        {t("profileRelations.none")}
      </Text>
    );
  }
  // Until a column is picked, the one with the most relationships shown is.
  const busiest = [...model.nodes].sort(
    (a, b) => edgesOf(edges, b.id).length - edgesOf(edges, a.id).length,
  )[0];
  const selected = model.nodes.some((n) => n.id === picked) ? picked : busiest.id;
  const s: Shared = {
    exportName: `${tableName}-relations`,
    model,
    edges,
    attraction,
    setAttraction,
    min,
    setMin,
    kinds,
    toggleKind: (k, on) => {
      const next = new Set(kinds);
      const members: EdgeKind[] =
        k === "correlation" ? ["spearman", "correlation_ratio"] : ["dependence"];
      for (const m of members) {
        if (on) next.add(m);
        else next.delete(m);
      }
      save("kinds", [...next]);
    },
    rotate,
    setRotate,
    grid,
    setGrid,
    resetSettings,
    selected,
    setSelected,
    hover,
    setHover,
  };
  return (
    <Expandable3D
      title={t("profileRelations.expandTitle", { table: tableName })}
      testId="relations"
      render={(expanded, onExpand) => (
        <RelationsView s={s} expanded={expanded} onExpand={onExpand} />
      )}
    />
  );
}
