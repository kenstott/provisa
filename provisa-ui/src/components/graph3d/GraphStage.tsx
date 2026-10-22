// Copyright (c) 2026 Kenneth Stott
// Canary: 7d3a9e15-2b6c-4f81-9e4a-0c5d8b2f6e39
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2128: the data product graph's canvas — the scene loaded on first use, redrawn as the graph
// shown changes, with its hover card and legend.

import { useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { Stage3D, type CameraCommand } from "../../pages/tables/profile3d/Stage3D";
import { useLazyScene } from "../../pages/tables/profile3d/useLazyScene";
import { edgesOf, type GraphData, type NodeType } from "./graphModel";
import type { GraphHover, GraphScene } from "./graphScene";
import { kindKey, labelClass, nodeLook, type Look } from "./nodeLook";

export interface StageState {
  graph: GraphData;
  at: Map<string, [number, number, number]>;
  /** Changes when the graph is laid out afresh (a preset), so the camera is reset to fit it. */
  frame: number;
  /** A flat layout (tree, lanes) is seen from the front. */
  frontal: boolean;
  selected: string | null;
  setSelected: (id: string) => void;
  hover: GraphHover | null;
  setHover: (h: GraphHover | null) => void;
  rotate: boolean;
  setRotate: (on: boolean) => void;
  /** Whether the floor grid is drawn. */
  grid: boolean;
}

/** The legend's order; it lists only the node types drawn. */
const LEGEND: NodeType[] = [
  "product",
  "domain",
  "table",
  "column",
  "command",
  "checker",
  "profile",
  "term",
  "tag",
  "relation",
];

export function GraphStage({
  s,
  command,
  testId,
}: {
  s: StageState;
  command: CameraCommand | null;
  testId: string;
}) {
  const { t } = useTranslation();
  const [tip, setTip] = useState<{ h: GraphHover; x: number; y: number } | null>(null);
  const latest = useRef(s);
  useEffect(() => {
    latest.current = s;
  });
  const { wrapRef, canvasRef, labelsRef, scene, error } = useLazyScene<GraphScene>(
    (canvas, labels, theme) =>
      import("./graphScene").then(({ GraphScene }) => {
        const sc = new GraphScene(
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
        sc.setFrontal(latest.current.frontal);
        sc.setGrid(latest.current.grid);
        sc.setData(latest.current.graph, latest.current.at, true);
        return sc;
      }),
  );
  const drawn = useRef<{
    graph: GraphData;
    at: Map<string, [number, number, number]>;
    frame: number;
  } | null>(null);
  useEffect(() => {
    if (!scene) return;
    const was = drawn.current;
    drawn.current = { graph: s.graph, at: s.at, frame: s.frame };
    // A new graph, or the same graph laid out anew (the attraction moved), is drawn again.
    if (was === null || (was.graph === s.graph && was.at === s.at)) return;
    scene.setFrontal(s.frontal);
    scene.setData(s.graph, s.at, was.frame !== s.frame);
  }, [scene, s.graph, s.at, s.frame, s.frontal]);
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
    const node = s.graph.nodes.find((n) => n.id === tip.h.id);
    if (node) {
      body = (
        <>
          <b>{node.label}</b>
          <span>{t(kindKey(node))}</span>
          <span className="p3d-num">
            {t("graph3d.tipLinks", { count: edgesOf(s.graph, node.id).length })}
          </span>
        </>
      );
    }
  } else if (tip?.h.kind === "edge") {
    const e = s.graph.edges.find((x) => x.id === tip.h.id);
    if (e) {
      const name = (id: string) => s.graph.nodes.find((n) => n.id === id)?.label ?? id;
      body = (
        <>
          <b>{`${name(e.a)} → ${name(e.b)}`}</b>
          <span>{e.label ?? t(`graph3d.edge_${e.kind}`)}</span>
          {(e.paths ?? 1) > 1 && (
            <span className="p3d-num">{t("graph3d.paths", { count: e.paths })}</span>
          )}
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
      canvasLabel={t("graph3d.canvasLabel")}
      tip={tip && body ? { x: tip.x, y: tip.y, body } : null}
      testId={testId}
      legend={<GraphLegend graph={s.graph} />}
    />
  );
}

/** Each look the graph draws -- a shape for the kind, a colour for the variant -- and, where terms
 * carry a status, the four styles a term's name takes. */
function GraphLegend({ graph }: { graph: GraphData }) {
  const { t } = useTranslation();
  const looks = new Map<string, { type: NodeType } & Look>();
  for (const type of LEGEND)
    for (const n of graph.nodes)
      if (n.type === type) {
        const look = nodeLook(n);
        looks.set(`${type}:${look.variant}`, { type, ...look });
      }
  const statuses = graph.nodes.some((n) => n.status !== undefined);
  return (
    <>
      {[...looks.entries()].map(([key, l]) => (
        <span key={key} data-testid={`legend-${key}`}>
          <i
            className={`g3d-shape g3d-shape-${l.shape}`}
            style={{ background: `var(--p3d-${l.colour})` }}
          />
          {l.variant === "plain" ? t(`graph3d.type_${l.type}`) : t(`graph3d.look_${l.variant}`)}
        </span>
      ))}
      {statuses &&
        (["live", "proposed", "deprecated", "retired"] as const).map((st) => (
          <span
            key={st}
            className={`g3d-legend-status ${labelClass(st)}`.trim()}
            data-testid={`legend-status-${st}`}
          >
            {t(`graph3d.status_${st}`)}
          </span>
        ))}
    </>
  );
}
