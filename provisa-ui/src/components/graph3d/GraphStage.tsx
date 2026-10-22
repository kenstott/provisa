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

export interface StageState {
  graph: GraphData;
  at: Map<string, [number, number, number]>;
  /** Changes when the graph is laid out afresh (a preset), so the camera is reset to fit it. */
  frame: number;
  selected: string | null;
  setSelected: (id: string) => void;
  hover: GraphHover | null;
  setHover: (h: GraphHover | null) => void;
  rotate: boolean;
  setRotate: (on: boolean) => void;
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
        sc.setData(latest.current.graph, latest.current.at, true);
        return sc;
      }),
  );
  const drawn = useRef<{ graph: GraphData; frame: number } | null>(null);
  useEffect(() => {
    if (!scene) return;
    const was = drawn.current;
    drawn.current = { graph: s.graph, frame: s.frame };
    if (was === null || was.graph === s.graph) return;
    scene.setData(s.graph, s.at, was.frame !== s.frame);
  }, [scene, s.graph, s.at, s.frame]);
  useEffect(() => scene?.setSelected(s.selected), [scene, s.selected]);
  useEffect(() => scene?.setHover(s.hover), [scene, s.hover]);
  useEffect(() => scene?.setRotate(s.rotate), [scene, s.rotate]);
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
          <span>{t(`graph3d.type_${node.type}`)}</span>
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
      legend={LEGEND.filter((type) => s.graph.nodes.some((n) => n.type === type)).map((type) => (
        <span key={type}>
          <i className={`p3d-sw g3d-sw-${type}`} />
          {t(`graph3d.type_${type}`)}
        </span>
      ))}
    />
  );
}
