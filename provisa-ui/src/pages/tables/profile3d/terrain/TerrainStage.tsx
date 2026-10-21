// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2123: the canvas a profile terrain is drawn on.

import { useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { Stage3D, type CameraCommand } from "../Stage3D";
import { useLazyScene } from "../useLazyScene";
import { pctText, type TerrainColumn } from "./terrainModel";
import type { TerrainHover, TerrainOptions, TerrainScene } from "./terrainScene";

export function TerrainStage({
  columns,
  order,
  options,
  rotate,
  selected,
  hover,
  command,
  onHover,
  onSelect,
  onUserMove,
}: {
  columns: TerrainColumn[];
  order: string[];
  options: TerrainOptions;
  rotate: boolean;
  selected: string | null;
  hover: TerrainHover | null;
  command: CameraCommand | null;
  onHover: (h: TerrainHover | null) => void;
  onSelect: (column: string) => void;
  onUserMove: () => void;
}) {
  const { t } = useTranslation();
  const [tip, setTip] = useState<{ h: TerrainHover; x: number; y: number } | null>(null);
  // The scene is built once; what it calls back must see this render's props.
  const latest = useRef({ columns, order, options, onHover, onSelect, onUserMove });
  useEffect(() => {
    latest.current = { columns, order, options, onHover, onSelect, onUserMove };
  });
  const { wrapRef, canvasRef, labelsRef, scene, error } = useLazyScene<TerrainScene>(
    (canvas, labels, theme) =>
      import("./terrainScene").then(({ TerrainScene }) => {
        const l = latest.current;
        const s = new TerrainScene(
          canvas,
          labels,
          {
            onHover: (h, x, y) => {
              setTip(h ? { h, x, y } : null);
              latest.current.onHover(h);
            },
            onSelect: (h) => latest.current.onSelect(h.column),
            onUserMove: () => latest.current.onUserMove(),
          },
          theme,
          l.options,
          {
            low: t("profileTerrain.axisLow"),
            high: t("profileTerrain.axisHigh"),
            nulls: t("profileTerrain.axisNulls"),
          },
        );
        s.setData(l.columns, l.order, true);
        return s;
      }),
  );

  const built = useRef<TerrainColumn[] | null>(null);
  useEffect(() => {
    if (!scene) return;
    if (built.current === null) built.current = latest.current.columns;
    else if (built.current !== columns) {
      built.current = columns;
      scene.setData(columns, latest.current.order, true);
    }
  }, [scene, columns]);
  useEffect(() => scene?.setOptions(options), [scene, options]);
  useEffect(() => scene?.setOrder(order), [scene, order]);
  useEffect(() => scene?.setSelected(selected), [scene, selected]);
  useEffect(() => scene?.setHover(hover), [scene, hover]);
  useEffect(() => scene?.setRotate(rotate), [scene, rotate]);
  useEffect(() => {
    if (!scene || command == null) return;
    if (command.kind === "reset") scene.resetView();
    else if (command.target != null) scene.focus(command.target);
  }, [scene, command]);

  const c = tip ? columns.find((x) => x.name === tip.h.column) : undefined;
  let body = null;
  if (tip && c) {
    // A null block's count is withheld, like the ridge, where the role may not see the column's
    // values; its share is still shown.
    const isNull = tip.h.bin === "null";
    const count = tip.h.bin === "null" ? c.nullCount : c.bins[tip.h.bin].count;
    const share = isNull ? c.nullShare : count != null && c.rows ? count / c.rows : null;
    body = (
      <>
        <b>{c.name}</b>
        <span>{tip.h.bin === "null" ? t("profileTerrain.tipNull") : c.bins[tip.h.bin].label}</span>
        {count != null && share != null && (
          <span className="p3d-num">
            {t("profileTerrain.tipRows", {
              rows: Math.round(count).toLocaleString(),
              pct: pctText(share),
            })}
          </span>
        )}
        {count == null && share != null && (
          <span className="p3d-num">{t("profileTerrain.tipShare", { pct: pctText(share) })}</span>
        )}
      </>
    );
  }

  return (
    <Stage3D
      wrapRef={wrapRef}
      canvasRef={canvasRef}
      labelsRef={labelsRef}
      ready={scene != null}
      error={error}
      canvasLabel={t("profileTerrain.canvasLabel")}
      tip={tip && body ? { x: tip.x, y: tip.y, body } : null}
      testId="terrain-stage"
      legend={
        <>
          <span>
            <i className="p3d-ramp" />
            {t("profileTerrain.legendDensity")}
          </span>
          <span>
            <i className="p3d-sw p3d-sw-tail" />
            {t("profileTerrain.legendTails")}
          </span>
          <span>
            <i className="p3d-sw p3d-sw-mid" />
            {t("profileTerrain.legendMedian")}
          </span>
          <span>
            <i className="p3d-sw p3d-sw-null" />
            {t("profileTerrain.legendNulls")}
          </span>
        </>
      }
    />
  );
}
