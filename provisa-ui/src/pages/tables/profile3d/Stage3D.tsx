// Copyright (c) 2026 Kenneth Stott
// Canary: 49fc18b0-cedc-4fae-9b3d-ae72cac1476c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2123: the canvas of a 3D profile view, with its label layer, loader, hover card, hint and
// legend. REQ-2239 (amended 2026-11-11, 2026-11-27): its view controls -- the shared commands first,
// Reset view, Zoom in, Zoom out, Full screen and Download, then Rotate and Grid where the scene draws
// a floor grid -- in the one stack every graph's canvas has (CanvasControls), bottom-left; the legend
// stands beside it.

import type { ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { Alert, Loader } from "@mantine/core";
import { Grid3x3, Rotate3d } from "lucide-react";
import { FitScreenIcon, ZoomInIcon, ZoomOutIcon } from "../../../components/graph/GraphIcons";
import { ZOOM_STEP } from "../../../components/provisa-graph/GraphControls";
import {
  CanvasButton,
  CanvasControls,
  CONTROL_ICON,
  FullscreenButton,
} from "../../../components/provisa-graph/CanvasControls";
import type { StageRefs } from "./useLazyScene";
import { ExportMenu } from "./ExportMenu";
import type { ExportableScene } from "./exportImage";

/** What a stage's scene does, once it is up: its exports, and a zoom button's step (BaseScene.zoom). */
export type StageScene = ExportableScene & { zoom: (factor: number) => void };

/** A one-shot camera move; ``seq`` changes for every request. */
export interface CameraCommand {
  kind: "reset" | "focus";
  target: string | null;
  seq: number;
}

/** A remembered on/off setting of the view (graphSettings.ts), shown as a pressed button. */
export interface ViewToggle {
  value: boolean;
  onChange: (on: boolean) => void;
}

/** What a 3D view's controls do. Their test ids are named after the view (``testId``). */
export interface ViewControls {
  testId: string;
  onReset: () => void;
  rotate: ViewToggle;
  /** Only where the scene draws a floor grid. */
  grid?: ViewToggle;
}

export interface StageTip {
  x: number;
  y: number;
  body: ReactNode;
}

export function Stage3D({
  wrapRef,
  canvasRef,
  labelsRef,
  ready,
  error,
  canvasLabel,
  tip,
  legend,
  scene,
  exportName,
  testId,
  controls,
  bare = false,
  flat = false,
}: StageRefs & {
  ready: boolean;
  error: string | null;
  canvasLabel: string;
  tip: StageTip | null;
  legend: ReactNode;
  /** The scene an export draws and the zoom buttons move, once it is up. */
  scene: StageScene | null;
  /** What the view's exports are named after (the graph and its view). */
  exportName: string;
  testId: string;
  controls: ViewControls;
  /** A view that is a glance only (a card): the canvas and its legend, no controls or hint. */
  bare?: boolean;
  /** REQ-2174: a 2D view, panned and zoomed only: its hint says so. */
  flat?: boolean;
}) {
  const { t } = useTranslation();
  return (
    // data-ready: the scene is drawn (three.js loads lazily), what a tour step waits for.
    <div className="p3d-stage" ref={wrapRef} data-testid={testId} data-ready={ready || undefined}>
      <canvas ref={canvasRef} aria-label={canvasLabel} />
      <div className="p3d-labels" ref={labelsRef} />
      {!ready && !error && (
        <div className="p3d-loading">
          <Loader size="sm" />
        </div>
      )}
      {error && (
        <Alert color="red" className="p3d-error" data-testid={`${testId}-error`}>
          {t("profile3d.webglError", { error })}
        </Alert>
      )}
      {tip && (
        <div
          className="p3d-tip"
          style={{ transform: `translate(${tip.x + 14}px, ${tip.y + 14}px)` }}
          data-testid={`${testId}-tip`}
        >
          {tip.body}
        </div>
      )}
      {!bare && (
        <>
          <CanvasControls testId={testId}>
            <CanvasButton
              label={t("profile3d.resetView")}
              icon={<FitScreenIcon size={CONTROL_ICON} />}
              onClick={controls.onReset}
              testId={`${controls.testId}-reset`}
            />
            {/* The shared ones first, as on every graph (GraphControls), then the view's own. */}
            <CanvasButton
              label={t("canvasControls.zoomIn")}
              icon={<ZoomInIcon size={CONTROL_ICON} />}
              onClick={() => scene?.zoom(ZOOM_STEP)}
              disabled={scene == null}
              testId={`${controls.testId}-zoom-in`}
            />
            <CanvasButton
              label={t("canvasControls.zoomOut")}
              icon={<ZoomOutIcon size={CONTROL_ICON} />}
              onClick={() => scene?.zoom(1 / ZOOM_STEP)}
              disabled={scene == null}
              testId={`${controls.testId}-zoom-out`}
            />
            <FullscreenButton stageRef={wrapRef} testId={testId} />
            <ExportMenu
              scene={ready ? scene : null}
              stage={wrapRef}
              name={exportName}
              testId={testId}
            />
            <CanvasButton
              label={t("profile3d.rotate")}
              icon={<Rotate3d size={CONTROL_ICON} />}
              pressed={controls.rotate.value}
              onClick={() => controls.rotate.onChange(!controls.rotate.value)}
              testId={`${controls.testId}-rotate`}
            />
            {controls.grid && (
              <CanvasButton
                label={t("profile3d.grid")}
                icon={<Grid3x3 size={CONTROL_ICON} />}
                pressed={controls.grid.value}
                onClick={() => controls.grid?.onChange(!controls.grid.value)}
                testId={`${controls.testId}-grid`}
              />
            )}
          </CanvasControls>
          <div className="p3d-hint">{t(flat ? "profile3d.hintFlat" : "profile3d.hint")}</div>
        </>
      )}
      <div className={bare ? "p3d-legend" : "p3d-legend p3d-legend-beside-controls"}>{legend}</div>
    </div>
  );
}
