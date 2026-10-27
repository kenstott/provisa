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
// legend.

import type { ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { Alert, Loader } from "@mantine/core";
import type { StageRefs } from "./useLazyScene";
import { ExportMenu } from "./ExportMenu";
import type { ExportableScene } from "./exportImage";

/** A one-shot camera move; ``seq`` changes for every request. */
export interface CameraCommand {
  kind: "reset" | "focus";
  target: string | null;
  seq: number;
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
  exportable,
  exportName,
  testId,
  bare = false,
  flat = false,
}: StageRefs & {
  ready: boolean;
  error: string | null;
  canvasLabel: string;
  tip: StageTip | null;
  legend: ReactNode;
  /** The scene an export draws, once it is up. */
  exportable: ExportableScene | null;
  /** What the view's exports are named after (the graph and its view). */
  exportName: string;
  testId: string;
  /** A view that is a glance only (a card): the canvas and its legend, no export or hint. */
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
          <ExportMenu
            scene={ready ? exportable : null}
            stage={wrapRef}
            name={exportName}
            testId={testId}
          />
          <div className="p3d-hint">{t(flat ? "profile3d.hintFlat" : "profile3d.hint")}</div>
        </>
      )}
      <div className="p3d-legend">{legend}</div>
    </div>
  );
}
