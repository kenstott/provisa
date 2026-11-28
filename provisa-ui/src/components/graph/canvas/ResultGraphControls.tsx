// Copyright (c) 2026 Kenneth Stott
// Canary: 5f8a1c73-d4e2-4b9f-a0c1-e3f7d9b2c8a4
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2239 (amended 2026-11-27): the network graph's (Cypher and SPARQL result) hover command bar is
// the one every graph shares (GraphControls): zoom, fit, full screen and download, then its own
// commands -- the layout toggle, the held re-layout and the edge length. Its downloads are the one
// image export every graph uses (exportImage.ts): on the canvas colour, named after the view, with the
// cluster hulls painted.

import type { MutableRefObject, RefObject } from "react";
import { Divider, Text } from "@mantine/core";
import { RefreshCw } from "lucide-react";
import { useTranslation } from "react-i18next";
import { FIT_MARGIN, type LayoutMode } from "./canvas-types";
import type { CyInstance } from "../cytoscape-types";
import { Tip } from "../../Tip";
import { setStored } from "../../../lib/browserStorage";
import { GraphControls } from "../../provisa-graph/GraphControls";
import { CanvasButton, CONTROL_ICON } from "../../provisa-graph/CanvasControls";
import { exportJpeg, exportPng, exportSvg, type GraphImage } from "../../provisa-graph/exportImage";

interface ResultGraphControlsProps {
  cyRef: MutableRefObject<CyInstance | null>;
  /** The canvas's own box: what goes full screen. */
  stageRef: RefObject<HTMLDivElement | null>;
  /** The canvas colour an image is drawn on. */
  canvas: string;
  /** The view's name, which a downloaded file takes. */
  exportName: string;
  layoutMode: LayoutMode;
  toggleLayout: () => void;
  nudgeHeldRef: MutableRefObject<boolean>;
  nudgeLayout: (freeNodes?: Set<string>, aggressive?: boolean) => void;
  nudgeLayoutRef: MutableRefObject<(freeNodes?: Set<string>, aggressive?: boolean) => void>;
  edgeDistance: number;
  setEdgeDistance: (v: number) => void;
  edgeDistanceRef: MutableRefObject<number>;
  nudgeTimerRef: MutableRefObject<ReturnType<typeof setTimeout> | null>;
}

export function ResultGraphControls({
  cyRef,
  stageRef,
  canvas,
  exportName,
  layoutMode,
  toggleLayout,
  nudgeHeldRef,
  nudgeLayout,
  nudgeLayoutRef,
  edgeDistance,
  setEdgeDistance,
  edgeDistanceRef,
  nudgeTimerRef,
}: ResultGraphControlsProps) {
  const { t } = useTranslation();
  // The cluster hulls drawn over the canvas (HullSvgOverlay), which an image export paints too; there
  // is no overlay while no cluster is drawn.
  const exportWith = (draw: (image: GraphImage) => void) => {
    const cy = cyRef.current;
    const stage = stageRef.current;
    if (cy && stage)
      draw({
        cy,
        bg: canvas,
        name: exportName,
        hulls: stage.querySelector<SVGSVGElement>("svg.gf-hulls"),
      });
  };
  const toggleLayoutLabel =
    layoutMode === "force"
      ? t("canvasControls.switchToHierarchy")
      : t("canvasControls.switchToForce");

  return (
    <GraphControls
      cyRef={cyRef}
      stageRef={stageRef}
      testId="canvas"
      fitPadding={FIT_MARGIN}
      exports={{
        svg: () => exportWith(exportSvg),
        png: () => exportWith(exportPng),
        jpeg: () => exportWith(exportJpeg),
      }}
      commands={[
        {
          label: toggleLayoutLabel,
          icon: (
            <Text component="span" size="sm" aria-hidden>
              {layoutMode === "force" ? "⋮" : "⊟"}
            </Text>
          ),
          onClick: toggleLayout,
          pressed: layoutMode === "hierarchy",
          testId: "canvas-toggle-layout",
        },
      ]}
    >
      <CanvasButton
        label={t("canvasControls.nudgeLayout")}
        icon={<RefreshCw size={CONTROL_ICON} aria-hidden />}
        testId="canvas-nudge-layout"
        onMouseDown={() => {
          nudgeHeldRef.current = true;
          const cy = cyRef.current;
          const sel = cy ? cy.nodes(":selected").not("[?_cluster]") : null;
          const freeNodes = sel && sel.length > 0 ? new Set(sel.map((n) => n.id())) : undefined;
          nudgeLayout(freeNodes, true);
        }}
        onMouseUp={() => {
          nudgeHeldRef.current = false;
        }}
        onMouseLeave={() => {
          nudgeHeldRef.current = false;
        }}
      />
      <Divider orientation="horizontal" className="gf-ctrl-divider" />
      <Tip text={t("canvasControls.edgeLength")}>
        <Text component="label" htmlFor="canvas-edge-distance" size="sm" c="dimmed" ta="center">
          ↔
        </Text>
      </Tip>
      {/* Native vertical range — Mantine Slider has no vertical orientation, and
          this control must stack in the narrow vertical canvas toolbar. */}
      <Tip text={t("canvasControls.edgeLengthValue", { value: edgeDistance })}>
        <input
          type="range"
          id="canvas-edge-distance"
          className="gf-ctrl-slider"
          min={40}
          max={400}
          step={10}
          value={edgeDistance}
          aria-label={t("canvasControls.edgeLength")}
          data-testid="canvas-edge-distance"
          onChange={(e) => {
            const v = Number(e.currentTarget.value);
            edgeDistanceRef.current = v;
            setEdgeDistance(v);
            setStored("provisa.graph.edgeDistance", String(v));
            if (nudgeTimerRef.current) clearTimeout(nudgeTimerRef.current);
            nudgeTimerRef.current = setTimeout(() => nudgeLayoutRef.current(), 150);
          }}
        />
      </Tip>
    </GraphControls>
  );
}
