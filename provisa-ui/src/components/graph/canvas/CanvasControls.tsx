// Copyright (c) 2026 Kenneth Stott
// Canary: 5f8a1c73-d4e2-4b9f-a0c1-e3f7d9b2c8a4
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { MutableRefObject } from "react";
import { ActionIcon, Divider, Group, Slider, Text, Tooltip } from "@mantine/core";
import { RefreshCw } from "lucide-react";
import { useTranslation } from "react-i18next";
import { ZoomInIcon, ZoomOutIcon, FitScreenIcon } from "../GraphIcons";
import type { LayoutMode } from "./canvas-types";
import type { CyInstance } from "../cytoscape-types";

interface CanvasControlsProps {
  cyRef: MutableRefObject<CyInstance | null>;
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

export function CanvasControls({
  cyRef,
  layoutMode,
  toggleLayout,
  nudgeHeldRef,
  nudgeLayout,
  nudgeLayoutRef,
  edgeDistance,
  setEdgeDistance,
  edgeDistanceRef,
  nudgeTimerRef,
}: CanvasControlsProps) {
  const { t } = useTranslation();
  const fitView = () => cyRef.current?.fit(undefined, 40);

  const toggleLayoutLabel =
    layoutMode === "force"
      ? t("canvasControls.switchToHierarchy")
      : t("canvasControls.switchToForce");

  return (
    <Group
      className="gf-canvas-controls"
      gap={4}
      wrap="nowrap"
      role="toolbar"
      aria-label={t("canvasControls.toolbarLabel")}
    >
      <Tooltip label={t("canvasControls.zoomIn")}>
        <ActionIcon
          variant="subtle"
          size="md"
          aria-label={t("canvasControls.zoomIn")}
          data-testid="canvas-zoom-in"
          onClick={() => cyRef.current?.zoom(cyRef.current.zoom() * 1.3)}
        >
          <ZoomInIcon size={15} />
        </ActionIcon>
      </Tooltip>
      <Tooltip label={t("canvasControls.zoomOut")}>
        <ActionIcon
          variant="subtle"
          size="md"
          aria-label={t("canvasControls.zoomOut")}
          data-testid="canvas-zoom-out"
          onClick={() => cyRef.current?.zoom(cyRef.current.zoom() * 0.77)}
        >
          <ZoomOutIcon size={15} />
        </ActionIcon>
      </Tooltip>
      <Tooltip label={t("canvasControls.fitToScreen")}>
        <ActionIcon
          variant="subtle"
          size="md"
          aria-label={t("canvasControls.fitToScreen")}
          data-testid="canvas-fit-view"
          onClick={fitView}
        >
          <FitScreenIcon size={15} />
        </ActionIcon>
      </Tooltip>
      <Divider orientation="vertical" className="gf-ctrl-divider" />
      <Tooltip label={toggleLayoutLabel}>
        <ActionIcon
          variant={layoutMode === "hierarchy" ? "filled" : "subtle"}
          size="md"
          aria-label={toggleLayoutLabel}
          aria-pressed={layoutMode === "hierarchy"}
          data-testid="canvas-toggle-layout"
          onClick={toggleLayout}
        >
          <Text component="span" size="sm" aria-hidden>
            {layoutMode === "force" ? "⋮" : "⊟"}
          </Text>
        </ActionIcon>
      </Tooltip>
      <Tooltip label={t("canvasControls.nudgeLayout")}>
        <ActionIcon
          variant="subtle"
          size="md"
          aria-label={t("canvasControls.nudgeLayout")}
          data-testid="canvas-nudge-layout"
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
        >
          <RefreshCw size={15} aria-hidden />
        </ActionIcon>
      </Tooltip>
      <Divider orientation="vertical" className="gf-ctrl-divider" />
      <Text component="label" htmlFor="canvas-edge-distance" size="sm" c="dimmed" title={t("canvasControls.edgeLength")}>
        ↔
      </Text>
      <Slider
        id="canvas-edge-distance"
        className="gf-ctrl-slider"
        w={90}
        size="sm"
        min={40}
        max={400}
        step={10}
        value={edgeDistance}
        label={(v) => t("canvasControls.edgeLengthValue", { value: v })}
        aria-label={t("canvasControls.edgeLength")}
        data-testid="canvas-edge-distance"
        onChange={(v) => {
          edgeDistanceRef.current = v;
          setEdgeDistance(v);
          localStorage.setItem("provisa.graph.edgeDistance", String(v));
          if (nudgeTimerRef.current) clearTimeout(nudgeTimerRef.current);
          nudgeTimerRef.current = setTimeout(() => nudgeLayoutRef.current(), 150);
        }}
      />
    </Group>
  );
}
