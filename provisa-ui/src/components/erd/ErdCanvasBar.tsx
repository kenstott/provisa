// Copyright (c) 2026 Kenneth Stott
// Canary: 9c4e1b27-6a3d-4f80-b5e2-7d1a0c8f3e64
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The ERD's image controls, in a vertical bar inside the diagram: zoom, fit, full screen, and one
// download with the file types under it. Shown while the pointer is over the diagram or a control in
// it has the keyboard's focus (App.css, .erd-stage__bar). The menu and the hints are drawn inside
// the diagram, not in a portal, so they are still seen while it is full screen.

import { useEffect, useState, type RefObject } from "react";
import { useTranslation } from "react-i18next";
import { ActionIcon, Menu, Stack } from "@mantine/core";
import { Download, Expand, Shrink } from "lucide-react";
import { FitScreenIcon, ZoomInIcon, ZoomOutIcon } from "../graph/GraphIcons";
import type { CyInstance } from "../graph/cytoscape-types";
import { Tip } from "../Tip";
import { FIT_PADDING, zoomAboutCentre, type ErdExports } from "./erd-viewport";

const ZOOM_STEP = 1.3;

export function ErdCanvasBar({
  cyRef,
  stageRef,
  exports,
}: {
  cyRef: RefObject<CyInstance | null>;
  /** The diagram's own box: what goes full screen. */
  stageRef: RefObject<HTMLDivElement | null>;
  exports: ErdExports;
}) {
  const { t } = useTranslation();
  const [full, setFull] = useState(false);
  useEffect(() => {
    const follow = () =>
      setFull(
        document.fullscreenElement !== null && document.fullscreenElement === stageRef.current,
      );
    document.addEventListener("fullscreenchange", follow);
    return () => document.removeEventListener("fullscreenchange", follow);
  }, [stageRef]);
  const toggleFull = () => {
    if (document.fullscreenElement) void document.exitFullscreen();
    else void stageRef.current?.requestFullscreen();
  };
  const fullLabel = full ? t("erdModal.exitFullscreen") : t("erdModal.fullscreen");
  const zoom = (factor: number) => {
    if (cyRef.current) zoomAboutCentre(cyRef.current, factor);
  };
  const items: { key: keyof ErdExports; label: string }[] = [
    { key: "svg", label: "SVG" },
    { key: "png", label: "PNG" },
    { key: "jpeg", label: "JPEG" },
    { key: "json", label: "JSON" },
  ];
  const hints: Record<keyof ErdExports, string> = {
    svg: t("erdModal.downloadSvg"),
    png: t("erdModal.downloadPng"),
    jpeg: t("erdModal.downloadJpeg"),
    json: t("erdModal.downloadJson"),
  };
  return (
    <Stack
      gap={2}
      className="erd-stage__bar"
      role="toolbar"
      aria-orientation="vertical"
      aria-label={t("canvasControls.toolbarLabel")}
      data-testid="erd-canvas-bar"
    >
      <Tip text={t("canvasControls.zoomIn")} position="left" named inPlace>
        <ActionIcon
          variant="subtle"
          size="md"
          aria-label={t("canvasControls.zoomIn")}
          onClick={() => zoom(ZOOM_STEP)}
          data-testid="erd-zoom-in"
        >
          <ZoomInIcon size={15} />
        </ActionIcon>
      </Tip>
      <Tip text={t("canvasControls.zoomOut")} position="left" named inPlace>
        <ActionIcon
          variant="subtle"
          size="md"
          aria-label={t("canvasControls.zoomOut")}
          onClick={() => zoom(1 / ZOOM_STEP)}
          data-testid="erd-zoom-out"
        >
          <ZoomOutIcon size={15} />
        </ActionIcon>
      </Tip>
      <Tip text={t("erdModal.fitAll")} position="left" named inPlace>
        <ActionIcon
          variant="subtle"
          size="md"
          aria-label={t("erdModal.fitAll")}
          onClick={() => cyRef.current?.fit(undefined, FIT_PADDING)}
          data-testid="erd-fit-all"
        >
          <FitScreenIcon size={15} />
        </ActionIcon>
      </Tip>
      <Tip text={fullLabel} position="left" named inPlace>
        <ActionIcon
          variant={full ? "light" : "subtle"}
          size="md"
          aria-label={fullLabel}
          aria-pressed={full}
          onClick={toggleFull}
          data-testid="erd-fullscreen"
        >
          {full ? <Shrink size={15} /> : <Expand size={15} />}
        </ActionIcon>
      </Tip>
      <Menu position="left-start" withinPortal={false} transitionProps={{ duration: 0 }}>
        <Menu.Target>
          <Tip text={t("erdModal.download")} position="left" named inPlace>
            <ActionIcon
              variant="subtle"
              size="md"
              aria-label={t("erdModal.download")}
              data-testid="erd-download"
            >
              <Download size={15} />
            </ActionIcon>
          </Tip>
        </Menu.Target>
        <Menu.Dropdown data-testid="erd-download-menu">
          {items.map(({ key, label }) => (
            <Tip key={key} text={hints[key]} position="left" inPlace>
              <Menu.Item onClick={exports[key]} data-testid={`erd-export-${key}`}>
                {label}
              </Menu.Item>
            </Tip>
          ))}
        </Menu.Dropdown>
      </Menu>
    </Stack>
  );
}
