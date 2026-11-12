// Copyright (c) 2026 Kenneth Stott
// Canary: 9c4e1b27-6a3d-4f80-b5e2-7d1a0c8f3e64
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2239: a graph's controls, in one vertical bar at the canvas's bottom-left, always in this order: zoom
// in, zoom out, fit, full screen, and one download with the file types under it; a copy of the
// graph as text where the view gives one. Shown while the pointer is over the graph or a control in
// it has the keyboard's focus (provisa-graph.css). The menu and the hints are drawn inside the
// graph, not in a portal, so they are still seen while it is full screen.

import { useEffect, useState, type RefObject } from "react";
import { useTranslation } from "react-i18next";
import { ActionIcon, Menu, Stack } from "@mantine/core";
import { Check, Copy, Download, Expand, Shrink } from "lucide-react";
import { FitScreenIcon, ZoomInIcon, ZoomOutIcon } from "../graph/GraphIcons";
import type { CyInstance } from "../graph/cytoscape-types";
import { Tip } from "../Tip";
import { fitWhole, zoomAboutCentre, type GraphExports } from "./viewport";

const ZOOM_STEP = 1.3;

/** Text a graph's controls copy: the button's words, before and after, and the text. */
export interface GraphCopy {
  label: string;
  done: string;
  text: () => string;
  testId: string;
}

export function GraphControls({
  cyRef,
  stageRef,
  exports,
  copy,
  testId,
}: {
  cyRef: RefObject<CyInstance | null>;
  /** The graph's own box: what goes full screen. */
  stageRef: RefObject<HTMLDivElement | null>;
  exports: GraphExports;
  copy?: GraphCopy;
  testId: string;
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
  const [copied, setCopied] = useState(false);
  const copyText = () => {
    if (!copy) return;
    void navigator.clipboard.writeText(copy.text()).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    });
  };
  const items = (
    [
      { key: "svg", label: "SVG" },
      { key: "png", label: "PNG" },
      { key: "jpeg", label: "JPEG" },
      { key: "json", label: "JSON" },
    ] as { key: keyof GraphExports; label: string }[]
  ).filter(({ key }) => exports[key] !== undefined);
  const hints: Record<keyof GraphExports, string> = {
    svg: t("erdModal.downloadSvg"),
    png: t("erdModal.downloadPng"),
    jpeg: t("erdModal.downloadJpeg"),
    json: t("erdModal.downloadJson"),
  };
  return (
    <Stack
      gap={2}
      className="pg-controls"
      role="toolbar"
      aria-orientation="vertical"
      aria-label={t("canvasControls.toolbarLabel")}
      data-testid={`${testId}-canvas-bar`}
    >
      <Tip text={t("canvasControls.zoomIn")} position="right" named inPlace>
        <ActionIcon
          variant="subtle"
          size="md"
          aria-label={t("canvasControls.zoomIn")}
          onClick={() => zoom(ZOOM_STEP)}
          data-testid={`${testId}-zoom-in`}
        >
          <ZoomInIcon size={15} />
        </ActionIcon>
      </Tip>
      <Tip text={t("canvasControls.zoomOut")} position="right" named inPlace>
        <ActionIcon
          variant="subtle"
          size="md"
          aria-label={t("canvasControls.zoomOut")}
          onClick={() => zoom(1 / ZOOM_STEP)}
          data-testid={`${testId}-zoom-out`}
        >
          <ZoomOutIcon size={15} />
        </ActionIcon>
      </Tip>
      <Tip text={t("erdModal.fitAll")} position="right" named inPlace>
        <ActionIcon
          variant="subtle"
          size="md"
          aria-label={t("erdModal.fitAll")}
          onClick={() => cyRef.current && fitWhole(cyRef.current)}
          data-testid={`${testId}-fit-all`}
        >
          <FitScreenIcon size={15} />
        </ActionIcon>
      </Tip>
      <Tip text={fullLabel} position="right" named inPlace>
        <ActionIcon
          variant={full ? "light" : "subtle"}
          size="md"
          aria-label={fullLabel}
          aria-pressed={full}
          onClick={toggleFull}
          data-testid={`${testId}-fullscreen`}
        >
          {full ? <Shrink size={15} /> : <Expand size={15} />}
        </ActionIcon>
      </Tip>
      <Menu position="right-end" withinPortal={false} transitionProps={{ duration: 0 }}>
        <Menu.Target>
          <Tip text={t("erdModal.download")} position="right" named inPlace>
            <ActionIcon
              variant="subtle"
              size="md"
              aria-label={t("erdModal.download")}
              data-testid={`${testId}-download`}
            >
              <Download size={15} />
            </ActionIcon>
          </Tip>
        </Menu.Target>
        <Menu.Dropdown data-testid={`${testId}-download-menu`}>
          {items.map(({ key, label }) => (
            <Tip key={key} text={hints[key]} position="right" inPlace>
              <Menu.Item onClick={exports[key]} data-testid={`${testId}-export-${key}`}>
                {label}
              </Menu.Item>
            </Tip>
          ))}
        </Menu.Dropdown>
      </Menu>
    {copy && (
        <Tip text={copied ? copy.done : copy.label} position="right" named inPlace>
          <ActionIcon
            variant="subtle"
            size="md"
            aria-label={copied ? copy.done : copy.label}
            onClick={copyText}
            data-testid={copy.testId}
          >
            {copied ? <Check size={15} /> : <Copy size={15} />}
          </ActionIcon>
        </Tip>
      )}
      {/* The copy said done, beside the bar, for as long as the check shows. */}
      {copy && copied && (
        <span className="pg-copied" role="status">
          {copy.done}
        </span>
      )}
    </Stack>
  );
}
