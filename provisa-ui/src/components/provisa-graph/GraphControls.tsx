// Copyright (c) 2026 Kenneth Stott
// Canary: 9c4e1b27-6a3d-4f80-b5e2-7d1a0c8f3e64
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2239: a 2D graph's controls, in the one stack every graph shares (CanvasControls), always in
// this order: zoom in, zoom out, fit, full screen, and one download with the file types under it; a
// copy of the graph as text where the view gives one; then the view's own commands. The menu and the hints are drawn inside the
// graph, not in a portal, so they are still seen while it is full screen.

import { useState, type ReactNode, type RefObject } from "react";
import { useTranslation } from "react-i18next";
import { Menu } from "@mantine/core";
import { Check, Copy, Download } from "lucide-react";
import { FitScreenIcon, ZoomInIcon, ZoomOutIcon } from "../graph/GraphIcons";
import type { CyInstance } from "../graph/cytoscape-types";
import { Tip } from "../Tip";
import { CanvasButton, CanvasControls, CONTROL_ICON, FullscreenButton } from "./CanvasControls";
import { fitWhole, zoomAboutCentre, type GraphExports } from "./viewport";

const ZOOM_STEP = 1.3;

/** A command of the view's own, after the shared ones: its name, icon and action. */
export interface GraphCommandButton {
  label: string;
  icon: ReactNode;
  onClick: () => void;
  testId: string;
  /** A toggle's state: the button shows it pressed. */
  pressed?: boolean;
}

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
  commands = [],
  fitPadding,
  children,
  testId,
}: {
  cyRef: RefObject<CyInstance | null>;
  /** The graph's own box: what goes full screen. */
  stageRef: RefObject<HTMLDivElement | null>;
  exports: GraphExports;
  copy?: GraphCopy;
  /** The view's own commands, after the shared ones. */
  commands?: GraphCommandButton[];
  /** The fit's padding where the view keeps its own (fitWhole's default otherwise). */
  fitPadding?: number;
  /** Controls of the view's own that are not plain buttons (a held button, a slider), last. */
  children?: ReactNode;
  testId: string;
}) {
  const { t } = useTranslation();
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
    <CanvasControls testId={testId}>
      <CanvasButton
        label={t("canvasControls.zoomIn")}
        icon={<ZoomInIcon size={CONTROL_ICON} />}
        onClick={() => zoom(ZOOM_STEP)}
        testId={`${testId}-zoom-in`}
      />
      <CanvasButton
        label={t("canvasControls.zoomOut")}
        icon={<ZoomOutIcon size={CONTROL_ICON} />}
        onClick={() => zoom(1 / ZOOM_STEP)}
        testId={`${testId}-zoom-out`}
      />
      <CanvasButton
        label={t("erdModal.fitAll")}
        icon={<FitScreenIcon size={CONTROL_ICON} />}
        onClick={() => cyRef.current && fitWhole(cyRef.current, fitPadding)}
        testId={`${testId}-fit-all`}
      />
      <FullscreenButton stageRef={stageRef} testId={testId} />
      <Menu position="right-end" withinPortal={false} transitionProps={{ duration: 0 }}>
        <Menu.Target>
          <CanvasButton
            label={t("erdModal.download")}
            icon={<Download size={CONTROL_ICON} />}
            testId={`${testId}-download`}
          />
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
        <CanvasButton
          label={copied ? copy.done : copy.label}
          icon={copied ? <Check size={CONTROL_ICON} /> : <Copy size={CONTROL_ICON} />}
          onClick={copyText}
          testId={copy.testId}
        />
      )}
      {commands.map((c) => (
        <CanvasButton
          key={c.testId}
          label={c.label}
          icon={c.icon}
          onClick={c.onClick}
          pressed={c.pressed}
          testId={c.testId}
        />
      ))}
      {children}
      {/* The copy said done, beside the bar, for as long as the check shows. */}
      {copy && copied && (
        <span className="pg-copied" role="status">
          {copy.done}
        </span>
      )}
    </CanvasControls>
  );
}
