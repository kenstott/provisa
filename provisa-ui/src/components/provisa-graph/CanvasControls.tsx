// Copyright (c) 2026 Kenneth Stott
// Canary: 6f2d9a14-8b3e-4c71-a5d0-2e9c7b4f1a83
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2239 (amended 2026-11-11, VIEW CONTROLS SIT IN THE CANVAS, IN 2D AND 3D): the one stack of
// controls that change how a graph is looked at, inside its canvas, the same on every 2D graph
// (GraphControls) and every 3D view (Stage3D). One vertical bar at the canvas's bottom-left, shown
// while the pointer is over the canvas or a control in it has the keyboard's focus
// (canvas-controls.css). Each button is an icon with its name as a one-line tooltip drawn in place,
// so it is still seen while the canvas is full screen; a toggle shows its state (light, pressed).

import {
  useEffect,
  useState,
  type HTMLAttributes,
  type ReactNode,
  type Ref,
  type RefObject,
} from "react";
import { useTranslation } from "react-i18next";
import { ActionIcon, Stack } from "@mantine/core";
import { Expand, Shrink } from "lucide-react";
import { Tip } from "../Tip";
import "./canvas-controls.css";

/** The size of every icon in the stack. */
export const CONTROL_ICON = 15;

export function CanvasControls({ testId, children }: { testId: string; children: ReactNode }) {
  const { t } = useTranslation();
  return (
    <Stack
      gap={2}
      className="pg-controls"
      role="toolbar"
      aria-orientation="vertical"
      aria-label={t("canvasControls.toolbarLabel")}
      data-testid={`${testId}-canvas-bar`}
    >
      {children}
    </Stack>
  );
}

/**
 * One button of the stack. `pressed` makes it a toggle. Whatever a menu's trigger hands its child
 * (its ref, click handler and aria state) goes on through the Tip to the button.
 */
export function CanvasButton({
  label,
  icon,
  onClick,
  pressed,
  disabled,
  testId,
  ref,
  ...handedOn
}: {
  label: string;
  icon: ReactNode;
  onClick?: () => void;
  pressed?: boolean;
  disabled?: boolean;
  testId: string;
  ref?: Ref<HTMLElement>;
} & Omit<HTMLAttributes<HTMLElement>, "content" | "children" | "onClick">) {
  return (
    <Tip text={label} position="right" named inPlace ref={ref} {...handedOn}>
      <ActionIcon
        variant={pressed ? "light" : "subtle"}
        size="md"
        aria-label={label}
        aria-pressed={pressed}
        disabled={disabled}
        onClick={onClick}
        data-testid={testId}
      >
        {icon}
      </ActionIcon>
    </Tip>
  );
}

/** Puts the canvas's own box full screen, and back: the stack is inside it, so it stays. */
export function FullscreenButton({
  stageRef,
  testId,
}: {
  stageRef: RefObject<HTMLElement | null>;
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
  const toggle = () => {
    if (document.fullscreenElement) void document.exitFullscreen();
    else void stageRef.current?.requestFullscreen();
  };
  return (
    <CanvasButton
      label={full ? t("erdModal.exitFullscreen") : t("erdModal.fullscreen")}
      icon={full ? <Shrink size={CONTROL_ICON} /> : <Expand size={CONTROL_ICON} />}
      pressed={full}
      onClick={toggle}
      testId={`${testId}-fullscreen`}
    />
  );
}
