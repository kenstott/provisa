// Copyright (c) 2026 Kenneth Stott
// Canary: 2922179b-04dc-4bd7-bd9a-f06b32569837
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState, type ReactNode } from "react";
import { ActionIcon } from "@mantine/core";
import { useTranslation } from "react-i18next";
import { Tip } from "../Tip";
import { readCollapsibleStates, writeCollapsibleState } from "./collapsibleState";
import "./collapsible.css";

/** The collapsible states' group the side panels are kept under, one item per view. */
const SIDE_PANELS = "side-panels";

/**
 * REQ-2239: a view's side panel, beside its canvas on the inline end, hidden and shown again by the
 * handle on its edge so the canvas takes the full width. The choice is kept per view for this
 * viewer (collapsibleState.ts), so the view opens with the panel as it was left.
 */
export function SidePanel({
  view,
  onToggle,
  testId,
  children,
}: {
  /** The view the choice is kept for (e.g. a graph's settings kind). */
  view: string;
  /** Told when the panel is hidden or shown: the canvas beside it has changed width. */
  onToggle?: (open: boolean) => void;
  testId: string;
  children: ReactNode;
}) {
  const { t } = useTranslation();
  // REQ-2239: shown until this viewer hides it in this view.
  const [open, setOpen] = useState(() => readCollapsibleStates()[SIDE_PANELS]?.[view] ?? true);
  const toggle = () => {
    const next = !open;
    setOpen(next);
    writeCollapsibleState(SIDE_PANELS, { [view]: next });
    onToggle?.(next);
  };
  const label = t(open ? "collapsible.hidePanel" : "collapsible.showPanel");
  return (
    <div
      className={`provisa-side-panel${open ? "" : " is-hidden"}`}
      data-testid={testId}
      data-open={open}
    >
      <Tip text={label} named position="left">
        <ActionIcon
          variant="default"
          radius={0}
          className="provisa-side-panel-handle"
          onClick={toggle}
          aria-label={label}
          aria-expanded={open}
          data-testid={`${testId}-toggle`}
        >
          <span aria-hidden="true">{open ? "›" : "‹"}</span>
        </ActionIcon>
      </Tip>
      {open && <div className="provisa-side-panel-body">{children}</div>}
    </div>
  );
}
