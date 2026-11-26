// Copyright (c) 2026 Kenneth Stott
// Canary: d58652be-3e0b-4f02-ac69-7e9450044435
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A detail panel's head toggle: an on/off state of the object shown (a term's approval, its pii
// mark, its exclusion from metadata export), beside the head's actions. An icon, named by what it
// sets; its tip names the state it is in. It takes the head's one style (PanelHeadStyle): no
// size of its own, and borderless where that style is not applied. Pressed, it is tinted (App.css, `aria-pressed`) and keeps the colour
// it is given. `fixed`: the state is shown but not changed here (an approved term leaves approval
// by Deprecate or Retire); the tip then says how.

import { ActionIcon } from "@mantine/core";
import type { MouseEvent, ReactNode } from "react";
import { Tip } from "../Tip";

export interface PanelToggleProps {
  /** What the toggle sets ("PII"): its accessible name. */
  label: string;
  /** Whether the state is on. */
  on: boolean;
  /** The tip: the state it is in, and what pressing does. */
  tip: string;
  icon: ReactNode;
  /** Pressed: turn the state over (or open what turning it over asks). */
  onToggle?: () => void;
  /** The state can't be changed now; `tip` says why. */
  disabled?: boolean;
  /** Shown but changed elsewhere; `tip` says where. */
  fixed?: boolean;
  /** The colour of the icon while the state is on. */
  color?: string;
  testId?: string;
}

export function PanelToggle({
  label,
  on,
  tip,
  icon,
  onToggle,
  disabled = false,
  fixed = false,
  color,
  testId,
}: PanelToggleProps) {
  const press = (e: MouseEvent) => {
    e.stopPropagation();
    if (!fixed) onToggle?.();
  };
  return (
    <Tip text={tip}>
      {/* A disabled control takes no pointer: the tip is held by its wrapper. */}
      <span>
        <ActionIcon
          // Borderless, as the panel head's one style (PanelHeadStyle) draws it, and the same where
          // that style is not applied.
          variant="subtle"
          aria-label={label}
          aria-pressed={on}
          aria-disabled={fixed || undefined}
          color={on ? color : undefined}
          disabled={disabled}
          onClick={press}
          data-testid={testId}
          data-state={on ? "on" : "off"}
        >
          {icon}
        </ActionIcon>
      </span>
    </Tip>
  );
}
