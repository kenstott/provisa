// Copyright (c) 2026 Kenneth Stott
// Canary: 5d1c8e37-b0a4-4f92-8c6e-27a9f3d41e85
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2277: the one step strip, shared by source setup and the modeling wizards. Every step is
// named; the current one is highlighted, the completed ones carry a check and go back when
// pressed, and the ones not yet reached do nothing. It lays out in a row while the panel is wide
// enough for every label, and stacks vertically below STACK_BELOW pixels.

import { useElementSize } from "@mantine/hooks";
import { Stepper } from "@mantine/core";

export interface Step {
  key: string;
  label: string;
  description?: string;
}

/** Below this width the strip stacks: a row of four or five labels no longer fits. */
const STACK_BELOW = 560;

export function StepStrip({
  steps,
  active,
  onStepClick,
  currentOpens = false,
  label,
  testId,
}: {
  steps: readonly Step[];
  /** Index of the current step. */
  active: number;
  /** A completed step (before `active`) was pressed, or the current one with `currentOpens`. */
  onStepClick: (index: number) => void;
  /** The current step can be pressed too: the strip shows progress, and the step on screen may be
   *  an earlier one (the modeling wizards, REQ-2199). */
  currentOpens?: boolean;
  label: string;
  testId: string;
}) {
  const { ref, width } = useElementSize();
  // Width is 0 until the first measure: the row is the first layout.
  const vertical = width > 0 && width < STACK_BELOW;
  return (
    <div
      ref={ref}
      data-testid={testId}
      data-active={active}
      data-orientation={vertical ? "vertical" : "horizontal"}
    >
      <Stepper
        active={active}
        onStepClick={onStepClick}
        allowNextStepsSelect={false}
        orientation={vertical ? "vertical" : "horizontal"}
        size="xs"
        iconSize={22}
        aria-label={label}
      >
        {steps.map((s, i) => (
          <Stepper.Step
            key={s.key}
            label={s.label}
            description={s.description}
            allowStepSelect={currentOpens ? i <= active : undefined}
            data-testid={`${testId}-${s.key}`}
          />
        ))}
      </Stepper>
    </div>
  );
}
