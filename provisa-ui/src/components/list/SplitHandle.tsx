// Copyright (c) 2026 Kenneth Stott
// Canary: 3c1f7a2e-9b84-4d0f-a6e5-2b7d8c41f903
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The splitter between two panes: a one-pixel divider that is also the drag handle, the one the
// list and detail panels use (SplitPane) and every Explore surface uses (REQ-2277). Dragged with
// the pointer or moved with the arrow keys; the size it sets is kept by useResizableSize.

import type { KeyboardEvent, PointerEvent } from "react";

export function SplitHandle({
  orientation,
  label,
  onPointerDown,
  onKeyDown,
  testId,
}: {
  /** `vertical`: a divider between panes side by side; `horizontal`: between panes stacked. */
  orientation: "vertical" | "horizontal";
  label: string;
  onPointerDown: (e: PointerEvent) => void;
  onKeyDown: (e: KeyboardEvent) => void;
  testId: string;
}) {
  return (
    <div
      role="separator"
      aria-orientation={orientation}
      aria-label={label}
      tabIndex={0}
      className={`split-pane__handle split-pane__handle--${orientation}`}
      onPointerDown={onPointerDown}
      onKeyDown={onKeyDown}
      data-testid={testId}
    >
      <div />
    </div>
  );
}
