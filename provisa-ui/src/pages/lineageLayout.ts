// Copyright (c) 2026 Kenneth Stott
// Canary: 3fafa280-216a-44c3-96fc-1a2c03ecda1e
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { CSSProperties } from "react";

// The query panel and the button panel share the row in a fixed proportion (3 : 1) with a floor
// each, so a narrowing window narrows them together. Zero flex-basis keeps the ratio exact; the
// wrap decision uses the min-widths, so below their sum the controls drop under the editor
// instead of the page scrolling sideways. minWidth keeps the editor from being sized by its
// content (CodeMirror reports its content width), so it scrolls inside itself.
export const LINEAGE_EDITOR_MIN_WIDTH = "20rem";
export const LINEAGE_CONTROLS_MIN_WIDTH = "14rem";

export const LINEAGE_EDITOR_PANEL_STYLE: CSSProperties = {
  flexGrow: 3,
  flexShrink: 1,
  flexBasis: 0,
  minWidth: LINEAGE_EDITOR_MIN_WIDTH,
  display: "flex",
  flexDirection: "column",
};

export const LINEAGE_CONTROLS_PANEL_STYLE: CSSProperties = {
  flexGrow: 1,
  flexShrink: 1,
  flexBasis: 0,
  minWidth: LINEAGE_CONTROLS_MIN_WIDTH,
};
