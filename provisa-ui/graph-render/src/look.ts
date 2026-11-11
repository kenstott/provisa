// Copyright (c) 2026 Kenneth Stott
// Canary: d6878e01-1a74-4696-9ec8-b702a8c1d20a
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// What a drawing looks like besides its stylesheet: the layouts a caller may name and the
// background each theme draws on. No DOM here, so the UI's tests read it as it is.

import { LAYOUT_OPTIONS } from "../../src/components/graph/canvas/canvas-types";
import { ERD_ELK_LAYOUT } from "../../src/components/provisa-graph/presets";

/** The layouts a caller may name: the graph page's force and hierarchy, and the ERD's ELK. */
export const LAYOUTS = {
  force: LAYOUT_OPTIONS.force,
  hierarchy: LAYOUT_OPTIONS.hierarchy,
  elk: ERD_ELK_LAYOUT,
} as const;
export type LayoutName = keyof typeof LAYOUTS;

/** What the graph canvas sits on: the page's --bg (theme/tokens.css); the stylesheet itself has
 *  no theme. Held equal to the tokens by the provisa-ui test graphRenderTheme.test.ts. */
export const BACKGROUND = { light: "#ffffff", dark: "#0f1117" } as const;
export type Theme = keyof typeof BACKGROUND;
