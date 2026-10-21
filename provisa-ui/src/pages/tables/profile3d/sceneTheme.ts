// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2123: the colours a 3D profile scene draws with, read from the --p3d-* tokens profile3d.css
// sets for each Mantine colour scheme.

import type { SceneTheme } from "./baseScene";

export function readSceneTheme(el: HTMLElement, dark: boolean): SceneTheme {
  const css = getComputedStyle(el);
  const v = (n: string) => css.getPropertyValue(`--p3d-${n}`).trim();
  return {
    canvas: v("canvas"),
    ground: v("ground"),
    grid: v("grid"),
    ramp: [v("ramp-0"), v("ramp-1"), v("ramp-2"), v("ramp-3"), v("ramp-4")],
    wallA: v("wall-a"),
    wallB: v("wall-b"),
    wallLine: v("wall-line"),
    nul: v("null"),
    qTail: v("q-tail"),
    qMid: v("q-mid"),
    hl: v("hl"),
    accent: v("accent"),
    warm: v("warm"),
    cool: v("cool"),
    family: {
      numeric: v("fam-numeric"),
      temporal: v("fam-temporal"),
      text: v("fam-text"),
      boolean: v("fam-boolean"),
      other: v("fam-other"),
      foreign: v("fam-foreign"),
    },
    dark,
  };
}
