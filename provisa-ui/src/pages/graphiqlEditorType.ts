// Copyright (c) 2026 Kenneth Stott
// Canary: 1e7b3d95-4a2c-4f68-b0d1-8c5e2a9f7b36
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2277: GraphiQL's editors take the type the other query editors use (--editor-font-mono and
// --editor-font-size, App.css). Monaco takes its type as editor options, not from CSS, so each
// editor is given the tokens' values as it is created; GraphiQL itself is not touched.

/** The tokens' values as monaco's options: the family as written, the size in pixels. */
export function editorTypeOptions(root: Element): { fontFamily: string; fontSize: number } {
  const style = getComputedStyle(root);
  const size = style.getPropertyValue("--editor-font-size").trim();
  const pixels = size.endsWith("rem")
    ? parseFloat(size) * parseFloat(style.fontSize)
    : size.endsWith("px")
      ? parseFloat(size)
      : NaN;
  if (Number.isNaN(pixels)) throw new Error(`--editor-font-size is not a size: "${size}"`);
  return { fontFamily: style.getPropertyValue("--editor-font-mono").trim(), fontSize: pixels };
}
