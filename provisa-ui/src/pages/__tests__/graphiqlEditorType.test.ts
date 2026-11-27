// Copyright (c) 2026 Kenneth Stott
// Canary: 0d5f9a27-6b1e-4c83-9e4a-2f7c1b8d5e60
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2277: GraphiQL's editors are given the query editors' type tokens as monaco options.

import { afterEach, describe, expect, it } from "vitest";
import { editorTypeOptions } from "../graphiqlEditorType";

const root = document.documentElement;
afterEach(() => {
  root.style.removeProperty("--editor-font-mono");
  root.style.removeProperty("--editor-font-size");
  root.style.removeProperty("font-size");
});

describe("editorTypeOptions", () => {
  it("reads the family as written and a rem size in pixels", () => {
    root.style.setProperty("--editor-font-mono", '"SF Mono", monospace');
    root.style.setProperty("--editor-font-size", "0.8rem");
    root.style.fontSize = "16px";
    expect(editorTypeOptions(root)).toEqual({ fontFamily: '"SF Mono", monospace', fontSize: 12.8 });
  });

  it("reads a pixel size as it is", () => {
    root.style.setProperty("--editor-font-mono", "monospace");
    root.style.setProperty("--editor-font-size", "13px");
    expect(editorTypeOptions(root).fontSize).toBe(13);
  });

  it("refuses a size it cannot read", () => {
    root.style.setProperty("--editor-font-size", "large");
    expect(() => editorTypeOptions(root)).toThrow(/--editor-font-size/);
  });
});
