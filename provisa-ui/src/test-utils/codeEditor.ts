// Copyright (c) 2026 Kenneth Stott
// Canary: d708edce-d27c-4622-83f0-add201a1e722
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1984: a code field is the app's code editor; a test reads and edits it through its view.

import { expect } from "vitest";
import { EditorView } from "@codemirror/view";
import { language } from "@codemirror/language";
import { screen } from "./render";

/** The editor under the element with ``testId``, with its line-number gutter. */
export function editorOf(testId: string): EditorView {
  const field = screen.getByTestId(testId);
  expect(field.querySelector("textarea")).toBeNull();
  expect(field.querySelector(".cm-lineNumbers")).not.toBeNull();
  const dom = field.querySelector(".cm-editor");
  expect(dom).not.toBeNull();
  const view = EditorView.findFromDOM(dom as HTMLElement);
  expect(view).not.toBeNull();
  return view as EditorView;
}

/** The name of the language the editor highlights. */
export function languageOf(view: EditorView): string | undefined {
  return view.state.facet(language)?.name;
}

/** Replace the editor's whole text, as a person typing it would. */
export function typeInto(view: EditorView, text: string): void {
  view.dispatch({ changes: { from: 0, to: view.state.doc.length, insert: text } });
}
