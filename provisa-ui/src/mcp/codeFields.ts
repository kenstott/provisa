// Copyright (c) 2026 Kenneth Stott
// Canary: 5af1adc4-3d9b-4b28-9b77-6d604a0bd328
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Requirements: REQ-2114, REQ-2115

import { EditorView, type Extension } from "@uiw/react-codemirror";

/**
 * REQ-2115: a form's code field (CodeEditorField) as Polly reads and fills it. The field marks its
 * editable surface, CodeMirror's content element (a textbox), with its language and its accessible
 * name; read_screen reports it from the editor's own state, and a fill replaces its text by a
 * CodeMirror transaction, so the field's onChange fires as for a person's typing. A page's query
 * editor carries no mark: it is reported and filled through its own state (screenState.ts
 * useScreenEditor, navigate's state).
 */
const CODE_FIELD_ATTR = "data-code-field";
export const CODE_FIELD_SELECTOR = `.cm-content[${CODE_FIELD_ATTR}]`;

/** How a code field is named: by its label's text, or by the element holding its label's text
 * (a label with help, FormField's naming), with the help as its description. */
export type CodeFieldName =
  { label: string; description?: string } | { labelledBy: string; description?: string };

/** The mark a code field puts on its editor: its language and its accessible name. */
export function codeFieldMark(name: CodeFieldName, language: string): Extension {
  const attrs: Record<string, string> =
    "label" in name ? { "aria-label": name.label } : { "aria-labelledby": name.labelledBy };
  if (name.description !== undefined) attrs["aria-description"] = name.description;
  return EditorView.contentAttributes.of({ ...attrs, [CODE_FIELD_ATTR]: language });
}

/** A code field's editor `el` is, is inside, or holds alone; null when it is none of these. */
export function codeFieldOf(el: Element): HTMLElement | null {
  const around = el.closest<HTMLElement>(CODE_FIELD_SELECTOR);
  if (around) return around;
  const inside = el.querySelectorAll<HTMLElement>(CODE_FIELD_SELECTOR);
  return inside.length === 1 ? inside[0] : null;
}

function viewOf(content: HTMLElement): EditorView {
  const view = EditorView.findFromDOM(content);
  // The mark is only ever on a mounted editor's content element, which CodeMirror owns.
  if (view === null) throw new Error("a marked code field has no CodeMirror view");
  return view;
}

export interface CodeFieldState {
  value: string;
  language: string;
  readOnly: boolean;
}

/** A code field's editor (an element CODE_FIELD_SELECTOR matches), read from its own state. */
export function readCodeField(content: HTMLElement): CodeFieldState {
  const view = viewOf(content);
  const language = content.getAttribute(CODE_FIELD_ATTR);
  // CODE_FIELD_SELECTOR matches only an element carrying the attribute.
  if (language === null) throw new Error(`a code field without ${CODE_FIELD_ATTR}`);
  return { value: view.state.doc.toString(), language, readOnly: view.state.readOnly };
}

/** Replaces a code field's whole text with `value`, as a person's typing would. */
export function replaceCodeFieldText(content: HTMLElement, value: string): void {
  const view = viewOf(content);
  view.dispatch({ changes: { from: 0, to: view.state.doc.length, insert: value } });
}
