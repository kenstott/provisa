// Copyright (c) 2026 Kenneth Stott
// Canary: 5e0c1f0a-6b7d-4f1e-9a52-2b8d3c4e7f61
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// What a file renders into a detail panel, read from its text: shared by the panel guards
// (panelHeadGuard, panelBodyGuard).

/** The text of the JSX element that opens at `start`, to its own `/>` or closing tag. */
export function element(text: string, start: number, name: string): string {
  let depth = 0;
  for (let i = start; i < text.length; i += 1) {
    const c = text[i];
    if (c === "{") depth += 1;
    else if (c === "}") depth -= 1;
    else if (depth === 0 && text.startsWith("/>", i)) return text.slice(start, i + 2);
    else if (depth === 0 && c === ">") {
      const end = text.indexOf(`</${name}>`, i);
      return text.slice(start, end < 0 ? text.length : end);
    }
  }
  return text.slice(start);
}

/** The parts of a file that are rendered in a panel's head. */
export function headParts(text: string): string[] {
  const parts: string[] = [];
  for (const m of text.matchAll(/<PanelActions>/g))
    parts.push(element(text, m.index, "PanelActions"));
  for (const m of text.matchAll(/<FormFooter\b/g)) {
    const footer = element(text, m.index, "FormFooter");
    if (
      /^<FormFooter[^>]*?\n?\s+panel\b/.test(footer) ||
      /\spanel(\s|=|\/|>)/.test(footer.split("action=")[0])
    ) {
      parts.push(footer);
    }
  }
  return parts;
}

/** A file that renders a detail panel: actions in its head, a form whose footer is the head's, or a wizard step. */
export function rendersPanel(text: string): boolean {
  return headParts(text).length > 0 || /from "\.\/WizardFrame"/.test(text);
}
