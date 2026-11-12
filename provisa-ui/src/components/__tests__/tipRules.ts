// Copyright (c) 2026 Kenneth Stott
// Canary: aa9739aa-b4d4-482e-8990-5c2dfb48c4a5
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// What the tooltip guard reads out of one component file (tipGuard.test.ts). One style of
// tooltip: components/Tip.tsx draws every hint, so nothing else imports the library's own
// tooltip pieces and no control carries a native `title`.

import ts from "typescript";

export interface TipViolation {
  rule: "library-tooltip" | "title-on-control";
  line: number;
  detail: string;
}

/** The library pieces only components/Tip.tsx may import. */
const LIBRARY_TOOLTIPS = new Set(["Tooltip", "HoverCard"]);

/**
 * Elements that act when pressed, or are an icon: a native `title` on one of these is a tooltip
 * in the browser's own style. (A `title` prop on Modal, Alert, HelpBubble, ConfirmDialog,
 * SplitPane, CollapsibleSection or Drawer is that component's heading; on an <iframe> it is the
 * frame's accessible name. Neither is a tooltip and neither is read here.)
 */
const CONTROLS = new Set([
  "button",
  "a",
  "input",
  "svg",
  "Button",
  "ActionIcon",
  "UnstyledButton",
  "CloseButton",
  "CopyButton",
  "Anchor",
  "Checkbox",
  "Switch",
  "ThemeIcon",
]);

/**
 * A hint whose target is also a trigger: the element directly inside a menu's, popover's or hover
 * card's `.Target`. The trigger hands its click handler, ref and aria state to that element, so
 * what sits there must pass them on. Tip and TipSurface do (Tip.test.tsx holds it); HelpBubble does
 * not. Returns the tag of each such child and its line.
 */
export function tipsInsideTriggers(file: string, source: string): { tag: string; line: number }[] {
  const sf = ts.createSourceFile(file, source, ts.ScriptTarget.Latest, true, ts.ScriptKind.TSX);
  const out: { tag: string; line: number }[] = [];
  const visit = (n: ts.Node): void => {
    if (ts.isJsxElement(n) && /\.Target$/.test(n.openingElement.tagName.getText())) {
      const child = n.children.find((c) => ts.isJsxElement(c) || ts.isJsxSelfClosingElement(c));
      if (child) {
        const opening = ts.isJsxElement(child) ? child.openingElement : child;
        const tag = (opening as ts.JsxOpeningLikeElement).tagName.getText();
        // CanvasButton (provisa-graph/CanvasControls.tsx) is a Tip round a canvas control.
        if (tag === "Tip" || tag === "TipSurface" || tag === "HelpBubble" || tag === "CanvasButton")
          out.push({ tag, line: sf.getLineAndCharacterOfPosition(child.getStart()).line + 1 });
      }
    }
    ts.forEachChild(n, visit);
  };
  visit(sf);
  return out;
}

export function checkTipRules(file: string, source: string): TipViolation[] {
  const sf = ts.createSourceFile(file, source, ts.ScriptTarget.Latest, true, ts.ScriptKind.TSX);
  const out: TipViolation[] = [];
  const lineOf = (n: ts.Node) => sf.getLineAndCharacterOfPosition(n.getStart()).line + 1;
  const visit = (n: ts.Node): void => {
    if (
      ts.isImportDeclaration(n) &&
      ts.isStringLiteral(n.moduleSpecifier) &&
      n.moduleSpecifier.text === "@mantine/core" &&
      n.importClause?.namedBindings &&
      ts.isNamedImports(n.importClause.namedBindings)
    ) {
      for (const el of n.importClause.namedBindings.elements) {
        const name = (el.propertyName ?? el.name).text;
        if (LIBRARY_TOOLTIPS.has(name))
          out.push({ rule: "library-tooltip", line: lineOf(el), detail: name });
      }
    }
    if (ts.isJsxOpeningElement(n) || ts.isJsxSelfClosingElement(n)) {
      const tag = n.tagName.getText();
      for (const at of n.attributes.properties) {
        if (!ts.isJsxAttribute(at) || at.name.getText() !== "title") continue;
        if (CONTROLS.has(tag))
          out.push({ rule: "title-on-control", line: lineOf(at), detail: tag });
      }
    }
    ts.forEachChild(n, visit);
  };
  visit(sf);
  return out;
}
