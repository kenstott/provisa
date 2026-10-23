// Copyright (c) 2026 Kenneth Stott
// Canary: 8fd2a4b5-8add-44d8-9272-2fb2fcb0848d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// What the toolbar guard reads out of one component file (toolbarGuard.test.ts). In a toolbar --
// an element whose class names a toolbar, a page's head actions, or one marked `data-toolbar` --
// the named controls come first
// (buttons with words, segmented controls, chips, selects, sliders), then the icon-only buttons,
// then the (?) help last. An icon-only button is borderless wherever it is: subtle or transparent,
// or light for a toggle while it is on.

import ts from "typescript";

export type ToolbarKind = "named" | "icon" | "help";

export interface ToolbarViolation {
  rule: "bordered-icon-button" | "toolbar-order";
  line: number;
  detail: string;
}

/** The variants an icon-only button may take: none draws a border. */
const BORDERLESS = new Set(["subtle", "transparent", "light"]);

/** Components that draw one icon-only button. */
const ICON_COMPONENTS = new Set([
  "ActionIcon",
  "ExpandButton",
  "OpenAsQuery",
  "ExportMenu",
  "OntologyToggle",
]);

/** Containers read through: their children stand in the toolbar in their order. */
const CONTAINERS = new Set(["Group", "div", "span", "Stack", ""]);

/** What a toolbar shows that is no control: its labels, badges and spacing. */
const NEUTRAL = new Set(["Text", "Divider", "Space", "Badge"]);

const isToolbar = (className: string) =>
  /\btoolbar\b|-toolbar\b|\bp3d-toolbar\b/.test(className) || /\bpage-actions\b/.test(className);

function attr(n: ts.JsxOpeningLikeElement, name: string): ts.JsxAttribute | undefined {
  return n.attributes.properties.find(
    (a): a is ts.JsxAttribute => ts.isJsxAttribute(a) && a.name.getText() === name,
  );
}

/** Every string a JSX attribute can take: its literal, or each literal in its expression. */
function attrStrings(a: ts.JsxAttribute): string[] {
  const init = a.initializer;
  if (!init) return [];
  if (ts.isStringLiteral(init)) return [init.text];
  const out: string[] = [];
  const visit = (n: ts.Node): void => {
    if (ts.isStringLiteral(n) || ts.isNoSubstitutionTemplateLiteral(n)) out.push(n.text);
    ts.forEachChild(n, visit);
  };
  visit(init);
  return out;
}

/** The strings an expression can come to: a literal, or either branch of a choice. */
function results(e: ts.Expression): string[] {
  if (ts.isStringLiteral(e) || ts.isNoSubstitutionTemplateLiteral(e)) return [e.text];
  if (ts.isParenthesizedExpression(e)) return results(e.expression);
  if (ts.isConditionalExpression(e)) return [...results(e.whenTrue), ...results(e.whenFalse)];
  if (
    ts.isBinaryExpression(e) &&
    (e.operatorToken.kind === ts.SyntaxKind.QuestionQuestionToken ||
      e.operatorToken.kind === ts.SyntaxKind.BarBarToken)
  )
    return [...results(e.left), ...results(e.right)];
  return [e.getText()];
}

/** The values a JSX attribute can take. */
function attrResults(a: ts.JsxAttribute): string[] {
  const init = a.initializer;
  if (!init) return [];
  if (ts.isStringLiteral(init)) return [init.text];
  return ts.isJsxExpression(init) && init.expression ? results(init.expression) : [];
}

const opening = (n: ts.Node): ts.JsxOpeningLikeElement | null =>
  ts.isJsxElement(n) ? n.openingElement : ts.isJsxSelfClosingElement(n) ? n : null;

/** The kinds of what a toolbar holds, in order, with each one's line. */
function kindsIn(nodes: readonly ts.Node[], sf: ts.SourceFile): [ToolbarKind, number][] {
  const out: [ToolbarKind, number][] = [];
  const line = (n: ts.Node) => sf.getLineAndCharacterOfPosition(n.getStart()).line + 1;
  const take = (n: ts.Node): void => {
    if (ts.isJsxFragment(n)) {
      n.children.forEach(take);
      return;
    }
    if (ts.isJsxExpression(n) || ts.isParenthesizedExpression(n) || ts.isBinaryExpression(n)) {
      ts.forEachChild(n, take);
      return;
    }
    if (ts.isConditionalExpression(n)) {
      take(n.whenTrue);
      take(n.whenFalse);
      return;
    }
    if (
      ts.isCallExpression(n) ||
      ts.isArrowFunction(n) ||
      ts.isBlock(n) ||
      ts.isReturnStatement(n)
    ) {
      ts.forEachChild(n, take);
      return;
    }
    const open = opening(n);
    if (!open) return;
    const tag = open.tagName.getText();
    const children = ts.isJsxElement(n) ? n.children : [];
    if (tag === "Tip" || tag === "TipSurface" || CONTAINERS.has(tag)) {
      children.forEach(take);
      return;
    }
    if (NEUTRAL.has(tag)) return;
    // A menu or popover stands in the toolbar as its trigger.
    if (tag === "Menu" || tag === "Popover" || tag === "HoverCard") {
      for (const c of children) {
        const o = opening(c);
        if (o && /\.Target$/.test(o.tagName.getText()) && ts.isJsxElement(c))
          c.children.forEach(take);
      }
      return;
    }
    if (tag === "HelpBubble") {
      out.push([attr(open, "target") ? "icon" : "help", line(n)]);
      return;
    }
    out.push([ICON_COMPONENTS.has(tag) ? "icon" : "named", line(n)]);
  };
  nodes.forEach(take);
  return out;
}

const RANK: Record<ToolbarKind, number> = { named: 0, icon: 1, help: 2 };

export function checkToolbarRules(file: string, source: string): ToolbarViolation[] {
  const sf = ts.createSourceFile(file, source, ts.ScriptTarget.Latest, true, ts.ScriptKind.TSX);
  const out: ToolbarViolation[] = [];
  const lineOf = (n: ts.Node) => sf.getLineAndCharacterOfPosition(n.getStart()).line + 1;
  const visit = (n: ts.Node): void => {
    const open = opening(n);
    if (open && open.tagName.getText() === "ActionIcon") {
      const variant = attr(open, "variant");
      const said = variant ? attrResults(variant) : [];
      const bordered = variant === undefined ? ["filled (the default)"] : said;
      const wrong = bordered.filter((v) => !BORDERLESS.has(v));
      if (wrong.length > 0)
        out.push({ rule: "bordered-icon-button", line: lineOf(n), detail: wrong.join(", ") });
    }
    if (open && ts.isJsxElement(n)) {
      const cls = attr(open, "className");
      if (attr(open, "data-toolbar") || (cls && attrStrings(cls).some(isToolbar))) {
        const kinds = kindsIn(n.children, sf);
        for (let i = 1; i < kinds.length; i++)
          if (RANK[kinds[i][0]] < RANK[kinds[i - 1][0]])
            out.push({
              rule: "toolbar-order",
              line: kinds[i][1],
              detail: `a ${kinds[i][0]} control after a ${kinds[i - 1][0]} one`,
            });
      }
    }
    ts.forEachChild(n, visit);
  };
  visit(sf);
  return out;
}
