// Copyright (c) 2026 Kenneth Stott
// Canary: 4a8e2c61-d97b-43f0-85e1-b3c6f0a9d274
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2277 rule 10: the dialog rules, read off the parsed TSX of one file. modalGuard.test.ts runs
// them over every component; the rules are here so the guard can also be shown to fail.

import ts from "typescript";

export type ModalRule =
  /** A dialog drawn with Mantine's Modal itself, not the shared ModalLayout. */
  | "not-modal-layout"
  /** A dialog's button in its scrolling content, not in the fixed bar (a FormFooter). */
  | "button-in-content";

export interface ModalViolation {
  rule: ModalRule;
  line: number;
  detail: string;
}

export interface ModalRuleOptions {
  /**
   * The file is the content of a dialog that another file opens: every button in it is held, not
   * only those inside a `<ModalLayout>` of its own.
   */
  dialogContent?: boolean;
  /** Buttons that act on part of the content, not on the dialog: each is matched by a substring
   *  of its own JSX. */
  contentButtons?: readonly string[];
}

/** The buttons a dialog's actions are drawn with. */
const BUTTONS = new Set(["Button", "SaveButton", "CancelButton", "DestructiveConfirmButton"]);

/**
 * Where a button acts on part of the content, never on the dialog: a row of a table, a menu, a
 * popover, a tab of the content.
 */
const CONTENT_PLACES = new Set([
  "Table.Td",
  "Table.Th",
  "td",
  "th",
  "Menu",
  "Popover",
  "Combobox",
  "HoverCard",
]);

/** A dialog: the shared layout, and Mantine's Modal, whose buttons are held as well while the
 *  file is told to draw it with the layout. */
const DIALOGS = new Set(["ModalLayout", "Modal"]);

type JsxEl = ts.JsxElement | ts.JsxSelfClosingElement;

function isJsxEl(n: ts.Node): n is JsxEl {
  return ts.isJsxElement(n) || ts.isJsxSelfClosingElement(n);
}

function tagOf(n: JsxEl): string {
  return (ts.isJsxElement(n) ? n.openingElement : n).tagName.getText();
}

/** The local components a file's `<ModalLayout>` renders, by the path their import names. */
export function dialogContentImports(fileName: string, source: string): string[] {
  const sf = ts.createSourceFile(fileName, source, ts.ScriptTarget.Latest, true, ts.ScriptKind.TSX);
  const imported = new Map<string, string>();
  for (const st of sf.statements) {
    if (!ts.isImportDeclaration(st) || !ts.isStringLiteral(st.moduleSpecifier)) continue;
    const from = st.moduleSpecifier.text;
    if (!from.startsWith(".")) continue;
    const named = st.importClause?.namedBindings;
    if (named && ts.isNamedImports(named)) {
      for (const el of named.elements) imported.set(el.name.text, from);
    }
    if (st.importClause?.name) imported.set(st.importClause.name.text, from);
  }
  const found = new Set<string>();
  const inDialog = (n: ts.Node) => {
    if (isJsxEl(n)) {
      const from = imported.get(tagOf(n));
      if (from !== undefined) found.add(from);
    }
    ts.forEachChild(n, inDialog);
  };
  const visit = (n: ts.Node) => {
    if (isJsxEl(n) && DIALOGS.has(tagOf(n))) {
      ts.forEachChild(n, inDialog);
      return;
    }
    ts.forEachChild(n, visit);
  };
  visit(sf);
  return [...found];
}

export function checkModalRules(
  fileName: string,
  source: string,
  options: ModalRuleOptions = {},
): ModalViolation[] {
  const sf = ts.createSourceFile(fileName, source, ts.ScriptTarget.Latest, true, ts.ScriptKind.TSX);
  const out: ModalViolation[] = [];
  const report = (rule: ModalRule, n: ts.Node, detail: string) =>
    out.push({ rule, line: sf.getLineAndCharacterOfPosition(n.getStart()).line + 1, detail });

  for (const st of sf.statements) {
    if (
      ts.isImportDeclaration(st) &&
      ts.isStringLiteral(st.moduleSpecifier) &&
      st.moduleSpecifier.text === "@mantine/core"
    ) {
      const named = st.importClause?.namedBindings;
      if (named && ts.isNamedImports(named)) {
        for (const el of named.elements) {
          if ((el.propertyName ?? el.name).text === "Modal") {
            report(
              "not-modal-layout",
              el,
              "imports Mantine's Modal: draw the dialog with <ModalLayout>",
            );
          }
        }
      }
    }
  }

  // The components of this file a dialog renders, and those they render in turn: their buttons
  // are the dialog's too.
  const components = new Map<string, ts.Node>();
  const nameOf = (n: ts.Node): string | undefined => {
    if (ts.isFunctionDeclaration(n) && n.name) return n.name.text;
    if (ts.isVariableDeclaration(n) && ts.isIdentifier(n.name) && n.initializer) {
      const init = n.initializer;
      if (ts.isArrowFunction(init) || ts.isFunctionExpression(init)) return n.name.text;
    }
    return undefined;
  };
  const collect = (n: ts.Node) => {
    const name = nameOf(n);
    if (name !== undefined && /^[A-Z]/.test(name)) components.set(name, n);
    ts.forEachChild(n, collect);
  };
  collect(sf);
  const rendered = (root: ts.Node, into: Set<string>) => {
    const walk = (n: ts.Node) => {
      if (isJsxEl(n) && components.has(tagOf(n))) into.add(tagOf(n));
      ts.forEachChild(n, walk);
    };
    ts.forEachChild(root, walk);
  };
  const dialogComponents = new Set<string>();
  const findDialogs = (n: ts.Node) => {
    if (isJsxEl(n) && DIALOGS.has(tagOf(n))) rendered(n, dialogComponents);
    ts.forEachChild(n, findDialogs);
  };
  findDialogs(sf);
  for (let grew = true; grew;) {
    const before = dialogComponents.size;
    for (const name of [...dialogComponents]) rendered(components.get(name)!, dialogComponents);
    grew = dialogComponents.size > before;
  }

  const excepted = (n: ts.Node) =>
    (options.contentButtons ?? []).some((marker) => n.getText().includes(marker));

  const checkButton = (n: JsxEl) => {
    let inDialog = options.dialogContent === true;
    for (let p: ts.Node | undefined = n.parent; p; p = p.parent) {
      const name = nameOf(p);
      if (name !== undefined && dialogComponents.has(name)) {
        inDialog = true;
        break;
      }
      if (!isJsxEl(p)) continue;
      const tag = tagOf(p);
      if (tag === "FormFooter" || CONTENT_PLACES.has(tag)) return;
      if (DIALOGS.has(tag)) {
        inDialog = true;
        break;
      }
    }
    if (!inDialog || excepted(n)) return;
    report(
      "button-in-content",
      n,
      `<${tagOf(n)}> in the dialog's scrolling content: put the dialog's actions in a <FormFooter>`,
    );
  };

  const visit = (n: ts.Node) => {
    if (isJsxEl(n) && BUTTONS.has(tagOf(n))) checkButton(n);
    ts.forEachChild(n, visit);
  };
  visit(sf);
  return out;
}
