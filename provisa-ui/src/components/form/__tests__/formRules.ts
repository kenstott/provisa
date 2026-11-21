// Copyright (c) 2026 Kenneth Stott
// Canary: 920ed371-ac39-490c-8db5-afdc7347ed97
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1968: the form rules, read off the parsed TSX of one file. formLayoutGuard.test.ts runs
// them over every component; the rules are here so the guard can also be shown to fail.

import ts from "typescript";

export type FormRule =
  /** A single-line field stretched across the form. */
  | "full-width-single-line"
  /** A form laying its fields out itself: its own grid, or a field outside a FormField. */
  | "outside-shared-layout"
  /** Action buttons aligned anywhere but the start of the line. */
  | "actions-not-at-start"
  /** A button stretched by `fullWidth` or by its container. */
  | "button-stretched"
  /**
   * Text between a label and its control, or an explanation too long to sit under a field: a
   * brief one is FormField's `hint` (below the control), a longer one its `help` (behind a (?)).
   */
  | "explanation-misplaced"
  /** A tick or switch outside the form's group of ticks. */
  | "tick-ungrouped"
  /** A Save or Cancel button that is not the shared footer's: its look and its saving state differ. */
  | "save-cancel-not-shared";

export interface FormViolation {
  rule: FormRule;
  line: number;
  detail: string;
}

export interface FormRuleOptions {
  /** The file holds controls that are not a form (a search box, a toolbar, a query editor). */
  notAForm?: boolean;
  /**
   * The file edits something in place in a small surface (a popover, a row of a list): its
   * controls are not a form, and its compact Save/Apply need not be the shared footer's. Its
   * Cancel is still the one Cancel (CancelButton): one look everywhere (maintainer, 2026-11-12).
   */
  inPlaceEdit?: boolean;
  /** Buttons that may fill their container: each is matched by a substring of its own JSX. */
  stretchedButtonExceptions?: readonly string[];
  /** The English text of a translation key, so the length of a hint can be held. */
  textOf?: (key: string) => string | undefined;
}

/** The longest explanation that may sit under a field, in English characters. */
export const HINT_MAX_CHARS = 90;

const SINGLE_LINE = new Set([
  "TextInput",
  "NumberInput",
  "PasswordInput",
  "Select",
  "MultiSelect",
  "Autocomplete",
  "NativeSelect",
  "TagsInput",
  "Checkbox",
  "Switch",
  "input",
  "select",
  // One reusable control each, placed by the form that uses it.
  "RegionSelect",
  "ReplicateSelect",
]);
const TICKS = new Set(["Checkbox", "Switch"]);
/** What may lay a FormField out. */
const FIELD_PARENTS = new Set([
  "FormLayout",
  "FieldGroup",
  "FormIdentifiers",
  "FormChecks",
  "CollapsibleSection",
  "FormFold",
]);
/** Options of one choice: the group is the field, its boxes are not ticks of the form. */
const CHOICE_GROUPS = new Set(["Checkbox.Group", "Radio.Group", "Switch.Group"]);
const MULTI_LINE = new Set(["Textarea", "JsonInput", "textarea"]);
const TABLE_CELLS = new Set(["Table.Td", "Table.Th", "td", "th"]);
/** Elements that render their child in place, so the child's layout parent is theirs. */
const TRANSPARENT = new Set(["Tooltip", "CapabilityGate", "Fragment", "React.Fragment"]);
/** Containers that set their own column count and breakpoints. */
const OWN_GRIDS = new Set(["SimpleGrid", "Grid"]);
const END_JUSTIFY = /^(flex-end|end|right)$/;
const SPREAD_JUSTIFY = /^(space-between|space-around|space-evenly|center)$/;
const ROW_SPAN = /gridColumn:\s*["'`]1\s*\/\s*-1["'`]/;

type JsxEl = ts.JsxElement | ts.JsxSelfClosingElement;

function isJsxEl(n: ts.Node): n is JsxEl {
  return ts.isJsxElement(n) || ts.isJsxSelfClosingElement(n);
}

function opening(n: JsxEl): ts.JsxOpeningElement | ts.JsxSelfClosingElement {
  return ts.isJsxElement(n) ? n.openingElement : n;
}

function tagOf(n: JsxEl): string {
  return opening(n).tagName.getText();
}

/** The source text of an attribute's value, `"true"` for a bare attribute, undefined when absent. */
function attr(n: JsxEl, name: string): string | undefined {
  for (const a of opening(n).attributes.properties) {
    if (ts.isJsxAttribute(a) && a.name.getText() === name) {
      if (!a.initializer) return "true";
      if (ts.isStringLiteral(a.initializer)) return a.initializer.text;
      if (ts.isJsxExpression(a.initializer) && a.initializer.expression) {
        const e = a.initializer.expression;
        return ts.isStringLiteralLike(e) ? e.text : e.getText();
      }
      return a.initializer.getText();
    }
  }
  return undefined;
}

export function checkFormRules(
  fileName: string,
  source: string,
  options: FormRuleOptions = {},
): FormViolation[] {
  const sf = ts.createSourceFile(fileName, source, ts.ScriptTarget.Latest, true, ts.ScriptKind.TSX);
  // A surface that edits in place holds controls that are not a form either.
  const notAForm = options.notAForm === true || options.inPlaceEdit === true;
  const violations: FormViolation[] = [];
  const report = (rule: FormRule, node: ts.Node, detail: string) =>
    violations.push({
      rule,
      line: sf.getLineAndCharacterOfPosition(node.getStart()).line + 1,
      detail,
    });

  // Style objects declared once and passed by name: `const full = { gridColumn: "1 / -1" }`.
  const rowSpanStyles = new Set<string>();
  const collectStyles = (n: ts.Node) => {
    if (
      ts.isVariableDeclaration(n) &&
      ts.isIdentifier(n.name) &&
      n.initializer &&
      ROW_SPAN.test(n.initializer.getText())
    ) {
      rowSpanStyles.add(n.name.text);
    }
    ts.forEachChild(n, collectStyles);
  };
  collectStyles(sf);

  const spansRow = (n: JsxEl): boolean => {
    const style = attr(n, "style");
    return style !== undefined && (ROW_SPAN.test(style) || rowSpanStyles.has(style));
  };

  /** The element that lays this one out: through fragments, conditions, `.map` and wrappers. */
  const layoutParent = (n: ts.Node): JsxEl | undefined => {
    for (let p = n.parent; p; p = p.parent) {
      if (ts.isJsxAttribute(p)) return undefined;
      if (ts.isJsxElement(p)) {
        if (TRANSPARENT.has(tagOf(p))) continue;
        return p;
      }
      if (ts.isArrowFunction(p) || ts.isFunctionExpression(p)) {
        if (!ts.isCallExpression(p.parent)) return undefined;
      } else if (ts.isFunctionDeclaration(p) || ts.isMethodDeclaration(p)) {
        return undefined;
      }
    }
    return undefined;
  };

  const hasAncestor = (n: ts.Node, tags: Set<string>): boolean => {
    for (let p = n.parent; p; p = p.parent) {
      if (ts.isJsxElement(p) && tags.has(tagOf(p))) return true;
    }
    return false;
  };

  /** The elements an element lays out directly, seen the way `layoutParent` sees them. */
  const layoutChildren = (parent: ts.JsxElement): JsxEl[] => {
    const found: JsxEl[] = [];
    const visit = (n: ts.Node) => {
      if (isJsxEl(n) && n !== parent) {
        if (layoutParent(n) === parent) found.push(n);
        if (!TRANSPARENT.has(tagOf(n))) return;
      }
      if (ts.isJsxAttribute(n)) return;
      ts.forEachChild(n, visit);
    };
    visit(parent);
    return found;
  };

  const holdsControl = (parent: ts.JsxElement): boolean => {
    let found = false;
    const look = (n: ts.Node) => {
      if (found) return;
      if (isJsxEl(n) && (SINGLE_LINE.has(tagOf(n)) || MULTI_LINE.has(tagOf(n)))) {
        found = !hasAncestor(n, TABLE_CELLS);
        return;
      }
      ts.forEachChild(n, look);
    };
    ts.forEachChild(parent, look);
    return found;
  };

  const isExceptedButton = (n: JsxEl): boolean =>
    (options.stretchedButtonExceptions ?? []).some((marker) => n.getText().includes(marker));

  const checkControl = (n: JsxEl, tag: string) => {
    if (attr(n, "type") === "hidden" || attr(n, "type") === "file") return;
    const single = SINGLE_LINE.has(tag);
    if (single && spansRow(n)) {
      report("full-width-single-line", n, `<${tag}> spans the row`);
    }
    if (notAForm || hasAncestor(n, TABLE_CELLS)) return;
    if (tag === "select" && !/\bform-select\b/.test(attr(n, "className") ?? "")) {
      report(
        "outside-shared-layout",
        n,
        `a native <select> in a form without className="form-select"`,
      );
    }
    if (tag === "NativeSelect") {
      report(
        "outside-shared-layout",
        n,
        '<NativeSelect>: use Select, or a native <select className="form-select">',
      );
    }
    if (attr(n, "description") !== undefined) {
      report(
        "explanation-misplaced",
        n,
        `<${tag} description>: use the FormField's hint (brief, below) or help (behind a (?))`,
      );
    }
    const grouped = hasAncestor(n, new Set(["FormChecks"]));
    // A tick that is the whole of a small component (its FormField is what the component
    // returns) is placed by the form that mounts it, inside that form's FormChecks.
    const ownField = layoutParent(n);
    const placedByMount =
      ownField !== undefined &&
      tagOf(ownField) === "FormField" &&
      layoutParent(ownField) === undefined;
    if (TICKS.has(tag) && !grouped && !placedByMount && !hasAncestor(n, CHOICE_GROUPS)) {
      report("tick-ungrouped", n, `<${tag}> is not in the form's <FormChecks>`);
    }
    if (!grouped && !hasAncestor(n, new Set(["FormField"]))) {
      report("outside-shared-layout", n, `<${tag}> is not inside a <FormField>`);
    }
  };

  /** The text an attribute shows: a literal, or the English of `t("key")`. */
  const shownText = (n: JsxEl, name: string): string | undefined => {
    for (const a of opening(n).attributes.properties) {
      if (!ts.isJsxAttribute(a) || a.name.getText() !== name || !a.initializer) continue;
      if (ts.isStringLiteral(a.initializer)) return a.initializer.text;
      const e = ts.isJsxExpression(a.initializer) ? a.initializer.expression : undefined;
      if (e && ts.isStringLiteralLike(e)) return e.text;
      if (e && ts.isCallExpression(e) && e.expression.getText() === "t") {
        const key = e.arguments[0];
        if (key && ts.isStringLiteralLike(key)) return options.textOf?.(key.text);
      }
    }
    return undefined;
  };

  const checkExplanation = (n: ts.JsxElement) => {
    const hint = attr(n, "hint");
    if (hint === undefined) return;
    const child = layoutChildren(n)[0];
    if (child && TICKS.has(tagOf(child))) {
      report("explanation-misplaced", n, "a tick takes help behind a (?), not a hint beside it");
    }
    const text = shownText(n, "hint");
    if (text !== undefined && text.length > HINT_MAX_CHARS) {
      report(
        "explanation-misplaced",
        n,
        `a hint of ${text.length} characters: over ${HINT_MAX_CHARS} it is help, behind a (?)`,
      );
    }
  };

  const checkFieldParent = (n: ts.JsxElement) => {
    const parent = layoutParent(n);
    if (parent && !FIELD_PARENTS.has(tagOf(parent))) {
      report("outside-shared-layout", n, `<FormField> laid out by a <${tagOf(parent)}>`);
    }
  };

  const checkFormField = (n: ts.JsxElement) => {
    if (attr(n, "size") !== "full") return;
    const children = layoutChildren(n);
    if (children.length === 1 && SINGLE_LINE.has(tagOf(children[0]))) {
      report("full-width-single-line", n, `<${tagOf(children[0])}> in a full-width FormField`);
    }
  };

  const checkActionRow = (n: ts.JsxElement, tag: string) => {
    // The actions of one row of a list sit at the end of the row: they are not a form's actions.
    if (hasAncestor(n, TABLE_CELLS)) return;
    const className = attr(n, "className") ?? "";
    if (/\bmodal-actions\b/.test(className)) {
      report("actions-not-at-start", n, "modal-actions aligns its buttons to the end");
      return;
    }
    const style = attr(n, "style") ?? "";
    const styleJustify = /justifyContent:\s*["'`]([a-z-]+)["'`]/.exec(style)?.[1];
    const justify = tag === "Group" || tag === "Flex" ? attr(n, "justify") : styleJustify;
    if (justify === undefined) return;
    const children = layoutChildren(n);
    const buttons = children.filter((c) => tagOf(c) === "Button");
    if (buttons.length === 0) return;
    const onlyButtons = buttons.length === children.length;
    if (END_JUSTIFY.test(justify) || (SPREAD_JUSTIFY.test(justify) && onlyButtons)) {
      report("actions-not-at-start", n, `buttons in a row justified "${justify}"`);
    }
  };

  /** The one text a button shows, when it is a literal or `t("key")`. */
  const buttonWord = (n: JsxEl): string | undefined => {
    if (!ts.isJsxElement(n)) return undefined;
    const parts = n.children.filter((c) => !(ts.isJsxText(c) && c.containsOnlyTriviaWhiteSpaces));
    if (parts.length !== 1) return undefined;
    const only = parts[0];
    if (ts.isJsxText(only)) return only.text.trim();
    const e = ts.isJsxExpression(only) ? only.expression : undefined;
    if (e && ts.isStringLiteralLike(e)) return e.text;
    if (e && ts.isCallExpression(e) && e.expression.getText() === "t") {
      const key = e.arguments[0];
      if (key && ts.isStringLiteralLike(key)) return options.textOf?.(key.text);
    }
    return undefined;
  };

  const checkButton = (n: JsxEl) => {
    const word = buttonWord(n);
    const compact = /^compact-/.test(attr(n, "size") ?? "");
    if (word === "Save" && !(options.inPlaceEdit && compact)) {
      report("save-cancel-not-shared", n, `a "Save" button outside <FormFooter onSave onCancel>`);
    }
    if (word === "Cancel") {
      report(
        "save-cancel-not-shared",
        n,
        `a "Cancel" button of its own: <FormFooter onCancel> or <CancelButton>`,
      );
    }
    if (isExceptedButton(n)) return;
    if (attr(n, "fullWidth") !== undefined && attr(n, "fullWidth") !== "false") {
      report("button-stretched", n, "<Button fullWidth>");
      return;
    }
    if (spansRow(n)) {
      report("button-stretched", n, "<Button> spans the row");
      return;
    }
    const parent = layoutParent(n);
    if (!parent) return;
    const parentTag = tagOf(parent);
    const style = attr(n, "style") ?? "";
    const keepsOwnWidth = attr(n, "w") !== undefined || /alignSelf|justifySelf/.test(style);
    if (parentTag === "Stack") {
      const align = attr(parent, "align");
      if ((align === undefined || align === "stretch") && !keepsOwnWidth) {
        report("button-stretched", n, "<Button> is a direct child of a stretching <Stack>");
      }
    } else if (
      /\bform-card\b/.test(attr(parent, "className") ?? "") &&
      parentTag !== "FormLayout"
    ) {
      if (!keepsOwnWidth) {
        report("button-stretched", n, "<Button> is a direct child of a form-card grid");
      }
    }
  };

  const visit = (n: ts.Node) => {
    if (isJsxEl(n)) {
      const tag = tagOf(n);
      if (SINGLE_LINE.has(tag) || MULTI_LINE.has(tag)) checkControl(n, tag);
      if (tag === "Button") checkButton(n);
      if (ts.isJsxElement(n)) {
        if (tag === "FormField") {
          checkFormField(n);
          checkExplanation(n);
          checkFieldParent(n);
        }
        checkActionRow(n, tag);
      }
      if (tag !== "FormLayout" && /\bform-(card|row)\b/.test(attr(n, "className") ?? "")) {
        report("outside-shared-layout", n, `<${tag} className="${attr(n, "className")}">`);
      }
      const isControl = SINGLE_LINE.has(tag) || MULTI_LINE.has(tag);
      if (!isControl && tag !== "Button" && spansRow(n)) {
        report("outside-shared-layout", n, `<${tag}> placed with gridColumn`);
      }
      if (OWN_GRIDS.has(tag) && ts.isJsxElement(n) && !notAForm && holdsControl(n)) {
        report("outside-shared-layout", n, `fields laid out by a <${tag}> with its own columns`);
      }
    }
    ts.forEachChild(n, visit);
  };
  visit(sf);
  return violations.sort((a, b) => a.line - b.line);
}
