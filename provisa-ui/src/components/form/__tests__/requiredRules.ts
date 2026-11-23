// Copyright (c) 2026 Kenneth Stott
// Canary: 28e64286-47ab-4ff7-a4c6-4a737d7b4587
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1968 (amended 2026-11-22): the rules of required fields, read off the parsed TSX of one file.
// formRequiredGuard.test.ts runs them over every component; they are here so the guard can also be
// shown to fail.
//
// - A form's primary action is never disabled by a missing or invalid field: it validates on the
//   click (useRequiredFields). It is disabled only when there is nothing to act on (`.length === 0`).
// - A field that blocks submit carries the required marker: a field a submit handler refuses while
//   empty is `required`, every field named to useRequiredFields is spread onto its control, and a
//   native select given `.native(...)` draws <RequiredMark /> in its label.

import ts from "typescript";

export type RequiredRule =
  /** A form's primary action disabled by a missing or invalid field. */
  | "primary-disabled-by-field"
  /** A field that blocks submit without the required marker. */
  | "required-unmarked";

export interface RequiredViolation {
  rule: RequiredRule;
  line: number;
  detail: string;
}

/** Names that state a field is missing or invalid: `incomplete`, `domainMissing`, `!ready`. */
const FIELD_STATE = /missing|incomplete|blank|^(ready|valid|.*Valid|.*Ready)$/i;
/** Controls whose `value` is a field the person fills in. */
const CONTROLS = new Set([
  "TextInput",
  "NumberInput",
  "PasswordInput",
  "Select",
  "Autocomplete",
  "Textarea",
  "JsonInput",
  "input",
  "select",
  "textarea",
]);

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

function attrNode(n: JsxEl, name: string): ts.JsxAttribute | undefined {
  for (const a of opening(n).attributes.properties) {
    if (ts.isJsxAttribute(a) && a.name.getText() === name) return a;
  }
  return undefined;
}

function attrExpr(n: JsxEl, name: string): ts.Expression | undefined {
  const a = attrNode(n, name);
  if (!a?.initializer) return undefined;
  return ts.isJsxExpression(a.initializer) ? a.initializer.expression : a.initializer;
}

const strip = (e: ts.Expression): ts.Expression => {
  let x = e;
  while (ts.isParenthesizedExpression(x) || ts.isNonNullExpression(x)) x = x.expression;
  return x;
};

const isEmptyLiteral = (e: ts.Expression) => {
  const x = strip(e);
  return (ts.isStringLiteralLike(x) && x.text === "") || x.kind === ts.SyntaxKind.NullKeyword;
};

/** `x.trim()` → `x`. */
function trimmed(e: ts.Expression): ts.Expression | undefined {
  const x = strip(e);
  if (
    ts.isCallExpression(x) &&
    ts.isPropertyAccessExpression(x.expression) &&
    x.expression.name.text === "trim"
  ) {
    return x.expression.expression;
  }
  return undefined;
}

const isName = (e: ts.Expression) =>
  ts.isIdentifier(e) || ts.isPropertyAccessExpression(e) || ts.isElementAccessExpression(e);

export function checkRequiredRules(fileName: string, source: string): RequiredViolation[] {
  const sf = ts.createSourceFile(fileName, source, ts.ScriptTarget.Latest, true, ts.ScriptKind.TSX);
  const violations: RequiredViolation[] = [];
  const report = (rule: RequiredRule, node: ts.Node, detail: string) =>
    violations.push({
      rule,
      line: sf.getLineAndCharacterOfPosition(node.getStart()).line + 1,
      detail,
    });

  // The fields of the file: each control's `value`, by its source text.
  const fields = new Map<string, JsxEl[]>();
  let isForm = false;
  const collect = (n: ts.Node) => {
    if (isJsxEl(n)) {
      const tag = tagOf(n);
      if (tag === "FormLayout") isForm = true;
      if (CONTROLS.has(tag)) {
        const value = attrExpr(n, "value");
        if (value && isName(strip(value))) {
          const key = strip(value).getText();
          fields.set(key, [...(fields.get(key) ?? []), n]);
        }
      }
    }
    ts.forEachChild(n, collect);
  };
  collect(sf);

  /** The fields an expression treats as missing: `!x`, `!x.trim()`, `x === ""`, `x === null`,
   *  `x.trim() === ""`, and names that say a field is missing or invalid. An empty list
   *  (`x.length === 0`) is nothing to act on, not a missing field. */
  const missingFields = (e: ts.Expression): string[] => {
    const found: string[] = [];
    const look = (n: ts.Node) => {
      if (ts.isPrefixUnaryExpression(n) && n.operator === ts.SyntaxKind.ExclamationToken) {
        const inner = strip(n.operand);
        const target = trimmed(inner) ?? inner;
        if (isName(target) && fields.has(target.getText())) found.push(target.getText());
      }
      if (
        ts.isBinaryExpression(n) &&
        [
          ts.SyntaxKind.EqualsEqualsEqualsToken,
          ts.SyntaxKind.EqualsEqualsToken,
        ].includes(n.operatorToken.kind)
      ) {
        for (const [side, other] of [
          [n.left, n.right],
          [n.right, n.left],
        ] as const) {
          if (!isEmptyLiteral(other)) continue;
          const target = trimmed(side) ?? strip(side);
          if (isName(target) && fields.has(target.getText())) found.push(target.getText());
        }
      }
      if (ts.isIdentifier(n) && FIELD_STATE.test(n.text)) {
        const p = n.parent;
        // The name itself, or a call of it: not a property of something else.
        const own = !(ts.isPropertyAccessExpression(p) && p.name === n);
        if (own) found.push(n.text);
      }
      ts.forEachChild(n, look);
    };
    look(e);
    return found;
  };

  const checkDisabled = (what: string, node: ts.Node, e: ts.Expression | undefined) => {
    if (!e) return;
    const missing = missingFields(e);
    if (missing.length > 0) {
      report(
        "primary-disabled-by-field",
        node,
        `${what} disabled by ${missing.join(", ")}: validate on the click (useRequiredFields)`,
      );
    }
  };

  const insideAttr = (n: ts.Node, name: string): boolean => {
    for (let p = n.parent; p; p = p.parent) {
      if (ts.isJsxAttribute(p)) return p.name.getText() === name;
    }
    return false;
  };

  const isPrimaryButton = (n: JsxEl): boolean => {
    if (tagOf(n) !== "Button") return false;
    if (insideAttr(n, "action")) return true;
    const type = attrExpr(n, "type");
    if (type && ts.isStringLiteralLike(type) && type.text === "submit") return true;
    const variant = attrExpr(n, "variant");
    const filled = variant === undefined || (ts.isStringLiteralLike(variant) && variant.text === "filled");
    return isForm && filled;
  };

  // What useRequiredFields is given, and which of its fields are spread onto a control.
  const declared = new Map<string, ts.Node>();
  const spread = new Set<string>();
  const declare = (obj: ts.Expression) => {
    const x = strip(obj);
    if (ts.isObjectLiteralExpression(x)) {
      for (const p of x.properties) {
        if (ts.isPropertyAssignment(p) || ts.isShorthandPropertyAssignment(p)) {
          declared.set(p.name.getText(), p);
        } else if (ts.isSpreadAssignment(p)) {
          declare(p.expression);
        }
      }
    } else if (ts.isConditionalExpression(x)) {
      declare(x.whenTrue);
      declare(x.whenFalse);
    }
  };

  const visit = (n: ts.Node) => {
    if (ts.isCallExpression(n)) {
      const callee = n.expression.getText();
      if (callee === "useRequiredFields" && n.arguments[0]) declare(n.arguments[0]);
      if (
        ts.isPropertyAccessExpression(n.expression) &&
        ["field", "native"].includes(n.expression.name.text) &&
        n.arguments[0] &&
        ts.isStringLiteralLike(n.arguments[0])
      ) {
        spread.add(n.arguments[0].text);
        if (n.expression.name.text === "native") {
          // A native select draws its marker in its label: <label>Text<RequiredMark /><select …>.
          const label = (() => {
            for (let p = n.parent; p; p = p.parent) {
              if (ts.isJsxElement(p) && tagOf(p) === "label") return p;
            }
            return undefined;
          })();
          const marked = label !== undefined && /<RequiredMark\s*\/>/.test(label.getText());
          if (!marked) {
            report(
              "required-unmarked",
              n,
              `a native select given .native("${n.arguments[0].text}") without <RequiredMark /> in its label`,
            );
          }
        }
      }
    }
    if (isJsxEl(n)) {
      const tag = tagOf(n);
      if (tag === "SaveButton") checkDisabled("<SaveButton>", n, attrExpr(n, "disabled"));
      if (isPrimaryButton(n)) checkDisabled("the primary <Button>", n, attrExpr(n, "disabled"));
      if (tag === "FormFooter") {
        const props = attrExpr(n, "saveProps");
        if (props && ts.isObjectLiteralExpression(strip(props))) {
          for (const p of (strip(props) as ts.ObjectLiteralExpression).properties) {
            if (ts.isPropertyAssignment(p) && p.name.getText() === "disabled") {
              checkDisabled("<FormFooter>'s Save", p, p.initializer);
            }
          }
        }
      }
    }
    // A submit handler that refuses an empty field: that field blocks submit, so it is marked.
    if (ts.isIfStatement(n)) {
      // It says why: an error set, not a bare return (an effect waiting for its inputs).
      const refuses = /\bset\w*(Error|Problem)\(/.test(n.thenStatement.getText());
      if (refuses) {
        for (const name of missingFields(n.expression)) {
          for (const control of fields.get(name) ?? []) {
            const marked =
              attrNode(control, "required") !== undefined ||
              /\.(field|native)\(/.test(opening(control).getText());
            if (!marked) {
              report(
                "required-unmarked",
                control,
                `<${tagOf(control)} value={${name}}> blocks submit (line ${
                  sf.getLineAndCharacterOfPosition(n.getStart()).line + 1
                }) without \`required\``,
              );
            }
          }
        }
      }
    }
    ts.forEachChild(n, visit);
  };
  visit(sf);

  for (const [name, node] of declared) {
    if (!spread.has(name)) {
      report(
        "required-unmarked",
        node,
        `required field "${name}" is never spread onto its control (.field / .native)`,
      );
    }
  }
  // One report per line and rule: a field refused by several checks is one finding.
  const seen = new Set<string>();
  return violations
    .filter((v) => {
      const key = `${v.rule}:${v.line}`;
      if (seen.has(key)) return false;
      seen.add(key);
      return true;
    })
    .sort((a, b) => a.line - b.line);
}
