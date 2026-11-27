// Copyright (c) 2026 Kenneth Stott
// Canary: 9d4b6e2a-3f1c-4a8e-b7d5-6c2e1f0a9b38
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2277 rule 7: a detail panel body has ONE vertical rhythm, defined once (PanelBody,
// PanelStrip, PanelSection and the `--panel-gap-*` tokens; a wizard's step is WizardStep). The
// space between the panel head, a strip, a section's title, its description, the first field and
// each field group is the standard's. A panel body that sets its own (a Stack with its gap as one
// of its parts, a margin on a part) brings back the cramped or uneven stacking this replaced, so
// it fails here.
//
// A panel body is what a file renders inside PanelBody, PanelSection or WizardStep. Its parts are
// the elements placed there directly (through fragments, conditions and maps); what a part holds
// inside itself (a field's value, an alert's lines) is the part's own.

import { readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import ts from "typescript";
import { describe, expect, it } from "vitest";
import { rendersPanel } from "./panelParts";

const SRC = join(__dirname, "..", "..", "..");

/** The standard's containers: their direct parts are held to the rhythm. */
const STANDARD = new Set(["PanelBody", "PanelSection", "WizardStep"]);

/**
 * Files that render a detail panel and do not lay its body out with the standard yet. This list
 * may only shrink: a file converted to PanelBody fails here until it is taken off.
 */
const NOT_YET_CONVERTED: readonly string[] = [
  "components/admin/ClassificationKindsTab.tsx",
  "components/admin/ContainerRegistriesTab.tsx",
  "components/admin/DomainsTab.tsx",
  "components/admin/EnvironmentActions.tsx",
  "components/admin/EnvironmentsTab.tsx",
  "components/admin/GrantEditor.tsx",
  "components/admin/OrgsTab.tsx",
  "components/admin/PackageRegistriesTab.tsx",
  "components/admin/PlatformRegionsTab.tsx",
  "components/admin/RegionEngineTabs.tsx",
  "components/admin/ReportsTab.tsx",
  "components/admin/ScheduledTasks.tsx",
  "components/admin/SecretsTab.tsx",
  "components/admin/TermEditForm.tsx",
  "components/admin/TermHeadActions.tsx",
  "components/admin/ontologyImport/ImportReviewPanel.tsx",
  "components/admin/syncing/EntryPanel.tsx",
  "components/erd/ErdFocusActions.tsx",
  "components/relationships/AddRelationshipForm.tsx",
  "components/relationships/RelationshipDetail.tsx",
  "components/relationships/RelationshipModals.tsx",
  "pages/CommandsPage.tsx",
  "pages/MetricsPage.tsx",
  "pages/RequestsPage.tsx",
  "pages/SecurityPage.tsx",
  "pages/TeamPage.tsx",
  "pages/data-products/DataProductDetailPanel.tsx",
  "pages/library/LibraryAddForm.tsx",
  "pages/library/LibraryDetail.tsx",
  "pages/metrics/MetricDetailPanel.tsx",
  "pages/security/DerivedRoles.tsx",
  "pages/security/SecurityRlsPage.tsx",
];

function sources(dir: string): string[] {
  return readdirSync(dir).flatMap((name) => {
    const path = join(dir, name);
    if (statSync(path).isDirectory()) return name === "__tests__" ? [] : sources(path);
    return /\.tsx$/.test(name) && !/\.test\.tsx$/.test(name) ? [path] : [];
  });
}

type Jsx = ts.JsxElement | ts.JsxSelfClosingElement;

function tagName(node: Jsx): string {
  const tag = ts.isJsxElement(node) ? node.openingElement.tagName : node.tagName;
  return tag.getText();
}

function attributes(node: Jsx): ts.JsxAttributes {
  return ts.isJsxElement(node) ? node.openingElement.attributes : node.attributes;
}

/** The elements an expression places where it stands. */
function placed(expr: ts.Expression | undefined): Jsx[] {
  if (!expr) return [];
  if (ts.isParenthesizedExpression(expr)) return placed(expr.expression);
  if (ts.isJsxElement(expr) || ts.isJsxSelfClosingElement(expr)) return partsOf([expr]);
  if (ts.isJsxFragment(expr)) return partsOf(expr.children);
  if (ts.isBinaryExpression(expr)) return [...placed(expr.left), ...placed(expr.right)];
  if (ts.isConditionalExpression(expr))
    return [...placed(expr.whenTrue), ...placed(expr.whenFalse)];
  if (ts.isCallExpression(expr)) {
    // `list.map((x) => <Part />)`: each item is a part.
    return expr.arguments.flatMap((arg) => {
      if (!ts.isArrowFunction(arg) && !ts.isFunctionExpression(arg)) return [];
      if (!ts.isBlock(arg.body)) return placed(arg.body);
      const returned: Jsx[] = [];
      const visit = (n: ts.Node) => {
        if (ts.isReturnStatement(n)) returned.push(...placed(n.expression));
        else if (!ts.isFunctionLike(n)) ts.forEachChild(n, visit);
      };
      ts.forEachChild(arg.body, visit);
      return returned;
    });
  }
  return [];
}

/** The direct parts among a container's children: through fragments, conditions and maps. */
function partsOf(children: readonly ts.Node[]): Jsx[] {
  return children.flatMap((child) => {
    if (ts.isJsxElement(child) || ts.isJsxSelfClosingElement(child)) {
      const name = tagName(child);
      if (name === "Fragment" || name === "React.Fragment")
        return ts.isJsxElement(child) ? partsOf(child.children) : [];
      return [child];
    }
    if (ts.isJsxFragment(child)) return partsOf(child.children);
    if (ts.isJsxExpression(child)) return placed(child.expression);
    return [];
  });
}

const MARGIN_PROPS = new Set(["m", "mt", "mb", "my"]);
const MARGIN_STYLES = /^margin(Top|Bottom|Block|BlockStart|BlockEnd)?$/;

function isZero(init: ts.Expression): boolean {
  return (
    (ts.isNumericLiteral(init) && Number(init.text) === 0) ||
    (ts.isStringLiteralLike(init) && /^0(px|rem|em)?$/.test(init.text))
  );
}

/** What a part sets of its own that is the rhythm's to set. */
function ownSpacing(part: Jsx): string[] {
  const name = tagName(part);
  const found: string[] = [];
  if (name === "Stack") found.push("<Stack> as a part (its gap is a rhythm of its own)");
  for (const attr of attributes(part).properties) {
    if (!ts.isJsxAttribute(attr)) continue;
    const prop = attr.name.getText();
    if (MARGIN_PROPS.has(prop)) found.push(`<${name} ${prop}=…>`);
    if (prop !== "style") continue;
    const value = attr.initializer;
    if (!value || !ts.isJsxExpression(value) || !value.expression) continue;
    if (!ts.isObjectLiteralExpression(value.expression)) continue;
    for (const p of value.expression.properties) {
      if (!ts.isPropertyAssignment(p)) continue;
      const key = p.name.getText().replace(/["']/g, "");
      if (MARGIN_STYLES.test(key) && !isZero(p.initializer))
        found.push(`<${name} style={{ ${key} }}>`);
    }
  }
  return found;
}

/** Every container of the standard in a file's source, and what its parts set of their own. */
export function bodyViolations(text: string, file = "x.tsx"): { uses: boolean; found: string[] } {
  const sf = ts.createSourceFile(file, text, ts.ScriptTarget.Latest, true, ts.ScriptKind.TSX);
  let uses = false;
  const found: string[] = [];
  const visit = (node: ts.Node) => {
    if (ts.isJsxElement(node) || ts.isJsxSelfClosingElement(node)) {
      if (STANDARD.has(tagName(node))) {
        uses = true;
        if (ts.isJsxElement(node))
          for (const part of partsOf(node.children)) found.push(...ownSpacing(part));
      }
    }
    ts.forEachChild(node, visit);
  };
  visit(sf);
  return { uses, found };
}

describe("the rule itself", () => {
  it("finds a Stack and a margin set as parts of a body", () => {
    const { uses, found } = bodyViolations(`const x = (
      <PanelBody>
        <Stack gap="xs"><Text>a</Text></Stack>
        {open && <Alert mb="md">b</Alert>}
        {rows.map((r) => <Group key={r} style={{ marginBottom: 8 }} />)}
        <>
          <Text mt={4}>c</Text>
        </>
      </PanelBody>
    );`);
    expect(uses).toBe(true);
    expect(found).toEqual([
      "<Stack> as a part (its gap is a rhythm of its own)",
      "<Alert mb=…>",
      "<Group style={{ marginBottom }}>",
      "<Text mt=…>",
    ]);
  });

  it("leaves what a part holds inside itself, a horizontal gap and a margin of zero", () => {
    const { found } = bodyViolations(`const x = (
      <PanelSection title="t">
        <FormField size="full"><Stack gap={2}><Text mb={4}>a</Text></Stack></FormField>
        <Group gap="xs"><Badge /></Group>
        <Table style={{ margin: 0 }} />
      </PanelSection>
    );`);
    expect(found).toEqual([]);
  });

  it("reads a wizard step as a body", () => {
    expect(bodyViolations(`<WizardStep index={0}><Stack /></WizardStep>`).found).toHaveLength(1);
    expect(bodyViolations(`<Stack gap="md"><Text /></Stack>`).uses).toBe(false);
  });
});

describe("every detail panel body keeps the one rhythm", () => {
  const files = sources(SRC).filter((file) => rendersPanel(readFileSync(file, "utf8")));
  const read = new Map(files.map((f) => [relative(SRC, f), readFileSync(f, "utf8")]));

  it("looks at every file that renders a detail panel", () => {
    expect(files.length).toBeGreaterThan(8);
  });

  it("the not-yet list only shrinks: each entry renders a panel and is not converted", () => {
    for (const name of NOT_YET_CONVERTED) {
      const text = read.get(name);
      expect(
        text,
        `${name}: no longer renders a panel; take it off NOT_YET_CONVERTED`,
      ).toBeDefined();
      expect(
        bodyViolations(text ?? "", name).uses,
        `${name}: uses PanelBody now; take it off NOT_YET_CONVERTED`,
      ).toBe(false);
    }
  });

  for (const [name, text] of read) {
    if (NOT_YET_CONVERTED.includes(name)) continue;
    it(name, () => {
      const { uses, found } = bodyViolations(text, name);
      expect(uses, `${name}: lay the panel body out with PanelBody (REQ-2277 rule 7)`).toBe(true);
      expect(found, `${name}: a part of the panel body sets its own spacing`).toEqual([]);
    });
  }
});
