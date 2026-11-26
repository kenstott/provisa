// Copyright (c) 2026 Kenneth Stott
// Canary: e5674817-53a5-4859-8b4c-3cfd3324fdf0
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2277: every object management page shares one anatomy. This guard reads every component that
// draws a page's title (PageTitle) and fails on a title outside the page header (div.page-header), a
// list filter (FilterInput) outside it, and a SegmentedControl switching the page's views -- sub-views
// are the standard tab strip (Mantine Tabs). A SegmentedControl is a form's choice only, inside a
// FormField. The work surfaces lay themselves out for their purpose, so the guard skips the files of
// REQ-2277's list (WORK_SURFACES) and nothing else: a management page that cannot comply is fixed.

import { describe, it, expect } from "vitest";
import { readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative, resolve } from "node:path";
import ts from "typescript";
import { WORK_SURFACE_FILES } from "../../lib/workSurfaces";
import { NAV_GROUPS } from "../navGroups";

const SRC = resolve(__dirname, "../..");

type Rule = "title-outside-header" | "filter-outside-header" | "segmented-tab-strip";

interface Violation {
  rule: Rule;
  line: number;
}

function tagName(n: ts.Node): string | null {
  if (ts.isJsxSelfClosingElement(n)) return n.tagName.getText();
  if (ts.isJsxElement(n)) return n.openingElement.tagName.getText();
  return null;
}

function opening(n: ts.Node): ts.JsxOpeningLikeElement | null {
  if (ts.isJsxSelfClosingElement(n)) return n;
  if (ts.isJsxElement(n)) return n.openingElement;
  return null;
}

/** The nearest element `n` is drawn inside, through conditions and expressions. */
function enclosingElement(n: ts.Node): ts.JsxElement | null {
  for (let p = n.parent; p; p = p.parent) {
    if (ts.isJsxElement(p)) return p;
    if (ts.isJsxFragment(p)) continue;
    if (ts.isArrowFunction(p) || ts.isFunctionDeclaration(p) || ts.isFunctionExpression(p))
      return null;
  }
  return null;
}

/** Whether an element's className names the page header. */
function isPageHeader(el: ts.JsxElement | null): boolean {
  if (el === null) return false;
  const cls = el.openingElement.attributes.properties.find(
    (a): a is ts.JsxAttribute => ts.isJsxAttribute(a) && a.name.getText() === "className",
  );
  const init = cls?.initializer;
  return init !== undefined && ts.isStringLiteral(init) && /\bpage-header\b/.test(init.text);
}

/** Whether `n` is drawn inside an element named `name`. */
function inside(n: ts.Node, name: string): boolean {
  for (let p = n.parent; p; p = p.parent) {
    if (ts.isJsxElement(p) && p.openingElement.tagName.getText() === name) return true;
  }
  return false;
}

/** The rules broken in one component's source. A file that draws no PageTitle is no page. */
export function checkPageHeader(source: string): Violation[] {
  const sf = ts.createSourceFile("x.tsx", source, ts.ScriptTarget.Latest, true, ts.ScriptKind.TSX);
  const out: Violation[] = [];
  const elements: ts.Node[] = [];
  const visit = (n: ts.Node): void => {
    if (opening(n)) elements.push(n);
    ts.forEachChild(n, visit);
  };
  visit(sf);
  if (!elements.some((n) => tagName(n) === "PageTitle")) return out;
  const line = (n: ts.Node) => sf.getLineAndCharacterOfPosition(n.getStart()).line + 1;
  for (const n of elements) {
    const name = tagName(n);
    if (name === "PageTitle" && !isPageHeader(enclosingElement(n)))
      out.push({ rule: "title-outside-header", line: line(n) });
    if (name === "FilterInput" && !isPageHeader(enclosingElement(n)))
      out.push({ rule: "filter-outside-header", line: line(n) });
    if (name === "SegmentedControl" && !inside(n, "FormField"))
      out.push({ rule: "segmented-tab-strip", line: line(n) });
  }
  return out;
}

function componentFiles(dir: string): string[] {
  return readdirSync(dir).flatMap((name) => {
    const path = join(dir, name);
    if (statSync(path).isDirectory()) return name === "__tests__" ? [] : componentFiles(path);
    return name.endsWith(".tsx") && !name.endsWith(".test.tsx") ? [path] : [];
  });
}

const rules = (source: string) => checkPageHeader(source).map((v) => v.rule);

describe("the page header rule fails what it is meant to fail", () => {
  it("passes the standard header: title, filter, actions", () => {
    expect(
      rules(
        `const x = <div className="page"><div className="page-header"><PageTitle icon={I}>T</PageTitle>` +
          `<FilterInput value={q} onChange={f} /><div className="page-actions"><Button>Add</Button></div>` +
          `</div><Tabs value={v}><Tabs.List /></Tabs></div>;`,
      ),
    ).toEqual([]);
  });

  it("fails a title or a filter outside the header", () => {
    expect(
      rules(
        `const x = <Group justify="space-between"><PageTitle icon={I}>T</PageTitle>` +
          `<FilterInput value={q} onChange={f} /></Group>;`,
      ),
    ).toEqual(["title-outside-header", "filter-outside-header"]);
    expect(
      rules(
        `const x = <><div className="page-header"><PageTitle icon={I}>T</PageTitle></div>` +
          `<Stack><FilterInput value={q} onChange={f} /></Stack></>;`,
      ),
    ).toEqual(["filter-outside-header"]);
  });

  it("reads through conditions, but not into a component drawn elsewhere", () => {
    expect(
      rules(
        `const x = <div className="page-header">{ok && <PageTitle icon={I}>T</PageTitle>}` +
          `{q !== null ? <FilterInput value={q} onChange={f} /> : null}</div>;`,
      ),
    ).toEqual([]);
    expect(
      rules(
        `const x = <div className="page-header"><PageTitle icon={I}>T</PageTitle></div>;` +
          `const y = <List render={() => <FilterInput value={q} onChange={f} />} />;`,
      ),
    ).toEqual(["filter-outside-header"]);
  });

  it("fails a SegmentedControl switching a page's views, not one in a form", () => {
    expect(
      rules(
        `const x = <div><div className="page-header"><PageTitle icon={I}>T</PageTitle></div>` +
          `<SegmentedControl data={d} value={v} /></div>;`,
      ),
    ).toEqual(["segmented-tab-strip"]);
    expect(
      rules(
        `const x = <div><div className="page-header"><PageTitle icon={I}>T</PageTitle></div>` +
          `<FormLayout><FormField><SegmentedControl data={d} value={v} /></FormField></FormLayout></div>;`,
      ),
    ).toEqual([]);
  });

  it("holds no file that draws no page title", () => {
    expect(rules(`const x = <Stack><FilterInput value={q} onChange={f} /></Stack>;`)).toEqual([]);
  });
});

describe("every object management page (REQ-2277)", () => {
  const found = componentFiles(SRC)
    .map((path) => relative(SRC, path))
    .filter((file) => !WORK_SURFACE_FILES.has(file))
    .map((file) => ({
      file,
      hits: checkPageHeader(readFileSync(join(SRC, file), "utf8")),
    }))
    .filter((f) => f.hits.length > 0)
    .map(({ file, hits }) => `${file}: ${hits.map((v) => `line ${v.line} ${v.rule}`).join("; ")}`);

  it("draws its title and its list filter in the page header, and its views as tabs", () => {
    expect(found).toEqual([]);
  });

  it("is read: the guard finds the pages it holds", () => {
    const pages = componentFiles(SRC).filter((p) => readFileSync(p, "utf8").includes("<PageTitle"));
    expect(pages.length).toBeGreaterThan(15);
  });
});

// REQ-2277: an admin page that manages objects (a list of orgs, secrets, registries, ...) holds the
// object management anatomy in full: the title, the list filter and the create action in the page
// header, the standard list beside a right-hand detail panel (SplitPane), and that panel's deep
// link (useRememberedDetail). Every admin nav entry is one of these or one object's settings
// (ADMIN_SETTINGS_PAGES), so a new admin page is classified before it ships.

type ObjectRule = "no-title" | "no-filter" | "no-create" | "no-panel" | "no-deep-link";

interface ObjectPage {
  /** The component, under src/, that draws the page. */
  file: string;
  /** Whether the page creates its objects, so its header carries the create action. */
  creates: boolean;
}

const ADMIN_OBJECT_PAGES: Readonly<Record<string, ObjectPage>> = {
  "/admin/orgs": { file: "components/admin/OrgsTab.tsx", creates: true },
  "/team": { file: "pages/TeamPage.tsx", creates: true },
  "/admin/domains": { file: "components/admin/DomainsTab.tsx", creates: true },
  "/admin/environments": { file: "components/admin/EnvironmentsTab.tsx", creates: true },
  "/admin/scheduled-tasks": { file: "components/admin/ScheduledTasks.tsx", creates: true },
  "/admin/secrets": { file: "components/admin/SecretsTab.tsx", creates: true },
  "/admin/my-secrets": { file: "components/admin/SecretsTab.tsx", creates: true },
  "/admin/platform": { file: "components/admin/PlatformRegionsTab.tsx", creates: true },
  "/admin/regions": { file: "components/admin/OrgRegionsTab.tsx", creates: false },
  "/admin/ai-models": { file: "components/admin/AiModelsTab.tsx", creates: true },
  "/admin/package-registries": { file: "components/admin/PackageRegistriesTab.tsx", creates: true },
  "/admin/container-registries": {
    file: "components/admin/ContainerRegistriesTab.tsx",
    creates: true,
  },
  "/admin/tags": { file: "components/admin/TagsTab.tsx", creates: true },
  "/admin/classification-kinds": {
    file: "components/admin/ClassificationKindsTab.tsx",
    creates: false,
  },
  "/admin/reports": { file: "components/admin/ReportsTab.tsx", creates: true },
  "/admin/requests": { file: "pages/RequestsPage.tsx", creates: false },
};

/** Admin pages that show one object's settings, a dashboard or a one-off tool: no list to manage. */
const ADMIN_SETTINGS_PAGES: ReadonlySet<string> = new Set([
  "/admin/overview",
  "/admin/cache",
  "/admin/federation-engine",
  "/admin/org-engine",
  "/admin/security",
  "/admin/email",
  "/admin/maintenance",
  "/admin/deployment-settings",
  "/admin/spark-cluster",
  "/admin/import",
  "/admin/observability",
  "/admin/platform-observability",
]);

/** Whether `n` is drawn inside an element whose className names `cls`. */
function insideClass(n: ts.Node, cls: string): boolean {
  const re = new RegExp(`\\b${cls}\\b`);
  for (let p = n.parent; p; p = p.parent) {
    if (!ts.isJsxElement(p)) continue;
    const attr = p.openingElement.attributes.properties.find(
      (a): a is ts.JsxAttribute => ts.isJsxAttribute(a) && a.name.getText() === "className",
    );
    const init = attr?.initializer;
    if (init !== undefined && ts.isStringLiteral(init) && re.test(init.text)) return true;
  }
  return false;
}

/** What an admin object page's source lacks of the object management anatomy. */
export function checkObjectPage(source: string, creates: boolean): ObjectRule[] {
  const sf = ts.createSourceFile("x.tsx", source, ts.ScriptTarget.Latest, true, ts.ScriptKind.TSX);
  const elements: ts.Node[] = [];
  const visit = (n: ts.Node): void => {
    if (opening(n)) elements.push(n);
    ts.forEachChild(n, visit);
  };
  visit(sf);
  const inHeader = (name: string) =>
    elements.some((n) => tagName(n) === name && insideClass(n, "page-header"));
  const out: ObjectRule[] = [];
  if (!inHeader("PageTitle")) out.push("no-title");
  if (!inHeader("FilterInput")) out.push("no-filter");
  if (
    creates &&
    !elements.some(
      (n) =>
        tagName(n) === "Button" && insideClass(n, "page-actions") && insideClass(n, "page-header"),
    )
  )
    out.push("no-create");
  if (!elements.some((n) => tagName(n) === "SplitPane")) out.push("no-panel");
  if (!source.includes("useRememberedDetail(")) out.push("no-deep-link");
  return out;
}

describe("the admin object page rule fails what it is meant to fail", () => {
  const whole =
    `const x = <><div className="page-header"><PageTitle icon={I}>T</PageTitle>` +
    `<FilterInput value={q} onChange={f} /><div className="page-actions"><Button>Add</Button></div>` +
    `</div><SplitPane list={l}>{p}</SplitPane></>; const d = useRememberedDetail("x", ids);`;

  it("passes the whole anatomy", () => {
    expect(checkObjectPage(whole, true)).toEqual([]);
  });

  it("fails each missing part", () => {
    expect(checkObjectPage(`const x = <Stack><Table /></Stack>;`, true)).toEqual([
      "no-title",
      "no-filter",
      "no-create",
      "no-panel",
      "no-deep-link",
    ]);
    expect(checkObjectPage(whole.replace("<Button>Add</Button>", ""), true)).toEqual(["no-create"]);
    expect(checkObjectPage(whole.replace("<Button>Add</Button>", ""), false)).toEqual([]);
  });

  it("fails a create action outside the header", () => {
    expect(
      checkObjectPage(
        whole.replace(`<div className="page-actions"><Button>Add</Button></div>`, "") +
          `const y = <Group className="page-actions"><Button>Add</Button></Group>;`,
        true,
      ),
    ).toEqual(["no-create"]);
  });
});

describe("every admin page (REQ-2277)", () => {
  const adminRoutes = (NAV_GROUPS.find((g) => g.id === "admin")?.items ?? []).map((i) => i.to);

  it("is classified: an object page or one object's settings", () => {
    const classified = new Set([...Object.keys(ADMIN_OBJECT_PAGES), ...ADMIN_SETTINGS_PAGES]);
    expect(adminRoutes.filter((r) => !classified.has(r))).toEqual([]);
    expect([...classified].filter((r) => !adminRoutes.includes(r))).toEqual([]);
  });

  it.each(Object.entries(ADMIN_OBJECT_PAGES))("%s holds the object management anatomy", (_, p) => {
    expect(checkObjectPage(readFileSync(join(SRC, p.file), "utf8"), p.creates)).toEqual([]);
  });

  it("the Admin page draws no second header over a page that draws its own", () => {
    const admin = readFileSync(join(SRC, "pages/AdminPage.tsx"), "utf8");
    const routes = admin.match(/const ROUTE_TO_SECTION[^}]*\}/s)?.[0] ?? "";
    const own = admin.match(/const OWN_HEADER[^;]*;/s)?.[0] ?? "";
    const ownSections = [...own.matchAll(/"([^"]+)"/g)].map((m) => m[1]);
    const objectSections = Object.keys(ADMIN_OBJECT_PAGES)
      .map((r) => routes.match(new RegExp(`"${r}":\\s*"([^"]+)"`))?.[1])
      .filter((s): s is string => s !== undefined);
    expect(objectSections.length).toBeGreaterThan(5);
    expect(new Set(ownSections)).toEqual(new Set(objectSections));
  });
});
