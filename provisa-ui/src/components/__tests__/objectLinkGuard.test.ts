// Copyright (c) 2026 Kenneth Stott
// Canary: 1bf90459-c328-4e2c-9244-f6d3fff4aa9d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2277 rule 6: wherever the UI names an object, the name is a link to that object's deep link
// (EntityLink, which shows it as plain text to a viewer who may not open it). This guard reads every
// component and fails on a name of another object drawn as plain text where a name is shown: a list
// cell, a detail panel's field, a chip, a line of text holding nothing else.

import { readFileSync, readdirSync, statSync } from "node:fs";
import { join, relative } from "node:path";
import { describe, expect, it } from "vitest";

const SRC = join(__dirname, "..", "..");

/** The fields that hold another object's name (its id where the object is named by it). */
const OBJECT_FIELD =
  /^(source|sourceId|source_id|sourceName|source_name|domain|domainId|domain_id|role|roleId|role_id|table|tableName|table_name|term|metric|product|productId|product_id|tag|command|relationship|document|classification|environment|env_name)$/;

/** Where a name is shown: the value alone in the element, or a detail field's value. */
const SHOWN: readonly RegExp[] = [
  /<Table\.Td\b[^>]*>\s*\{\s*([\w.?!]+)\s*\}\s*<\/Table\.Td>/g,
  // The value, or a mark where it has none.
  /<Table\.Td\b[^>]*>\s*\{\s*([\w.?!]+)\s*(?:\|\||\?\?)[^{}]*\}\s*<\/Table\.Td>/g,
  /<Badge\b[^>]*>\s*\{\s*([\w.?!]+)\s*\}\s*<\/Badge>/g,
  /<Text\b[^>]*>\s*\{\s*([\w.?!]+)\s*\}\s*<\/Text>/g,
  // A detail field ({ label, value }): a select's options put the value first, and are not names.
  /\blabel:\s*[^,{}\n]+,\s*value:\s*([\w.?!]+)\s*[,}]/g,
];

/** Fields that name the object the panel itself shows, not another one: there is nothing to follow. */
const OWN_NAME: ReadonlySet<string> = new Set([
  // A table's identity names the table its own panel shows.
  "pages/tables/TableSectionsRead.tsx: table.tableName",
]);

/**
 * Names not yet drawn as links, by file and expression: the page groups the sweep has still to
 * reach (REQ-2277 rule 6). The list only shrinks; a name added anywhere else fails.
 */
const NOT_YET_SWEPT: ReadonlySet<string> = new Set([
  // Admin pages (admin-pages-standard is rewriting them).
  "components/admin/OrgsTab.tsx: inv.role_id",
  "components/admin/CacheManager.tsx: tbl.domainId",
  "components/admin/CacheManager.tsx: h.tableName",
  "components/admin/DebugTracePanel.tsx: r.role_id",
  "components/admin/LocalUsersTab.tsx: a.role_id",
  "components/admin/LocalUsersTab.tsx: a.domain_id",
  "components/admin/SyntheticDatasetsPanel.tsx: r.table",
  "components/admin/ScheduledTasks.tsx: task.role",
  "components/entitlements/EntitlementGrants.tsx: g.role_id",
  "components/PersonalAccessTokens.tsx: tok.env_name",
  // Context pages (context-pages-standard is rewriting them).
  "components/admin/TermReadView.tsx: ref.source_id",
  "components/admin/TermReadView.tsx: ref.domain_id",
  "pages/library/LibraryDetail.tsx: c.term",
  "pages/library/LibraryPage.tsx: d.source",
  // A metric names its term by name; the term's panel is opened by its id.
  "pages/metrics/MetricDetailPanel.tsx: m.term",
  "pages/LineagePage.tsx: c.classification",
  // Data products (product-consumer-view is rewriting them).
  "pages/data-products/offers/OfferCard.tsx: offer.role_id",
  "pages/data-products/ProductHeader.tsx: product.domainId",
  "pages/data-products/tabs/ProfileTab.tsx: p.table",
  "pages/DataProductsPage.tsx: p.domainId",
  "pages/data-products/DataProductDetailPanel.tsx: product.domainId",
  "pages/data-products/tabs/RequestsTab.tsx: bid.terms.role_id",
  // Work surfaces.
  "components/canvas/CanvasTableCard.tsx: ct.tableName",
  "components/graph/GraphStatsModal.tsx: s.source",
  "components/GovernedTableViewer.tsx: table.sourceId",
]);

function sources(dir: string): string[] {
  return readdirSync(dir).flatMap((name) => {
    const path = join(dir, name);
    if (statSync(path).isDirectory()) return name === "__tests__" ? [] : sources(path);
    return /\.tsx$/.test(name) && !/\.test\.tsx$/.test(name) ? [path] : [];
  });
}

/** The text of a file with its comments blanked: a comment may show a name's shape. */
function code(text: string): string {
  return text
    .replace(/\/\*[\s\S]*?\*\//g, (c) => c.replace(/[^\n]/g, " "))
    .replace(/(^|[^:])\/\/.*$/gm, "$1");
}

/** Each object name `text` shows as plain text, as `<expression>`. */
function plainNames(text: string): string[] {
  const body = code(text);
  return SHOWN.flatMap((pattern) =>
    [...body.matchAll(pattern)]
      .map((m) => m[1])
      .filter((expr) =>
        OBJECT_FIELD.test(
          expr
            .split(/[.?!]+/)
            .filter(Boolean)
            .pop() ?? "",
        ),
      ),
  );
}

describe("every object name is a link (REQ-2277 rule 6)", () => {
  it("finds a name drawn as plain text", () => {
    expect(plainNames("<Table.Td>{r.sourceId}</Table.Td>")).toEqual(["r.sourceId"]);
    expect(plainNames('<Badge size="sm">{g.role_id}</Badge>')).toEqual(["g.role_id"]);
    expect(plainNames('<Table.Td>{wh.domainId || t("dash")}</Table.Td>')).toEqual(["wh.domainId"]);
    expect(plainNames("{ label: t('x.domain'), value: table.domainId },")).toEqual([
      "table.domainId",
    ]);
    // Linked, the name is not plain text; a select's option is not a name shown.
    expect(
      plainNames('<Table.Td><EntityLink kind="source" id={r.sourceId}>{r.sourceId}</EntityLink>'),
    ).toEqual([]);
    expect(plainNames("({ value: tb.tableName, label: tb.tableName })")).toEqual([]);
    // A row's own fields that name nothing else are not names of another object.
    expect(plainNames("<Table.Td>{r.description}</Table.Td>")).toEqual([]);
  });

  it("finds none outside the groups still to sweep", () => {
    const found = sources(SRC).flatMap((path) =>
      plainNames(readFileSync(path, "utf8")).map((expr) => `${relative(SRC, path)}: ${expr}`),
    );
    expect(found.filter((f) => !NOT_YET_SWEPT.has(f) && !OWN_NAME.has(f))).toEqual([]);
  });

  it("lists no swept name as still to sweep", () => {
    const found = new Set(
      sources(SRC).flatMap((path) =>
        plainNames(readFileSync(path, "utf8")).map((expr) => `${relative(SRC, path)}: ${expr}`),
      ),
    );
    expect([...NOT_YET_SWEPT, ...OWN_NAME].filter((entry) => !found.has(entry))).toEqual([]);
  });
});
