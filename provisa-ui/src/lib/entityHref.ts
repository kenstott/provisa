// Copyright (c) 2026 Kenneth Stott
// Canary: 03f3917a-6f6c-48a8-a041-095eb80d9f5c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The page each thing a graph draws is edited on, as a link: one function for every graph, so a
// thing is reached the same way whichever graph it is picked in. A link is a plain URL -- followed
// as any link is, and Back returns to the graph as it was.

import { detailHref } from "../components/list/useDetailParam";
import { NAV_BAR, NAV_GROUPS } from "../components/navGroups";
import { meetsRequirement, type CapabilityRequirement } from "./capabilities";

/** Each thing a graph draws or joins, by the page it lives on. */
export type EntityKind =
  | "term"
  | "product"
  | "command"
  | "table"
  | "column"
  | "domain"
  | "source"
  | "relationship"
  | "tag"
  | "checker"
  | "profile"
  | "lineage"
  // REQ-2259: the context layer's metrics and classification kinds.
  | "metric"
  | "classification";

const q = (v: string | number) => encodeURIComponent(String(v));

/** The link to ``kind`` ``id``: its panel's link, from detailHref (REQ-2240). A column is named by
 * its table and its name (``table`` and ``id``); a profile and a checker by the table they belong
 * to. A command's ``id`` is its ``commands`` id (commandId). */
export function entityHref(kind: EntityKind, id: string | number, table?: number): string {
  switch (kind) {
    case "term":
      return detailHref("ontology", id);
    case "product":
      return detailHref("products", id);
    case "command":
      return detailHref("commands", id);
    case "table":
      return detailHref("tables", id);
    case "column":
      return detailHref("tables", table ?? "", "columns", { column: String(id) });
    case "checker":
    case "profile":
      // Its table's quality: the checks it runs, the profiles it holds.
      return detailHref("tables", table ?? id, "quality");
    case "domain":
      return detailHref("domains", id);
    case "source":
      return detailHref("sources", id);
    case "relationship":
      return detailHref("relationships", id);
    case "tag":
      return detailHref("tags", id);
    case "lineage":
      // The lineage graph centred on it: a view of the graph, not a panel.
      return `/lineage?focus=${q(id)}`;
    case "metric":
      return detailHref("metrics", id);
    case "classification":
      // The classification kinds' page; it has no panel per kind, so the link names the page.
      return "/admin/classification-kinds";
  }
}

/** REQ-2259: the right that opens the page ``href`` leads to -- the gate of its navigation entry,
 * which is the gate of its route (adminNavCapabilities.test). Every page an entity link names has
 * one, so a path with none is a defect, not a page every member opens. */
export function pageRequirement(href: string): CapabilityRequirement {
  const path = href.split("?")[0];
  const link = NAV_BAR.find((e) => e.kind === "link" && e.to === path);
  if (link?.kind === "link") return link;
  for (const group of NAV_GROUPS) {
    const item = group.items.find((i) => i.to === path && !i.comingSoon);
    if (item) return item;
  }
  throw new Error(`no navigation entry gates ${path}`);
}

/** REQ-2259: the link to ``kind`` ``id`` when ``capabilities`` open the page it leads to, else
 * `undefined` -- the caller then shows the name as plain text. */
export function entityLink(
  capabilities: string[],
  kind: EntityKind,
  id: string | number,
  table?: number,
): string | undefined {
  const href = entityHref(kind, id, table);
  return meetsRequirement(capabilities, pageRequirement(href)) ? href : undefined;
}

/** A link to the ERD view of a page (REQ-2187): the focus, and the panel to open. */
export function erdViewHref(
  page: "tables" | "relationships",
  link: {
    domains?: string[];
    objects?: string[];
    table?: string | number;
    relationship?: string | number;
    /** REQ-2189: the saved layout shown; none for Automatic. */
    layout?: string | null;
  },
): string {
  const list = (names: string[]) => names.map(q).join(",");
  const query = ["view=erd"];
  if (link.domains?.length) query.push(`domains=${list(link.domains)}`);
  if (link.objects?.length) query.push(`objects=${list(link.objects)}`);
  if (link.table !== undefined) query.push(`table=${q(link.table)}`);
  if (link.relationship !== undefined) query.push(`relationship=${q(link.relationship)}`);
  if (link.layout) query.push(`layout=${q(link.layout)}`);
  return `/${page}?${query.join("&")}`;
}
