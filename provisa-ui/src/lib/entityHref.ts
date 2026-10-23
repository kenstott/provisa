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

/** Each thing a graph draws or joins, by the page it lives on. */
export type EntityKind =
  | "term"
  | "product"
  | "command"
  | "table"
  | "column"
  | "domain"
  | "relationship"
  | "tag"
  | "checker"
  | "profile"
  | "lineage";

const q = (v: string | number) => encodeURIComponent(String(v));

/** The link to ``kind`` ``id``. A column is named by its table and its name (``table`` and
 * ``id``); a profile and a checker by the table they belong to. */
export function entityHref(kind: EntityKind, id: string | number, table?: number): string {
  switch (kind) {
    case "term":
      return `/admin/ontology?term=${q(id)}`;
    case "product":
      return `/data-products?product=${q(id)}`;
    case "command":
      return `/commands?command=${q(id)}`;
    case "table":
      return `/tables?table=${q(id)}`;
    case "column":
      return `/tables?table=${q(table ?? "")}&section=columns&column=${q(id)}`;
    case "checker":
    case "profile":
      // Its table's quality: the checks it runs, the profiles it holds.
      return `/tables?table=${q(table ?? id)}&section=quality`;
    case "domain":
      return `/admin/domains?domain=${q(id)}`;
    case "relationship":
      return `/relationships?relationship=${q(id)}`;
    case "tag":
      return `/admin/tags?tag=${q(id)}`;
    case "lineage":
      return `/lineage?focus=${q(id)}`;
  }
}
