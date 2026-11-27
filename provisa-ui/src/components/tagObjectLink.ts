// Copyright (c) 2026 Kenneth Stott
// Canary: 9b1e6d34-0c7a-4f52-8a3d-2e5f7c1b8d46
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A tagged object named by its canonical key ("table:7", "section:3:heading:Scope"): the panel it
// opens, built by detailHref, and what to call it.

import { detailHref } from "./list/useDetailParam";

type Panel = Parameters<typeof detailHref>[0];

const PANELS: Record<string, Panel> = {
  source: "sources",
  table: "tables",
  column: "tables",
  relationship: "relationships",
  command: "commands",
  command_column: "commands", // REQ-1159: a command's output column opens its command
  product: "products",
  term: "ontology",
  rls_rule: "rls",
  role: "roles",
  metric: "metrics",
  document: "library",
  section: "library",
};

/** The object's key without its kind: a column's is its table's id then its name. */
export function objectName(objectKey: string): string {
  return objectKey.slice(objectKey.indexOf(":") + 1);
}

/** The link to the panel of the object a tag assignment is on, by the assignment's object type
 *  and key. A column opens its table, a section its document, a command's output column its
 *  command. */
export function objectHref(objectType: string, objectKey: string): string {
  const panel = PANELS[objectType];
  const name = objectName(objectKey);
  const part = ["column", "section", "command_column"].includes(objectType);
  return detailHref(panel, part ? name.split(":")[0] : name);
}
