// Copyright (c) 2026 Kenneth Stott
// Canary: 8131e848-d525-4ff5-ad2c-8518e5f0dd9d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A graph node's page (lib/entityHref): each kind the graphs draw is read out of its id here, so
// every kind has one -- the map below is over every NodeType, and a kind added without a page
// does not compile.

import { entityHref } from "../../lib/entityHref";
import type { GraphNode, NodeType } from "./graphModel";

/** ``prefix:rest`` -> rest. */
const rest = (id: string) => id.slice(id.indexOf(":") + 1);

/** A column's key, ``<table id>:<column>``: its table and its name. */
function columnOf(key: string): [number, string] {
  const at = key.indexOf(":");
  return [Number(key.slice(0, at)), key.slice(at + 1)];
}

const HREF: Record<NodeType, (n: GraphNode) => string> = {
  term: (n) => entityHref("term", rest(n.id)),
  product: (n) => entityHref("product", rest(n.id)),
  command: (n) => entityHref("command", rest(n.id)),
  table: (n) => entityHref("table", rest(n.id)),
  column: (n) => {
    const [table, name] = columnOf(rest(n.id));
    return entityHref("column", name, table);
  },
  domain: (n) => entityHref("domain", rest(n.id)),
  tag: (n) => entityHref("tag", rest(n.id)),
  // A checker is the table it scans; a profile is its table's (its parent, "t:<id>").
  checker: (n) => entityHref("checker", rest(n.id)),
  profile: (n) => entityHref("profile", rest(n.id), Number(rest(n.parent ?? ""))),
  // Outside a product: a table ("x:<id>") or a relation lineage reaches ("l:<name>").
  relation: (n) =>
    n.id.startsWith("x:") ? entityHref("table", rest(n.id)) : entityHref("lineage", rest(n.id)),
};

export const nodeHref = (n: GraphNode): string => HREF[n.type](n);
