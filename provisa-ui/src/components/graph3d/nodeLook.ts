// Copyright (c) 2026 Kenneth Stott
// Canary: cdf3907f-35ba-4e2e-a66b-070af2495a34
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// How a graph node looks: its shape says its kind and its colour the variant, and every node is
// solid and opaque at one size. A term's status shows on its label alone (labelClass). The scene
// draws from this and the legend keys from it, so the two cannot drift apart.

import type { TermGrounding } from "../../api/glossary";
import type { GraphNode, NodeType, TermStatus } from "./graphModel";
import { NODE_TYPES, type NodeType as RoleType } from "../provisa-graph/tokens";
import { typeOf } from "./laneGraph";

export type Shape =
  | "icosahedron"
  | "diamond"
  | "cube"
  | "box"
  | "sphere"
  | "cone"
  | "tetrahedron"
  | "cylinder"
  | "dodecahedron"
  | "torus";

/** A colour: the shared graph's node type whose colour it is (provisa-graph NODE_TYPES), so a
 * role is one colour in the 3D graph, the 2D graphs and their legends (REQ-2239: each node role
 * has its own colour across kinds). */
export type Colour = RoleType;

/** A look's colour, resolved. */
export const lookColour = (c: Colour): string => NODE_TYPES[c].colour;

/** A node kind's variant: a term's role, a column in a data product or not. */
export type Variant = "plain" | "concept" | "class" | "property" | "in_product" | "out_of_product";

export interface Look {
  shape: Shape;
  colour: Colour;
  variant: Variant;
}

const BY_TYPE: Record<Exclude<NodeType, "term" | "column">, { shape: Shape; colour: Colour }> = {
  product: { shape: "icosahedron", colour: "product" },
  domain: { shape: "sphere", colour: "domain" },
  // A table is a cylinder in every graph, a product's member or not.
  table: { shape: "cylinder", colour: "table" },
  relation: { shape: "cylinder", colour: "relation" },
  command: { shape: "cone", colour: "command" },
  checker: { shape: "tetrahedron", colour: "checker" },
  profile: { shape: "torus", colour: "profile" },
  tag: { shape: "dodecahedron", colour: "tag" },
};

export function nodeLook(n: GraphNode): Look {
  if (n.type === "term") {
    // A term is coloured by its role, as the 2D graphs colour it; a graph that does not read the
    // role draws every term alike, in the term's colour.
    return n.role
      ? { shape: "diamond", colour: n.role, variant: n.role }
      : { shape: "diamond", colour: "term", variant: "plain" };
  }
  if (n.type === "column") {
    // A column a data product holds takes the product's colour; any other, the column's.
    return n.inProduct
      ? { shape: "cube", colour: "product", variant: "in_product" }
      : { shape: "cube", colour: "column", variant: "out_of_product" };
  }
  return { ...BY_TYPE[n.type], variant: "plain" };
}

/** The label class a term's grounding puts on its name: the ground symbol before it (graph3d.css);
 * a term that reaches no data, and any other node, has none. */
export function groundClass(grounding: TermGrounding | undefined): string {
  switch (grounding) {
    case "grounded":
      return "g3d-label-grounded";
    case "grounded_pending":
      return "g3d-label-grounded-pending";
    default:
      return "";
  }
}

/** The label class a term's status puts on its name; an approved term (or any other node) has none. */
export function labelClass(status: TermStatus | undefined): string {
  switch (status) {
    case "draft":
      return "g3d-label-draft";
    case "deprecated":
      return "g3d-label-deprecated";
    case "retired":
      return "g3d-label-retired";
    default:
      return "";
  }
}

/** The i18n key naming what a node is, as its look shows it: a table drawn as stored is a table,
 * not a product's member. */
export function kindKey(n: GraphNode): string {
  return n.type === "table" && n.stored ? "graph3d.look_stored" : `graph3d.type_${n.type}`;
}

/** One entry of a graph's shown list: what is drawn (an i18n key), in the colour its canvas draws
 * it, and how many. */
export interface ShownKind {
  key: string;
  colour: string;
  count: number;
}

/** Each kind drawn, as the canvas of the view draws it (REQ-2239: a sidebar legend uses the canvas
 * colour of its view): in 2D by the shared graph's type (a term by its role), in 3D by its look.
 * A table's folded columns are one node drawn, not one column. Most first. */
export function shownKinds(nodes: GraphNode[], flat: boolean): ShownKind[] {
  const by = new Map<string, ShownKind>();
  for (const n of nodes) {
    if (n.folded) continue;
    let key: string;
    let colour: string;
    if (flat) {
      key = n.type === "term" && n.role ? `graph3d.look_${n.role}` : kindKey(n);
      colour = NODE_TYPES[typeOf(n)].colour;
    } else {
      const look = nodeLook(n);
      key = look.variant === "plain" ? kindKey(n) : `graph3d.look_${look.variant}`;
      colour = lookColour(look.colour);
    }
    const seen = by.get(key);
    if (seen) seen.count += 1;
    else by.set(key, { key, colour, count: 1 });
  }
  return [...by.values()].sort((a, b) => b.count - a.count);
}
