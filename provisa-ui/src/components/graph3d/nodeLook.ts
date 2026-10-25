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

import type { GraphNode, NodeType, TermStatus } from "./graphModel";

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

/** A colour, as the --p3d-* token (profile3d.css) the scene's theme reads it from. */
export type Colour =
  | "accent"
  | "fam-temporal"
  | "fam-numeric"
  | "fam-foreign"
  | "fam-other"
  | "warm"
  | "q-tail"
  | "q-mid"
  | "fam-boolean"
  | "cool"
  | "hl";

/** A node kind's variant: a term's role, a column in a data product or not. */
export type Variant = "plain" | "concept" | "class" | "property" | "in_product" | "out_of_product";

/** A term's colour by its role, as its role badge colours it (TermRoleBadge); the shape is the
 * term's own, so a grey concept is never taken for a grey column. */
const ROLE_COLOUR: Record<"concept" | "class" | "property", Colour> = {
  concept: "fam-other",
  class: "fam-boolean",
  property: "cool",
};

export interface Look {
  shape: Shape;
  colour: Colour;
  variant: Variant;
}

const BY_TYPE: Record<Exclude<NodeType, "term" | "column">, { shape: Shape; colour: Colour }> = {
  product: { shape: "icosahedron", colour: "accent" },
  domain: { shape: "sphere", colour: "fam-temporal" },
  // A table is a cylinder in every graph, a product's member or not.
  table: { shape: "cylinder", colour: "fam-numeric" },
  relation: { shape: "cylinder", colour: "fam-foreign" },
  command: { shape: "cone", colour: "warm" },
  checker: { shape: "tetrahedron", colour: "q-tail" },
  profile: { shape: "torus", colour: "q-mid" },
  tag: { shape: "dodecahedron", colour: "fam-boolean" },
};

export function nodeLook(n: GraphNode): Look {
  if (n.type === "term") {
    // A graph that does not read the role draws every term alike.
    return n.role
      ? { shape: "diamond", colour: ROLE_COLOUR[n.role], variant: n.role }
      : { shape: "diamond", colour: "cool", variant: "plain" };
  }
  if (n.type === "column") {
    return n.inProduct
      ? { shape: "cube", colour: "fam-numeric", variant: "in_product" }
      : { shape: "cube", colour: "fam-other", variant: "out_of_product" };
  }
  return { ...BY_TYPE[n.type], variant: "plain" };
}

/** The label class a term's status puts on its name; a live term (or any other node) has none. */
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
