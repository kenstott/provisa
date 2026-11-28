// Copyright (c) 2026 Kenneth Stott
// Canary: 7d3b1f60-2a94-4c8e-b5d1-9e0f6a4c2b17
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2239: the graph's visual tokens (SPEC.md), taken from the detail cards and read from
// the palette Mantine's theme is built from, so the graph and the cards cannot drift apart. Cytoscape draws on a canvas
// and cannot read CSS variables: the values are resolved here, per colour scheme.

import { BRAND, SCALES, type ScaleName } from "../../theme/palette";
import { ICONS, type IconName } from "./icons";

const gray = SCALES.gray;
const dark = SCALES.dark;
const brand = BRAND;
const hue = (name: ScaleName, shade = 6) => SCALES[name][shade];

export interface GraphTokens {
  canvas: string;
  nodeFill: string;
  nodeBorder: string;
  shadow: string;
  shadowOpacity: number;
  text: string;
  textMuted: string;
  edge: string;
  accent: string;
  /** The junction diamond's fill: a tint of its grape. */
  junctionFill: string;
}

const LIGHT: GraphTokens = {
  canvas: gray[0],
  nodeFill: "#ffffff",
  nodeBorder: gray[4],
  shadow: "#000000",
  shadowOpacity: 0.06,
  text: gray[9],
  textMuted: gray[6],
  edge: gray[5],
  accent: brand[6],
  junctionFill: hue("grape", 0),
};

const DARK: GraphTokens = {
  canvas: dark[7],
  nodeFill: dark[6],
  nodeBorder: dark[4],
  shadow: "#000000",
  shadowOpacity: 0.12,
  text: dark[0],
  textMuted: dark[2],
  edge: dark[3],
  accent: brand[4],
  junctionFill: dark[6],
};

export function graphTokens(isDark: boolean): GraphTokens {
  return isDark ? DARK : LIGHT;
}

/** Sizes, in model pixels (SPEC.md). The node style is compact (REQ-2239, amended 2026-11-28): a
 * node spends its space on its text, not on decoration. */
export const SIZE = {
  /** The least width of a box; it grows with its text to NODE_MAX_WIDTH (model.ts). */
  nodeWidth: 64,
  /** A box with its name only. */
  nodeHeight: 24,
  /** The type's colour: a thin edge on the box's leading side. */
  band: 3,
  radius: 4,
  /** The text's inset from the band and the right edge, and from the top and bottom. */
  padX: 6,
  padY: 5,
  /** The name: bold, one row. */
  name: 11,
  nameStep: 14,
  /** The body's rows: monospace, small and tight. */
  lineFont: 9.5,
  lineStep: 11,
  /** Between the name and the body. */
  bodyGap: 2,
  edge: 1,
  edgeLabel: 9,
  groupLabel: 10,
  /** A diamond (the junction, the one exception to the node shape): wide enough for its name. */
  diamondWidth: 140,
  diamondHeight: 56,
  /** The room a diamond's name keeps from its side corners, each side. */
  diamondInset: 18,
} as const;

/** What a node is, as the graph draws it: the band's colour, the icon, the type's name (an i18n
 * key under ``provisaGraph.type``). Every view draws from this one list. */
export interface NodeTypeLook {
  colour: string;
  icon: IconName;
  /** The one exception to the single node shape (REQ-2239, A JUNCTION IS A DIAMOND): a junction
   * table is drawn as Chen's relationship diamond, in its band's colour, keeping its legs. */
  shape?: "diamond";
}

export const NODE_TYPES = {
  product: { colour: brand[6], icon: "package" },
  source: { colour: hue("teal"), icon: "database" },
  domain: { colour: hue("violet"), icon: "folder" },
  // A group is a domain's box under another name: the same concept, the same look.
  group: { colour: hue("violet"), icon: "folder" },
  table: { colour: hue("blue"), icon: "table" },
  view: { colour: hue("cyan"), icon: "eye" },
  // A table outside the model (an input a product reads that no member is): "outside", the same
  // concept as lineage's external field, in one colour; the icon says table-level.
  relation: { colour: hue("cyan", 8), icon: "merge" },
  junction: { colour: hue("grape"), icon: "link", shape: "diamond" },
  column: { colour: hue("gray"), icon: "column" },
  term: { colour: hue("orange"), icon: "book" },
  // A concept is a term by its role: the term's own colour (REQ-2239, each role its own colour).
  concept: { colour: hue("orange"), icon: "book" },
  class: { colour: hue("yellow", 7), icon: "book" },
  property: { colour: hue("lime", 7), icon: "book" },
  command: { colour: hue("pink"), icon: "terminal" },
  // Lineage's fields, by their role in the flow (REQ-1665, REQ-2116, REQ-2229). Each role its own
  // colour across every kind of graph (REQ-2239, amended 2026-11-27): none borrows an ontology
  // role's colour or icon.
  intermediate: { colour: hue("indigo"), icon: "step" },
  output: { colour: hue("green", 8), icon: "column" },
  native: { colour: hue("violet", 4), icon: "database" },
  job: { colour: hue("orange", 8), icon: "terminal" },
  external: { colour: hue("cyan", 8), icon: "external" },
  checker: { colour: hue("red"), icon: "shield" },
  profile: { colour: hue("green"), icon: "chart" },
  tag: { colour: hue("grape", 8), icon: "tag" },
  // A security role (REQ-2161): a role's inheritance graph draws each role as one box, its
  // capabilities its body.
  role: { colour: hue("teal", 9), icon: "user" },
  // A fold node stands for a table's columns not drawn: the column's concept.
  more: { colour: hue("gray"), icon: "more" },
} as const satisfies Record<string, NodeTypeLook>;

export type NodeType = keyof typeof NODE_TYPES;

/** The types that are one concept under two names, and so share a colour (REQ-2239: each node
 * role has its own colour across kinds, save where two names are one role). Every other type's
 * colour is its own. */
export const SAME_CONCEPT: readonly (readonly NodeType[])[] = [
  ["domain", "group"],
  ["term", "concept"],
  ["column", "more"],
  ["relation", "external"],
];

/** The shape a type is drawn in: the rounded rectangle, or the junction's diamond. */
export function typeShape(type: NodeType): "box" | "diamond" {
  const look: NodeTypeLook = NODE_TYPES[type];
  return look.shape ?? "box";
}

/** A type's icon, drawn in its colour, as an image cytoscape paints. */
export function typeIcon(type: NodeType): string {
  const look: NodeTypeLook = NODE_TYPES[type];
  return ICONS[look.icon](look.colour);
}

/** The hues a group is drawn in: one per group id, the same everywhere. */
const GROUP_HUES = [
  "blue",
  "teal",
  "violet",
  "orange",
  "pink",
  "cyan",
  "lime",
  "grape",
  "indigo",
  "yellow",
] as const;

export function groupHue(id: string): string {
  let h = 0;
  for (const c of id) h = (h * 31 + c.charCodeAt(0)) >>> 0;
  return hue(GROUP_HUES[h % GROUP_HUES.length]);
}
