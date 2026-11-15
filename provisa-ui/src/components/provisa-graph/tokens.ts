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
// Mantine's own scales, so the graph and the cards cannot drift apart. Cytoscape draws on a canvas
// and cannot read CSS variables: the values are resolved here, per colour scheme.

import { DEFAULT_THEME } from "@mantine/core";
import { theme } from "../../theme/theme";
import { ICONS, type IconName } from "./icons";

const gray = DEFAULT_THEME.colors.gray;
const dark = DEFAULT_THEME.colors.dark;
const brand = theme.colors!.brand!;
const hue = (name: keyof typeof DEFAULT_THEME.colors, shade = 6) =>
  DEFAULT_THEME.colors[name][shade];

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
  /** The ERD junction's hue: the VIA badge's grape (REQ-1588). */
  via: string;
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
  via: hue("grape"),
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
  via: hue("grape", 4),
  junctionFill: dark[6],
};

export function graphTokens(isDark: boolean): GraphTokens {
  return isDark ? DARK : LIGHT;
}

/** Sizes, in model pixels (SPEC.md). */
export const SIZE = {
  nodeWidth: 180,
  nodeHeight: 44,
  band: 6,
  radius: 4,
  icon: 16,
  name: 11,
  sub: 9,
  line: 13,
  edge: 1,
  edgeLabel: 9,
  groupLabel: 10,
  /** A diamond (the junction, the one exception to the node shape): wide enough for its name. */
  diamondWidth: 150,
  diamondHeight: 90,
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
  group: { colour: hue("violet"), icon: "folder" },
  table: { colour: hue("blue"), icon: "table" },
  view: { colour: hue("cyan"), icon: "eye" },
  relation: { colour: hue("cyan"), icon: "merge" },
  junction: { colour: hue("grape"), icon: "link", shape: "diamond" },
  column: { colour: hue("gray"), icon: "column" },
  term: { colour: hue("orange"), icon: "book" },
  concept: { colour: hue("orange"), icon: "book" },
  class: { colour: hue("yellow", 7), icon: "book" },
  property: { colour: hue("lime", 7), icon: "book" },
  command: { colour: hue("pink"), icon: "terminal" },
  checker: { colour: hue("red"), icon: "shield" },
  profile: { colour: hue("green"), icon: "chart" },
  tag: { colour: hue("violet"), icon: "tag" },
  more: { colour: hue("gray"), icon: "more" },
} as const satisfies Record<string, NodeTypeLook>;

export type NodeType = keyof typeof NODE_TYPES;

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
