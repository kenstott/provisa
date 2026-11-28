// Copyright (c) 2026 Kenneth Stott
// Canary: fb4b8e57-1b97-4cac-a50d-eee51629574a
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2239: the layered preset's ELK layout: layered, upward, children laid out with their
// parents, through Provisa's own adapter (elkLayout), which says a failed layout. The MCP server's
// render_graph draws with it as "elk" (provisa-ui/graph-render imports it).

import type { CyLayoutOptions } from "../graph/cytoscape-types";
import { PROVISA_ELK } from "./elkLayout";
import { groupSizeOptions } from "./model";

export const ERD_ELK_LAYOUT = {
  name: PROVISA_ELK,
  animate: false,
  elk: {
    algorithm: "layered",
    "elk.direction": "UP",
    "elk.hierarchyHandling": "INCLUDE_CHILDREN",
    "elk.layered.spacing.nodeNodeBetweenLayers": "90",
    "elk.spacing.nodeNode": "32",
    "elk.layered.nodePlacement.strategy": "NETWORK_SIMPLEX",
    "elk.layered.crossingMinimization.strategy": "LAYER_SWEEP",
    "elk.padding": "[top=30,left=16,bottom=16,right=16]",
  },
} as CyLayoutOptions;

/** ProvisaGraph's layered layout (ERD_ELK_LAYOUT): each of its groups keeps at least its title's
 * width (groupMinWidth, on the group's data as ``minW``; REQ-2239, amended 2026-11-28). Every
 * parent ProvisaGraph draws is a group. */
export const GROUPED_ELK_LAYOUT = {
  ...ERD_ELK_LAYOUT,
  nodeLayoutOptions: (n: { isParent(): boolean; data(k: string): unknown }) =>
    n.isParent() ? groupSizeOptions(n.data("minW") as number) : {},
} as CyLayoutOptions;
