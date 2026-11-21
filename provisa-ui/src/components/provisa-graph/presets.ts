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

export const ERD_ELK_LAYOUT = {
  name: PROVISA_ELK,
  animate: false,
  elk: {
    algorithm: "layered",
    "elk.direction": "UP",
    "elk.hierarchyHandling": "INCLUDE_CHILDREN",
    "elk.layered.spacing.nodeNodeBetweenLayers": "150",
    "elk.spacing.nodeNode": "60",
    "elk.layered.nodePlacement.strategy": "NETWORK_SIMPLEX",
    "elk.layered.crossingMinimization.strategy": "LAYER_SWEEP",
    "elk.padding": "[top=40,left=20,bottom=20,right=20]",
  },
} as CyLayoutOptions;
