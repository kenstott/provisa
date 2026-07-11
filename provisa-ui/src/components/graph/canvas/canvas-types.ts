// Copyright (c) 2026 Kenneth Stott
// Canary: d1e8f4a2-5b3c-4d7e-9a0f-16b2c8e9f7d3
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { Relationship } from "../../../types/admin";
import type { GNode, GEdge, GraphStats, RelLineOverride } from "../graph-model";
import type { ClusterLevel } from "../graph-clusters";
import type { CyLayoutOptions, CyInstance } from "../cytoscape-types";

export interface CanvasProps {
  nodes: Map<string, GNode>;
  edges: Map<string, GEdge>;
  overlayNodes: Map<string, GNode>;
  overlayEdges: Map<string, GEdge>;
  onSelect: (item: { kind: "node"; data: GNode; graphStats?: GraphStats } | { kind: "edge"; data: GEdge } | null) => void;
  colorOverrides: Record<string, string>;
  sizeOverrides: Record<string, number>;
  labelProperty: Record<string, string>;
  sizeByProperty: Record<string, string>;
  sizeMultiplier: Record<string, number>;
  relLineOverrides: Record<string, RelLineOverride>;
  onExcludeNode: (nodeKeys: string[]) => void;
  pkMap: Record<string, string[]>;
  labelToTableLabel: Record<string, string>;
  relationships: Relationship[];
  labelSiblings?: Record<string, string[]>;
  showingChildrenNatural: Set<string>;
  onToggleChildren: (nodeKey: string) => void;
  onToggleChildrenBatch: (nodeKeys: string[], circular?: boolean) => void;
  showingChildrenCircular: Set<string>;
  onToggleChildrenCircular: (nodeKey: string) => void;
  showingParents: Set<string>;
  onToggleParents: (nodeKey: string) => void;
  onToggleParentsBatch: (nodeKeys: string[], circular?: boolean) => void;
  showingParentsCircular: Set<string>;
  onToggleParentsCircular: (nodeKey: string) => void;
  onCyReady?: (cy: CyInstance | null) => void;
  clusterLevel: ClusterLevel;
  hullSvgRef?: React.Ref<SVGSVGElement>;
  isExpanded?: boolean;
}

export type LayoutMode = "force" | "hierarchy";

export type NodeRingMenuState = {
  x: number;
  y: number;
  nodeKey: string;
  node: GNode;
  graphStats?: GraphStats;
  isLocked: boolean;
};

export const LAYOUT_OPTIONS: Record<LayoutMode, CyLayoutOptions> = {
  force: {
    name: "fcose",
    animate: false,
    packComponents: true,
    nodeRepulsion: () => 10000,
    idealEdgeLength: () => 80,
    gravity: 0.25,
    numIter: 2500,
    nodeSeparation: 80,
    tilingPaddingVertical: 20,
    tilingPaddingHorizontal: 20,
  } as CyLayoutOptions,
  hierarchy: {
    name: "breadthfirst",
    animate: false,
    directed: true,
    padding: 20,
    spacingFactor: 1.4,
  } as CyLayoutOptions,
};
