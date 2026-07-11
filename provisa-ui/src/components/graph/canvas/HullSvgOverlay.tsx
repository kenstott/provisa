// Copyright (c) 2026 Kenneth Stott
// Canary: 7b2d9e4f-1a8c-4f3d-b5e0-c6d8a3f2e971
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { Ref, MutableRefObject } from "react";
import { clusterColor } from "../graph-model";
import { cidToId } from "../graph-clusters";
import type { ClusterLevel } from "../graph-clusters";
import type { CyInstance } from "../cytoscape-types";

interface HullSvgOverlayProps {
  hullCircles: Array<{ cid: string; x: number; y: number; rx: number; ry: number }>;
  hullSvgRef?: Ref<SVGSVGElement>;
  clusterLevelRef: MutableRefObject<ClusterLevel>;
  cyRef: MutableRefObject<CyInstance | null>;
  hullDragRef: MutableRefObject<{
    cid: string;
    lastX: number;
    lastY: number;
    startX: number;
    startY: number;
  } | null>;
  toggleCollapse: (cid: string) => void;
}

export function HullSvgOverlay({
  hullCircles,
  hullSvgRef,
  clusterLevelRef,
  cyRef,
  hullDragRef,
  toggleCollapse,
}: HullSvgOverlayProps) {
  if (hullCircles.length === 0) return null;
  return (
    <svg
      ref={hullSvgRef}
      style={{
        position: "absolute",
        inset: 0,
        width: "100%",
        height: "100%",
        pointerEvents: "none",
      }}
    >
      {hullCircles.map(({ cid, x, y, rx, ry }) => (
        <g key={cid}>
          <ellipse
            cx={x}
            cy={y}
            rx={rx}
            ry={ry}
            fill={clusterColor(cid)}
            fillOpacity={0.1}
            stroke={clusterColor(cid)}
            strokeWidth={8}
            strokeOpacity={0}
            style={{ pointerEvents: "stroke", cursor: "grab" }}
            onMouseDown={(e) => {
              e.preventDefault();
              const cy = cyRef.current;
              if (cy) {
                const clusterId = `__cluster_${clusterLevelRef.current}_${cidToId(cid)}`;
                cy.getElementById(clusterId).children().forEach((n) => n.unlock());
              }
              hullDragRef.current = {
                cid,
                lastX: e.clientX,
                lastY: e.clientY,
                startX: e.clientX,
                startY: e.clientY,
              };
            }}
          />
          <ellipse
            cx={x}
            cy={y}
            rx={rx}
            ry={ry}
            fill="none"
            stroke={clusterColor(cid)}
            strokeWidth={1.5}
            strokeOpacity={0.75}
            style={{ pointerEvents: "none" }}
          />
          <text
            x={x}
            y={y - ry - 6}
            textAnchor="middle"
            fill={clusterColor(cid)}
            fontSize={11}
            fontWeight="bold"
            fontFamily="sans-serif"
            style={{ pointerEvents: "all", cursor: "pointer", userSelect: "none" }}
            onClick={() => toggleCollapse(cid)}
          >
            <title>Click to collapse group</title>
            {cid} ⊟
          </text>
        </g>
      ))}
    </svg>
  );
}
