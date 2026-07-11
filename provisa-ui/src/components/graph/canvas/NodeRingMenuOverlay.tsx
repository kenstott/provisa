// Copyright (c) 2026 Kenneth Stott
// Canary: 2e6b0d84-c3f7-4a1e-9d5b-8f0a2c7e4b96
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useRef, useEffect, type ReactNode, type MutableRefObject } from "react";
import type { CyInstance } from "../cytoscape-types";
import type { NodeRingMenuState } from "./canvas-types";

interface NodeRingMenuOverlayProps {
  nodeRingMenu: NodeRingMenuState;
  setNodeRingMenu: (v: NodeRingMenuState | null) => void;
  hoveredSector: string | null;
  setHoveredSector: (v: string | null) => void;
  cyRef: MutableRefObject<CyInstance | null>;
  anchoredRef: MutableRefObject<Set<string>>;
  nudgeLayoutRef: MutableRefObject<(freeNodes?: Set<string>, aggressive?: boolean) => void>;
  showingChildrenNatural: Set<string>;
  onToggleChildren: (nodeKey: string) => void;
}

export function NodeRingMenuOverlay({
  nodeRingMenu,
  setNodeRingMenu,
  hoveredSector,
  setHoveredSector,
  cyRef,
  anchoredRef,
  nudgeLayoutRef,
  showingChildrenNatural,
  onToggleChildren,
}: NodeRingMenuOverlayProps) {
  const ringMenuRef = useRef<HTMLDivElement>(null);

  // Track node position as viewport changes (pan / zoom)
  useEffect(() => {
    if (!cyRef.current) return;
    const cy = cyRef.current;
    const update = () => {
      if (!ringMenuRef.current) return;
      const cyNode = cy.$id(nodeRingMenu.nodeKey);
      if (!cyNode || (cyNode as unknown as { length: number }).length === 0) return;
      const rp = (cyNode as unknown as { renderedPosition: () => { x: number; y: number } }).renderedPosition();
      const nodeW = (cyNode as unknown as { renderedWidth(): number }).renderedWidth();
      // R1=26 inner radius in 120-unit viewBox must clear the node radius + 4px gap
      const minFromNode = Math.ceil((nodeW / 2 + 4) * (120 / 26));
      const size = Math.max(80, Math.min(400, minFromNode));
      ringMenuRef.current.style.left = `${rp.x}px`;
      ringMenuRef.current.style.top = `${rp.y}px`;
      const svg = ringMenuRef.current.querySelector<SVGSVGElement>("svg");
      if (svg) { svg.setAttribute("width", String(size)); svg.setAttribute("height", String(size)); }
    };
    update();
    cy.on("viewport", update);
    return () => { cy.off("viewport", update); };
  }, [nodeRingMenu, cyRef]);

  const R1 = 26, R2 = 54;
  // Full 120° sector, no gap — separator lines drawn on top
  const arcSector = (centerDeg: number) => {
    const a1 = ((centerDeg - 60) * Math.PI) / 180;
    const a2 = ((centerDeg + 60) * Math.PI) / 180;
    const ox1 = R2 * Math.cos(a1), oy1 = R2 * Math.sin(a1);
    const ox2 = R2 * Math.cos(a2), oy2 = R2 * Math.sin(a2);
    const ix1 = R1 * Math.cos(a2), iy1 = R1 * Math.sin(a2);
    const ix2 = R1 * Math.cos(a1), iy2 = R1 * Math.sin(a1);
    return `M ${ox1.toFixed(2)} ${oy1.toFixed(2)} A ${R2} ${R2} 0 0 1 ${ox2.toFixed(2)} ${oy2.toFixed(2)} L ${ix1.toFixed(2)} ${iy1.toFixed(2)} A ${R1} ${R1} 0 0 0 ${ix2.toFixed(2)} ${iy2.toFixed(2)} Z`;
  };
  // Boundary angles between the 3 sectors (at ±60° from each center)
  const separatorLines = [90, 210, 330].map((deg) => {
    const rad = (deg * Math.PI) / 180;
    return { x1: R1 * Math.cos(rad), y1: R1 * Math.sin(rad), x2: R2 * Math.cos(rad), y2: R2 * Math.sin(rad) };
  });
  const midPos = (centerDeg: number) => {
    const r = (R1 + R2) / 2;
    const rad = (centerDeg * Math.PI) / 180;
    return { x: r * Math.cos(rad), y: r * Math.sin(rad) };
  };
  const sectors: { angle: number; key: string; title: string; active: boolean; iconPath: ReactNode }[] = [
    {
      angle: 270, key: "lock", active: nodeRingMenu.isLocked,
      title: nodeRingMenu.isLocked ? "Unlock position" : "Lock position",
      iconPath: (
        <>
          {/* padlock body */}
          <rect x="-3.8" y="0" width="7.6" height="5.5" rx="1.2" fill="currentColor"/>
          {/* shackle - open: left arm seated, right arm raised */}
          <path d="M-2.5 0 v-2.5 a2.5 2.5 0 0 1 5 0" fill="none" stroke="currentColor" strokeWidth="1.1" strokeLinecap="round"/>
          {/* keyhole */}
          <circle cx="0" cy="2.8" r="1.1" fill="#111318"/>
          <rect x="-0.55" y="3.4" width="1.1" height="1.5" rx="0.3" fill="#111318"/>
        </>
      ),
    },
    {
      angle: 30, key: "children", active: showingChildrenNatural.has(nodeRingMenu.nodeKey),
      title: showingChildrenNatural.has(nodeRingMenu.nodeKey) ? "Hide children" : "Show children",
      iconPath: (
        <>
          {/* center hub */}
          <circle cx="0" cy="0" r="1.5" fill="currentColor"/>
          {/* satellites at varying angles and distances */}
          <circle cx="0.66" cy="-3.74" r="1.2" fill="#111318" stroke="currentColor" strokeWidth="0.9"/>
          <circle cx="4.33" cy="-2.5" r="1.2" fill="#111318" stroke="currentColor" strokeWidth="0.9"/>
          <circle cx="3.72" cy="2.15" r="1.2" fill="#111318" stroke="currentColor" strokeWidth="0.9"/>
          <circle cx="-4.17" cy="1.95" r="1.2" fill="#111318" stroke="currentColor" strokeWidth="0.9"/>
          {/* spokes */}
          <line x1="0.26" y1="-1.48" x2="0.45" y2="-2.56" stroke="currentColor" strokeWidth="0.9"/>
          <line x1="1.3" y1="-0.75" x2="3.29" y2="-1.9" stroke="currentColor" strokeWidth="0.9"/>
          <line x1="1.3" y1="0.75" x2="2.68" y2="1.55" stroke="currentColor" strokeWidth="0.9"/>
          <line x1="-1.36" y1="0.63" x2="-3.08" y2="1.44" stroke="currentColor" strokeWidth="0.9"/>
        </>
      ),
    },
    {
      angle: 150, key: "exclude", active: false,
      title: "Remove node",
      iconPath: (
        <>
          {/* eye */}
          <path d="M-5 0 C-3.5-3 3.5-3 5 0 C3.5 3-3.5 3-5 0 Z" fill="none" stroke="currentColor" strokeWidth="0.9"/>
          <circle cx="0" cy="-0.3" r="1.4" fill="none" stroke="currentColor" strokeWidth="0.9"/>
          {/* circle+minus badge offset lower-right */}
          <circle cx="3.2" cy="3.2" r="2" fill="#111318" stroke="currentColor" strokeWidth="0.9"/>
          <line x1="2.5" y1="3.2" x2="3.9" y2="3.2" stroke="currentColor" strokeWidth="0.9" strokeLinecap="round"/>
        </>
      ),
    },
  ];

  return (
    <>
      <div
        style={{ position: "absolute", inset: 0, zIndex: 899 }}
        onClick={() => setNodeRingMenu(null)}
      />
      <div
        ref={ringMenuRef}
        className="gf-node-ring-menu"
        style={{ left: nodeRingMenu.x, top: nodeRingMenu.y }}
      >
        <svg
          viewBox="-60 -60 120 120"
          width="120"
          height="120"
          style={{ overflow: "visible", display: "block", pointerEvents: "all" }}
        >
          {sectors.map(({ angle, key, title, active, iconPath }) => {
            const mp = midPos(angle);
            return (
              <g
                key={key}
                style={{ cursor: "pointer" }}
                onMouseEnter={() => setHoveredSector(key)}
                onMouseLeave={() => setHoveredSector(null)}
                onClick={(e) => {
                  e.stopPropagation();
                  if (key === "lock") {
                    const cy = cyRef.current;
                    if (!cy) return;
                    const cyNode = cy.$id(nodeRingMenu.nodeKey);
                    if (nodeRingMenu.isLocked) {
                      cyNode.unlock();
                      anchoredRef.current.delete(nodeRingMenu.nodeKey);
                      cyNode.removeClass("pinned");
                      nudgeLayoutRef.current();
                    } else {
                      cyNode.lock();
                      anchoredRef.current.add(nodeRingMenu.nodeKey);
                      cyNode.addClass("pinned");
                    }
                  } else if (key === "children") {
                    onToggleChildren(nodeRingMenu.nodeKey);
                  } else {
                    const cy = cyRef.current;
                    if (cy) {
                      anchoredRef.current.delete(nodeRingMenu.nodeKey);
                      cy.remove(cy.$id(nodeRingMenu.nodeKey));
                    }
                  }
                  setNodeRingMenu(null);
                }}
              >
                <title>{title}</title>
                <path
                  d={arcSector(angle)}
                  fill={active ? "rgba(99,102,241,0.45)" : hoveredSector === key ? "rgba(99,102,241,0.2)" : "rgba(17,19,24,0.92)"}
                  stroke={hoveredSector === key ? "#6366f1" : "#3a3d52"}
                  strokeWidth="1"
                />
                <path d={arcSector(angle)} fill="transparent" stroke="transparent" strokeWidth="10"/>
                <g
                  transform={`translate(${mp.x.toFixed(2)},${mp.y.toFixed(2)})`}
                  color={active || hoveredSector === key ? "#a5b4fc" : "#9ca3af"}
                >
                  {iconPath}
                </g>
              </g>
            );
          })}
          {separatorLines.map(({ x1, y1, x2, y2 }, i) => (
            <line key={i} x1={x1.toFixed(2)} y1={y1.toFixed(2)} x2={x2.toFixed(2)} y2={y2.toFixed(2)} stroke="#3a3d52" strokeWidth="1.5" style={{ pointerEvents: "none" }} />
          ))}
        </svg>
      </div>
    </>
  );
}
