// Copyright (c) 2026 Kenneth Stott
// Canary: 5f8a1c73-d4e2-4b9f-a0c1-e3f7d9b2c8a4
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { MutableRefObject } from "react";
import { ZoomInIcon, ZoomOutIcon, FitScreenIcon } from "../GraphIcons";
import type { LayoutMode } from "./canvas-types";
import type { CyInstance } from "../cytoscape-types";

interface CanvasControlsProps {
  cyRef: MutableRefObject<CyInstance | null>;
  layoutMode: LayoutMode;
  toggleLayout: () => void;
  nudgeHeldRef: MutableRefObject<boolean>;
  nudgeLayout: (freeNodes?: Set<string>, aggressive?: boolean) => void;
  nudgeLayoutRef: MutableRefObject<(freeNodes?: Set<string>, aggressive?: boolean) => void>;
  edgeDistance: number;
  setEdgeDistance: (v: number) => void;
  edgeDistanceRef: MutableRefObject<number>;
  nudgeTimerRef: MutableRefObject<ReturnType<typeof setTimeout> | null>;
}

export function CanvasControls({
  cyRef,
  layoutMode,
  toggleLayout,
  nudgeHeldRef,
  nudgeLayout,
  nudgeLayoutRef,
  edgeDistance,
  setEdgeDistance,
  edgeDistanceRef,
  nudgeTimerRef,
}: CanvasControlsProps) {
  const fitView = () => cyRef.current?.fit(undefined, 40);
  return (
    <div className="gf-canvas-controls">
      <button
        className="gf-ctrl-btn"
        onClick={() => cyRef.current?.zoom(cyRef.current.zoom() * 1.3)}
        title="Zoom in"
      >
        <ZoomInIcon size={15} />
      </button>
      <button
        className="gf-ctrl-btn"
        onClick={() => cyRef.current?.zoom(cyRef.current.zoom() * 0.77)}
        title="Zoom out"
      >
        <ZoomOutIcon size={15} />
      </button>
      <button className="gf-ctrl-btn" onClick={fitView} title="Fit to screen">
        <FitScreenIcon size={15} />
      </button>
      <div className="gf-ctrl-divider" />
      <button
        className={`gf-ctrl-btn${layoutMode === "hierarchy" ? " active" : ""}`}
        onClick={toggleLayout}
        title={
          layoutMode === "force" ? "Switch to hierarchical layout" : "Switch to force layout"
        }
      >
        {layoutMode === "force" ? "⋮" : "⊟"}
      </button>
      <button
        className="gf-ctrl-btn"
        onMouseDown={() => {
          nudgeHeldRef.current = true;
          const cy = cyRef.current;
          const sel = cy ? cy.nodes(":selected").not("[?_cluster]") : null;
          const freeNodes = sel && sel.length > 0 ? new Set(sel.map((n) => n.id())) : undefined;
          nudgeLayout(freeNodes, true);
        }}
        onMouseUp={() => {
          nudgeHeldRef.current = false;
        }}
        onMouseLeave={() => {
          nudgeHeldRef.current = false;
        }}
        title="Nudge layout — nudges selected nodes (or all if none selected); hold to keep iterating"
      >
        ⟳
      </button>
      <div className="gf-ctrl-divider" />
      <label className="gf-ctrl-label" title="Edge length">
        ↔
      </label>
      <input
        type="range"
        className="gf-ctrl-slider"
        min={40}
        max={400}
        step={10}
        value={edgeDistance}
        onChange={(e) => {
          const v = Number(e.target.value);
          edgeDistanceRef.current = v;
          setEdgeDistance(v);
          localStorage.setItem("provisa.graph.edgeDistance", String(v));
          if (nudgeTimerRef.current) clearTimeout(nudgeTimerRef.current);
          nudgeTimerRef.current = setTimeout(() => nudgeLayoutRef.current(), 150);
        }}
        title={`Edge length: ${edgeDistance}px`}
      />
    </div>
  );
}
