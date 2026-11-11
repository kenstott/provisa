// Copyright (c) 2026 Kenneth Stott
// Canary: a6c3e8f1-2d54-4b97-8e0a-7f1b9d4c2e58
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2239: a group's resize handles, on its corners while it is hovered. Dragging one scales the
// group's members to the box drawn; they then stand where they were put.

import { useCallback, useEffect, useRef, useState, type MutableRefObject } from "react";
import { useTranslation } from "react-i18next";
import type { CyInstance } from "../graph/cytoscape-types";
import type { Pt } from "./places";

type Corner = "se" | "sw" | "ne" | "nw";
interface Box {
  x: number;
  y: number;
  w: number;
  h: number;
}

const CORNERS: Corner[] = ["nw", "ne", "sw", "se"];
const LEAST = { w: 80, h: 60 };

export function GroupResize({
  cyRef,
  groupId,
  groupIds,
  accent,
  drawnAt,
  onLeave,
  onResized,
}: {
  cyRef: MutableRefObject<CyInstance | null>;
  /** The group hovered. */
  groupId: string | null;
  groupIds: string[];
  accent: string;
  /** Bumped by each draw: the box is measured again. */
  drawnAt: number;
  onLeave: () => void;
  /** The members' new places, by node id. */
  onResized: (moved: Map<string, Pt>) => void;
}) {
  const { t } = useTranslation();
  const [box, setBox] = useState<Box | null>(null);
  const handleHover = useRef(false);
  const drag = useRef<{ corner: Corner; x: number; y: number; start: Box } | null>(null);

  const measure = useCallback(
    (id: string | null) => {
      const cy = cyRef.current;
      if (!id || !cy || !groupIds.includes(id)) return setBox(null);
      const node = cy.getElementById(id);
      if (node.empty()) return setBox(null);
      // Rendered coordinates are the canvas's, and the box is placed inside it.
      const bb = (
        node as unknown as {
          renderedBoundingBox(o: object): { x1: number; y1: number; w: number; h: number };
        }
      ).renderedBoundingBox({ includeLabels: false });
      setBox({ x: bb.x1, y: bb.y1, w: bb.w, h: bb.h });
    },
    [cyRef, groupIds],
  );

  useEffect(() => {
    if (!drag.current) measure(groupId);
  }, [groupId, drawnAt, measure]);

  // The box follows the camera.
  useEffect(() => {
    const cy = cyRef.current as unknown as {
      on(e: string, fn: () => void): void;
      off(e: string, fn: () => void): void;
    } | null;
    if (!cy) return;
    const follow = () => measure(groupId);
    cy.on("pan zoom", follow);
    return () => cy.off("pan zoom", follow);
  }, [cyRef, groupId, drawnAt, measure]);

  if (!box || !groupId) return null;

  const down = (corner: Corner, e: React.PointerEvent<HTMLDivElement>) => {
    e.stopPropagation();
    e.currentTarget.setPointerCapture(e.pointerId);
    drag.current = { corner, x: e.clientX, y: e.clientY, start: { ...box } };
  };
  const move = (e: React.PointerEvent<HTMLDivElement>) => {
    const d = drag.current;
    if (!d) return;
    const dx = e.clientX - d.x;
    const dy = e.clientY - d.y;
    const b = d.start;
    const west = d.corner === "sw" || d.corner === "nw";
    const north = d.corner === "ne" || d.corner === "nw";
    setBox({
      x: west ? b.x + dx : b.x,
      y: north ? b.y + dy : b.y,
      w: Math.max(LEAST.w, west ? b.w - dx : b.w + dx),
      h: Math.max(LEAST.h, north ? b.h - dy : b.h + dy),
    });
  };
  const up = (e: React.PointerEvent<HTMLDivElement>) => {
    const d = drag.current;
    drag.current = null;
    const cy = cyRef.current;
    if (!d || !cy) return;
    e.stopPropagation();
    // The box drawn, in model space.
    const zoom = cy.zoom();
    const pan = (cy as unknown as { pan(): Pt }).pan();
    const x1 = (box.x - pan.x) / zoom;
    const y1 = (box.y - pan.y) / zoom;
    const w = box.w / zoom;
    const h = box.h / zoom;
    const group = cy.getElementById(groupId);
    const members = cy.nodes(`[parent = "${groupId}"]`);
    const moved = new Map<string, Pt>();
    if (!members.empty()) {
      const old = (
        group as unknown as {
          boundingBox(o: object): { x1: number; y1: number; w: number; h: number };
        }
      ).boundingBox({ includeLabels: false });
      cy.batch(() => {
        // A saved layout's least size gives way to the size the reader draws.
        (group as unknown as { removeStyle(s: string): void }).removeStyle(
          "width height min-width min-height",
        );
        // Each member keeps its place relative to the box.
        members.forEach((m) => {
          const p = m.position();
          const at = {
            x: x1 + (old.w > 0 ? (p.x - old.x1) / old.w : 0.5) * w,
            y: y1 + (old.h > 0 ? (p.y - old.y1) / old.h : 0.5) * h,
          };
          m.position(at);
          moved.set(m.id(), at);
        });
      });
    }
    onResized(moved);
    requestAnimationFrame(() => measure(groupId));
  };

  return (
    <div
      className="pg-resize"
      style={{
        left: box.x,
        top: box.y,
        width: box.w,
        height: box.h,
        borderColor: accent,
      }}
    >
      {CORNERS.map((corner) => (
        <div
          key={corner}
          role="button"
          aria-label={t("erdModal.resizeHandle", { corner })}
          className={`pg-resize-handle pg-resize-${corner}`}
          style={{ background: accent }}
          onPointerDown={(e) => down(corner, e)}
          onPointerMove={move}
          onPointerUp={up}
          onMouseEnter={() => {
            handleHover.current = true;
          }}
          onMouseLeave={() => {
            handleHover.current = false;
            if (!drag.current) onLeave();
          }}
        />
      ))}
    </div>
  );
}
