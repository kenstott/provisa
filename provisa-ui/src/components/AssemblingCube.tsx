// Copyright (c) 2026 Kenneth Stott
// Canary: 6b567ae2-3be1-447f-a6b5-040def8b66c3
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A long wait's loader: a 3x3x3 isometric cube whose small cubes drop in layer by layer, hold,
// and reset. Inline SVG and CSS only. One timeline (--t, 0 to 1) runs on the SVG and each small
// cube derives its own drop from it and its start (--s), so all 27 keep step and reset together.
// Under prefers-reduced-motion the cube stands assembled and only pulses.

import { useReducedMotion } from "@mantine/hooks";
import type { CSSProperties } from "react";
import "./AssemblingCube.css";

const C = Math.cos(Math.PI / 6);
const GAP = 0.06;
type P3 = [number, number, number];
const at = ([x, y, z]: P3) => `${((x - y) * C).toFixed(3)} ${((x + y) / 2 - z).toFixed(3)}`;
const face = (pts: P3[]) => `M${pts.map(at).join("L")}Z`;

/** The 27 small cubes, back to front (painter's order), each with its three visible faces and the
 * point of the timeline it starts to drop: by layer, then back to front within the layer. */
const CUBES = (() => {
  const out: { s: number; top: string; left: string; right: string; key: string }[] = [];
  for (let z = 0; z < 3; z++) {
    const layer: P3[] = [];
    for (let x = 0; x < 3; x++) for (let y = 0; y < 3; y++) layer.push([x, y, z]);
    layer.sort((a, b) => a[0] + a[1] - (b[0] + b[1]) || a[0] - b[0]);
    layer.forEach(([x, y], rank) => {
      const [x0, y0, z0] = [x + GAP, y + GAP, z + GAP];
      const [x1, y1, z1] = [x + 1 - GAP, y + 1 - GAP, z + 1 - GAP];
      out.push({
        key: `${x}${y}${z}`,
        s: 0.06 + z * 0.2 + rank * 0.018,
        top: face([[x0, y0, z1], [x1, y0, z1], [x1, y1, z1], [x0, y1, z1]]),
        left: face([[x0, y1, z0], [x1, y1, z0], [x1, y1, z1], [x0, y1, z1]]),
        right: face([[x1, y0, z0], [x1, y1, z0], [x1, y1, z1], [x1, y0, z1]]),
      });
    });
  }
  return out;
})();

export function AssemblingCube({
  size = 72,
  label,
  testId,
}: {
  /** Width in pixels; the height follows. */
  size?: number;
  /** What is being waited for, read by screen readers. */
  label: string;
  testId?: string;
}) {
  const still = useReducedMotion();
  return (
    <span
      role="status"
      aria-label={label}
      className="assembling-cube"
      data-motion={still ? "reduced" : "full"}
      data-testid={testId}
    >
      <svg
        className={still ? "ac ac--still" : "ac ac--moving"}
        viewBox="-2.75 -3.2 5.5 6.4"
        width={size}
        height={(size * 6.4) / 5.5}
        aria-hidden="true"
        focusable="false"
      >
        {CUBES.map((c) => (
          <g key={c.key} className="ac-cube" style={{ "--s": c.s } as CSSProperties}>
            <path className="ac-left" d={c.left} />
            <path className="ac-right" d={c.right} />
            <path className="ac-top" d={c.top} />
          </g>
        ))}
      </svg>
    </span>
  );
}
