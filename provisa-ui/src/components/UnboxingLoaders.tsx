// Copyright (c) 2026 Kenneth Stott
// Canary: b9e225d1-76e7-4ed4-aec9-156e15ea6a3c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Two loaders for a long wait, pure SVG and CSS. The cardboard box: four flaps fold open one after
// another, a purple Provisa cube lifts out and hovers. The gift box: the ribbons unravel, the lid
// lifts, the Provisa "P" rises out. Both loop. Under prefers-reduced-motion each stands still,
// open, with its contents showing.

import { useReducedMotion } from "@mantine/hooks";
import type { CSSProperties } from "react";
import "./UnboxingLoaders.css";

type Pt = [number, number];
const d = (pts: Pt[]) => `M${pts.map((p) => `${p[0].toFixed(2)} ${p[1].toFixed(2)}`).join("L")}Z`;

/** The three visible faces of an isometric box whose top face is centred on (cx, cy). */
function faces(cx: number, cy: number, hw: number, hh: number, h: number) {
  const B: Pt = [cx, cy - hh];
  const R: Pt = [cx + hw, cy];
  const F: Pt = [cx, cy + hh];
  const L: Pt = [cx - hw, cy];
  const down = ([x, y]: Pt): Pt => [x, y + h];
  return {
    top: d([B, R, F, L]),
    left: d([L, F, down(F), down(L)]),
    right: d([F, R, down(R), down(F)]),
  };
}

// ---- Cardboard box ---------------------------------------------------------------------------
const BX = { cx: 60, cy: 62, hw: 36, hh: 18, h: 34 };
const BC: Pt = [BX.cx, BX.cy];
const BB: Pt = [BX.cx, BX.cy - BX.hh];
const BR: Pt = [BX.cx + BX.hw, BX.cy];
const BF: Pt = [BX.cx, BX.cy + BX.hh];
const BL: Pt = [BX.cx - BX.hw, BX.cy];
const BOX = faces(BX.cx, BX.cy, BX.hw, BX.hh, BX.h);

/** A flap is the triangle from its hinge edge to the box centre; folding it open is a scale of -k
 * along the hinge's normal, about the hinge (the matrix is built in CSS from these numbers). */
function flap(a: Pt, b: Pt) {
  const mx = (a[0] + b[0]) / 2;
  const my = (a[1] + b[1]) / 2;
  const ex = b[0] - a[0];
  const ey = b[1] - a[1];
  const len = Math.hypot(ex, ey);
  const nx = -ey / len;
  const ny = ex / len;
  return {
    path: d([a, b, BC]),
    style: { "--nx2": nx * nx, "--nxy": nx * ny, "--ny2": ny * ny, "--mx": mx, "--my": my } as CSSProperties,
  };
}
// back flaps fold up and behind; front flaps fall forward over the front faces
const BACK_FLAPS = [flap(BB, BL), flap(BB, BR)];
const FRONT_FLAPS = [flap(BL, BF), flap(BR, BF)];

const CUBE = faces(0, 0, 11, 6.5, 13);

function ProvisaCube({ eyes }: { eyes: boolean }) {
  return (
    <g className="ub-cube">
      <path className="ub-cube-left" d={CUBE.left} />
      <path className="ub-cube-right" d={CUBE.right} />
      <path className="ub-cube-top" d={CUBE.top} />
      {eyes && (
        <g className="ub-eyes">
          <circle cx="-7.5" cy="7.1" r="1.15" />
          <circle cx="-3.6" cy="9.3" r="1.15" />
        </g>
      )}
    </g>
  );
}

export function CardboardBoxLoader({
  size = 88,
  label,
  testId,
}: {
  size?: number;
  label: string;
  testId?: string;
}) {
  const still = useReducedMotion();
  return (
    <span
      role="status"
      aria-label={label}
      className="unboxing"
      data-motion={still ? "reduced" : "full"}
      data-testid={testId}
    >
      <svg
        className={`ub ub-box ${still ? "ub--still" : "ub--moving"}`}
        viewBox="0 0 120 120"
        width={size}
        height={size}
        aria-hidden="true"
        focusable="false"
      >
        <ellipse className="ub-shadow" cx="60" cy="97" rx="40" ry="8" />
        {BACK_FLAPS.map((f, i) => (
          <path key={i} className={`ub-flap ub-flap--${i + 1}`} d={f.path} style={f.style} />
        ))}
        <path className="ub-inside" d={BOX.top} />
        <g transform="translate(60 56)">
          <g className="ub-lift">
            <g className="ub-bob">
              <ProvisaCube eyes />
            </g>
          </g>
        </g>
        <path className="ub-side-l" d={BOX.left} />
        <path className="ub-side-r" d={BOX.right} />
        {/* "This way up", the way a box says it */}
        <g className="ub-arrows" transform="translate(42 90)">
          <path d="M0 -9 L5 -2 H1.8 V6 H-1.8 V-2 H-5Z" transform="translate(-4 0) skewY(26.5)" />
          <path d="M0 -9 L5 -2 H1.8 V6 H-1.8 V-2 H-5Z" transform="translate(5 2.6) skewY(26.5)" />
        </g>
        {FRONT_FLAPS.map((f, i) => (
          <path key={i} className={`ub-flap ub-flap--${i + 3}`} d={f.path} style={f.style} />
        ))}
        <path className="ub-tape" d="M57 44.5 L63 41.5 L63 79 L57 76Z" />
        <path className="ub-tape ub-tape-down" d="M57 76 L63 79 L63 94 L57 91Z" />
      </svg>
    </span>
  );
}

// ---- Gift box --------------------------------------------------------------------------------
const GB = { cx: 60, cy: 66, hw: 33, hh: 16.5, h: 32 };
const GIFT = faces(GB.cx, GB.cy, GB.hw, GB.hh, GB.h);
const LID = faces(GB.cx, GB.cy - 11, GB.hw + 3, GB.hh + 1.5, 11);

// the two ribbons cross on the top: each runs down one visible face and over the top
const RIB_LID_Y = GB.cy - 11;
const ribbon = {
  left: `M${GB.cx - GB.hw / 2} ${GB.cy + GB.hh / 2 + 32} V${GB.cy + GB.hh / 2 - 11}`,
  right: `M${GB.cx + GB.hw / 2} ${GB.cy + GB.hh / 2 + 32} V${GB.cy + GB.hh / 2 - 11}`,
  topA: `M${GB.cx - GB.hw / 2} ${RIB_LID_Y + GB.hh / 2 + 1} L${GB.cx + GB.hw / 2} ${RIB_LID_Y - GB.hh / 2 + 1}`,
  topB: `M${GB.cx + GB.hw / 2} ${RIB_LID_Y + GB.hh / 2 + 1} L${GB.cx - GB.hw / 2} ${RIB_LID_Y - GB.hh / 2 + 1}`,
};

export function GiftBoxLoader({
  size = 88,
  label,
  testId,
}: {
  size?: number;
  label: string;
  testId?: string;
}) {
  const still = useReducedMotion();
  return (
    <span
      role="status"
      aria-label={label}
      className="unboxing"
      data-motion={still ? "reduced" : "full"}
      data-testid={testId}
    >
      <svg
        className={`ub ub-gift ${still ? "ub--still" : "ub--moving"}`}
        viewBox="0 0 120 120"
        width={size}
        height={size}
        aria-hidden="true"
        focusable="false"
      >
        <ellipse className="ub-shadow" cx="60" cy="100" rx="40" ry="8" />
        <path className="ub-gift-left" d={GIFT.left} />
        <path className="ub-gift-right" d={GIFT.right} />
        <path className="ub-inside" d={GIFT.top} />
        <g className="ub-gift-mark">
          <g className="ub-bob">
            <svg x="45" y="26" width="30" height="30" viewBox="0 0 512 512">
              <rect width="512" height="512" rx="112" fill="#1F2933" />
              <g transform="translate(48,66) scale(4.0)">
                <rect x="30" y="18" width="15" height="64" rx="7" fill="#ffffff" />
                <circle cx="52" cy="35" r="22" fill="#ffffff" />
                <circle cx="52" cy="35" r="10.5" fill="#1F2933" />
                <circle cx="52" cy="35" r="4.5" fill="#10B981" />
              </g>
            </svg>
          </g>
        </g>
        <g className="ub-lid">
          <path className="ub-lid-left" d={LID.left} />
          <path className="ub-lid-right" d={LID.right} />
          <path className="ub-lid-top" d={LID.top} />
        </g>
        <g className="ub-ribbons">
          {[ribbon.left, ribbon.right, ribbon.topA, ribbon.topB].map((p, i) => (
            <path key={i} className={`ub-rib ub-rib--${i + 1}`} d={p} pathLength={1} />
          ))}
          <g className="ub-bow" transform={`translate(${GB.cx} ${RIB_LID_Y + 1})`}>
            <g className="ub-bow-spin">
              <ellipse className="ub-bow-loop" cx="-7" cy="-4" rx="7" ry="4" transform="rotate(-24 -7 -4)" />
              <ellipse className="ub-bow-loop" cx="7" cy="-4" rx="7" ry="4" transform="rotate(24 7 -4)" />
              <circle className="ub-bow-knot" cx="0" cy="-1" r="2.8" />
            </g>
          </g>
        </g>
      </svg>
    </span>
  );
}
