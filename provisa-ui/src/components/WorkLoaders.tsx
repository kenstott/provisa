// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Three loaders for a long wait that show the sandbox being worked on, pure SVG and CSS. The pot:
// ingredients drop in and a spoon stirs, looping. The plant: a seed is planted and watered, then
// grows with the progress it is given. The score: a sheet of music is written, then a baton waves
// over it and a note rises. Under prefers-reduced-motion each stands still, its work done.

import { useReducedMotion } from "@mantine/hooks";
import type { ReactNode } from "react";
import "./WorkLoaders.css";

interface LoaderProps {
  size?: number;
  label: string;
  testId?: string;
}

function Frame({
  name,
  size = 88,
  label,
  testId,
  still,
  children,
}: LoaderProps & { name: string; still: boolean | undefined; children: ReactNode }) {
  return (
    <span
      role="status"
      aria-label={label}
      className="work-loader"
      data-motion={still ? "reduced" : "full"}
      data-testid={testId}
    >
      <svg
        className={`wl wl-${name} ${still ? "wl--still" : "wl--moving"}`}
        viewBox="0 0 120 120"
        width={size}
        height={size}
        aria-hidden="true"
        focusable="false"
      >
        {children}
      </svg>
    </span>
  );
}

// ---- Cooking pot ------------------------------------------------------------------------------
/** What drops in: each falls into the pot on its own beat. */
const INGREDIENTS = [
  { x: 44, shape: "M-4.5 0a4.5 4.5 0 1 0 9 0a4.5 4.5 0 1 0 -9 0Z", tone: "tomato" },
  { x: 60, shape: "M-5 3 L0 -5 L5 3Z", tone: "carrot" },
  { x: 76, shape: "M-4 -4 H4 V4 H-4Z", tone: "leek" },
];

export function CookingPotLoader(props: LoaderProps) {
  const still = useReducedMotion();
  return (
    <Frame name="cook" still={still} {...props}>
      <ellipse className="wl-shadow" cx="60" cy="104" rx="42" ry="6" />
      {INGREDIENTS.map((item, i) => (
        <g key={item.tone} transform={`translate(${item.x} 58)`}>
          <path className={`wl-food wl-food--${item.tone} wl-drop wl-drop--${i + 1}`} d={item.shape} />
        </g>
      ))}
      {/* The spoon stands behind the pot's front wall; only its handle shows above the rim. */}
      <g className="wl-spoon">
        <path className="wl-spoon-handle" d="M60 66 L72 22" />
      </g>
      <ellipse className="wl-stew" cx="60" cy="62" rx="34" ry="8" />
      <path
        className="wl-pot"
        d="M26 62 C26 66 94 66 94 62 L90 94 C89 100 84 102 78 102 H42 C36 102 31 100 30 94Z"
      />
      <path className="wl-pot-shine" d="M34 72 L37 92" />
      <path className="wl-pot-handle" d="M26 70 H17 M94 70 H103" />
      <path className="wl-steam wl-steam--1" d="M48 50 c-4 -5 4 -8 0 -13" />
      <path className="wl-steam wl-steam--2" d="M84 50 c-4 -5 4 -8 0 -13" />
    </Frame>
  );
}

// ---- Growing plant ----------------------------------------------------------------------------
/** The stem's height at full growth, from the soil, in viewBox units. */
const STEM_H = 58;
const SOIL_Y = 80;
/** The growth at which the seed has sprouted away, and at which the watering stops. */
const SEED_GONE = 0.08;
const WATERED = 0.5;

/** How grown the plant is, 0 to 1, for a wait `progress` percent done. */
function plantGrowth(progress: number): number {
  return Math.min(Math.max(progress, 0), 100) / 100;
}

export function GrowingPlantLoader({ progress = 0, ...props }: LoaderProps & { progress?: number }) {
  const still = useReducedMotion();
  // Reduced motion shows the plant grown as far as the progress has come, and it still follows
  // the bar: a step of the wait is information, not motion.
  const grown = plantGrowth(progress);
  const top = SOIL_Y - STEM_H * grown;
  return (
    <Frame name="plant" still={still} {...props}>
      <ellipse className="wl-shadow" cx="60" cy="104" rx="42" ry="6" />
      <ellipse className="wl-soil" cx="60" cy={SOIL_Y} rx="24" ry="4" />
      {/* The seed is planted, and the can waters it, while the plant is young. */}
      {grown < SEED_GONE && <circle className="wl-seed" cx="60" cy={SOIL_Y - 2} r="3" />}
      {grown < WATERED && (
        <g className="wl-can">
          <path className="wl-can-body" d="M84 24 H104 V42 H84Z" />
          <path className="wl-can-spout" d="M84 34 L70 26" />
          <path className="wl-can-grip" d="M104 28 h6 v10 h-6" />
          <circle className="wl-water wl-water--1" cx="68" cy="30" r="1.5" />
          <circle className="wl-water wl-water--2" cx="62" cy="32" r="1.5" />
          <circle className="wl-water wl-water--3" cx="56" cy="30" r="1.5" />
        </g>
      )}
      {/* The stem and its leaves are drawn from the plant's growth, not a timeline. */}
      <g data-testid="plant-growth" data-grown={grown.toFixed(2)}>
        <path className="wl-stem" d={`M60 ${SOIL_Y} V${top.toFixed(1)}`} />
        {grown >= 0.2 && (
          <path
            className="wl-leaf"
            d={`M60 ${(top + 16).toFixed(1)} c-14 -2 -18 -10 -20 -16 c10 0 18 6 20 16Z`}
          />
        )}
        {grown >= 0.45 && (
          <path
            className="wl-leaf"
            d={`M60 ${(top + 10).toFixed(1)} c14 -2 18 -10 20 -16 c-10 0 -18 6 -20 16Z`}
          />
        )}
        {grown >= 0.75 && (
          <g className="wl-flower" transform={`translate(60 ${top.toFixed(1)})`}>
            <circle cx="-5" cy="0" r="4" />
            <circle cx="5" cy="0" r="4" />
            <circle cx="0" cy="-5" r="4" />
            <circle cx="0" cy="5" r="4" />
            <circle className="wl-flower-eye" cx="0" cy="0" r="3" />
          </g>
        )}
      </g>
      <path className="wl-terracotta-rim" d="M34 82 H86 V89 H34Z" />
      <path className="wl-terracotta" d="M37 89 H83 L79 104 H41Z" />
    </Frame>
  );
}

// ---- Score and baton --------------------------------------------------------------------------
/** The five lines of the staff. */
const STAFF = [40, 47, 54, 61, 68];
/** The notes written in, one after another: x, the height of the head, and its stem up. */
const NOTES = [
  { x: 34, y: 61 },
  { x: 50, y: 54 },
  { x: 66, y: 47 },
  { x: 82, y: 57.5 },
];

export function ScoreLoader(props: LoaderProps) {
  const still = useReducedMotion();
  return (
    <Frame name="score" still={still} {...props}>
      <ellipse className="wl-shadow" cx="60" cy="104" rx="42" ry="6" />
      <path className="wl-sheet" d="M18 28 H102 V96 H18Z" />
      {STAFF.map((y) => (
        <path key={y} className="wl-staff" d={`M24 ${y} H96`} />
      ))}
      <path className="wl-clef" d="M29 72 C24 66 34 58 30 46 C28 38 36 38 34 46 C31 58 24 56 28 50" />
      {NOTES.map((n, i) => (
        <g key={n.x} className={`wl-note wl-note--${i + 1}`}>
          <ellipse cx={n.x} cy={n.y} rx="4" ry="3" transform={`rotate(-20 ${n.x} ${n.y})`} />
          <path d={`M${n.x + 3.6} ${n.y - 1} V${n.y - 21}`} />
        </g>
      ))}
      {/* The conductor's baton, waving over the finished page. */}
      <g className="wl-baton">
        <path className="wl-baton-stick" d="M92 90 L46 14" />
        <circle className="wl-baton-grip" cx="93" cy="92" r="4" />
      </g>
      {/* A note rises off the page as the baton comes down. */}
      <g className="wl-rising">
        <ellipse cx="60" cy="30" rx="4" ry="3" transform="rotate(-20 60 30)" />
        <path d="M63.6 29 V10 L72 14" />
      </g>
    </Frame>
  );
}
