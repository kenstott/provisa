// Copyright (c) 2026 Kenneth Stott
// Canary: 7d3c1e52-94ab-4f0e-b8a6-3f1a2c5d9e41
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1387: the electrical-ground symbol (a stem over three shortening bars) drawn as a lucide icon
// does: a 24 grid, a 2px stroke, round caps and joins, the colour of the text around it. It marks a
// term that reaches data: green when the path to data is approved, muted while it awaits approval.

import type { SVGProps } from "react";
import { GROUND_PATHS, type GroundState } from "./groundPaths";


type Props = Omit<SVGProps<SVGSVGElement>, "stroke" | "viewBox"> & {
  state: GroundState;
  size?: number;
  /** The accessible name; without one the icon is decoration beside text that says the same. */
  label?: string;
};

export function GroundIcon({ state, size = 16, label, ...rest }: Props) {
  const pending = state === "grounded_pending";
  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      width={size}
      height={size}
      viewBox="0 0 24 24"
      fill="none"
      stroke="currentColor"
      strokeWidth={2}
      strokeLinecap="round"
      strokeLinejoin="round"
      // Green when grounded; the muted text colour while the path to data awaits approval.
      color={pending ? "var(--mantine-color-dimmed)" : "var(--mantine-color-teal-6)"}
      role={label ? "img" : undefined}
      aria-label={label}
      aria-hidden={label ? undefined : true}
      data-icon="ground"
      data-ground-state={state}
      {...rest}
    >
      {GROUND_PATHS.map((d) => (
        <path key={d} d={d} />
      ))}
    </svg>
  );
}
