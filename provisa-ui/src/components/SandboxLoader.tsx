// Copyright (c) 2026 Kenneth Stott
// Canary: 21c38b06-1b76-40d8-aa81-011fd714fbb7
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The "Opening your demo sandbox" loader: one of three animations, chosen at random each time the
// screen opens (the cube that assembles, a cardboard box that unfolds, a gift box that unwraps).
// The choice is made once per mount; tests inject the random source.

import { useState } from "react";
import { AssemblingCube } from "./AssemblingCube";
import { pickLoader, type SandboxLoaderKind } from "./sandboxLoaderKind";
import { CardboardBoxLoader, GiftBoxLoader } from "./UnboxingLoaders";

export function SandboxLoader({
  size = 88,
  label,
  testId,
  random,
  kind,
}: {
  /** Width in pixels. */
  size?: number;
  /** What is being waited for, read by screen readers. */
  label: string;
  testId?: string;
  /** Random source for the choice (tests). */
  random?: () => number;
  /** Force one loader (previews, tests); otherwise chosen at random on mount. */
  kind?: SandboxLoaderKind;
}) {
  const [chosen] = useState(() => kind ?? pickLoader(random));
  const shown = kind ?? chosen;
  return (
    <span className="sandbox-loader" data-loader={shown} style={{ display: "inline-flex" }}>
      {shown === "box" ? (
        <CardboardBoxLoader size={size} label={label} testId={testId} />
      ) : shown === "gift" ? (
        <GiftBoxLoader size={size} label={label} testId={testId} />
      ) : (
        <AssemblingCube size={size} label={label} testId={testId} />
      )}
    </span>
  );
}
