// Copyright (c) 2026 Kenneth Stott
// Canary: 7a1f3c58-2e94-4b6d-8d07-c5e9b2a41f83
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A listed name wraps, never cut (the list rule: words wrap). A name is often one long identifier
// ("find_pets_by_status.status"): it may break after each "_", "|" and "." and nowhere else.

import { Fragment } from "react";
import "./BreakableName.css";

/** ``name`` split after each "_", "|" and ".": the places a line may break. */
const nameParts = (name: string): string[] => name.split(/(?<=[_|.])/);

/** ``name`` with a break opportunity after each "_", "|" and ".", at its whole width. */
export function BreakableName({ name }: { name: string }) {
  const parts = nameParts(name);
  return (
    <span className="name-breakable">
      {parts.map((p, i) => (
        <Fragment key={i}>
          {p}
          {i < parts.length - 1 && <wbr />}
        </Fragment>
      ))}
    </span>
  );
}
