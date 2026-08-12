// Copyright (c) 2026 Kenneth Stott
// Canary: c8d01986-24cd-4af8-8bf2-d9eb11768b90
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { createContext, useContext } from "react";

// Lets a page portal a small control (e.g. a view-toggle SegmentedControl) into
// the right-hand side of NavBar's subnav row, instead of spending its own
// vertical real estate on a second toolbar row.
//
// The context and its hook live apart from the provider component so that the
// provider file exports components only, which is what React Fast Refresh needs
// to swap it without dropping state.
export interface SubnavExtraSlot {
  node: HTMLDivElement | null;
  setNode: (n: HTMLDivElement | null) => void;
}

export const SubnavExtraContext = createContext<SubnavExtraSlot | null>(null);

export function useSubnavExtraSlot(): SubnavExtraSlot {
  const ctx = useContext(SubnavExtraContext);
  if (!ctx) throw new Error("useSubnavExtraSlot must be used within SubnavExtraProvider");
  return ctx;
}
