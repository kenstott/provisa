// Copyright (c) 2026 Kenneth Stott
// Canary: a3d9e2f1-7b4c-4a8e-9d5f-2c1b6e3a7f8d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { createContext, useContext, useState, type ReactNode } from "react";

// Lets a page portal a small control (e.g. a view-toggle SegmentedControl) into
// the right-hand side of NavBar's subnav row, instead of spending its own
// vertical real estate on a second toolbar row.
interface SubnavExtraSlot {
  node: HTMLDivElement | null;
  setNode: (n: HTMLDivElement | null) => void;
}

const SubnavExtraContext = createContext<SubnavExtraSlot | null>(null);

export function SubnavExtraProvider({ children }: { children: ReactNode }) {
  const [node, setNode] = useState<HTMLDivElement | null>(null);
  return (
    <SubnavExtraContext.Provider value={{ node, setNode }}>
      {children}
    </SubnavExtraContext.Provider>
  );
}

export function useSubnavExtraSlot(): SubnavExtraSlot {
  const ctx = useContext(SubnavExtraContext);
  if (!ctx) throw new Error("useSubnavExtraSlot must be used within SubnavExtraProvider");
  return ctx;
}
