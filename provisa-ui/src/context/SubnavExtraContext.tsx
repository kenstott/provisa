// Copyright (c) 2026 Kenneth Stott
// Canary: a3d9e2f1-7b4c-4a8e-9d5f-2c1b6e3a7f8d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState, type ReactNode } from "react";
import { SubnavExtraContext } from "./subnavExtraSlot";

export function SubnavExtraProvider({ children }: { children: ReactNode }) {
  const [node, setNode] = useState<HTMLDivElement | null>(null);
  return (
    <SubnavExtraContext.Provider value={{ node, setNode }}>{children}</SubnavExtraContext.Provider>
  );
}
