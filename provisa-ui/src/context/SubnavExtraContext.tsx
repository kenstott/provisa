// Copyright (c) 2026 Kenneth Stott
// Canary: 69ac80c4-87da-4791-a915-8608bc0fa4ee
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
