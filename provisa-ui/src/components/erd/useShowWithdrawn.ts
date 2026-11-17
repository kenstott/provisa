// Copyright (c) 2026 Kenneth Stott
// Canary: 3d7f2a91-5e4b-4c06-8b1d-9a6e0c4f72b8
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1921: a graph view's Show withdrawn. Only a person who may preview withdrawn objects is
// offered it: a holder of table registration, the right the server answers includeWithdrawn by,
// domain by domain. Off, the view reads and draws no withdrawn table.

import { useState } from "react";
import { useCapability } from "../../hooks/useCapability";
import type { ShowWithdrawn } from "../provisa-graph/GraphOptionsBar";

export function useShowWithdrawn(): {
  /** Whether the view reads the withdrawn tables and relationships. */
  include: boolean;
  /** The switch, for a person who may preview withdrawn objects; undefined for anyone else. */
  control: ShowWithdrawn | undefined;
} {
  const allowed = useCapability("table_registration");
  const [shown, setShown] = useState(false);
  return {
    include: allowed && shown,
    control: allowed ? { shown, onChange: setShown } : undefined,
  };
}
