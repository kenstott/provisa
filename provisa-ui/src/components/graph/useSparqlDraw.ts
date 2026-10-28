// Copyright (c) 2026 Kenneth Stott
// Canary: 9a3d6b25-1e70-4c84-b8f2-5d0a7c1e3f46
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2135: how a SPARQL answer is drawn as asked (SparqlDrawControls offers the choices).

import { useMemo, useState } from "react";
import type { FrameData } from "./graph-model";
import { sparqlDrawn } from "./sparql";

export interface SparqlDraw {
  /** The frame as drawn. */
  frame: FrameData;
  literalNodes: boolean;
  setLiteralNodes: (on: boolean) => void;
  drawSelect: boolean;
  setDrawSelect: (on: boolean) => void;
}

export function useSparqlDraw(answered: FrameData): SparqlDraw {
  const [literalNodes, setLiteralNodes] = useState(false);
  const [drawSelect, setDrawSelect] = useState(false);
  const frame = useMemo(
    () => sparqlDrawn(answered, literalNodes, drawSelect),
    [answered, literalNodes, drawSelect],
  );
  return { frame, literalNodes, setLiteralNodes, drawSelect, setDrawSelect };
}
