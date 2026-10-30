// Copyright (c) 2026 Kenneth Stott
// Canary: 5b2d8e71-3c64-4f9a-a0e7-91d4c6b82f35
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useMemo } from "react";
import {
  INJECTED_PROPS,
  type FrameData,
  type GEdge,
  type GNode,
  type GraphStats,
} from "../graph-model";
import { describedProperties } from "../sparql";

type Selection =
  | { kind: "node"; data: GNode; graphStats?: GraphStats }
  | { kind: "edge"; data: GEdge }
  | null;

// What a SELECT's node carries of itself, and the graph metrics the drawing injects: a property
// beyond these was read from the model, so there is nothing more to ask.
const IDENTITY = new Set(["iri", "name", "class"]);

/** A SELECT's bindings are IRIs, so its nodes hold no properties. A SPARQL node selected while it
 * holds none is described once (``onDescribe``, which keeps the answer in the frame's
 * ``sparqlDescribed``); the Properties panel then lists its literals beside what it had, or the
 * error the request met. The drawing is not touched. */
export function useSparqlNodeProperties(
  frame: FrameData,
  selected: Selection,
  onDescribe: ((frameId: string, iri: string) => void) | undefined,
): { inspected: Selection; describeError: string | undefined } {
  const node = selected?.kind === "node" ? selected.data : null;
  const iri = typeof node?.properties.iri === "string" ? node.properties.iri : null;
  const bare =
    node !== null &&
    Object.keys(node.properties).every((k) => IDENTITY.has(k) || INJECTED_PROPS.has(k));
  const described = iri === null ? undefined : frame.sparqlDescribed?.[iri];

  useEffect(() => {
    if (onDescribe && iri !== null && bare && described === undefined) onDescribe(frame.id, iri);
  }, [onDescribe, frame.id, iri, bare, described]);

  return useMemo(() => {
    if (!selected || selected.kind !== "node" || !described) {
      return { inspected: selected, describeError: undefined };
    }
    if ("error" in described) return { inspected: selected, describeError: described.error };
    const { properties, propertyIris } = describedProperties(described.literals);
    const data: GNode = {
      ...selected.data,
      properties: { ...selected.data.properties, ...properties },
      propertyIris: { ...selected.data.propertyIris, ...propertyIris },
    };
    return { inspected: { ...selected, data }, describeError: undefined };
  }, [selected, described]);
}
