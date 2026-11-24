// Copyright (c) 2026 Kenneth Stott
// Canary: f6272d75-1d43-48e5-a030-7054b81179c0
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useState } from "react";
import type { Dispatch, SetStateAction } from "react";
import type { GNode, GEdge, FrameData } from "../graph-model";
import { extractElements } from "../graph-model";

type OverlayMap = Map<string, { nodes: Map<string, GNode>; edges: Map<string, GEdge> }>;

/** A relationship type the imputation could not read, with the server's reason (REQ-392). */
export type UnreadRel = { type: string; error: string };

/** Keeps the `__remaining_rels` overlay in sync with the autoImpute toggle, and returns each
 * relationship type the last imputation could not read (REQ-392: shown, never hidden).
 *
 * Split out of GraphFrame: the imputation round-trip and its teardown are self-contained, and
 * the overlay key it owns is written nowhere else.
 */
export function useAutoImpute(
  frame: FrameData,
  autoImpute: boolean,
  setOverlayData: Dispatch<SetStateAction<OverlayMap>>,
): UnreadRel[] {
  // What the last imputation could not read, and the node set it was asked about: shown only
  // while auto-impute is on and the frame still holds that node set.
  const [unread, setUnread] = useState<{ nodes: FrameData["nodes"]; rels: UnreadRel[] } | null>(
    null,
  );

  // When autoImpute is turned off, clear its overlay
  useEffect(() => {
    if (!autoImpute) {
      setOverlayData((prev) => {
        const next = new Map(prev);
        next.delete("__remaining_rels");
        return next;
      });
    }
  }, [autoImpute, setOverlayData]);

  // Run imputation whenever the frame result changes or autoImpute toggles on
  useEffect(() => {
    if (!autoImpute || frame.status !== "done" || frame.nodes.size === 0) return;
    let cancelled = false;
    const nodes = frame.nodes;
    const nodeList = [...frame.nodes.values()].map((n) => ({ label: n.label, id: n.id }));
    (async () => {
      const res = await fetch("/data/impute-relationships", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ nodes: nodeList }),
      });
      if (cancelled) return;
      if (!res.ok) {
        const text = await res.text().catch(() => "");
        let err: unknown;
        try {
          err = JSON.parse(text);
        } catch {
          err = text;
        }
        console.error("impute-relationships failed (HTTP", res.status, "):", err);
        return;
      }
      const data = await res.json();
      const rows: Record<string, unknown>[] = data.rows ?? [];
      const result = extractElements(rows);
      if (cancelled) return;
      setUnread({ nodes, rels: data.unread as UnreadRel[] });
      if (!cancelled && (result.nodes.size > 0 || result.edges.size > 0)) {
        setOverlayData((prev) => new Map(prev).set("__remaining_rels", result));
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [autoImpute, frame.status, frame.nodes, setOverlayData]);

  return autoImpute && unread !== null && unread.nodes === frame.nodes ? unread.rels : [];
}
