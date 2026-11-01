// Copyright (c) 2026 Kenneth Stott
// Canary: c29f4e80-6b13-47da-91e5-3f07a8d6b2c4
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useCallback, type RefObject } from "react";
import type { DropNotice, FrameData, GNode } from "../components/graph/graph-model";
import type { SparqlVocabulary } from "../components/graph/sparql";
import { droppedQuery, type DropNode, type SparqlDrop } from "../components/graph/sparqlDrop";
import { nodePathQuery } from "./graph-drop";

/** REQ-2135: what a pill on a SPARQL frame and a node on a Cypher node ask of the page. */
export function useFramePaths({
  framesRef,
  vocabulary,
  prefixes,
  pkMap,
  changeFrame,
  rerunFrame,
  runQuery,
}: {
  framesRef: RefObject<FrameData[]>;
  vocabulary: SparqlVocabulary | null;
  prefixes: Record<string, string>;
  pkMap: Record<string, string[]>;
  changeFrame: (id: string, change: (frame: FrameData) => Partial<FrameData>) => void;
  rerunFrame: (
    id: string,
    typed: string,
    drop?: { notices: DropNotice[]; limit?: number },
  ) => Promise<void>;
  runQuery: (typed: string) => Promise<void>;
}) {
  /** The drop edits the frame's query and runs it; one that can't join the result says why on the
   * frame and changes nothing. */
  const onSparqlDrop = useCallback(
    (frameId: string, drop: SparqlDrop, on: DropNode | null) => {
      const frame = framesRef.current.find((fr) => fr.id === frameId);
      if (!frame || !vocabulary) return;
      const done = droppedQuery(frame.query, drop, { ...vocabulary, prefixes }, on, frame.rows);
      if ("refused" in done) {
        const { refused } = done;
        changeFrame(frameId, () => ({ dropNotices: [refused], dropLimit: undefined }));
        return;
      }
      void rerunFrame(frameId, done.query, { notices: done.notices, limit: done.limit });
    },
    [framesRef, vocabulary, prefixes, changeFrame, rerunFrame],
  );

  /** A node dragged onto another asks for the paths between the two, in a frame of its own. */
  const onNodePath = useCallback(
    (frameId: string, from: GNode, to: GNode) => {
      const query = nodePathQuery(from, to, (label) => pkMap[label]?.[0]);
      if (query === null) {
        const notice = {
          key: "graphFrame.cypherPathNoKey",
          values: { from: from.label, to: to.label },
        };
        changeFrame(frameId, () => ({ dropNotices: [notice] }));
        return;
      }
      void runQuery(query);
    },
    [pkMap, changeFrame, runQuery],
  );

  return { onSparqlDrop, onNodePath };
}
