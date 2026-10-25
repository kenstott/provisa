// Copyright (c) 2026 Kenneth Stott
// Canary: 8a668514-61f7-49cd-87cb-ee62bef33d0c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { TFunction } from "i18next";
import type { FrameData } from "../graph-model";

/** A done frame's header count: its nodes and relationships when it draws any, else the rows of a
 * table answer (a read of values only), never "0 nodes · 0 rels" over rows. */
export function frameMetaText(frame: FrameData, t: TFunction): string {
  const drawn = frame.nodes.size > 0 || frame.edges.size > 0;
  if (!drawn && frame.rows.length > 0) {
    return frame.elapsed !== undefined
      ? t("graphFrame.metaRowsElapsed", { count: frame.rows.length, elapsed: frame.elapsed })
      : t("graphFrame.metaRows", { count: frame.rows.length });
  }
  return frame.elapsed !== undefined
    ? t("graphFrame.metaCountElapsed", {
        nodes: frame.nodes.size,
        edges: frame.edges.size,
        elapsed: frame.elapsed,
      })
    : t("graphFrame.metaCount", { nodes: frame.nodes.size, edges: frame.edges.size });
}
