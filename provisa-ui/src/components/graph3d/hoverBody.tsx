// Copyright (c) 2026 Kenneth Stott
// Canary: 2e7d4b19-8a3c-4f50-b6e1-d9c2a7f4e083
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2128: a graph's hover card -- a node's name, kind and connections, an edge's ends and kind;
// REQ-2174: in the lane diagram, with the node's facts (nodeFacts).

import type { ReactNode } from "react";
import type { TFunction } from "i18next";
import { edgesOf, type GraphData } from "./graphModel";
import type { GraphHover } from "./graphScene";
import { kindKey } from "./nodeLook";
import { NodeFacts } from "./nodeFacts";

/** The hover card's body for what ``h`` points at in ``graph``; with ``facts``, a node's facts
 * read from the whole graph. */
export function hoverBody(
  graph: GraphData,
  h: GraphHover,
  t: TFunction,
  facts?: { data: GraphData; testId: string },
): ReactNode {
  if (h.kind === "node") {
    const node = graph.nodes.find((n) => n.id === h.id);
    if (!node) return null;
    return (
      <>
        <b>{node.label}</b>
        <span>{t(kindKey(node))}</span>
        <span className="p3d-num">
          {t("graph3d.tipLinks", { count: edgesOf(graph, node.id).length })}
        </span>
        {facts && <NodeFacts node={node} data={facts.data} definition testId={facts.testId} />}
      </>
    );
  }
  const e = graph.edges.find((x) => x.id === h.id);
  if (!e) return null;
  const name = (id: string) => graph.nodes.find((n) => n.id === id)?.label ?? id;
  return (
    <>
      <b>{`${name(e.a)} → ${name(e.b)}`}</b>
      <span>{e.label ?? t(`graph3d.edge_${e.kind}`)}</span>
      {(e.paths ?? 1) > 1 && (
        <span className="p3d-num">{t("graph3d.paths", { count: e.paths })}</span>
      )}
    </>
  );
}
