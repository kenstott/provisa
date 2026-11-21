// Copyright (c) 2026 Kenneth Stott
// Canary: 9c4f1a62-7e3b-4d85-a2c0-6b8e3f1d7a49
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2174 (amended 2026-10-28; 2026-11-07): the graph's 2D view, the lane diagram (laneDiagram.ts),
// drawn by the one graph (REQ-2239) in its lanes preset: products to terms left to right, products,
// tables and columns sorted by the lane on their left, each term level with its column (amended
// 2026-11-11); each term on its own row, the links between terms (rank links and the others, named
// by their type) run beside the term lane. A click picks a node (the detail panel), a hover shows
// its card, a "+N columns" node expands in place; the diagram copies as Mermaid text. A card holds the node's facts with links to
// their pages, and stays while the pointer is on it.

import { useCallback, useMemo } from "react";
import { useTranslation } from "react-i18next";
import type { CameraCommand } from "../../pages/tables/profile3d/Stage3D";
import type { GraphHover } from "./graphScene";
import { hoverBody } from "./hoverBody";
import { MoreFacts } from "./nodeFacts";
import { LANES, connectedTo, laneMermaid, type Lane, type LaneDiagram } from "./laneDiagram";
import { ProvisaGraph } from "../provisa-graph/ProvisaGraph";
import type { LanesOptions } from "../provisa-graph/lanes";
import { laneGraph } from "./laneGraph";

export function LaneStage({
  diagram,
  selected,
  setSelected,
  hover,
  setHover,
  onMore,
  command,
  opensOn = null,
  testId,
  bare = false,
}: {
  diagram: LaneDiagram;
  selected: string | null;
  setSelected: (id: string) => void;
  hover: GraphHover | null;
  setHover: (h: GraphHover | null) => void;
  /** A "+N columns" node was clicked: its table's columns are drawn one by one. */
  onMore: (table: string) => void;
  command: CameraCommand | null;
  /** The focus the diagram is drawn round: it opens, and Reset view comes back, with it in view. */
  opensOn?: string | null;
  testId: string;
  /** A glance only (a card): no controls, legend or hint. */
  bare?: boolean;
}) {
  const { t } = useTranslation();
  const laneTitle = useCallback((l: Lane) => t(`graph3d.lane_${l}`), [t]);
  const moreLabel = useCallback((count: number) => t("graph3d.moreColumns", { count }), [t]);
  const graph = useMemo(() => laneGraph(diagram, moreLabel), [diagram, moreLabel]);
  const lanes = useMemo<LanesOptions>(
    () => ({
      lanes: LANES.map((l) => ({ id: l, title: laneTitle(l) })),
      beside: "term",
      tight: bare,
    }),
    [laneTitle, bare],
  );
  const neighbourhood = useCallback((id: string) => connectedTo(diagram, id), [diagram]);
  // A "+N columns" node's line is no edge of the graph: it has no card.
  const edgeHasCard = useCallback(
    (id: string) => diagram.graph.edges.some((e) => e.id === id),
    [diagram],
  );
  const hoverCard = (h: GraphHover) => {
    const more = diagram.more.find((m) => m.id === h.id);
    return more ? (
      <>
        <b>{moreLabel(more.count)}</b>
        <MoreFacts columns={more.columns} testId={`${testId}-tip-facts`} />
      </>
    ) : (
      hoverBody(diagram.graph, h, t, { data: diagram.data, testId: `${testId}-tip-facts` })
    );
  };
  return (
    <div className="p3d-stage g3d-lanes" data-ready>
      <ProvisaGraph
        graph={graph}
        preset="lanes"
        lanes={lanes}
        testId={testId}
        ariaLabel={t("graph3d.lanesLabel")}
        exportName="lanes"
        bare={bare}
        selected={selected}
        onNodeClick={(id) => setSelected(id)}
        onMore={(_id, data) => onMore(data("table") as string)}
        hover={hover}
        onHover={setHover}
        neighbourhood={neighbourhood}
        edgeHasCard={edgeHasCard}
        hoverCard={hoverCard}
        command={command}
        opensOn={opensOn}
        copy={{
          label: t("graph3d.copyMermaid"),
          done: t("graph3d.mermaidCopied"),
          text: () => laneMermaid(diagram, laneTitle, moreLabel),
          testId: `${testId}-mermaid`,
        }}
        hint={t("provisaGraph.hint")}
      />
    </div>
  );
}
