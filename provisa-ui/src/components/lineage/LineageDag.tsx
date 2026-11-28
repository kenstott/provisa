// Copyright (c) 2026 Kenneth Stott
// Canary: 2d7b9e14-6a03-4c58-8f21-9b0c3e7a5d46
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1160/REQ-1161, REQ-1665: the column lineage graph, left to right, source to output.
// REQ-2239 (amended 2026-11-27): drawn by the one graph component, as the 2D ontology graph is, in
// its flow preset; lineage keeps what is its own: a relation is a group of its fields (or one node,
// collapsed), a field's role in the flow is its node type, and each line carries its transform.
// A click on a relation's group (or its collapsed node) collapses or expands it; a click on a field
// or a command's line opens where it is defined, a double click on a relation opens the relation
// (REQ-2116); hovering a field lights everything upstream and downstream of it.

import { useCallback, useMemo, type ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { Text } from "@mantine/core";
import { ChevronsDownUp, ChevronsUpDown } from "lucide-react";
import { ExpandModalIcon } from "../graph/GraphIcons";
import type { LineageGraphData } from "../../api/lineage";
import { CONTROL_ICON } from "../provisa-graph/CanvasControls";
import type { GraphCommandButton } from "../provisa-graph/GraphControls";
import { ProvisaGraph, type GraphHover } from "../provisa-graph/ProvisaGraph";
import type { DescribeColumn } from "./column-descriptions";
import { buildLineageModel, edgeWords } from "./lineage-layout";
import type { LineageLinks } from "./lineage-links";
import { lineageSpec, lineageTrace, type LineageWords } from "./lineageSpec";

interface LineageDagProps {
  graph: LineageGraphData;
  height?: number | string;
  // REQ-1627: relations drawn as ONE node with their column lines rolled up. Federation-scale
  // graphs are unreadable column by column, so Complete Lineage collapses everything by default and
  // the reader expands the relations they are actually tracing.
  collapsedRelations?: ReadonlySet<string>;
  onToggleRelation?: (relation: string) => void;
  onCollapseAll?: () => void;
  onExpandAll?: () => void;
  // Opens the same graph in a near-fullscreen modal; omitted when already rendering inside one.
  onOpenModal?: () => void;
  // REQ-1665: draw every column. Complete Lineage is read from a role's vantage point and every
  // column that role can see is in use, so none is folded into "+N unused".
  allColumns?: boolean;
  // Column and line counts, over the canvas's top-left corner.
  stats?: boolean;
  // A field's registered description, said on its card.
  describeColumn?: DescribeColumn;
  // How a relation is named (lineage-layout's relationLabel).
  relationLabel?: (relation: string) => string;
  // REQ-2116: where each element is defined, as far as the user's rights open it.
  links?: LineageLinks;
  onOpenLink?: (href: string) => void;
  testId?: string;
}

export function LineageDag({
  graph,
  height = 520,
  collapsedRelations,
  onToggleRelation,
  onCollapseAll,
  onExpandAll,
  onOpenModal,
  allColumns = false,
  stats = false,
  describeColumn,
  relationLabel,
  links,
  onOpenLink,
  testId = "lineage-dag",
}: LineageDagProps): React.ReactElement {
  const { t } = useTranslation();
  const words = useMemo<LineageWords>(
    () => ({
      ...edgeWords(t),
      unused: (count) => t("lineageDag.unused", { count }),
      materialized: t("lineageDag.materialized"),
      feedbackCycle: t("lineageDag.feedbackCycle"),
      errorCycle: t("lineageDag.errorCycle"),
    }),
    [t],
  );
  const spec = useMemo(() => {
    const model = buildLineageModel(graph, collapsedRelations, {
      allColumns,
      relationLabel,
      reportedWords: {
        job: t("lineageDag.job"),
        outside: t("lineageDag.outsideModel"),
        reported: t("lineageDag.reportedEdge"),
      },
    });
    const drawn = lineageSpec(model, words, describeColumn);
    // What a click on a relation does, on its card: the affordance is otherwise invisible.
    if (!onToggleRelation) return drawn;
    const hint = (relation: string, collapsed: boolean) =>
      [
        t(collapsed ? "lineageDag.clickToExpand" : "lineageDag.clickToCollapse"),
        links?.relation(relation) ? t("lineageDag.doubleClickToOpen") : null,
      ]
        .filter(Boolean)
        .join(" · ");
    return {
      ...drawn,
      groups: (drawn.groups ?? []).map((g) => {
        const relation = g.data?.relation as string;
        return relation
          ? {
              ...g,
              description: [g.description, hint(relation, false)].filter(Boolean).join(" · "),
            }
          : g;
      }),
      nodes: drawn.nodes.map((n) =>
        n.data?.collapsed === "yes"
          ? {
              ...n,
              description: `${hint(n.data.relation as string, true)} · ${n.description}`,
            }
          : n,
      ),
    };
  }, [
    graph,
    collapsedRelations,
    allColumns,
    relationLabel,
    describeColumn,
    words,
    links,
    onToggleRelation,
    t,
  ]);

  // A collapsed relation's card previews its every column; an open one shows them.
  const columnsOf = useCallback(
    (relation: string) => graph.nodes.filter((n) => n.relation === relation).map((n) => n.column),
    [graph],
  );
  const hoverCard = useCallback(
    (h: GraphHover): ReactNode => {
      if (h.kind === "edge") {
        const e = spec.edges.find((x) => x.id === h.id);
        return e?.label ? <b>{e.label}</b> : null;
      }
      const n = spec.nodes.find((x) => x.id === h.id);
      if (!n) return null;
      const collapsed = n.data?.collapsed === "yes";
      return (
        <div data-testid="lineage-box-hint">
          <b>{n.name}</b>
          {n.description && <div className="pg-tip-body">{n.description}</div>}
          {collapsed &&
            columnsOf(n.data?.relation as string).map((c) => (
              <div key={c} style={{ fontFamily: "var(--mantine-font-family-monospace)" }}>
                {c}
              </div>
            ))}
        </div>
      );
    },
    [spec, columnsOf],
  );

  const neighbourhood = useCallback((id: string) => lineageTrace(spec, id), [spec]);
  const collapsed = useMemo(() => new Set<string>(), []);

  const commands: GraphCommandButton[] = [
    ...(onExpandAll
      ? [
          {
            label: t("lineageDag.expandAll"),
            icon: <ChevronsUpDown size={CONTROL_ICON} />,
            onClick: onExpandAll,
            testId: "lineage-expand-all",
          },
        ]
      : []),
    ...(onCollapseAll
      ? [
          {
            label: t("lineageDag.collapseAll"),
            icon: <ChevronsDownUp size={CONTROL_ICON} />,
            onClick: onCollapseAll,
            testId: "lineage-collapse-all",
          },
        ]
      : []),
    ...(onOpenModal
      ? [
          {
            label: t("lineageDag.enlarge"),
            // REQ-2239: the enlarge-into-a-dialog icon the result graph's Enlarge uses; the Expand
            // icon is the bar's Full screen.
            icon: <ExpandModalIcon size={CONTROL_ICON} />,
            onClick: onOpenModal,
            testId: "lineage-open-modal",
          },
        ]
      : []),
  ];

  const open = (href: string | null | undefined) => {
    if (href) onOpenLink?.(href);
  };

  return (
    <div style={{ position: "relative", width: "100%", height }}>
      <ProvisaGraph
        graph={spec}
        preset="flow"
        testId={testId}
        ariaLabel={t("lineagePage.title")}
        exportName="lineage"
        commands={commands}
        hoverCard={hoverCard}
        neighbourhood={neighbourhood}
        // A click on a group toggles its relation: ProvisaGraph asks for the next set, which is
        // the toggled one alone.
        collapsed={collapsed}
        onCollapsedChange={
          onToggleRelation
            ? (next) => {
                for (const id of next) {
                  const relation = spec.groups?.find((g) => g.id === id)?.data?.relation;
                  if (relation) onToggleRelation(relation as string);
                }
              }
            : undefined
        }
        onNodeClick={(id, data) => {
          if (data("collapsed") === "yes") {
            onToggleRelation?.(data("relation") as string);
            return;
          }
          if (data("row") === "yes") open(links?.node(id));
        }}
        onDoubleClick={(_id, data) => {
          const relation = data("relation") as string;
          if (relation) open(links?.relation(relation));
        }}
        onEdgeClick={(_id, data) => {
          const name = data("commandName") as string;
          if (name) open(links?.command(name));
        }}
        overlay={
          stats ? (
            <Text size="xs" c="dimmed" data-testid="lineage-stats">
              {t("lineageDag.stats", { columns: graph.nodes.length, edges: graph.edges.length })}
            </Text>
          ) : undefined
        }
        hint={t("provisaGraph.hint")}
      />
    </div>
  );
}
