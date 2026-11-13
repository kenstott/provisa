// Copyright (c) 2026 Kenneth Stott
// Canary: e6c28a42-41ba-4749-925b-3a53ce28f1c4
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import React from "react";
import { useTranslation } from "react-i18next";
import { Table } from "@mantine/core";
import type { ExplainNodeDto, ExplainResponse } from "../../api/admin";
import { MermaidDiagram } from "../../components/MermaidDiagram";
import { Tip } from "../../components/Tip";
import { ListCell } from "../../components/list/ListCell";
import { ListTable } from "../../components/list/ListTable";

/** The panes' height in the results strip; in the Query plan dialog they fill it. */
const INLINE_HEIGHT = "420px";

// A deep plan (hundreds of nested operators) would indent its rows off the pane: past this depth
// the indent stops growing and the row names its depth instead.
const MAX_INDENT_DEPTH = 8;

// REQ-1519: the operator tree read top-down, each row carrying its depth so the list reads like
// the diagram.
function flattenPlan(
  nodes: ExplainNodeDto[],
  depth = 0,
): { node: ExplainNodeDto; depth: number }[] {
  return nodes.flatMap((node) => [{ node, depth }, ...flattenPlan(node.children, depth + 1)]);
}

// The server draws the operators in the order of this list (depth-first, top-down), operator n
// (from 1) as Mermaid node p{n} (provisa/executor/explain.py build_explain_mermaid).
const nodeKey = (row: number) => `p${row + 1}`;
const rowOf = (key: string): number | null => {
  const m = /^p(\d+)$/.exec(key);
  return m ? Number(m[1]) - 1 : null;
};

// REQ-1519: one rendering of the plan, used inline in the results strip and again at full size
// in the Query plan dialog, so both readings stay identical. The diagram and the list are linked:
// an operator picked in either is marked in both, the list scrolled to its row and the diagram
// moved to its node.
export function AnalyzePanes({ plan, fill = false }: { plan: ExplainResponse; fill?: boolean }) {
  const { t } = useTranslation();
  // REQ-1521: the rule between the panes is the handle — the split a reader wants depends on
  // whether the tree is deep or the diagram is wide, so they set it by dragging it.
  const [listPct, setListPct] = React.useState(50);
  const [picked, setPicked] = React.useState<number | null>(null);
  const rows = React.useMemo(() => flattenPlan(plan.plan), [plan.plan]);
  const rowRefs = React.useRef<(HTMLTableRowElement | null)[]>([]);

  const pickNode = (key: string) => {
    const row = rowOf(key);
    if (row === null || row >= rows.length) return;
    setPicked(row);
    rowRefs.current[row]?.scrollIntoView({ block: "nearest" });
  };

  const startResize = (e: React.MouseEvent) => {
    e.preventDefault();
    // The separator is only ever rendered as a child of the pane row, so its parent is the row.
    const row = e.currentTarget.parentElement as HTMLElement;
    const rtl = window.getComputedStyle(row).direction === "rtl";
    const onMove = (ev: MouseEvent) => {
      const rect = row.getBoundingClientRect();
      const offset = rtl ? rect.right - ev.clientX : ev.clientX - rect.left;
      setListPct(Math.min(85, Math.max(15, (offset / rect.width) * 100)));
    };
    const onUp = () => {
      document.removeEventListener("mousemove", onMove);
      document.removeEventListener("mouseup", onUp);
    };
    document.addEventListener("mousemove", onMove);
    document.addEventListener("mouseup", onUp);
  };

  return (
    <div
      style={{
        display: "flex",
        gap: "0.75rem",
        alignItems: "stretch",
        height: fill ? "100%" : INLINE_HEIGHT,
      }}
      data-testid="analyze-panes"
    >
      {rows.length > 0 && (
        <div style={{ flex: `0 0 ${listPct}%`, minWidth: 0, overflow: "auto" }}>
          <ListTable style={{ fontSize: "0.68rem" }}>
            <Table.Thead>
              <Table.Tr>
                <Table.Th>{t("sqlResultsPanel.analyzeColOperator")}</Table.Th>
                <Table.Th>{t("sqlResultsPanel.analyzeColRows")}</Table.Th>
                <Table.Th>
                  {plan.analyzed
                    ? t("sqlResultsPanel.analyzeColActualMs")
                    : t("sqlResultsPanel.analyzeColCost")}
                </Table.Th>
                <Table.Th>{t("sqlResultsPanel.analyzeColDetail")}</Table.Th>
              </Table.Tr>
            </Table.Thead>
            <Table.Tbody>
              {rows.map((row, i) => (
                <Table.Tr
                  key={i}
                  ref={(el: HTMLTableRowElement | null) => {
                    rowRefs.current[i] = el;
                  }}
                  className={picked === i ? "clickable row-selected" : "clickable"}
                  aria-selected={picked === i}
                  onClick={() => setPicked(i)}
                  data-testid={`analyze-row-${i}`}
                >
                  <Table.Td
                    style={{
                      fontFamily: "monospace",
                      paddingInlineStart: `${0.4 + Math.min(row.depth, MAX_INDENT_DEPTH) * 0.9}rem`,
                    }}
                  >
                    {row.depth > MAX_INDENT_DEPTH && (
                      <span
                        data-testid="analyze-depth"
                        style={{ color: "var(--text-muted)", marginInlineEnd: "0.3rem" }}
                      >
                        ↳{row.depth}
                      </span>
                    )}
                    {row.node.op}
                  </Table.Td>
                  <Table.Td>{row.node.rows ?? t("sqlResultsPanel.dash")}</Table.Td>
                  <Table.Td>
                    {(plan.analyzed ? row.node.actual_ms : row.node.cost) ??
                      t("sqlResultsPanel.dash")}
                  </Table.Td>
                  <Table.Td style={{ color: "var(--text-muted)" }}>
                    <ListCell
                      text={Object.entries(row.node.detail)
                        .map(([k, v]) => `${k}=${v}`)
                        .join("  ")}
                    />
                  </Table.Td>
                </Table.Tr>
              ))}
            </Table.Tbody>
          </ListTable>
        </div>
      )}
      {rows.length > 0 && plan.mermaid && (
        <Tip text={t("sqlResultsPanel.analyzeResize")} named>
          <div
            role="separator"
            aria-orientation="vertical"
            aria-label={t("sqlResultsPanel.analyzeResize")}
            onMouseDown={startResize}
            data-testid="analyze-resizer"
            className="analyze-resizer"
          />
        </Tip>
      )}
      {plan.mermaid && (
        <div style={{ flex: "1 1 0", minWidth: 0, minHeight: 0 }}>
          <MermaidDiagram
            chart={plan.mermaid}
            height="100%"
            selected={picked === null ? null : nodeKey(picked)}
            onNodeClick={pickNode}
          />
        </div>
      )}
    </div>
  );
}
