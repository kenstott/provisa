// Copyright (c) 2026 Kenneth Stott
// Canary: 8b8297fa-6195-4f5c-bfab-d1febbddc49d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/* eslint-disable react-hooks/refs --
   A latest-value ref mirrors colWidths so the drag handler reads current widths
   without re-binding listeners; writing it during render is the mirror pattern. */

import { useRef, useCallback } from "react";
import { useTranslation } from "react-i18next";
import { Table, Text } from "@mantine/core";
import { CopySymbolButton } from "../CopyButton";
import { toCSV } from "./graph-export";

function cellText(v: unknown): string {
  if (v === null || v === undefined) return "";
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}

export function TableView({
  columns,
  rows,
  wrap,
  height,
  colWidths,
  setColWidths,
}: {
  columns: string[];
  rows: Record<string, unknown>[];
  wrap?: boolean;
  height?: number;
  colWidths: number[];
  setColWidths: (w: number[]) => void;
}) {
  const { t } = useTranslation();
  const dragRef = useRef<{ colIdx: number; startX: number; startW: number } | null>(null);

  const colWidthsRef = useRef(colWidths);
  colWidthsRef.current = colWidths;

  const onResizeStart = useCallback(
    (e: React.MouseEvent, idx: number) => {
      e.preventDefault();
      dragRef.current = { colIdx: idx, startX: e.clientX, startW: colWidthsRef.current[idx] };
      const onMove = (me: MouseEvent) => {
        if (!dragRef.current) return;
        const delta = me.clientX - dragRef.current.startX;
        const newW = Math.max(40, dragRef.current.startW + delta);
        setColWidths(
          colWidthsRef.current.map((w, i) => (i === dragRef.current!.colIdx ? newW : w)),
        );
      };
      const onUp = () => {
        dragRef.current = null;
        window.removeEventListener("mousemove", onMove);
        window.removeEventListener("mouseup", onUp);
      };
      window.addEventListener("mousemove", onMove);
      window.addEventListener("mouseup", onUp);
    },
    [setColWidths],
  );

  if (rows.length === 0) {
    return (
      <Text className="gf-table-empty" data-testid="table-view-empty">
        {t("graphTableView.noRows")}
      </Text>
    );
  }
  return (
    <div className="gf-table-outer" style={height !== undefined ? { height } : undefined}>
      <CopySymbolButton
        text={toCSV(columns, rows)}
        className="gf-tbl-copy-btn"
        title={t("graphTableView.copyAsCsv")}
      />
      <div className="gf-table-wrap">
        <Table
          className="gf-table"
          aria-label={t("graphTableView.table")}
          data-testid="table-view-table"
          style={{ tableLayout: "fixed", width: colWidths.reduce((a, b) => a + b, 0) + 40 }}
        >
          <colgroup>
            <col style={{ width: 40 }} />
            {colWidths.map((w, i) => (
              <col key={i} style={{ width: w }} />
            ))}
          </colgroup>
          <Table.Thead>
            <Table.Tr>
              <Table.Th className="gf-th-rownum" />
              {columns.map((c, i) => (
                <Table.Th key={c} style={{ position: "relative", width: colWidths[i] }}>
                  <span className="gf-th-label">{c}</span>
                  <span
                    className="gf-col-resize"
                    role="separator"
                    aria-orientation="vertical"
                    aria-label={c}
                    onMouseDown={(e) => onResizeStart(e, i)}
                  />
                </Table.Th>
              ))}
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {rows.map((r, i) => (
              <Table.Tr key={i}>
                <Table.Td className="gf-td-rownum">{i + 1}</Table.Td>
                {columns.map((c) => (
                  <Table.Td key={c} className={wrap ? "gf-td-wrap" : ""}>
                    {cellText(r[c])}
                  </Table.Td>
                ))}
              </Table.Tr>
            ))}
          </Table.Tbody>
        </Table>
      </div>
    </div>
  );
}

export function JsonCopyButton({ text }: { text: string }) {
  const { t } = useTranslation();
  return (
    <CopySymbolButton
      text={text}
      className="gf-json-copy-btn"
      title={t("graphTableView.copyJson")}
    />
  );
}
