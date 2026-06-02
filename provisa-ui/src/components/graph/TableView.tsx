// Copyright (c) 2026 Kenneth Stott
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

import { useRef, useState, useCallback } from "react";
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

  if (rows.length === 0) return <div className="gf-table-empty">No rows</div>;
  return (
    <div className="gf-table-outer" style={height !== undefined ? { height } : undefined}>
      <CsvCopyButton columns={columns} rows={rows} />
      <div className="gf-table-wrap">
        <table
          className="gf-table"
          style={{ tableLayout: "fixed", width: colWidths.reduce((a, b) => a + b, 0) + 40 }}
        >
          <colgroup>
            <col style={{ width: 40 }} />
            {colWidths.map((w, i) => (
              <col key={i} style={{ width: w }} />
            ))}
          </colgroup>
          <thead>
            <tr>
              <th className="gf-th-rownum" />
              {columns.map((c, i) => (
                <th key={c} style={{ position: "relative", width: colWidths[i] }}>
                  <span className="gf-th-label">{c}</span>
                  <span className="gf-col-resize" onMouseDown={(e) => onResizeStart(e, i)} />
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {rows.map((r, i) => (
              <tr key={i}>
                <td className="gf-td-rownum">{i + 1}</td>
                {columns.map((c) => (
                  <td key={c} className={wrap ? "gf-td-wrap" : ""}>
                    {cellText(r[c])}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function CsvCopyButton({ columns, rows }: { columns: string[]; rows: Record<string, unknown>[] }) {
  const [copied, setCopied] = useState(false);
  const copy = useCallback(() => {
    navigator.clipboard.writeText(toCSV(columns, rows)).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    });
  }, [columns, rows]);
  return (
    <button
      className={`gf-tbl-copy-btn${copied ? " copied" : ""}`}
      onClick={copy}
      title="Copy as CSV"
    >
      {copied ? "✓" : "⎘"}
    </button>
  );
}

export function JsonCopyButton({ text }: { text: string }) {
  const [copied, setCopied] = useState(false);
  const copy = useCallback(() => {
    navigator.clipboard.writeText(text).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    });
  }, [text]);
  return (
    <button
      className={`gf-json-copy-btn${copied ? " copied" : ""}`}
      onClick={copy}
      title="Copy JSON"
    >
      {copied ? "✓" : "⎘"}
    </button>
  );
}
