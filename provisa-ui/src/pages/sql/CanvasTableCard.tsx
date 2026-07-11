// Copyright (c) 2026 Kenneth Stott
// Canary: e203b774-09b9-4f3a-a172-efc74bdcf20b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import React, { useRef } from "react";
import { CARD_W, CARD_HEADER_H, COL_ROW_H } from "./types";
import type { CanvasTableCardProps } from "./types";

export function CanvasTableCard({
  ct,
  tbl,
  onMove,
  onRemove,
  onStartConnect,
  selectedCols,
  onToggleCol,
}: CanvasTableCardProps) {
  const dragRef = useRef<{
    startMouseX: number;
    startMouseY: number;
    startCardX: number;
    startCardY: number;
  } | null>(null);

  const handleHeaderMouseDown = (e: React.MouseEvent) => {
    if ((e.target as HTMLElement).closest("[data-col]")) return;
    e.preventDefault();
    dragRef.current = {
      startMouseX: e.clientX,
      startMouseY: e.clientY,
      startCardX: ct.x,
      startCardY: ct.y,
    };
    const onMove_ = (ev: MouseEvent) => {
      if (!dragRef.current) return;
      const dx = ev.clientX - dragRef.current.startMouseX;
      const dy = ev.clientY - dragRef.current.startMouseY;
      onMove(dragRef.current.startCardX + dx, dragRef.current.startCardY + dy);
    };
    const onUp = () => {
      dragRef.current = null;
      document.removeEventListener("mousemove", onMove_);
      document.removeEventListener("mouseup", onUp);
    };
    document.addEventListener("mousemove", onMove_);
    document.addEventListener("mouseup", onUp);
  };

  const cardH = CARD_HEADER_H + tbl.columns.length * COL_ROW_H;

  return (
    <div
      style={{
        position: "absolute",
        left: ct.x,
        top: ct.y,
        width: CARD_W,
        height: cardH,
        background: "var(--surface)",
        border: "1px solid var(--border)",
        borderRadius: "6px",
        boxShadow: "0 2px 8px rgba(0,0,0,0.18)",
        zIndex: 10,
        userSelect: "none",
      }}
    >
      <div
        onMouseDown={handleHeaderMouseDown}
        style={{
          height: CARD_HEADER_H,
          background: "var(--primary)",
          color: "#fff",
          display: "flex",
          alignItems: "center",
          justifyContent: "space-between",
          padding: "0 8px",
          borderRadius: "5px 5px 0 0",
          cursor: "grab",
          fontSize: "0.78rem",
          fontWeight: 600,
        }}
      >
        <span
          style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", flex: 1 }}
        >
          {ct.tableName}
        </span>
        <button
          onMouseDown={(e) => e.stopPropagation()}
          onClick={onRemove}
          style={{
            background: "none",
            border: "none",
            color: "rgba(255,255,255,0.7)",
            cursor: "pointer",
            padding: "0 0 0 4px",
            lineHeight: 1,
            fontSize: "0.75rem",
          }}
        >
          ✕
        </button>
      </div>

      {tbl.columns.map((col) => (
        <div
          key={col.columnName}
          data-table={ct.tableName}
          data-col={col.columnName}
          onClick={() => onToggleCol(col.columnName)}
          style={{
            height: COL_ROW_H,
            display: "flex",
            alignItems: "center",
            borderTop: "1px solid var(--border)",
            position: "relative",
            fontSize: "0.72rem",
            fontFamily: "monospace",
            padding: "0 14px",
            color: "var(--text)",
            cursor: "pointer",
          }}
        >
          <div
            style={{
              position: "absolute",
              left: -5,
              top: "50%",
              transform: "translateY(-50%)",
              width: 9,
              height: 9,
              borderRadius: "50%",
              background: "var(--border)",
              border: "1px solid var(--text-muted)",
              pointerEvents: "none",
            }}
          />
          <span
            style={{
              display: "inline-flex",
              alignItems: "center",
              justifyContent: "center",
              width: 13,
              height: 13,
              borderRadius: 2,
              border: `1px solid ${selectedCols.has(col.columnName) ? "var(--primary)" : "var(--border)"}`,
              background: selectedCols.has(col.columnName) ? "var(--primary)" : "transparent",
              color: "#fff",
              fontSize: "0.6rem",
              flexShrink: 0,
              marginRight: 5,
            }}
          >
            {selectedCols.has(col.columnName) ? "✓" : ""}
          </span>
          <span
            style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", flex: 1 }}
          >
            {col.columnName}
          </span>
          {col.dataType && (
            <span
              style={{
                fontSize: "0.6rem",
                color: "var(--text-muted)",
                opacity: 0.5,
                marginLeft: 4,
              }}
            >
              {col.dataType}
            </span>
          )}
          <div
            onMouseDown={(e) => {
              e.preventDefault();
              e.stopPropagation();
              onStartConnect(col.columnName);
            }}
            style={{
              position: "absolute",
              right: -5,
              top: "50%",
              transform: "translateY(-50%)",
              width: 9,
              height: 9,
              borderRadius: "50%",
              background: "var(--primary)",
              border: "1px solid var(--primary)",
              cursor: "crosshair",
              zIndex: 20,
            }}
          />
        </div>
      ))}
    </div>
  );
}
