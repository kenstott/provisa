// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A table on a join canvas: the SQL Explorer's (pages/sql/JoinCanvas) and the SQL modeling
// modal's (components/sql-modeling/JoinCanvas). Its columns can be selected where the canvas
// selects them, with one row above them that checks or unchecks them all.

import React, { useRef } from "react";
import { X, Check, Minus } from "lucide-react";
import { ActionIcon, Text } from "@mantine/core";
import { useTranslation } from "react-i18next";
import { CARD_HEADER_H, CARD_W, COL_ROW_H, columnsTop } from "./geometry";

export interface ColumnSelection {
  selected: Set<string>;
  onToggle: (colName: string) => void;
  /** Every column selected (``cols`` all of them) or none (``cols`` empty). */
  onSetAll: (cols: string[]) => void;
}

export interface CanvasTableCardProps {
  ct: { tableName: string; x: number; y: number };
  tbl: { columns: { columnName: string; dataType?: string | null }[] };
  onMove: (x: number, y: number) => void;
  onRemove: () => void;
  onStartConnect: (colName: string) => void;
  /** Absent where the canvas selects no columns. */
  selection?: ColumnSelection;
}

function Box({ state }: { state: "on" | "off" | "some" }) {
  const filled = state !== "off";
  return (
    <span
      style={{
        display: "inline-flex",
        alignItems: "center",
        justifyContent: "center",
        width: 13,
        height: 13,
        borderRadius: 2,
        border: `1px solid ${filled ? "var(--primary)" : "var(--border)"}`,
        background: filled ? "var(--primary)" : "transparent",
        color: "#fff",
        flexShrink: 0,
        marginInlineEnd: 5,
      }}
    >
      {state === "on" && <Check size={9} strokeWidth={3} />}
      {state === "some" && <Minus size={9} strokeWidth={3} />}
    </span>
  );
}

const rowStyle: React.CSSProperties = {
  height: COL_ROW_H,
  display: "flex",
  alignItems: "center",
  borderTop: "1px solid var(--border)",
  position: "relative",
  fontSize: "0.72rem",
  fontFamily: "monospace",
  padding: "0 14px",
  color: "var(--text)",
};

function activate(e: React.KeyboardEvent, act: () => void) {
  if (e.key === "Enter" || e.key === " ") {
    e.preventDefault();
    act();
  }
}

export function CanvasTableCard({
  ct,
  tbl,
  onMove,
  onRemove,
  onStartConnect,
  selection,
}: CanvasTableCardProps) {
  const { t } = useTranslation();
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

  const names = tbl.columns.map((c) => c.columnName);
  const chosen = selection ? names.filter((n) => selection.selected.has(n)).length : 0;
  const all: "on" | "off" | "some" = chosen === 0 ? "off" : chosen === names.length ? "on" : "some";
  const toggleAll = () => selection?.onSetAll(all === "on" ? [] : names);
  const cardH = columnsTop(selection !== undefined) + tbl.columns.length * COL_ROW_H;

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
      {/* Header */}
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
        <Text
          span
          c="inherit"
          fz="0.78rem"
          fw={600}
          style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", flex: 1 }}
        >
          {ct.tableName}
        </Text>
        <ActionIcon
          type="button"
          variant="transparent"
          size="xs"
          c="rgba(255,255,255,0.7)"
          aria-label={t("sqlCanvasTableCard.removeTable", { tableName: ct.tableName })}
          data-testid="canvas-table-card-remove"
          onMouseDown={(e) => e.stopPropagation()}
          onClick={onRemove}
        >
          <X size={11} />
        </ActionIcon>
      </div>

      {/* All columns */}
      {selection && (
        <div
          data-col=""
          role="checkbox"
          aria-checked={all === "some" ? "mixed" : all === "on"}
          tabIndex={0}
          data-testid="canvas-table-card-all"
          onClick={toggleAll}
          onKeyDown={(e) => activate(e, toggleAll)}
          style={{ ...rowStyle, cursor: "pointer", background: "var(--surface-alt, transparent)" }}
        >
          <Box state={all} />
          <Text
            span
            c="var(--text-muted)"
            fz="0.72rem"
            style={{ flex: 1, fontFamily: "var(--mantine-font-family)" }}
          >
            {t("sqlCanvasTableCard.allColumns", { selected: chosen, total: names.length })}
          </Text>
        </div>
      )}

      {/* Columns */}
      {tbl.columns.map((col) => {
        const selected = selection?.selected.has(col.columnName) ?? false;
        const toggle = () => selection?.onToggle(col.columnName);
        return (
          <div
            key={col.columnName}
            data-table={ct.tableName}
            data-col={col.columnName}
            {...(selection
              ? {
                  role: "checkbox",
                  "aria-checked": selected,
                  tabIndex: 0,
                  "aria-label": t("sqlCanvasTableCard.toggleColumn", {
                    columnName: col.columnName,
                  }),
                  onClick: toggle,
                  onKeyDown: (e: React.KeyboardEvent) => activate(e, toggle),
                }
              : {})}
            data-testid={`canvas-table-card-col-${col.columnName}`}
            style={{ ...rowStyle, cursor: selection ? "pointer" : undefined }}
          >
            {/* Left dot (visual only) */}
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
            {selection && <Box state={selected ? "on" : "off"} />}
            <Text
              span
              c="var(--text)"
              fz="0.72rem"
              ff="monospace"
              style={{
                overflow: "hidden",
                textOverflow: "ellipsis",
                whiteSpace: "nowrap",
                flex: 1,
              }}
            >
              {col.columnName}
            </Text>
            {col.dataType && (
              <Text
                span
                c="var(--text-muted)"
                fz="0.6rem"
                ff="monospace"
                style={{ opacity: 0.5, marginInlineStart: 4 }}
              >
                {col.dataType}
              </Text>
            )}
            {/* Right dot (connect handle) */}
            <button
              type="button"
              aria-label={t("sqlCanvasTableCard.connectColumn", { columnName: col.columnName })}
              data-testid={`canvas-table-card-connect-${col.columnName}`}
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
                padding: 0,
                borderRadius: "50%",
                background: "var(--primary)",
                border: "1px solid var(--primary)",
                cursor: "crosshair",
                zIndex: 20,
              }}
            />
          </div>
        );
      })}
    </div>
  );
}
