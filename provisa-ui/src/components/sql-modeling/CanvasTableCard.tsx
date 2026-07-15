// Copyright (c) 2026 Kenneth Stott
// Canary: 533b4a73-f17a-4246-b87c-6d963fc99ff0
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import React, { useRef } from "react";
import { X } from "lucide-react";
import { ActionIcon, Text } from "@mantine/core";
import { useTranslation } from "react-i18next";
import type { CanvasTableCardProps } from "./types";
import { CARD_W, CARD_HEADER_H, COL_ROW_H } from "./types";

export function CanvasTableCard({ ct, tbl, onMove, onRemove, onStartConnect }: CanvasTableCardProps) {
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
          aria-label={t("sqlModelingCanvasTableCard.removeTable", { tableName: ct.tableName })}
          data-testid="canvas-table-card-remove"
          onMouseDown={(e) => e.stopPropagation()}
          onClick={onRemove}
        >
          <X size={11} />
        </ActionIcon>
      </div>

      {/* Columns */}
      {tbl.columns.map((col) => (
        <div
          key={col.columnName}
          data-table={ct.tableName}
          data-col={col.columnName}
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
          }}
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
          <Text
            span
            c="var(--text)"
            fz="0.72rem"
            ff="monospace"
            style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", flex: 1 }}
          >
            {col.columnName}
          </Text>
          {col.dataType && (
            <Text
              span
              c="var(--text-muted)"
              fz="0.6rem"
              ff="monospace"
              style={{ opacity: 0.5, marginLeft: 4 }}
            >
              {col.dataType}
            </Text>
          )}
          {/* Right dot (connect handle) */}
          <button
            type="button"
            aria-label={t("sqlModelingCanvasTableCard.connectColumn", { columnName: col.columnName })}
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
      ))}
    </div>
  );
}
