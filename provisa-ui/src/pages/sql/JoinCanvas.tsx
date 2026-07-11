// Copyright (c) 2026 Kenneth Stott
// Canary: e203b774-09b9-4f3a-a172-efc74bdcf20b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import React, { useState, useCallback, useMemo, useRef } from "react";
import { Network } from "lucide-react";
import { CanvasTableCard } from "./CanvasTableCard";
import { autoAliasConflicts } from "./sqlHelpers";
import { CARD_W, CARD_HEADER_H, COL_ROW_H } from "./types";
import type { CanvasTable, CanvasJoin, JoinCanvasProps } from "./types";
import type { RegisteredTable } from "../../types/admin";

export function JoinCanvas({ tables, existingRels, onGenerateSql }: JoinCanvasProps) {
  const [canvasTables, setCanvasTables] = useState<CanvasTable[]>([]);
  const [canvasJoins, setCanvasJoins] = useState<CanvasJoin[]>([]);
  const [selectedColumns, setSelectedColumns] = useState<Map<string, Set<string>>>(new Map());
  const [connectingMouse, setConnectingMouse] = useState<{ x: number; y: number } | null>(null);
  const connectingRef = useRef<{ tableName: string; colName: string; colIdx: number } | null>(null);
  const canvasRef = useRef<HTMLDivElement>(null);

  const tableMap = useMemo(() => {
    const m: Record<string, RegisteredTable> = {};
    for (const t of tables) m[t.tableName] = t;
    return m;
  }, [tables]);

  const handleDrop = (e: React.DragEvent) => {
    e.preventDefault();
    const tableName = e.dataTransfer.getData("tableName");
    if (!tableName) return;
    if (canvasTables.some((ct) => ct.tableName === tableName)) return;
    const rect = canvasRef.current?.getBoundingClientRect();
    if (!rect) return;
    const x = e.clientX - rect.left - CARD_W / 2;
    const y = e.clientY - rect.top - CARD_HEADER_H / 2;
    setCanvasTables((prev) => {
      const next = [...prev, { tableName, x: Math.max(0, x), y: Math.max(0, y) }];
      // Auto-wire PK/FK joins between the new table and tables already on the canvas
      const existingNames = new Set(prev.map((ct) => ct.tableName));
      const newJoins: CanvasJoin[] = [];
      for (const rel of existingRels) {
        const isNew = rel.sourceTableName === tableName || rel.targetTableName === tableName;
        const otherTable =
          rel.sourceTableName === tableName ? rel.targetTableName : rel.sourceTableName;
        if (!isNew || !existingNames.has(otherTable) || !rel.targetColumn) continue;
        // Always orient FK→PK: the FK column (many side) references the PK column (one side).
        // one-to-many: source=parent(PK), target=child(FK) → fromTable=target, fromCol=targetColumn
        // many-to-one: source=child(FK), target=parent(PK) → fromTable=source, fromCol=sourceColumn
        const isFkOnTarget = rel.cardinality === "one-to-many";
        const fkTable = isFkOnTarget ? rel.targetTableName : rel.sourceTableName;
        const fkCol = isFkOnTarget ? rel.targetColumn : rel.sourceColumn;
        const pkTable = isFkOnTarget ? rel.sourceTableName : rel.targetTableName;
        const pkCol = isFkOnTarget ? rel.sourceColumn : rel.targetColumn;
        // Canonical join ID normalizes direction so forward+reverse config entries dedup correctly.
        const joinId = `${fkTable}__${fkCol}__${pkTable}__${pkCol}`;
        newJoins.push({
          id: joinId,
          fromTable: fkTable,
          fromCol: fkCol,
          toTable: pkTable,
          toCol: pkCol,
          cardinality: "many-to-one",
        });
      }
      if (newJoins.length > 0) {
        setCanvasJoins((prevJ) => {
          const existingIds = new Set(prevJ.map((j) => j.id));
          return [...prevJ, ...newJoins.filter((j) => !existingIds.has(j.id))];
        });
      }
      return next;
    });
  };

  const handleDragOver = (e: React.DragEvent) => {
    e.preventDefault();
  };

  const handleMoveCard = useCallback((tableName: string, x: number, y: number) => {
    setCanvasTables((prev) =>
      prev.map((ct) => (ct.tableName === tableName ? { ...ct, x, y } : ct)),
    );
  }, []);

  const handleRemoveCard = useCallback((tableName: string) => {
    setCanvasTables((prev) => prev.filter((ct) => ct.tableName !== tableName));
    setCanvasJoins((prev) =>
      prev.filter((j) => j.fromTable !== tableName && j.toTable !== tableName),
    );
    setSelectedColumns((prev) => {
      const next = new Map(prev);
      next.delete(tableName);
      return next;
    });
  }, []);

  const handleStartConnect = useCallback(
    (tableName: string, colName: string) => {
      const ct = canvasTables.find((c) => c.tableName === tableName);
      if (!ct) return;
      const tbl = tableMap[tableName];
      if (!tbl) return;
      const colIdx = tbl.columns.findIndex((c) => c.columnName === colName);
      if (colIdx === -1) return;
      connectingRef.current = { tableName, colName, colIdx };

      const onMouseMove = (ev: MouseEvent) => {
        const rect = canvasRef.current?.getBoundingClientRect();
        if (!rect) return;
        setConnectingMouse({ x: ev.clientX - rect.left, y: ev.clientY - rect.top });
      };

      const onMouseUp = (ev: MouseEvent) => {
        document.removeEventListener("mousemove", onMouseMove);
        document.removeEventListener("mouseup", onMouseUp);

        const target = (ev.target as HTMLElement).closest("[data-col]") as HTMLElement | null;
        if (target && connectingRef.current) {
          const toTable = target.dataset.table;
          const toCol = target.dataset.col;
          const from = connectingRef.current;
          if (toTable && toCol && (toTable !== from.tableName || toCol !== from.colName)) {
            const id = `${from.tableName}-${from.colName}-to-${toTable}-${toCol}`;
            setCanvasJoins((prev) => {
              if (prev.some((j) => j.id === id)) return prev;
              return [
                ...prev,
                {
                  id,
                  fromTable: from.tableName,
                  fromCol: from.colName,
                  toTable,
                  toCol,
                  cardinality: "many-to-one",
                },
              ];
            });
          }
        }
        connectingRef.current = null;
        setConnectingMouse(null);
      };

      document.addEventListener("mousemove", onMouseMove);
      document.addEventListener("mouseup", onMouseUp);
    },
    [canvasTables, tableMap],
  );

  const handleGenerateSql = () => {
    if (canvasTables.length === 0) return;
    const aliasOf = (name: string) => name.replace(/\W/g, "_").toLowerCase();
    const normDomain = (id: string) => id.replace(/[^a-zA-Z0-9]/g, "_").replace(/^_+|_+$/g, "");
    const schemaOf = (tbl: RegisteredTable | undefined) =>
      tbl?.domainId ? normDomain(tbl.domainId) : (tbl?.schemaName ?? "public");
    const tbl0 = canvasTables[0];
    const tblObj0 = tableMap[tbl0.tableName];
    // Build SELECT clause: use checked columns if any, otherwise SELECT *.
    // When the same column name appears in multiple checked tables, alias as table_column.
    const checkedEntries = canvasTables.flatMap((ct) =>
      [...(selectedColumns.get(ct.tableName) ?? [])].map((col) => ({ tbl: ct.tableName, col })),
    );
    let selectClause: string;
    if (checkedEntries.length > 0) {
      const colFreq = new Map<string, number>();
      checkedEntries.forEach(({ col }) => colFreq.set(col, (colFreq.get(col) ?? 0) + 1));
      selectClause = checkedEntries
        .map(({ tbl, col }) =>
          colFreq.get(col)! > 1
            ? `${aliasOf(tbl)}."${col}" ${tbl}_${col}`
            : `${aliasOf(tbl)}."${col}"`,
        )
        .join(", ");
    } else {
      selectClause = "*";
    }
    let s = `SELECT ${selectClause}\nFROM "${schemaOf(tblObj0)}"."${tbl0.tableName}" ${aliasOf(tbl0.tableName)}`;
    const inQuery = new Set([tbl0.tableName]);
    // Track emitted ON/AND conditions as canonical "tA.cA=tB.cB" keys (both directions) to skip duplicates.
    const emittedConds = new Set<string>();
    const condKey = (t1: string, c1: string, t2: string, c2: string) =>
      [t1, c1, t2, c2].join("\x00") < [t2, c2, t1, c1].join("\x00")
        ? `${t1}\x00${c1}\x00${t2}\x00${c2}`
        : `${t2}\x00${c2}\x00${t1}\x00${c1}`;
    for (const join of canvasJoins) {
      const toInQuery = inQuery.has(join.toTable);
      const fromInQuery = inQuery.has(join.fromTable);
      let newTable: string, newCol: string, existingTable: string, existingCol: string;
      if (!toInQuery) {
        newTable = join.toTable;
        newCol = join.toCol;
        existingTable = join.fromTable;
        existingCol = join.fromCol;
      } else if (!fromInQuery) {
        newTable = join.fromTable;
        newCol = join.fromCol;
        existingTable = join.toTable;
        existingCol = join.toCol;
      } else {
        // Both tables already in query — emit AND only if condition is genuinely new.
        const key = condKey(join.fromTable, join.fromCol, join.toTable, join.toCol);
        if (!emittedConds.has(key)) {
          s += `\n  AND ${aliasOf(join.fromTable)}."${join.fromCol}" = ${aliasOf(join.toTable)}."${join.toCol}"`;
          emittedConds.add(key);
        }
        continue;
      }
      const key = condKey(existingTable, existingCol, newTable, newCol);
      if (emittedConds.has(key)) continue; // duplicate join entry — skip
      const newTbl = tableMap[newTable];
      s += `\nJOIN "${schemaOf(newTbl)}"."${newTable}" ${aliasOf(newTable)} ON ${aliasOf(existingTable)}."${existingCol}" = ${aliasOf(newTable)}."${newCol}"`;
      emittedConds.add(key);
      inQuery.add(newTable);
    }
    onGenerateSql(autoAliasConflicts(s));
  };

  const handleClear = () => {
    setCanvasTables([]);
    setCanvasJoins([]);
    setConnectingMouse(null);
    connectingRef.current = null;
  };

  const fromPort = (ct: CanvasTable, colIdx: number) => ({
    x: ct.x + CARD_W,
    y: ct.y + CARD_HEADER_H + colIdx * COL_ROW_H + COL_ROW_H / 2,
  });
  const toPort = (ct: CanvasTable, colIdx: number) => ({
    x: ct.x,
    y: ct.y + CARD_HEADER_H + colIdx * COL_ROW_H + COL_ROW_H / 2,
  });

  const bezierPath = (from: { x: number; y: number }, to: { x: number; y: number }) => {
    const dx = Math.max(40, Math.abs(to.x - from.x) * 0.5);
    return `M ${from.x},${from.y} C ${from.x + dx},${from.y} ${to.x - dx},${to.y} ${to.x},${to.y}`;
  };

  const bezierMid = (from: { x: number; y: number }, to: { x: number; y: number }) => {
    const dx = Math.max(40, Math.abs(to.x - from.x) * 0.5);
    const cp1x = from.x + dx,
      cp1y = from.y;
    const cp2x = to.x - dx,
      cp2y = to.y;
    const t = 0.5;
    const x =
      Math.pow(1 - t, 3) * from.x +
      3 * Math.pow(1 - t, 2) * t * cp1x +
      3 * (1 - t) * Math.pow(t, 2) * cp2x +
      Math.pow(t, 3) * to.x;
    const y =
      Math.pow(1 - t, 3) * from.y +
      3 * Math.pow(1 - t, 2) * t * cp1y +
      3 * (1 - t) * Math.pow(t, 2) * cp2y +
      Math.pow(t, 3) * to.y;
    return { x, y };
  };

  return (
    <div style={{ flex: 1, display: "flex", flexDirection: "column", overflow: "hidden" }}>
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: "0.5rem",
          padding: "0.4rem 0.75rem",
          borderBottom: "1px solid var(--border)",
          flexShrink: 0,
          background: "var(--surface)",
        }}
      >
        <button
          className="btn-primary"
          style={{ fontSize: "0.78rem", padding: "0.25rem 0.6rem" }}
          onClick={handleGenerateSql}
          disabled={canvasTables.length === 0}
        >
          → SQL
        </button>
        <button
          className="btn-secondary"
          style={{ fontSize: "0.78rem", padding: "0.25rem 0.6rem" }}
          onClick={handleClear}
          disabled={canvasTables.length === 0}
        >
          Clear
        </button>
        <span style={{ fontSize: "0.72rem", color: "var(--text-muted)", marginLeft: "0.5rem" }}>
          {canvasTables.length > 0
            ? `${canvasTables.length} table${canvasTables.length !== 1 ? "s" : ""}, ${canvasJoins.length} join${canvasJoins.length !== 1 ? "s" : ""}`
            : ""}
        </span>
      </div>

      <div
        ref={canvasRef}
        onDrop={handleDrop}
        onDragOver={handleDragOver}
        style={{
          flex: 1,
          position: "relative",
          overflow: "hidden",
          background: "var(--bg)",
        }}
      >
        <div
          style={{
            position: "absolute",
            inset: 0,
            backgroundImage: "radial-gradient(circle, rgba(100,100,100,0.25) 1px, transparent 1px)",
            backgroundSize: "22px 22px",
            pointerEvents: "none",
          }}
        />

        {canvasTables.length === 0 && (
          <div
            style={{
              position: "absolute",
              inset: 0,
              display: "flex",
              flexDirection: "column",
              alignItems: "center",
              justifyContent: "center",
              gap: "0.75rem",
              color: "var(--text-muted)",
              pointerEvents: "none",
            }}
          >
            <Network size={40} style={{ opacity: 0.3 }} />
            <span style={{ fontSize: "0.85rem", opacity: 0.6 }}>
              Drag tables from the sidebar onto this canvas
            </span>
          </div>
        )}

        <svg
          style={{
            position: "absolute",
            inset: 0,
            width: "100%",
            height: "100%",
            pointerEvents: "none",
            overflow: "visible",
            zIndex: 5,
          }}
        >
          {canvasJoins.map((join) => {
            const fromCt = canvasTables.find((c) => c.tableName === join.fromTable);
            const toCt = canvasTables.find((c) => c.tableName === join.toTable);
            if (!fromCt || !toCt) return null;
            const fromTbl = tableMap[join.fromTable];
            const toTbl = tableMap[join.toTable];
            if (!fromTbl || !toTbl) return null;
            const fromColIdx = fromTbl.columns.findIndex((c) => c.columnName === join.fromCol);
            const toColIdx = toTbl.columns.findIndex((c) => c.columnName === join.toCol);
            if (fromColIdx === -1 || toColIdx === -1) return null;
            const fp = fromPort(fromCt, fromColIdx);
            const tp = toPort(toCt, toColIdx);
            return (
              <path
                key={join.id}
                d={bezierPath(fp, tp)}
                fill="none"
                stroke="var(--primary)"
                strokeWidth={2}
                opacity={0.7}
              />
            );
          })}

          {/* eslint-disable react-hooks/refs --
              transient connecting-line overlay reads connectingRef.current during
              render; the read is paired with connectingMouse state, which is updated
              on every mousemove and drives this re-render, so the ref value is current */}
          {connectingMouse &&
            connectingRef.current &&
            (() => {
              const fromCt = canvasTables.find(
                (c) => c.tableName === connectingRef.current!.tableName,
              );
              if (!fromCt) return null;
              const fp = fromPort(fromCt, connectingRef.current.colIdx);
              const tp = connectingMouse;
              const dx = Math.max(40, Math.abs(tp.x - fp.x) * 0.5);
              return (
                <path
                  d={`M ${fp.x},${fp.y} C ${fp.x + dx},${fp.y} ${tp.x - dx},${tp.y} ${tp.x},${tp.y}`}
                  fill="none"
                  stroke="var(--primary)"
                  strokeWidth={1.5}
                  strokeDasharray="5,4"
                  opacity={0.6}
                />
              );
            })()}
          {/* eslint-enable react-hooks/refs */}
        </svg>

        {canvasJoins.map((join) => {
          const fromCt = canvasTables.find((c) => c.tableName === join.fromTable);
          const toCt = canvasTables.find((c) => c.tableName === join.toTable);
          if (!fromCt || !toCt) return null;
          const fromTbl = tableMap[join.fromTable];
          const toTbl = tableMap[join.toTable];
          if (!fromTbl || !toTbl) return null;
          const fromColIdx = fromTbl.columns.findIndex((c) => c.columnName === join.fromCol);
          const toColIdx = toTbl.columns.findIndex((c) => c.columnName === join.toCol);
          if (fromColIdx === -1 || toColIdx === -1) return null;
          const fp = fromPort(fromCt, fromColIdx);
          const tp = toPort(toCt, toColIdx);
          const mid = bezierMid(fp, tp);
          return (
            <div
              key={`label-${join.id}`}
              style={{
                position: "absolute",
                left: mid.x,
                top: mid.y,
                transform: "translate(-50%, -50%)",
                zIndex: 20,
                display: "flex",
                alignItems: "center",
                gap: "0.25rem",
                background: "var(--surface)",
                border: "1px solid var(--border)",
                borderRadius: "12px",
                padding: "2px 6px",
                fontSize: "0.68rem",
                boxShadow: "0 1px 4px rgba(0,0,0,0.15)",
              }}
            >
              <select
                value={join.cardinality}
                onChange={(e) =>
                  setCanvasJoins((prev) =>
                    prev.map((j) =>
                      j.id === join.id
                        ? { ...j, cardinality: e.target.value as "many-to-one" | "one-to-many" }
                        : j,
                    ),
                  )
                }
                style={{
                  fontSize: "0.68rem",
                  background: "none",
                  border: "none",
                  color: "var(--text)",
                  cursor: "pointer",
                  padding: 0,
                }}
              >
                <option value="many-to-one">N:1</option>
                <option value="one-to-many">1:N</option>
              </select>
              <button
                onClick={() => setCanvasJoins((prev) => prev.filter((j) => j.id !== join.id))}
                style={{
                  background: "none",
                  border: "none",
                  color: "var(--text-muted)",
                  cursor: "pointer",
                  padding: 0,
                  lineHeight: 1,
                  fontSize: "0.7rem",
                }}
              >
                ✕
              </button>
            </div>
          );
        })}

        {canvasTables.map((ct) => {
          const tbl = tableMap[ct.tableName];
          if (!tbl) return null;
          return (
            <CanvasTableCard
              key={ct.tableName}
              ct={ct}
              tbl={tbl}
              onMove={(x, y) => handleMoveCard(ct.tableName, x, y)}
              onRemove={() => handleRemoveCard(ct.tableName)}
              onStartConnect={(colName) => handleStartConnect(ct.tableName, colName)}
              selectedCols={selectedColumns.get(ct.tableName) ?? new Set()}
              onToggleCol={(colName) =>
                setSelectedColumns((prev) => {
                  const next = new Map(prev);
                  const cols = new Set(next.get(ct.tableName) ?? []);
                  if (cols.has(colName)) cols.delete(colName);
                  else cols.add(colName);
                  next.set(ct.tableName, cols);
                  return next;
                })
              }
            />
          );
        })}
      </div>
    </div>
  );
}
