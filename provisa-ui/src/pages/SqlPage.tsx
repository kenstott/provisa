// Copyright (c) 2026 Kenneth Stott
// Canary: PLACEHOLDER
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import React, { useState, useCallback, useMemo, useRef, useEffect, Fragment } from "react";
import { useLocation } from "react-router-dom";
import { get as idbGet, set as idbSet } from "idb-keyval";
import { Play, ChevronRight, ChevronDown, Table2, Columns3, History, Copy, Check, BarChart2, Network, X } from "lucide-react";
import CodeMirror from "@uiw/react-codemirror";
import { sql, PostgreSQL } from "@codemirror/lang-sql";
import { oneDark } from "@codemirror/theme-one-dark";
import { EditorView } from "@codemirror/view";
import { useDomainFilter } from "../context/DomainFilterContext";
import { runSql, fetchRoles, fetchDomains, fetchTables, fetchRelationships, registerTable, nlToSql, updateTable } from "../api/admin";
import type { Domain, Relationship, RegisteredTable } from "../types/admin";
import { useCapability } from "../hooks/useCapability";
import { MultiSelect } from "../components/MultiSelect";

type ResultTab = "results" | "profile" | "errors" | "history";
type TopTab = "sql" | "canvas";

interface HistoryEntry {
  sql: string;
  role: string;
  executedAt: number;
  durationMs: number;
  rowCount: number;
  error: string;
}

// Canvas types
interface CanvasTable { tableName: string; x: number; y: number; }
interface CanvasJoin { id: string; fromTable: string; fromCol: string; toTable: string; toCol: string; cardinality: "many-to-one" | "one-to-many"; }
interface JoinCanvasProps { tables: RegisteredTable[]; existingRels: Relationship[]; onGenerateSql: (sql: string) => void; }
interface CanvasTableCardProps { ct: CanvasTable; tbl: RegisteredTable; onMove: (x: number, y: number) => void; onRemove: () => void; onStartConnect: (colName: string) => void; selectedCols: Set<string>; onToggleCol: (colName: string) => void; }

const CARD_W = 200;
const CARD_HEADER_H = 34;
const COL_ROW_H = 27;

const HISTORY_KEY = "sql_modeling_history";
const HISTORY_MAX = 50;
const SQL_QUERY_KEY = "provisa.sql.query";
const SQL_RESULTS_KEY = "provisa.sql.results";

interface SqlResults {
  columns: string[];
  rows: Record<string, unknown>[];
  error: string;
}

interface ViewColumnConfig {
  name: string;
  alias: string;
  description: string;
  scope: "domain" | "public" | "restricted";
  visibleTo: string[];
  maskType: "" | "regex" | "constant" | "truncate";
  maskPattern: string;
  maskReplace: string;
  maskValue: string;
  maskPrecision: string;
  unmaskedTo: string;
}

function loadHistory(): HistoryEntry[] {
  try {
    return JSON.parse(localStorage.getItem(HISTORY_KEY) ?? "[]");
  } catch {
    return [];
  }
}

function saveHistory(entries: HistoryEntry[]) {
  localStorage.setItem(HISTORY_KEY, JSON.stringify(entries.slice(0, HISTORY_MAX)));
}

/** Add `table_col` aliases to any SELECT column whose bare name conflicts with another. */
function autoAliasConflicts(sql: string): string {
  // Isolate SELECT item list (between SELECT and the first FROM not inside parens)
  const selectRe = /\bSELECT\s+([\s\S]+?)\s+FROM\b/i;
  const m = sql.match(selectRe);
  if (!m) return sql;
  const colList = m[1];

  // Split items by top-level commas
  const items: string[] = [];
  let depth = 0, cur = "";
  for (const ch of colList) {
    if (ch === "(") depth++;
    else if (ch === ")") depth--;
    else if (ch === "," && depth === 0) { items.push(cur.trim()); cur = ""; continue; }
    cur += ch;
  }
  if (cur.trim()) items.push(cur.trim());

  // Extract bare column name from patterns like: tbl."col", tbl.col, "tbl"."col", "tbl".col
  // Ignore items that already have an alias (contain AS or a second word after the ref).
  const colRef = /^"?(\w+)"?\."?(\w+)"?\s*$/i;
  const parsed = items.map((item) => {
    const alreadyAliased = /\bas\s+\w+/i.test(item) || /^"?\w+"?\."?\w+"?\s+\w+\s*$/i.test(item);
    const match = item.trim().match(colRef);
    if (!match || alreadyAliased) return { item, colLower: null as string | null, tableAlias: null as string | null };
    return { item, colLower: match[2].toLowerCase(), tableAlias: match[1] };
  });

  // Count each bare column name
  const freq = new Map<string, number>();
  parsed.forEach(({ colLower }) => { if (colLower) freq.set(colLower, (freq.get(colLower) ?? 0) + 1); });

  // Rebuild, appending alias where needed
  const newItems = parsed.map(({ item, colLower, tableAlias }) => {
    if (!colLower || !tableAlias || (freq.get(colLower) ?? 0) <= 1) return item;
    const origCol = item.trim().match(/"?(\w+)"?\s*$/i)?.[1] ?? colLower;
    return `${item} ${tableAlias}_${origCol}`;
  });

  return sql.replace(selectRe, `SELECT ${newItems.join(", ")} FROM`);
}

function loadSqlQuery(): string {
  return localStorage.getItem(SQL_QUERY_KEY) ?? "";
}

function saveSqlQuery(text: string) {
  try { localStorage.setItem(SQL_QUERY_KEY, text); } catch { /* quota */ }
}

async function saveSqlResults(results: SqlResults) {
  await idbSet(SQL_RESULTS_KEY, results);
}

// ── CanvasTableCard ──────────────────────────────────────────────────────────

function CanvasTableCard({ ct, tbl, onMove, onRemove, onStartConnect, selectedCols, onToggleCol }: CanvasTableCardProps) {
  const dragRef = useRef<{ startMouseX: number; startMouseY: number; startCardX: number; startCardY: number } | null>(null);

  const handleHeaderMouseDown = (e: React.MouseEvent) => {
    if ((e.target as HTMLElement).closest("[data-col]")) return;
    e.preventDefault();
    dragRef.current = { startMouseX: e.clientX, startMouseY: e.clientY, startCardX: ct.x, startCardY: ct.y };
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
        <span style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", flex: 1 }}>{ct.tableName}</span>
        <button
          onMouseDown={(e) => e.stopPropagation()}
          onClick={onRemove}
          style={{ background: "none", border: "none", color: "rgba(255,255,255,0.7)", cursor: "pointer", padding: "0 0 0 4px", lineHeight: 1, fontSize: "0.75rem" }}
        >✕</button>
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
          >{selectedCols.has(col.columnName) ? "✓" : ""}</span>
          <span style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", flex: 1 }}>{col.columnName}</span>
          {col.dataType && (
            <span style={{ fontSize: "0.6rem", color: "var(--text-muted)", opacity: 0.5, marginLeft: 4 }}>{col.dataType}</span>
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

// ── JoinCanvas ───────────────────────────────────────────────────────────────

function JoinCanvas({ tables, existingRels, onGenerateSql }: JoinCanvasProps) {
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
        const otherTable = rel.sourceTableName === tableName ? rel.targetTableName : rel.sourceTableName;
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
        newJoins.push({ id: joinId, fromTable: fkTable, fromCol: fkCol, toTable: pkTable, toCol: pkCol, cardinality: "many-to-one" });
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

  const handleDragOver = (e: React.DragEvent) => { e.preventDefault(); };

  const handleMoveCard = useCallback((tableName: string, x: number, y: number) => {
    setCanvasTables((prev) => prev.map((ct) => ct.tableName === tableName ? { ...ct, x, y } : ct));
  }, []);

  const handleRemoveCard = useCallback((tableName: string) => {
    setCanvasTables((prev) => prev.filter((ct) => ct.tableName !== tableName));
    setCanvasJoins((prev) => prev.filter((j) => j.fromTable !== tableName && j.toTable !== tableName));
    setSelectedColumns((prev) => { const next = new Map(prev); next.delete(tableName); return next; });
  }, []);

  const handleStartConnect = useCallback((tableName: string, colName: string) => {
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
            return [...prev, { id, fromTable: from.tableName, fromCol: from.colName, toTable, toCol, cardinality: "many-to-one" }];
          });
        }
      }
      connectingRef.current = null;
      setConnectingMouse(null);
    };

    document.addEventListener("mousemove", onMouseMove);
    document.addEventListener("mouseup", onMouseUp);
  }, [canvasTables, tableMap]);

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
      [...(selectedColumns.get(ct.tableName) ?? [])].map((col) => ({ tbl: ct.tableName, col }))
    );
    let selectClause: string;
    if (checkedEntries.length > 0) {
      const colFreq = new Map<string, number>();
      checkedEntries.forEach(({ col }) => colFreq.set(col, (colFreq.get(col) ?? 0) + 1));
      selectClause = checkedEntries
        .map(({ tbl, col }) => colFreq.get(col)! > 1 ? `${aliasOf(tbl)}."${col}" ${tbl}_${col}` : `${aliasOf(tbl)}."${col}"`)
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
        newTable = join.toTable; newCol = join.toCol;
        existingTable = join.fromTable; existingCol = join.fromCol;
      } else if (!fromInQuery) {
        newTable = join.fromTable; newCol = join.fromCol;
        existingTable = join.toTable; existingCol = join.toCol;
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
      if (emittedConds.has(key)) continue;  // duplicate join entry — skip
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
    const cp1x = from.x + dx, cp1y = from.y;
    const cp2x = to.x - dx, cp2y = to.y;
    const t = 0.5;
    const x = Math.pow(1 - t, 3) * from.x + 3 * Math.pow(1 - t, 2) * t * cp1x + 3 * (1 - t) * Math.pow(t, 2) * cp2x + Math.pow(t, 3) * to.x;
    const y = Math.pow(1 - t, 3) * from.y + 3 * Math.pow(1 - t, 2) * t * cp1y + 3 * (1 - t) * Math.pow(t, 2) * cp2y + Math.pow(t, 3) * to.y;
    return { x, y };
  };

  return (
    <div style={{ flex: 1, display: "flex", flexDirection: "column", overflow: "hidden" }}>
      <div style={{ display: "flex", alignItems: "center", gap: "0.5rem", padding: "0.4rem 0.75rem", borderBottom: "1px solid var(--border)", flexShrink: 0, background: "var(--surface)" }}>
        <button
          className="btn-primary"
          style={{ fontSize: "0.78rem", padding: "0.25rem 0.6rem" }}
          onClick={handleGenerateSql}
          disabled={canvasTables.length === 0}
        >→ SQL</button>
        <button
          className="btn-secondary"
          style={{ fontSize: "0.78rem", padding: "0.25rem 0.6rem" }}
          onClick={handleClear}
          disabled={canvasTables.length === 0}
        >Clear</button>
        <span style={{ fontSize: "0.72rem", color: "var(--text-muted)", marginLeft: "0.5rem" }}>
          {canvasTables.length > 0 ? `${canvasTables.length} table${canvasTables.length !== 1 ? "s" : ""}, ${canvasJoins.length} join${canvasJoins.length !== 1 ? "s" : ""}` : ""}
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
          <div style={{ position: "absolute", inset: 0, display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", gap: "0.75rem", color: "var(--text-muted)", pointerEvents: "none" }}>
            <Network size={40} style={{ opacity: 0.3 }} />
            <span style={{ fontSize: "0.85rem", opacity: 0.6 }}>Drag tables from the sidebar onto this canvas</span>
          </div>
        )}

        <svg style={{ position: "absolute", inset: 0, width: "100%", height: "100%", pointerEvents: "none", overflow: "visible", zIndex: 5 }}>
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
              <path key={join.id} d={bezierPath(fp, tp)} fill="none" stroke="var(--primary)" strokeWidth={2} opacity={0.7} />
            );
          })}

          {connectingMouse && connectingRef.current && (() => {
            const fromCt = canvasTables.find((c) => c.tableName === connectingRef.current!.tableName);
            if (!fromCt) return null;
            const fp = fromPort(fromCt, connectingRef.current.colIdx);
            const tp = connectingMouse;
            const dx = Math.max(40, Math.abs(tp.x - fp.x) * 0.5);
            return (
              <path
                d={`M ${fp.x},${fp.y} C ${fp.x + dx},${fp.y} ${tp.x - dx},${tp.y} ${tp.x},${tp.y}`}
                fill="none" stroke="var(--primary)" strokeWidth={1.5} strokeDasharray="5,4" opacity={0.6}
              />
            );
          })()}
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
                onChange={(e) => setCanvasJoins((prev) => prev.map((j) => j.id === join.id ? { ...j, cardinality: e.target.value as "many-to-one" | "one-to-many" } : j))}
                style={{ fontSize: "0.68rem", background: "none", border: "none", color: "var(--text)", cursor: "pointer", padding: 0 }}
              >
                <option value="many-to-one">N:1</option>
                <option value="one-to-many">1:N</option>
              </select>
              <button
                onClick={() => setCanvasJoins((prev) => prev.filter((j) => j.id !== join.id))}
                style={{ background: "none", border: "none", color: "var(--text-muted)", cursor: "pointer", padding: 0, lineHeight: 1, fontSize: "0.7rem" }}
              >✕</button>
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
              onToggleCol={(colName) => setSelectedColumns((prev) => {
                const next = new Map(prev);
                const cols = new Set(next.get(ct.tableName) ?? []);
                if (cols.has(colName)) cols.delete(colName); else cols.add(colName);
                next.set(ct.tableName, cols);
                return next;
              })}
            />
          );
        })}
      </div>
    </div>
  );
}

// ── SqlPage ──────────────────────────────────────────────────────────────────

export function SqlPage() {
  const { checkedDomains } = useDomainFilter();
  const location = useLocation();
  const canCreateView = useCapability("create_view");
  const canRequestView = useCapability("query_development");
  const [viewModal, setViewModal] = useState(false);
  const [viewId, setViewId] = useState("");
  const [viewDescription, setViewDescription] = useState("");
  const [viewDomainId, setViewDomainId] = useState("");
  const [viewSaving, setViewSaving] = useState(false);
  const [viewMsg, setViewMsg] = useState("");
  const [viewColumns, setViewColumns] = useState<ViewColumnConfig[]>([]);
  const [tables, setTables] = useState<RegisteredTable[]>([]);
  const [existingRels, setExistingRels] = useState<Relationship[]>([]);
  const [topTab, setTopTab] = useState<TopTab>("sql");
  const viewTable = (location.state as { viewTable?: RegisteredTable } | null)?.viewTable ?? null;
  const [sqlText, setSqlText] = useState(() => {
    const locSql = (location.state as { sql?: string } | null)?.sql;
    return locSql ?? loadSqlQuery();
  });
  const [viewSaving, setViewSaving] = useState(false);
  const [role, setRole] = useState("admin");
  const [roles, setRoles] = useState<string[]>(["admin"]);
  const [domainMap, setDomainMap] = useState<Record<string, Domain>>({});
  const [running, setRunning] = useState(false);
  const [sampleMode, setSampleMode] = useState<"first" | "last" | "random">("first");
  const [sampleSize, setSampleSize] = useState(100);
  const [resultTab, setResultTab] = useState<ResultTab>("results");
  const [resultColumns, setResultColumns] = useState<string[]>([]);
  const [resultRows, setResultRows] = useState<Record<string, unknown>[]>([]);
  const [resultError, setResultError] = useState("");
  const [execMs, setExecMs] = useState<number | null>(null);
  const [errors, _setErrors] = useState<string[]>([]);
  const [expandedDomains, setExpandedDomains] = useState<Set<string>>(new Set());
  const [expandedTables, setExpandedTables] = useState<Set<string>>(new Set());
  const [history, setHistory] = useState<HistoryEntry[]>(loadHistory);
  const [copied, setCopied] = useState(false);
  const [copiedResults, setCopiedResults] = useState(false);
  const [sorts, setSorts] = useState<{ col: string; dir: "asc" | "desc" }[]>([]);
  const [filters, setFilters] = useState<Record<string, string>>({});
  const [colWidths, setColWidths] = useState<Record<string, number>>({});
  const [sidebarOpen, setSidebarOpen] = useState(true);
  const resizingRef = useRef<{ col: string; startX: number; startW: number } | null>(null);
  const editorViewRef = useRef<EditorView | null>(null);
  const pendingAutoRunRef = useRef((location.state as any)?.autoRun === true);
  const [nlText, setNlText] = useState("");
  const [nlLoading, setNlLoading] = useState(false);
  const [nlError, setNlError] = useState("");

  useEffect(() => {
    idbGet<SqlResults>(SQL_RESULTS_KEY).then((saved) => {
      if (saved) {
        setResultColumns(saved.columns);
        setResultRows(saved.rows);
        setResultError(saved.error);
      }
    });
  }, []);

  useEffect(() => {
    localStorage.removeItem("provisa.sql.pending_query");
    fetchRoles().catch(() => []).then((r) => {
      const ids = r.map((x: any) => x.id);
      if (ids.length) setRoles(ids);
    });
    fetchDomains().catch(() => []).then((ds: Domain[]) => {
      setDomainMap(Object.fromEntries(ds.map((d) => [normalizeDomain(d.id), d])));
    });
    fetchTables().catch(() => []).then(setTables);
    fetchRelationships().catch(() => []).then(setExistingRels);
  }, []);

  const normalizeDomain = (id: string) =>
    id.replace(/[^a-zA-Z0-9]/g, "_").replace(/^_+|_+$/g, "");

  const sqlSchema = useMemo(() => {
    const schema: Record<string, string[] | Record<string, string[]>> = {};
    for (const t of tables) {
      const cols = t.columns.flatMap((c) =>
        c.nativeFilterType ? [c.columnName, `_nf_${c.columnName}`] : [c.columnName],
      );
      schema[t.tableName] = cols;
      if (t.alias) schema[t.alias] = cols;
      if (t.schemaName) {
        const schemaEntry = schema[t.schemaName] as Record<string, string[]> | undefined;
        if (!schemaEntry || Array.isArray(schemaEntry)) {
          schema[t.schemaName] = { [t.tableName]: cols };
        } else {
          schemaEntry[t.tableName] = cols;
        }
      }
    }
    return schema;
  }, [tables]);

  const sqlExtensions = useMemo(
    () => [sql({ dialect: PostgreSQL, schema: sqlSchema })],
    [sqlSchema],
  );

  const viewSqlExtensions = useMemo(
    () => [sql({ dialect: PostgreSQL }), EditorView.lineWrapping],
    [],
  );

  const viewSqlNormalized = useMemo(() => {
    const COMMENT_PREFIX = "-- provisa-params:";
    const PARAM_RE = /\$(\d+)=(NULL|TRUE|FALSE|-?\d+(?:\.\d+)?|'(?:[^']|'')*')/g;

    const lines = sqlText.trim().replace(/;+$/, "").split("\n");
    const params: Record<number, string> = {};
    let filtered = lines;
    for (let i = 0; i < lines.length; i++) {
      if (lines[i].trim().startsWith(COMMENT_PREFIX)) {
        for (const m of lines[i].matchAll(PARAM_RE)) params[parseInt(m[1])] = m[2];
        filtered = [...lines.slice(0, i), ...lines.slice(i + 1)];
        break;
      }
    }
    let sql = filtered.join("\n");
    if (Object.keys(params).length > 0) {
      sql = sql.replace(/\$(\d+)/g, (_, n) => params[parseInt(n)] ?? `$${n}`);
    }
    // Strip trailing LIMIT (and optional OFFSET) — views must not have a fixed limit
    return sql.replace(/\s+LIMIT\s+\d+(\s+OFFSET\s+\d+)?$/i, "").replace(/\s+OFFSET\s+\d+\s+LIMIT\s+\d+$/i, "").trim();
  }, [sqlText]);

  const viewHasParams = useMemo(() => /\$\d+/.test(viewSqlNormalized), [viewSqlNormalized]);

  const domainGroups = useMemo(() => {
    const groups: Record<string, RegisteredTable[]> = {};
    for (const t of tables) {
      const isImplicitDomain = t.domainId === "meta" || t.domainId === "ops";
      if (!isImplicitDomain && checkedDomains.size > 0 && t.domainId && !checkedDomains.has(t.domainId)) continue;
      const d = t.domainId ? normalizeDomain(t.domainId) : "(no domain)";
      (groups[d] = groups[d] || []).push(t);
    }
    return groups;
  }, [tables, checkedDomains]);

  const insertAtCursor = useCallback((text: string) => {
    const view = editorViewRef.current;
    if (!view) {
      setSqlText((prev) => prev + text);
      return;
    }
    const { from, to } = view.state.selection.main;
    view.dispatch({
      changes: { from, to, insert: text },
      selection: { anchor: from + text.length },
    });
    view.focus();
  }, []);

  const toggleDomain = (d: string) =>
    setExpandedDomains((prev) => {
      const next = new Set(prev);
      next.has(d) ? next.delete(d) : next.add(d);
      return next;
    });

  const toggleTable = (t: string) =>
    setExpandedTables((prev) => {
      const next = new Set(prev);
      next.has(t) ? next.delete(t) : next.add(t);
      return next;
    });

  const handleCopy = useCallback(() => {
    navigator.clipboard.writeText(sqlText).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 1500);
    });
  }, [sqlText]);

  const handleSort = useCallback((col: string) => {
    setSorts((prev) => {
      const idx = prev.findIndex((s) => s.col === col);
      if (idx === -1) return [...prev, { col, dir: "asc" }];
      if (prev[idx].dir === "asc") return prev.map((s, i) => i === idx ? { ...s, dir: "desc" } : s);
      return prev.filter((_, i) => i !== idx);
    });
  }, []);

  const handleResizeStart = useCallback((col: string, e: React.MouseEvent) => {
    e.preventDefault();
    e.stopPropagation();
    const th = (e.currentTarget as HTMLElement).closest("th") as HTMLElement;
    const startW = th.offsetWidth;
    resizingRef.current = { col, startX: e.clientX, startW };
    const onMove = (ev: MouseEvent) => {
      if (!resizingRef.current) return;
      const delta = ev.clientX - resizingRef.current.startX;
      const newW = Math.max(60, resizingRef.current.startW + delta);
      setColWidths((prev) => ({ ...prev, [resizingRef.current!.col]: newW }));
    };
    const onUp = () => {
      resizingRef.current = null;
      document.removeEventListener("mousemove", onMove);
      document.removeEventListener("mouseup", onUp);
    };
    document.addEventListener("mousemove", onMove);
    document.addEventListener("mouseup", onUp);
  }, []);

  const [page, setPage] = useState(0);
  const PAGE_SIZE = 100;
  const COL_MAX = 280;
  const COL_MIN = 60;
  const CHAR_PX = 7.5;

  const autoWidths = useMemo(() => {
    const cols = resultColumns.length > 0 ? resultColumns : Object.keys(resultRows[0] ?? {});
    const widths: Record<string, number> = {};
    for (const col of cols) {
      const headerLen = col.length;
      const maxDataLen = resultRows.slice(0, 50).reduce((m, r) => {
        const v = r[col];
        return Math.max(m, v == null ? 4 : String(v).length);
      }, 0);
      widths[col] = Math.min(COL_MAX, Math.max(COL_MIN, Math.max(headerLen, maxDataLen) * CHAR_PX + 24));
    }
    return widths;
  }, [resultRows, resultColumns]);

  const displayRows = useMemo(() => {
    const cols = resultColumns.length > 0 ? resultColumns : Object.keys(resultRows[0] ?? {});
    let rows = [...resultRows];
    for (const col of cols) {
      const f = filters[col];
      if (!f) continue;
      const lower = f.toLowerCase();
      rows = rows.filter((r) => {
        const v = r[col];
        return v != null && String(v).toLowerCase().includes(lower);
      });
    }
    if (sorts.length > 0) {
      rows.sort((a, b) => {
        for (const { col, dir } of sorts) {
          const av = a[col], bv = b[col];
          let cmp = 0;
          if (av == null && bv == null) continue;
          if (av == null) { cmp = 1; }
          else if (bv == null) { cmp = -1; }
          else if (typeof av === "number" && typeof bv === "number") { cmp = av - bv; }
          else { cmp = String(av).localeCompare(String(bv)); }
          if (cmp !== 0) return dir === "asc" ? cmp : -cmp;
        }
        return 0;
      });
    }
    return rows;
  }, [resultRows, resultColumns, filters, sorts]);

  const pagedRows = useMemo(
    () => displayRows.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE),
    [displayRows, page],
  );

  const totalPages = Math.max(1, Math.ceil(displayRows.length / PAGE_SIZE));

  const handleDownloadCsv = useCallback(() => {
    const cols = resultColumns.length > 0 ? resultColumns : Object.keys(resultRows[0] ?? {});
    const escape = (v: unknown) => {
      const s = v == null ? "" : String(v);
      return s.includes(",") || s.includes('"') || s.includes("\n")
        ? `"${s.replace(/"/g, '""')}"` : s;
    };
    const lines = [cols.map(escape).join(",")];
    for (const row of displayRows) lines.push(cols.map((c) => escape(row[c])).join(","));
    const blob = new Blob([lines.join("\n")], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url; a.download = "results.csv"; a.click();
    URL.revokeObjectURL(url);
  }, [displayRows, resultColumns, resultRows]);

  const handleCopyResults = useCallback(() => {
    const cols = resultColumns.length > 0 ? resultColumns : Object.keys(resultRows[0] ?? {});
    const lines = [cols.join("\t")];
    for (const row of displayRows) lines.push(cols.map((c) => (row[c] == null ? "" : String(row[c]))).join("\t"));
    navigator.clipboard.writeText(lines.join("\n")).then(() => {
      setCopiedResults(true);
      setTimeout(() => setCopiedResults(false), 1500);
    });
  }, [displayRows, resultColumns, resultRows]);

  interface ColumnProfile {
    col: string;
    nullCount: number;
    blankCount: number;
    distinctCount: number;
    constantValue: unknown | undefined;
    min: number | string | null;
    max: number | string | null;
    mean: number | null;
    topValues: { value: string; count: number }[];
  }

  const profile = useMemo((): ColumnProfile[] => {
    if (resultRows.length === 0) return [];
    const cols = resultColumns.length > 0 ? resultColumns : Object.keys(resultRows[0] ?? {});
    return cols.map((col) => {
      const vals = resultRows.map((r) => r[col]);
      const nullCount = vals.filter((v) => v === null || v === undefined).length;
      const blankCount = vals.filter((v) => typeof v === "string" && v.trim() === "").length;
      const nonNull = vals.filter((v) => v !== null && v !== undefined);
      const freq: Map<string, number> = new Map();
      for (const v of vals) {
        const k = v === null || v === undefined ? "NULL" : String(v);
        freq.set(k, (freq.get(k) ?? 0) + 1);
      }
      const distinctCount = freq.size;
      const constantValue = distinctCount === 1 ? vals[0] : undefined;
      const numbers = nonNull.filter((v) => typeof v === "number") as number[];
      const mean = numbers.length > 0 ? numbers.reduce((a, b) => a + b, 0) / numbers.length : null;
      const sorted = [...nonNull].sort((a, b) => (a! < b! ? -1 : a! > b! ? 1 : 0));
      const min = sorted.length > 0 ? (sorted[0] as string | number) : null;
      const max = sorted.length > 0 ? (sorted[sorted.length - 1] as string | number) : null;
      const topValues = [...freq.entries()]
        .sort((a, b) => b[1] - a[1])
        .slice(0, 5)
        .map(([value, count]) => ({ value, count }));
      return { col, nullCount, blankCount, distinctCount, constantValue, min, max, mean, topValues };
    });
  }, [resultRows, resultColumns]);

  const handleDownloadProfile = useCallback(() => {
    const blob = new Blob([JSON.stringify(profile, null, 2)], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url; a.download = "profile.json"; a.click();
    URL.revokeObjectURL(url);
  }, [profile]);

  const updateViewCol = useCallback(<K extends keyof ViewColumnConfig>(index: number, key: K, value: ViewColumnConfig[K]) => {
    setViewColumns((prev) => prev.map((c, i) => i === index ? { ...c, [key]: value } : c));
  }, []);

  const handleSaveView = useCallback(async () => {
    if (!viewId.trim() || !viewDomainId.trim()) return;
    setViewSaving(true);
    setViewMsg("");
    try {
      const governance = canCreateView ? "pre-approved" : "suggested";
      const sql = viewSqlNormalized;
      const columns = viewColumns.map((c) => ({
        name: c.name,
        alias: c.alias || undefined,
        description: c.description || undefined,
        scope: c.scope,
        visibleTo: c.visibleTo,
        unmaskedTo: c.unmaskedTo ? c.unmaskedTo.split(",").map((s) => s.trim()).filter(Boolean) : undefined,
        maskType: c.maskType || undefined,
        maskPattern: c.maskPattern || undefined,
        maskReplace: c.maskReplace || undefined,
        maskValue: c.maskValue || undefined,
        maskPrecision: c.maskPrecision || undefined,
      }));
      await registerTable({
        sourceId: "__provisa__",
        domainId: viewDomainId.trim(),
        schemaName: "views",
        tableName: viewId.trim(),
        governance,
        alias: viewId.trim(),
        description: viewDescription.trim() || undefined,
        viewSql: sql,
        columns,
      });
      setViewMsg(canCreateView ? "View created." : "View request submitted.");
      fetchTables().catch(() => []).then(setTables);
      fetchRelationships().catch(() => []).then(setExistingRels);
      localStorage.setItem("provisa.schema.version", String(Date.now()));
      window.dispatchEvent(new StorageEvent("storage", { key: "provisa.schema.version" }));
      setTimeout(() => { setViewModal(false); setViewMsg(""); setViewId(""); setViewDescription(""); setViewDomainId(""); setViewColumns([]); }, 1500);
    } catch (e: any) {
      setViewMsg(`Error: ${e.message}`);
    } finally {
      setViewSaving(false);
    }
  }, [viewId, viewDescription, viewDomainId, viewSqlNormalized, canCreateView, viewColumns, setTables, setExistingRels]);

  const handleRun = useCallback(async () => {
    if (!sqlText.trim()) return;
    setRunning(true);
    setResultError("");
    const t0 = performance.now();
    const inner = sqlText.trim().replace(/;+$/, "");
    const sampledSql =
      sampleMode === "first"
        ? `SELECT * FROM (\n${inner}\n) _sample LIMIT ${sampleSize}`
        : sampleMode === "last"
        ? `SELECT * FROM (\n${inner}\n) _sample ORDER BY 1 DESC LIMIT ${sampleSize}`
        : `SELECT * FROM (\n${inner}\n) _sample ORDER BY random() LIMIT ${sampleSize}`;
    const result = await runSql(sampledSql, role);
    const durationMs = Math.round(performance.now() - t0);
    setExecMs(durationMs);
    if (result.error) {
      setResultError(result.error);
      setResultColumns([]);
      setResultRows([]);
      saveSqlResults({ columns: [], rows: [], error: result.error });
    } else {
      setResultColumns(result.columns);
      setResultRows(result.rows);
      saveSqlResults({ columns: result.columns, rows: result.rows, error: "" });
    }
    setSorts([]);
    setFilters({});
    setColWidths({});
    setPage(0);
    setResultTab("results");
    setRunning(false);
    const entry: HistoryEntry = {
      sql: sqlText.trim(),
      role,
      executedAt: Date.now(),
      durationMs,
      rowCount: result.error ? 0 : result.rows.length,
      error: result.error ?? "",
    };
    setHistory((prev) => {
      const next = [entry, ...prev.filter((h) => h.sql !== entry.sql || h.role !== entry.role)];
      saveHistory(next);
      return next;
    });
  }, [sqlText, role, sampleMode, sampleSize]);

  useEffect(() => {
    if (pendingAutoRunRef.current && sqlText.trim()) {
      pendingAutoRunRef.current = false;
      handleRun();
    }
  }, [sqlText, handleRun]);

  return (
    <div style={{ flex: 1, minHeight: 0, display: "flex", flexDirection: "column", overflow: "hidden", background: "var(--bg)" }}>
      {/* Header */}
      <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", padding: "0.75rem 1rem", borderBottom: "1px solid var(--border)", flexShrink: 0 }}>
        <div style={{ display: "flex", alignItems: "center", gap: "0.75rem" }}>
          <span style={{ fontWeight: 600, fontSize: "0.9rem", letterSpacing: "0.02em" }}>SQL Explorer</span>
          <div style={{ display: "flex", alignItems: "center", gap: 0, border: "1px solid var(--border)", borderRadius: "5px", overflow: "hidden", marginLeft: "0.5rem" }}>
            {(["sql", "canvas"] as TopTab[]).map((tab) => (
              <button
                key={tab}
                onClick={() => setTopTab(tab)}
                style={{
                  padding: "0.2rem 0.6rem",
                  fontSize: "0.75rem",
                  background: topTab === tab ? "var(--primary)" : "none",
                  color: topTab === tab ? "#fff" : "var(--text-muted)",
                  border: "none",
                  cursor: "pointer",
                  textTransform: "capitalize",
                  fontWeight: topTab === tab ? 600 : 400,
                }}
              >{tab}</button>
            ))}
          </div>
        </div>
      </div>

      {/* Body: sidebar + right pane */}
      <div style={{ display: "flex", flex: 1, overflow: "hidden" }}>

        {/* Schema browser drawer */}
        <div style={{ display: "flex", flexShrink: 0, borderRight: "1px solid var(--border)", position: "relative" }}>
          <button
            onClick={() => setSidebarOpen((v) => !v)}
            title={sidebarOpen ? "Collapse schema panel" : "Expand schema panel"}
            style={{
              position: "absolute",
              right: -13,
              top: "50%",
              transform: "translateY(-50%)",
              zIndex: 30,
              width: 13,
              height: 40,
              background: "var(--surface)",
              border: "1px solid var(--border)",
              borderLeft: "none",
              borderRadius: "0 4px 4px 0",
              cursor: "pointer",
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              padding: 0,
              color: "var(--text-muted)",
              fontSize: "0.55rem",
            }}
          >{sidebarOpen ? "‹" : "›"}</button>

          <div style={{
            width: sidebarOpen ? 210 : 0,
            overflow: "hidden",
            transition: "width 0.18s ease",
            background: "var(--bg)",
          }}>
            <div style={{ width: 210, overflow: "auto", height: "100%", padding: "0.5rem 0" }}>
              <div style={{ padding: "0 0.75rem 0.4rem", fontSize: "0.65rem", fontWeight: 700, letterSpacing: "0.08em", textTransform: "uppercase", color: "var(--text-muted)" }}>Schema</div>
              {Object.entries(domainGroups).map(([domain, domainTables]) => {
                const domainOpen = expandedDomains.has(domain);
                return (
                  <div key={domain}>
                    <button
                      onClick={() => toggleDomain(domain)}
                      title={domainMap[domain]?.description || undefined}
                      style={{ width: "100%", textAlign: "left", background: "none", border: "none", cursor: "pointer", padding: "0.2rem 0.75rem", display: "flex", alignItems: "center", gap: "0.25rem", color: "var(--text-muted)", fontSize: "0.75rem", fontWeight: 600 }}
                    >
                      {domainOpen ? <ChevronDown size={10} /> : <ChevronRight size={10} />}
                      <span style={{ flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{domain}</span>
                      {domainMap[domain]?.description && (
                        <span style={{ flexShrink: 0, color: "var(--text-muted)", opacity: 0.5, fontSize: "0.65rem", lineHeight: 1 }}>ⓘ</span>
                      )}
                    </button>
                    {domainOpen && domainTables.map((t) => {
                      const tOpen = expandedTables.has(t.tableName);
                      return (
                        <div key={t.tableName}>
                          <div style={{ display: "flex", alignItems: "center" }}>
                            <button
                              onClick={() => toggleTable(t.tableName)}
                              draggable={topTab === "canvas"}
                              onDragStart={topTab === "canvas" ? (e) => e.dataTransfer.setData("tableName", t.tableName) : undefined}
                              style={{ flex: 1, minWidth: 0, textAlign: "left", background: "none", border: "none", cursor: topTab === "canvas" ? "grab" : "pointer", padding: "0.18rem 0 0.18rem 1.5rem", display: "flex", alignItems: "center", gap: "0.3rem", color: "var(--text)", fontSize: "0.75rem" }}
                              title={topTab === "canvas" ? "Drag to canvas" : "Double-click to insert qualified name"}
                              onDoubleClick={topTab === "sql" ? () => insertAtCursor(`"${normalizeDomain(t.domainId || t.schemaName)}"."${t.tableName}"`) : undefined}
                            >
                              {tOpen ? <ChevronDown size={9} /> : <ChevronRight size={9} />}
                              <Table2 size={9} style={{ flexShrink: 0, color: "var(--primary)" }} />
                              <span style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>{t.tableName}</span>
                            </button>
                            <button
                              onClick={() => insertAtCursor(`"${normalizeDomain(t.domainId || t.schemaName)}"."${t.tableName}"`)}
                              title="Insert table reference in SQL editor"
                              style={{ flexShrink: 0, background: "none", border: "none", cursor: "pointer", padding: "0 0.35rem 0 0.1rem", color: "var(--primary)", fontSize: "0.7rem", opacity: 0.6, lineHeight: 1 }}
                            >→</button>
                            {t.description && (
                              <span
                                title={t.description}
                                style={{ flexShrink: 0, paddingRight: "0.35rem", color: "var(--primary)", opacity: 0.7, fontSize: "0.65rem", cursor: "default", lineHeight: 1 }}
                              >ⓘ</span>
                            )}
                          </div>
                          {tOpen && [
                            ...t.columns.map((col) => ({ columnName: col.columnName, dataType: col.dataType, description: col.description, virtual: false })),
                            { columnName: "_name_", dataType: "text", description: "Table alias/name", virtual: true },
                            { columnName: "_domain_", dataType: "text", description: "Domain ID", virtual: true },
                          ].map((col) => (
                            <div key={col.columnName} style={{ display: "flex", alignItems: "center" }}>
                              <button
                                onClick={() => topTab === "sql" ? insertAtCursor(`"${t.tableName}"."${col.columnName}"`) : undefined}
                                style={{ flex: 1, minWidth: 0, textAlign: "left", background: "none", border: "none", cursor: topTab === "sql" ? "pointer" : "default", padding: "0.15rem 0 0.15rem 2.5rem", display: "flex", alignItems: "center", gap: "0.3rem", color: col.virtual ? "var(--text-muted)" : "var(--text-muted)", fontSize: "0.72rem", fontFamily: "monospace", opacity: col.virtual ? 0.6 : 1 }}
                                title={col.description ?? (topTab === "sql" ? "Click to insert quoted column" : undefined)}
                              >
                                <Columns3 size={8} style={{ flexShrink: 0, color: col.virtual ? "var(--accent)" : undefined }} />
                                <span style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap", flex: 1 }}>{col.columnName}</span>
                                {col.dataType && (
                                  <span style={{ flexShrink: 0, fontSize: "0.6rem", color: "var(--text-muted)", opacity: 0.5, fontFamily: "monospace", paddingRight: "0.1rem" }}>{col.dataType}</span>
                                )}
                              </button>
                              {col.description && (
                                <span
                                  title={col.description}
                                  style={{ flexShrink: 0, paddingRight: "0.5rem", color: "var(--text-muted)", opacity: 0.6, fontSize: "0.65rem", cursor: "default", lineHeight: 1 }}
                                >ⓘ</span>
                              )}
                            </div>
                          ))}
                        </div>
                      );
                    })}
                  </div>
                );
              })}
            </div>
          </div>
        </div>

        {/* Right pane */}
        <div style={{ flex: 1, display: "flex", flexDirection: "column", overflow: "hidden" }}>

          <div style={{ display: topTab === "canvas" ? "flex" : "none", flex: 1, overflow: "hidden", flexDirection: "column" }}>
            <JoinCanvas
              tables={tables}
              existingRels={existingRels}
              onGenerateSql={(generatedSql) => { setSqlText(generatedSql); setTopTab("sql"); }}
            />
          </div>

          <div style={{ display: topTab === "sql" ? "flex" : "none", flex: 1, overflow: "hidden", flexDirection: "column" }}>
            <>
              {/* NL-to-SQL strip */}
              <div style={{ display: "flex", alignItems: "center", gap: "0.5rem", padding: "0.35rem 0.75rem", borderBottom: "1px solid var(--border)", background: "var(--surface)", flexShrink: 0 }}>
                <div className="nl-input-wrap" style={{ flex: 1, position: "relative", display: "flex", alignItems: "center" }}>
                  <input
                    type="text"
                    placeholder="Ask in plain English — generates SQL…"
                    value={nlText}
                    onChange={(e) => { setNlText(e.target.value); setNlError(""); }}
                    onKeyDown={async (e) => {
                      if (e.key === "Enter" && nlText.trim() && !nlLoading) {
                        setNlLoading(true);
                        setNlError("");
                        const result = await nlToSql(nlText.trim(), role);
                        setNlLoading(false);
                        if (result.error) { setNlError(result.error); }
                        else { setSqlText(result.sql); }
                      }
                    }}
                    style={{ width: "100%", fontSize: "0.8rem", padding: "0.25rem 1.6rem 0.25rem 0.5rem", borderRadius: "4px", border: "1px solid var(--border)", background: "var(--bg)", color: "var(--text)", outline: "none" }}
                  />
                  {nlText && (
                    <button
                      onClick={() => { setNlText(""); setNlError(""); }}
                      title="Clear"
                      className="nl-clear-btn"
                      style={{ position: "absolute", right: "0.3rem", background: "none", border: "none", padding: "0.1rem", cursor: "pointer", color: "var(--text-muted)", display: "flex", alignItems: "center", lineHeight: 1 }}
                    >
                      <X size={12} />
                    </button>
                  )}
                </div>
                <button
                  className="btn-primary"
                  disabled={nlLoading || !nlText.trim()}
                  onClick={async () => {
                    setNlLoading(true);
                    setNlError("");
                    const result = await nlToSql(nlText.trim(), role);
                    setNlLoading(false);
                    if (result.error) { setNlError(result.error); }
                    else { setSqlText(result.sql); }
                  }}
                  style={{ fontSize: "0.8rem", padding: "0.25rem 0.6rem", whiteSpace: "nowrap" }}
                >
                  {nlLoading ? "Generating…" : "Generate SQL"}
                </button>
                {nlError && <span style={{ fontSize: "0.75rem", color: "var(--error)", maxWidth: "300px", overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }} title={nlError}>{nlError}</span>}
              </div>

              {/* Editor */}
              <div style={{ flex: "0 0 220px", overflow: "hidden", borderBottom: "1px solid var(--border)", position: "relative" }}
                onMouseEnter={(e) => { const btn = e.currentTarget.querySelector<HTMLElement>('.copy-sql-btn'); if (btn) btn.style.opacity = '1'; }}
                onMouseLeave={(e) => { const btn = e.currentTarget.querySelector<HTMLElement>('.copy-sql-btn'); if (btn) btn.style.opacity = '0'; }}
              >
                {nlLoading && (
                  <div style={{ position: "absolute", inset: 0, zIndex: 10, background: "rgba(0,0,0,0.45)", display: "flex", alignItems: "center", justifyContent: "center", gap: "0.5rem", color: "var(--text-muted)", fontSize: "0.8rem", pointerEvents: "none" }}>
                    <span style={{ display: "inline-block", width: "14px", height: "14px", border: "2px solid var(--text-muted)", borderTopColor: "transparent", borderRadius: "50%", animation: "spin 0.7s linear infinite" }} />
                    Generating SQL…
                  </div>
                )}
                <CodeMirror
                  value={sqlText}
                  height="220px"
                  theme={oneDark}
                  extensions={sqlExtensions}
                  onChange={(v) => { setSqlText(v); saveSqlQuery(v); }}
                  onCreateEditor={(view) => { editorViewRef.current = view; }}
                  style={{ fontSize: "0.8rem" }}
                />
                <button
                  className="copy-sql-btn"
                  onClick={handleCopy}
                  title="Copy SQL"
                  style={{ position: "absolute", top: "0.4rem", right: "0.4rem", opacity: 0, transition: "opacity 0.15s", background: "rgba(30,30,40,0.85)", border: "1px solid var(--border)", borderRadius: "4px", color: "var(--text-muted)", cursor: "pointer", padding: "0.2rem 0.35rem", display: "flex", alignItems: "center", gap: "0.25rem", fontSize: "0.72rem" }}
                >
                  {copied ? <Check size={11} style={{ color: "var(--approve)" }} /> : <Copy size={11} />}
                  {copied ? "Copied" : "Copy"}
                </button>
              </div>

              {/* Toolbar */}
              <div style={{ display: "flex", alignItems: "center", gap: "0.5rem", padding: "0.4rem 0.75rem", borderBottom: "1px solid var(--border)", flexShrink: 0, background: "var(--surface)" }}>
                <button
                  className="btn-primary"
                  style={{ display: "flex", alignItems: "center", gap: "0.3rem", fontSize: "0.8rem", padding: "0.25rem 0.6rem" }}
                  onClick={handleRun}
                  disabled={running || !sqlText.trim()}
                >
                  <Play size={11} />{running ? "Running…" : "Sample >"}
                </button>
                <button
                  className="btn-secondary"
                  style={{ fontSize: "0.8rem", padding: "0.25rem 0.6rem" }}
                  title="Add table_col aliases where column names conflict across tables"
                  onClick={() => setSqlText((prev) => { const fixed = autoAliasConflicts(prev); saveSqlQuery(fixed); return fixed; })}
                  disabled={!sqlText.trim()}
                >
                  Auto-alias
                </button>
                {viewTable && (
                  <button
                    className="btn-primary"
                    style={{ fontSize: "0.8rem", padding: "0.25rem 0.6rem" }}
                    title={`Save current SQL as the definition for view "${viewTable.tableName}"`}
                    disabled={viewSaving || !sqlText.trim()}
                    onClick={async () => {
                      setViewSaving(true);
                      await updateTable({
                        sourceId: viewTable.sourceId,
                        domainId: viewTable.domainId ?? "",
                        schemaName: viewTable.schemaName ?? "",
                        tableName: viewTable.tableName,
                        governance: viewTable.governance ?? "pre-approved",
                        alias: viewTable.alias ?? undefined,
                        description: viewTable.description ?? undefined,
                        watermarkColumn: viewTable.watermarkColumn ?? null,
                        viewSql: sqlText.trim(),
                        dataProduct: viewTable.dataProduct ?? false,
                        columns: viewTable.columns.map((c) => ({
                          name: c.columnName,
                          visibleTo: c.visibleTo ?? [],
                          writableBy: c.writableBy,
                          unmaskedTo: c.unmaskedTo,
                          maskType: c.maskType ?? undefined,
                          maskPattern: c.maskPattern ?? undefined,
                          maskReplace: c.maskReplace ?? undefined,
                          maskValue: c.maskValue ?? undefined,
                          maskPrecision: c.maskPrecision ?? undefined,
                          alias: c.alias ?? undefined,
                          description: c.description ?? undefined,
                          nativeFilterType: c.nativeFilterType ?? undefined,
                          isPrimaryKey: c.isPrimaryKey ?? undefined,
                          isForeignKey: c.isForeignKey ?? undefined,
                          isAlternateKey: c.isAlternateKey ?? undefined,
                          scope: c.scope ?? "domain",
                        })),
                      });
                      setViewSaving(false);
                    }}
                  >
                    {viewSaving ? "Saving…" : `Update "${viewTable.tableName}"`}
                  </button>
                )}
                <select
                  value={sampleMode}
                  onChange={(e) => setSampleMode(e.target.value as "first" | "last" | "random")}
                  className="toolbar-select"
                >
                  <option value="first">First</option>
                  <option value="last">Last</option>
                  <option value="random">Random</option>
                </select>
                <input
                  type="number"
                  value={sampleSize}
                  min={1}
                  max={10000}
                  onChange={(e) => setSampleSize(Math.max(1, parseInt(e.target.value) || 100))}
                  className="toolbar-input"
                  style={{ width: "60px" }}
                  title="Row count"
                />
                <select
                  value={role}
                  onChange={(e) => setRole(e.target.value)}
                  className="toolbar-select"
                >
                  {roles.map((r) => <option key={r} value={r}>{r}</option>)}
                </select>
                <div style={{ flex: 1 }} />
                {(canCreateView || canRequestView) && sqlText.trim() && (
                  <button
                    className="btn-secondary"
                    style={{ fontSize: "0.78rem", padding: "0.25rem 0.6rem" }}
                    onClick={() => {
                      setViewId("");
                      setViewDescription("");
                      setViewDomainId("");
                      setViewMsg("");
                      setViewColumns(resultColumns.map((name) => ({
                        name,
                        alias: "",
                        description: "",
                        scope: "domain" as const,
                        visibleTo: roles,
                        maskType: "" as const,
                        maskPattern: "",
                        maskReplace: "",
                        maskValue: "",
                        maskPrecision: "",
                        unmaskedTo: "",
                      })));
                      setViewModal(true);
                    }}
                  >
                    {canCreateView ? "Create View" : "Request View"}
                  </button>
                )}
              </div>

              {/* Results tabs + content */}
              <div style={{ flex: 1, display: "flex", flexDirection: "column", overflow: "hidden" }}>
                <div style={{ display: "flex", alignItems: "center", gap: 0, borderBottom: "1px solid var(--border)", flexShrink: 0, background: "var(--surface)" }}>
                  {(["results", "profile", "errors", "history"] as ResultTab[]).map((tab) => {
                    const count = tab === "results" ? resultRows.length : tab === "profile" ? profile.length : tab === "errors" ? errors.length : history.length;
                    const active = resultTab === tab;
                    return (
                      <button
                        key={tab}
                        onClick={() => setResultTab(tab)}
                        style={{ padding: "0.35rem 0.8rem", fontSize: "0.75rem", background: "none", border: "none", borderBottom: active ? "2px solid var(--primary)" : "2px solid transparent", color: active ? "var(--text)" : "var(--text-muted)", cursor: "pointer", textTransform: "capitalize", display: "flex", alignItems: "center", gap: "0.3rem" }}
                      >
                        {tab === "history" ? <History size={11} /> : tab === "profile" ? <BarChart2 size={11} /> : null}
                        {tab}
                        {count > 0 && (
                          <span style={{ background: tab === "errors" ? "var(--destructive)" : "var(--primary)", color: "#fff", borderRadius: "8px", fontSize: "0.65rem", padding: "0 0.35rem", lineHeight: "1.4" }}>{count}</span>
                        )}
                      </button>
                    );
                  })}
                  {execMs !== null && (
                    <span style={{ marginLeft: "auto", paddingRight: "0.75rem", fontSize: "0.7rem", color: "var(--text-muted)" }}>{execMs}ms</span>
                  )}
                </div>

                <div style={{ flex: 1, overflow: "auto" }}>
                  {resultTab === "results" && (
                    resultError ? (
                      <pre style={{ margin: "0.75rem", fontSize: "0.8rem", color: "var(--destructive)", whiteSpace: "pre-wrap", fontFamily: "monospace" }}>{resultError}</pre>
                    ) : resultRows.length === 0 ? (
                      <div style={{ padding: "1.5rem", textAlign: "center", color: "var(--text-muted)", fontSize: "0.85rem" }}>
                        {sqlText.trim() ? "No results." : "Write SQL and click Sample to execute."}
                      </div>
                    ) : (() => {
                      const displayCols = resultColumns.length > 0
                        ? resultColumns
                        : resultRows[0] != null ? Object.keys(resultRows[0] as object) : [];
                      return (
                        <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
                          <div style={{ display: "flex", alignItems: "center", gap: "0.5rem", padding: "0.25rem 0.75rem", borderBottom: "1px solid var(--border)", flexShrink: 0, background: "var(--surface)", fontSize: "0.72rem", color: "var(--text-muted)" }}>
                            <button
                              onClick={handleDownloadCsv}
                              style={{ fontSize: "0.72rem", padding: "0.15rem 0.45rem", background: "none", border: "1px solid var(--border)", borderRadius: "3px", color: "var(--text-muted)", cursor: "pointer" }}
                            >
                              ↓ CSV
                            </button>
                            <button
                              onClick={handleCopyResults}
                              title="Copy results as TSV"
                              style={{ fontSize: "0.72rem", padding: "0.15rem 0.45rem", background: "none", border: "1px solid var(--border)", borderRadius: "3px", color: "var(--text-muted)", cursor: "pointer", display: "flex", alignItems: "center", gap: "0.25rem" }}
                            >
                              {copiedResults ? <Check size={11} style={{ color: "var(--approve)" }} /> : <Copy size={11} />}
                              {copiedResults ? "Copied" : "Copy"}
                            </button>
                            <span>{displayRows.length} row{displayRows.length !== 1 ? "s" : ""}{displayRows.length < resultRows.length ? ` (filtered from ${resultRows.length})` : ""}</span>
                            <div style={{ flex: 1 }} />
                            {totalPages > 1 && (
                              <>
                                <button onClick={() => setPage(0)} disabled={page === 0} style={{ background: "none", border: "none", cursor: "pointer", color: page === 0 ? "var(--text-muted)" : "var(--text)", fontSize: "0.75rem" }}>«</button>
                                <button onClick={() => setPage((p) => Math.max(0, p - 1))} disabled={page === 0} style={{ background: "none", border: "none", cursor: "pointer", color: page === 0 ? "var(--text-muted)" : "var(--text)", fontSize: "0.75rem" }}>‹</button>
                                <span>Page {page + 1} / {totalPages}</span>
                                <button onClick={() => setPage((p) => Math.min(totalPages - 1, p + 1))} disabled={page >= totalPages - 1} style={{ background: "none", border: "none", cursor: "pointer", color: page >= totalPages - 1 ? "var(--text-muted)" : "var(--text)", fontSize: "0.75rem" }}>›</button>
                                <button onClick={() => setPage(totalPages - 1)} disabled={page >= totalPages - 1} style={{ background: "none", border: "none", cursor: "pointer", color: page >= totalPages - 1 ? "var(--text-muted)" : "var(--text)", fontSize: "0.75rem" }}>»</button>
                              </>
                            )}
                          </div>
                          <div style={{ flex: 1, overflow: "auto" }}>
                            <table className="data-table sql-results-table" style={{ fontSize: "0.78rem", tableLayout: "fixed", width: "max-content", minWidth: "100%" }}>
                              <thead>
                                <tr>
                                  {displayCols.map((c) => {
                                    const sortIdx = sorts.findIndex((s) => s.col === c);
                                    const sortEntry = sortIdx !== -1 ? sorts[sortIdx] : null;
                                    return (
                                      <th key={c} className={sortEntry ? "col-sorted" : undefined} style={{ width: colWidths[c] ?? autoWidths[c] ?? 140, minWidth: COL_MIN, position: "relative" }}>
                                        <div className="th-label" onClick={() => handleSort(c)}>
                                          <span style={{ overflow: "hidden", textOverflow: "ellipsis", flex: 1 }}>{c}</span>
                                          {sortEntry ? (
                                            <span style={{ display: "flex", alignItems: "center", gap: "0.1rem", flexShrink: 0, fontSize: "0.62rem", color: "var(--primary)" }}>
                                              {sorts.length > 1 && <span style={{ opacity: 0.7 }}>{sortIdx + 1}</span>}
                                              <span>{sortEntry.dir === "asc" ? "▲" : "▼"}</span>
                                            </span>
                                          ) : (
                                            <span style={{ fontSize: "0.6rem", color: "var(--text-muted)", opacity: 0.3 }}>⇅</span>
                                          )}
                                        </div>
                                        <input
                                          className="th-filter"
                                          value={filters[c] ?? ""}
                                          onChange={(e) => { setFilters((prev) => ({ ...prev, [c]: e.target.value })); setPage(0); }}
                                          onClick={(e) => e.stopPropagation()}
                                          placeholder="filter…"
                                        />
                                        <div
                                          onMouseDown={(e) => handleResizeStart(c, e)}
                                          style={{ position: "absolute", right: 0, top: 0, bottom: 0, width: "5px", cursor: "col-resize" }}
                                        />
                                      </th>
                                    );
                                  })}
                                </tr>
                              </thead>
                              <tbody>
                                {pagedRows.map((row, i) => (
                                  <tr key={i}>
                                    {displayCols.map((c) => {
                                      const v = row[c];
                                      const isNum = typeof v === "number";
                                      return (
                                        <td key={c} className={isNum ? "col-num" : undefined} style={{ overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
                                          {v != null ? String(v) : <span className="null-val">null</span>}
                                        </td>
                                      );
                                    })}
                                  </tr>
                                ))}
                              </tbody>
                            </table>
                          </div>
                        </div>
                      );
                    })()
                  )}

                  {resultTab === "profile" && (
                    profile.length === 0 ? (
                      <div style={{ padding: "1.5rem", textAlign: "center", color: "var(--text-muted)", fontSize: "0.85rem" }}>
                        Sample a query to profile the result columns.
                      </div>
                    ) : (
                      <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
                        <div style={{ display: "flex", alignItems: "center", padding: "0.25rem 0.75rem", borderBottom: "1px solid var(--border)", flexShrink: 0, background: "var(--surface)" }}>
                          <button
                            onClick={handleDownloadProfile}
                            style={{ fontSize: "0.72rem", padding: "0.15rem 0.45rem", background: "none", border: "1px solid var(--border)", borderRadius: "3px", color: "var(--text-muted)", cursor: "pointer" }}
                          >
                            ↓ JSON
                          </button>
                        </div>
                        <div style={{ flex: 1, overflow: "auto" }}>
                          <table className="data-table" style={{ fontSize: "0.75rem" }}>
                            <thead>
                              <tr>
                                <th>Column</th>
                                <th title="Rows where value is NULL">Nulls</th>
                                <th title="Rows where value is empty string">Blanks</th>
                                <th title="Number of unique values">Distinct</th>
                                <th title="Column has only one unique value">Constant?</th>
                                <th>Min</th>
                                <th>Max</th>
                                <th title="Mean of numeric values">Mean</th>
                                <th>Top values</th>
                              </tr>
                            </thead>
                            <tbody>
                              {profile.map((p) => {
                                const total = resultRows.length;
                                const nullPct = total > 0 ? Math.round(p.nullCount / total * 100) : 0;
                                const isHighNull = nullPct >= 50;
                                const isConstant = p.constantValue !== undefined;
                                return (
                                  <tr key={p.col}>
                                    <td style={{ fontFamily: "monospace", fontWeight: 600 }}>{p.col}</td>
                                    <td style={{ color: isHighNull ? "var(--destructive)" : p.nullCount > 0 ? "var(--text)" : "var(--text-muted)" }}>
                                      {p.nullCount > 0 ? `${p.nullCount} (${nullPct}%)` : <span style={{ color: "var(--text-muted)" }}>—</span>}
                                    </td>
                                    <td style={{ color: p.blankCount > 0 ? "var(--text)" : "var(--text-muted)" }}>
                                      {p.blankCount > 0 ? p.blankCount : <span style={{ color: "var(--text-muted)" }}>—</span>}
                                    </td>
                                    <td style={{ color: isConstant ? "var(--text-muted)" : "var(--text)" }}>{p.distinctCount}</td>
                                    <td style={{ color: isConstant ? "var(--destructive)" : "var(--text-muted)" }}>
                                      {isConstant ? <span title={String(p.constantValue)}>Yes ({String(p.constantValue).slice(0, 12)})</span> : <span style={{ color: "var(--text-muted)" }}>—</span>}
                                    </td>
                                    <td style={{ fontFamily: "monospace" }}>{p.min !== null ? String(p.min).slice(0, 16) : <span style={{ color: "var(--text-muted)" }}>—</span>}</td>
                                    <td style={{ fontFamily: "monospace" }}>{p.max !== null ? String(p.max).slice(0, 16) : <span style={{ color: "var(--text-muted)" }}>—</span>}</td>
                                    <td style={{ fontFamily: "monospace" }}>{p.mean !== null ? p.mean.toFixed(2) : <span style={{ color: "var(--text-muted)" }}>—</span>}</td>
                                    <td>
                                      <div style={{ display: "flex", flexWrap: "wrap", gap: "0.2rem" }}>
                                        {p.topValues.map(({ value, count }) => (
                                          <span key={value} style={{ background: "var(--surface)", border: "1px solid var(--border)", borderRadius: "3px", padding: "0 0.3rem", fontSize: "0.7rem", fontFamily: "monospace", whiteSpace: "nowrap" }}>
                                            {value.slice(0, 20)}<span style={{ color: "var(--text-muted)" }}>×{count}</span>
                                          </span>
                                        ))}
                                      </div>
                                    </td>
                                  </tr>
                                );
                              })}
                            </tbody>
                          </table>
                        </div>
                      </div>
                    )
                  )}

                  {resultTab === "errors" && (
                    errors.length === 0 ? (
                      <div style={{ padding: "1.5rem", textAlign: "center", color: "var(--text-muted)", fontSize: "0.85rem" }}>No unsupported conditions.</div>
                    ) : (
                      <div style={{ padding: "0.75rem" }}>
                        <p style={{ color: "var(--destructive)", fontSize: "0.8rem", fontWeight: 600, marginBottom: "0.5rem" }}>Unsupported ON conditions — simplify using a view:</p>
                        <ul style={{ margin: 0, paddingLeft: "1.25rem", display: "flex", flexDirection: "column", gap: "0.3rem" }}>
                          {errors.map((e, i) => (
                            <li key={i} style={{ fontSize: "0.8rem", color: "var(--destructive)", fontFamily: "monospace" }}>{e}</li>
                          ))}
                        </ul>
                      </div>
                    )
                  )}

                  {resultTab === "history" && (
                    history.length === 0 ? (
                      <div style={{ padding: "1.5rem", textAlign: "center", color: "var(--text-muted)", fontSize: "0.85rem" }}>No queries run yet. History persists across sessions.</div>
                    ) : (
                      <table className="data-table" style={{ fontSize: "0.75rem" }}>
                        <thead>
                          <tr>
                            <th>Time</th>
                            <th>Role</th>
                            <th>Duration</th>
                            <th>Rows</th>
                            <th style={{ width: "50%" }}>SQL</th>
                            <th></th>
                          </tr>
                        </thead>
                        <tbody>
                          {history.map((h, i) => {
                            const ts = new Date(h.executedAt);
                            const timeLabel = ts.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
                            const dateLabel = ts.toLocaleDateString([], { month: "short", day: "numeric" });
                            const isToday = ts.toDateString() === new Date().toDateString();
                            return (
                              <tr key={i} style={{ verticalAlign: "top" }}>
                                <td style={{ whiteSpace: "nowrap", color: "var(--text-muted)" }}>
                                  <div>{timeLabel}</div>
                                  {!isToday && <div style={{ fontSize: "0.68rem" }}>{dateLabel}</div>}
                                </td>
                                <td style={{ color: "var(--text-muted)", whiteSpace: "nowrap" }}>{h.role}</td>
                                <td style={{ whiteSpace: "nowrap", color: h.error ? "var(--destructive)" : "var(--text-muted)" }}>{h.durationMs}ms</td>
                                <td style={{ whiteSpace: "nowrap", color: h.error ? "var(--destructive)" : "var(--text)" }}>
                                  {h.error ? <span title={h.error}>error</span> : h.rowCount}
                                </td>
                                <td>
                                  <pre style={{ margin: 0, fontSize: "0.72rem", whiteSpace: "pre-wrap", wordBreak: "break-all", color: "var(--text)", maxHeight: "4.5em", overflow: "hidden" }}>{h.sql}</pre>
                                </td>
                                <td style={{ whiteSpace: "nowrap" }}>
                                  <button
                                    className="btn-secondary"
                                    style={{ fontSize: "0.7rem", padding: "0.15rem 0.45rem" }}
                                    onClick={() => { setSqlText(h.sql); setRole(h.role); setResultTab("results"); }}
                                  >
                                    Restore
                                  </button>
                                </td>
                              </tr>
                            );
                          })}
                        </tbody>
                      </table>
                    )
                  )}
                </div>
              </div>
            </>
          </div>
        </div>
      </div>

      {viewModal && (
        <div className="modal-overlay" onClick={() => setViewModal(false)}>
          <div className="modal" style={{ width: "90vw", maxWidth: "90vw", maxHeight: "90vh", display: "flex", flexDirection: "column" }} onClick={(e) => e.stopPropagation()}>
            <div style={{ display: "flex", alignItems: "center", justifyContent: "space-between", marginBottom: "1rem", flexShrink: 0 }}>
              <span style={{ fontWeight: 600, fontSize: "0.9rem" }}>
                {canCreateView ? "Create View" : "Request View"}
              </span>
              <div style={{ display: "flex", alignItems: "center", gap: "0.5rem" }}>
                {viewMsg && (
                  <span style={{ fontSize: "0.78rem", color: viewMsg.startsWith("Error") ? "var(--destructive)" : "var(--approve)" }}>{viewMsg}</span>
                )}
                <button
                  className="btn-primary"
                  onClick={handleSaveView}
                  disabled={viewSaving || !viewId.trim() || !viewDomainId.trim() || viewHasParams}
                  style={{ fontSize: "0.8rem", padding: "0.3rem 0.75rem" }}
                >
                  {viewSaving ? "Saving…" : canCreateView ? "Create" : "Submit Request"}
                </button>
                <button className="modal-close" onClick={() => setViewModal(false)}><X size={14} /></button>
              </div>
            </div>
            {!canCreateView && (
              <p style={{ fontSize: "0.78rem", color: "var(--text-muted)", marginBottom: "0.75rem", flexShrink: 0 }}>
                You do not have <code>create_view</code>. This will be submitted as a suggested view pending approval.
              </p>
            )}
            <div style={{ display: "flex", flexDirection: "column", gap: "0.6rem", overflow: "auto", flex: 1, paddingRight: "1rem", paddingLeft: "2px" }}>
              <div style={{ display: "flex", gap: "0.75rem", flexShrink: 0 }}>
                <label style={{ display: "flex", flexDirection: "column", gap: "0.25rem", fontSize: "0.875rem", color: "var(--text-muted)", flex: 1, minWidth: 0 }}>
                  <span style={{ display: "flex", alignItems: "center", gap: "0.25rem" }}>View ID <span style={{ color: "var(--destructive)" }}>*</span></span>
                  <input
                    value={viewId}
                    onChange={(e) => setViewId(e.target.value)}
                    placeholder="e.g. my_view"
                    style={{ background: "var(--bg)", color: "var(--text)", border: "1px solid var(--border)", padding: "0.5rem", borderRadius: 4, fontSize: "0.875rem", width: "100%", boxSizing: "border-box" }}
                  />
                </label>
                <label style={{ display: "flex", flexDirection: "column", gap: "0.25rem", fontSize: "0.875rem", color: "var(--text-muted)", flex: 1, minWidth: 0 }}>
                  <span style={{ display: "flex", alignItems: "center", gap: "0.25rem" }}>Domain <span style={{ color: "var(--destructive)" }}>*</span></span>
                  <select
                    value={viewDomainId}
                    onChange={(e) => setViewDomainId(e.target.value)}
                    style={{ background: "var(--bg)", color: "var(--text)", border: "1px solid var(--border)", padding: "0.5rem", borderRadius: 4, fontSize: "0.875rem", width: "100%", boxSizing: "border-box" }}
                  >
                    <option value="">— select domain —</option>
                    {Object.values(domainMap).filter((d) => d.id && d.id !== "meta" && d.id !== "ops").map((d) => (
                      <option key={d.id} value={d.id}>{d.id}{d.description ? ` — ${d.description}` : ""}</option>
                    ))}
                  </select>
                </label>
              </div>
              <label style={{ display: "flex", flexDirection: "column", gap: "0.25rem", fontSize: "0.875rem", color: "var(--text-muted)", flexShrink: 0 }}>
                Description
                <textarea
                  value={viewDescription}
                  onChange={(e) => setViewDescription(e.target.value)}
                  placeholder="Optional"
                  rows={2}
                  style={{ resize: "vertical", background: "var(--bg)", color: "var(--text)", border: "1px solid var(--border)", padding: "0.5rem", borderRadius: 4, fontSize: "0.875rem", width: "100%", boxSizing: "border-box" }}
                />
              </label>
              {viewHasParams && (
                <p style={{ fontSize: "0.78rem", color: "var(--destructive)", margin: 0, flexShrink: 0 }}>
                  SQL contains unresolved parameter placeholders ($1, $2, …). Edit the SQL to replace them with literal values.
                </p>
              )}
              <div style={{ resize: "vertical", overflow: "auto", minHeight: 80, height: 120, flexShrink: 0 }}>
                <CodeMirror
                  value={viewSqlNormalized}
                  extensions={viewSqlExtensions}
                  theme={oneDark}
                  editable={false}
                  height="100%"
                  basicSetup={{ lineNumbers: false, foldGutter: false }}
                />
              </div>
              {viewColumns.length > 0 && (
                <div className="column-editor">
                    <div className="column-editor-header">
                      <span className="col-name-header">Column</span>
                      <span className="col-flex-header">Alias</span>
                      <span className="col-flex-header">Description</span>
                      <span className="col-flex-header">Visible To</span>
                      <span className="col-flex-header">Masking</span>
                      <span className="col-flex-header">Scope</span>
                    </div>
                    {viewColumns.map((col, i) => (
                      <Fragment key={col.name}>
                        <div className="column-editor-row">
                          <span className="col-name">{col.name}</span>
                          <input
                            value={col.alias}
                            onChange={(e) => updateViewCol(i, "alias", e.target.value)}
                            placeholder={col.name}
                            className="col-flex-input"
                          />
                          <input
                            value={col.description}
                            onChange={(e) => updateViewCol(i, "description", e.target.value)}
                            placeholder="description"
                            className="col-flex-input"
                          />
                          <MultiSelect
                            options={roles.map((r) => ({ id: r, label: r }))}
                            value={col.visibleTo}
                            onChange={(selected) => updateViewCol(i, "visibleTo", selected)}
                            className="col-flex-input"
                          />
                          <select
                            value={col.maskType}
                            onChange={(e) => updateViewCol(i, "maskType", e.target.value as ViewColumnConfig["maskType"])}
                            className="col-flex-input"
                          >
                            <option value="">None</option>
                            <option value="regex">Regex</option>
                            <option value="constant">Constant</option>
                            <option value="truncate">Truncate</option>
                          </select>
                          <select
                            value={col.scope}
                            onChange={(e) => updateViewCol(i, "scope", e.target.value as ViewColumnConfig["scope"])}
                            className="col-flex-input"
                          >
                            <option value="domain">domain</option>
                            <option value="public">public</option>
                            <option value="restricted">restricted</option>
                          </select>
                        </div>
                        {col.maskType && (
                          <div className="column-editor-row column-mask-row">
                            <span className="col-name" style={{ color: "var(--text-muted)", fontSize: "0.75rem", marginLeft: "0.25rem" }}>↳ masking</span>
                            {col.maskType === "regex" && (
                              <>
                                <input
                                  value={col.maskPattern}
                                  onChange={(e) => updateViewCol(i, "maskPattern", e.target.value)}
                                  placeholder="regex pattern"
                                  className="col-flex-input"
                                />
                                <input
                                  value={col.maskReplace}
                                  onChange={(e) => updateViewCol(i, "maskReplace", e.target.value)}
                                  placeholder="replacement"
                                  className="col-flex-input"
                                />
                              </>
                            )}
                            {col.maskType === "constant" && (
                              <input
                                value={col.maskValue}
                                onChange={(e) => updateViewCol(i, "maskValue", e.target.value)}
                                placeholder="constant value (NULL, 0, ***)"
                                className="col-flex-input"
                              />
                            )}
                            {col.maskType === "truncate" && (
                              <select
                                value={col.maskPrecision}
                                onChange={(e) => updateViewCol(i, "maskPrecision", e.target.value)}
                                className="col-flex-input"
                              >
                                <option value="">Select precision...</option>
                                <option value="year">Year</option>
                                <option value="month">Month</option>
                                <option value="day">Day</option>
                                <option value="hour">Hour</option>
                              </select>
                            )}
                            <input
                              value={col.unmaskedTo}
                              onChange={(e) => updateViewCol(i, "unmaskedTo", e.target.value)}
                              placeholder="unmasked roles (csv)"
                              className="col-flex-input"
                            />
                          </div>
                        )}
                      </Fragment>
                    ))}
                </div>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
