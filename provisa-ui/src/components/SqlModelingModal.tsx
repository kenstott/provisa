// Copyright (c) 2026 Kenneth Stott
// Canary: 5a6c46df-f69e-4e4e-8c02-4a34f361ded7
// Canary: PLACEHOLDER
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import React, { useState, useCallback, useMemo, useRef } from "react";
import { X, Play, History, Copy, Check, BarChart2 } from "lucide-react";
import CodeMirror from "@uiw/react-codemirror";
import { sql, PostgreSQL } from "@codemirror/lang-sql";
import { oneDark } from "@codemirror/theme-one-dark";
import type { EditorView } from "@codemirror/view";
import { runSql, nlToSql } from "../api/admin";
import { useRoles, useDomains } from "../hooks/useAdminQueries";
import type { Domain } from "../types/admin";
import type {
  Props,
  ResultTab,
  TopTab,
  ModelingCandidate,
  ColumnProfile,
} from "./sql-modeling/types";
import { normalizeDomain } from "./sql-modeling/types";
import { loadHistory, saveHistory } from "./sql-modeling/history";
import { JoinCanvas } from "./sql-modeling/JoinCanvas";
import { SchemaSidebar } from "./sql-modeling/SchemaSidebar";
import { ResultsPanel } from "./sql-modeling/ResultsPanel";
import { ProfilePanel } from "./sql-modeling/ProfilePanel";
import { CandidatesPanel } from "./sql-modeling/CandidatesPanel";
import { ErrorsPanel } from "./sql-modeling/ErrorsPanel";
import { HistoryPanel } from "./sql-modeling/HistoryPanel";

// ── SqlModelingModal ─────────────────────────────────────────────────────────

export function SqlModelingModal({ tables, existingRels, onClose, onPromote }: Props) {
  const [topTab, setTopTab] = useState<TopTab>("sql");
  const [sqlText, setSqlText] = useState("");
  const [role, setRole] = useState("admin");
  const { roles: roleObjs } = useRoles();
  const { domains } = useDomains();
  const roles = useMemo(() => {
    const ids = roleObjs.map((r) => r.id);
    return ids.length ? ids : ["admin"];
  }, [roleObjs]);
  const domainMap = useMemo(
    () => Object.fromEntries(domains.map((d: Domain) => [normalizeDomain(d.id), d])),
    [domains],
  );
  const [running, setRunning] = useState(false);
  const [sampleMode, setSampleMode] = useState<"first" | "last" | "random">("first");
  const [sampleSize, setSampleSize] = useState(100);
  const [resultTab, setResultTab] = useState<ResultTab>("results");
  const [resultColumns, setResultColumns] = useState<string[]>([]);
  const [resultRows, setResultRows] = useState<Record<string, unknown>[]>([]);
  const [resultError, setResultError] = useState("");
  const [execMs, setExecMs] = useState<number | null>(null);
  const [candidates, setCandidates] = useState<ModelingCandidate[]>([]);
  const [errors, setErrors] = useState<string[]>([]);
  const [expandedDomains, setExpandedDomains] = useState<Set<string>>(new Set());
  const [expandedTables, setExpandedTables] = useState<Set<string>>(new Set());
  const [history, setHistory] = useState(loadHistory);
  const [copied, setCopied] = useState(false);
  const [sorts, setSorts] = useState<{ col: string; dir: "asc" | "desc" }[]>([]);
  const [filters, setFilters] = useState<Record<string, string>>({});
  const [colWidths, setColWidths] = useState<Record<string, number>>({});
  const [nlText, setNlText] = useState("");
  const [nlLoading, setNlLoading] = useState(false);
  const [nlError, setNlError] = useState("");
  const [page, setPage] = useState(0);
  const resizingRef = useRef<{ col: string; startX: number; startW: number } | null>(null);
  const editorViewRef = useRef<EditorView | null>(null);

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

  const tableNameSet = useMemo(
    () => new Set(tables.map((t) => t.tableName.toLowerCase())),
    [tables],
  );

  // Group tables by normalized domain
  const domainGroups = useMemo(() => {
    const groups: Record<string, import("../types/admin").RegisteredTable[]> = {};
    for (const t of tables) {
      const d = t.domainId ? normalizeDomain(t.domainId) : "(no domain)";
      (groups[d] = groups[d] || []).push(t);
    }
    return groups;
  }, [tables]);

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
      if (next.has(d)) next.delete(d);
      else next.add(d);
      return next;
    });

  const toggleTable = (t: string) =>
    setExpandedTables((prev) => {
      const next = new Set(prev);
      if (next.has(t)) next.delete(t);
      else next.add(t);
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
      if (prev[idx].dir === "asc")
        return prev.map((s, i) => (i === idx ? { ...s, dir: "desc" } : s));
      return prev.filter((_, i) => i !== idx); // desc → natural (remove)
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
          const av = a[col],
            bv = b[col];
          if (av == null && bv == null) continue;
          let cmp: number;
          if (av == null) {
            cmp = 1;
          } else if (bv == null) {
            cmp = -1;
          } else if (typeof av === "number" && typeof bv === "number") {
            cmp = av - bv;
          } else {
            cmp = String(av).localeCompare(String(bv));
          }
          if (cmp !== 0) return dir === "asc" ? cmp : -cmp;
        }
        return 0;
      });
    }
    return rows;
  }, [resultRows, resultColumns, filters, sorts]);

  const handleDownloadCsv = useCallback(() => {
    const cols = resultColumns.length > 0 ? resultColumns : Object.keys(resultRows[0] ?? {});
    const escape = (v: unknown) => {
      const s = v == null ? "" : String(v);
      return s.includes(",") || s.includes('"') || s.includes("\n")
        ? `"${s.replace(/"/g, '""')}"`
        : s;
    };
    const lines = [cols.map(escape).join(",")];
    for (const row of displayRows) lines.push(cols.map((c) => escape(row[c])).join(","));
    const blob = new Blob([lines.join("\n")], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "results.csv";
    a.click();
    URL.revokeObjectURL(url);
  }, [displayRows, resultColumns, resultRows]);

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
      return {
        col,
        nullCount,
        blankCount,
        distinctCount,
        constantValue,
        min,
        max,
        mean,
        topValues,
      };
    });
  }, [resultRows, resultColumns]);

  const handleDownloadProfile = useCallback(() => {
    const blob = new Blob([JSON.stringify(profile, null, 2)], { type: "application/json" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "profile.json";
    a.click();
    URL.revokeObjectURL(url);
  }, [profile]);

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
    const result = await runSql(sampledSql, role, true);
    const durationMs = Math.round(performance.now() - t0);
    setExecMs(durationMs);
    if (result.error) {
      setResultError(result.error);
      setResultColumns([]);
      setResultRows([]);
    } else {
      setResultColumns(result.columns);
      setResultRows(result.rows);
    }
    setSorts([]);
    setFilters({});
    setColWidths({});
    setPage(0);
    setResultTab("results");
    setRunning(false);
    const entry = {
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

  const splitTopLevelAnd = (clause: string): string[] => {
    const parts: string[] = [];
    let depth = 0,
      start = 0,
      i = 0;
    while (i < clause.length) {
      const ch = clause[i];
      if (ch === "(") {
        depth++;
        i++;
        continue;
      }
      if (ch === ")") {
        depth--;
        i++;
        continue;
      }
      if (depth === 0 && /^and\b/i.test(clause.slice(i))) {
        parts.push(clause.slice(start, i).trim());
        i += 3;
        start = i;
        continue;
      }
      i++;
    }
    parts.push(clause.slice(start).trim());
    return parts.filter(Boolean);
  };

  const handleExtractJoins = useCallback(() => {
    const aliasMap: Record<string, string> = {};
    // matches: FROM/JOIN [schema.]table [alias] — handles double-quoted identifiers
    const tableRefRe =
      /(?:from|join)\s+(?:(?:"[^"]+"|[\w$]+)\.)?(?:"([^"]+)"|([\w$]+))(?:\s+(?:as\s+)?(?!"[^"]*")([\w$]+))?/gi;
    let m: RegExpExecArray | null;
    while ((m = tableRefRe.exec(sqlText)) !== null) {
      const tbl = (m[1] || m[2]).toLowerCase();
      const alias = (m[3] || tbl).toLowerCase();
      aliasMap[alias] = tbl;
      aliasMap[tbl] = tbl;
    }
    const colRef = String.raw`(?:"[^"]+"|[\w$]+)\.(?:"[^"]+"|[\w$]+)`;
    const castRef = String.raw`cast\s*\(\s*${colRef}\s+as\s+[\w$]+\s*\)`;
    const colRefCapture = String.raw`(?:(?:"([^"]+)"|([\w$]+))\.(?:"([^"]+)"|([\w$]+)))`;
    const castRefCapture = String.raw`cast\s*\(\s*(?:(?:"([^"]+)"|([\w$]+))\.(?:"([^"]+)"|([\w$]+)))\s+as\s+[\w$]+\s*\)`;
    const stripCast = (s: string): [string, string] | null => {
      const t = s.trim();
      const cm = new RegExp(`^${castRefCapture}$`, "i").exec(t);
      if (cm) return [cm[1] || cm[2], cm[3] || cm[4]];
      const pm = new RegExp(`^${colRefCapture}$`).exec(t);
      if (pm) return [pm[1] || pm[2], pm[3] || pm[4]];
      return null;
    };
    const eqToken = `(?:${castRef}|${colRef})`;
    const eqRe = new RegExp(`^(${eqToken})\\s*=\\s*(${eqToken})$`, "i");
    const newCandidates: ModelingCandidate[] = [];
    const newErrors: string[] = [];
    const findExisting = (
      lt: string,
      lc: string,
      rt: string,
      rc: string,
    ): import("../types/admin").Relationship | undefined =>
      existingRels.find(
        (r) =>
          (r.sourceTableName === lt &&
            r.sourceColumn === lc &&
            r.targetTableName === rt &&
            r.targetColumn === rc) ||
          (r.sourceTableName === rt &&
            r.sourceColumn === rc &&
            r.targetTableName === lt &&
            r.targetColumn === lc),
      );
    const onBlockRe =
      /\bon\s+(.*?)(?=\s+(?:inner|left|right|full|cross|join|where|group|order|having|limit)\b|$)/gi;
    while ((m = onBlockRe.exec(sqlText)) !== null) {
      for (const cond of splitTopLevelAnd(m[1].trim())) {
        const eq = eqRe.exec(cond.trim());
        if (!eq) {
          newErrors.push(cond.trim());
          continue;
        }
        const lhs = stripCast(eq[1]);
        const rhs = stripCast(eq[2]);
        if (!lhs || !rhs) {
          newErrors.push(cond.trim());
          continue;
        }
        const [la, lc] = lhs,
          [ra, rc] = rhs;
        const lt = aliasMap[la.toLowerCase()] || la.toLowerCase();
        const rt = aliasMap[ra.toLowerCase()] || ra.toLowerCase();
        const existingRel = findExisting(lt, lc, rt, rc);
        if (existingRel) continue;
        newCandidates.push({
          id: `${lt}-${lc}-to-${rt}`,
          sourceTable: lt,
          sourceCol: lc,
          targetTable: rt,
          targetCol: rc,
          cardinality: "many-to-one",
          promoted: false,
        });
      }
    }
    setCandidates(newCandidates);
    setErrors(newErrors);
    setResultTab(newErrors.length > 0 && newCandidates.length === 0 ? "errors" : "candidates");
  }, [sqlText, existingRels]);

  const handlePromote = useCallback(
    async (idx: number) => {
      if (!onPromote) return;
      await onPromote(candidates[idx]);
      setCandidates((prev) => prev.map((c, i) => (i === idx ? { ...c, promoted: true } : c)));
    },
    [candidates, onPromote],
  );

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div
        className="modal"
        style={{
          width: "90vw",
          maxWidth: "90vw",
          height: "90vh",
          maxHeight: "90vh",
          padding: 0,
          display: "flex",
          flexDirection: "column",
          overflow: "hidden",
        }}
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header */}
        <div
          style={{
            display: "flex",
            alignItems: "center",
            justifyContent: "space-between",
            padding: "0.75rem 1rem",
            borderBottom: "1px solid var(--border)",
            flexShrink: 0,
          }}
        >
          <div style={{ display: "flex", alignItems: "center", gap: "0.75rem" }}>
            <span style={{ fontWeight: 600, fontSize: "0.9rem", letterSpacing: "0.02em" }}>
              SQL Modeling
            </span>
            <span style={{ color: "var(--text-muted)", fontSize: "0.75rem" }}>
              Extract JOIN conditions as new relationship candidates — existing relationships are excluded
            </span>
            {/* SQL | Canvas toggle */}
            <div
              style={{
                display: "flex",
                alignItems: "center",
                gap: 0,
                border: "1px solid var(--border)",
                borderRadius: "5px",
                overflow: "hidden",
                marginLeft: "0.5rem",
              }}
            >
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
                >
                  {tab}
                </button>
              ))}
            </div>
          </div>
          <button className="modal-close" onClick={onClose}>
            <X size={14} />
          </button>
        </div>

        {/* Body: sidebar + right pane */}
        <div style={{ display: "flex", flex: 1, overflow: "hidden" }}>
          <SchemaSidebar
            domainGroups={domainGroups}
            expandedDomains={expandedDomains}
            toggleDomain={toggleDomain}
            domainMap={domainMap}
            expandedTables={expandedTables}
            toggleTable={toggleTable}
            topTab={topTab}
            insertAtCursor={insertAtCursor}
          />

          {/* Right pane */}
          <div style={{ flex: 1, display: "flex", flexDirection: "column", overflow: "hidden" }}>
            <div
              style={{
                display: topTab === "canvas" ? "flex" : "none",
                flex: 1,
                overflow: "hidden",
                flexDirection: "column",
              }}
            >
              <JoinCanvas
                tables={tables}
                existingRels={existingRels}
                onGenerateSql={(generatedSql) => {
                  setSqlText(generatedSql);
                  setTopTab("sql");
                }}
              />
            </div>

            <div
              style={{
                display: topTab === "sql" ? "flex" : "none",
                flex: 1,
                overflow: "hidden",
                flexDirection: "column",
              }}
            >
              <>
                {/* NL prompt bar */}
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
                  <div style={{ flex: 1, position: "relative", minWidth: 0 }}>
                    <input
                      type="text"
                      value={nlText}
                      placeholder="Ask in plain English — generates SQL…"
                      onChange={(e) => {
                        setNlText(e.target.value);
                        setNlError("");
                      }}
                      onKeyDown={async (e) => {
                        if (e.key === "Enter" && nlText.trim() && !nlLoading) {
                          setNlLoading(true);
                          setNlError("");
                          const result = await nlToSql(nlText.trim(), role);
                          setNlLoading(false);
                          if (result.error) {
                            setNlError(result.error);
                          } else {
                            setSqlText(result.sql);
                          }
                        }
                      }}
                      style={{
                        width: "100%",
                        fontSize: "0.8rem",
                        padding: "0.25rem 1.6rem 0.25rem 0.5rem",
                        borderRadius: "4px",
                        border: nlError
                          ? "1px solid var(--destructive)"
                          : "1px solid var(--border)",
                        background: "var(--bg)",
                        color: "var(--text)",
                        outline: "none",
                        opacity: nlLoading ? 0.6 : 1,
                      }}
                      disabled={nlLoading}
                      title={nlError || undefined}
                    />
                    {nlLoading && (
                      <span
                        style={{
                          position: "absolute",
                          right: "0.4rem",
                          top: "50%",
                          transform: "translateY(-50%)",
                          fontSize: "0.7rem",
                          color: "var(--text-muted)",
                        }}
                      >
                        …
                      </span>
                    )}
                  </div>
                  <button
                    className="btn-primary"
                    style={{ fontSize: "0.78rem", padding: "0.25rem 0.6rem", flexShrink: 0 }}
                    disabled={!nlText.trim() || nlLoading}
                    onClick={async () => {
                      if (!nlText.trim() || nlLoading) return;
                      setNlLoading(true);
                      setNlError("");
                      const result = await nlToSql(nlText.trim(), role);
                      setNlLoading(false);
                      if (result.error) {
                        setNlError(result.error);
                      } else {
                        setSqlText(result.sql);
                      }
                    }}
                  >
                    {nlLoading ? "Generating…" : "Generate SQL"}
                  </button>
                </div>

                {/* Editor */}
                <div
                  style={{
                    flex: "0 0 220px",
                    overflow: "hidden",
                    borderBottom: "1px solid var(--border)",
                    position: "relative",
                  }}
                  onMouseEnter={(e) => {
                    const btn = e.currentTarget.querySelector<HTMLElement>(".copy-sql-btn");
                    if (btn) btn.style.opacity = "1";
                  }}
                  onMouseLeave={(e) => {
                    const btn = e.currentTarget.querySelector<HTMLElement>(".copy-sql-btn");
                    if (btn) btn.style.opacity = "0";
                  }}
                >
                  <CodeMirror
                    value={sqlText}
                    height="220px"
                    theme={oneDark}
                    extensions={sqlExtensions}
                    onChange={(v) => setSqlText(v)}
                    onCreateEditor={(view) => {
                      editorViewRef.current = view;
                    }}
                    style={{ fontSize: "0.8rem" }}
                  />
                  <button
                    className="copy-sql-btn"
                    onClick={handleCopy}
                    title="Copy SQL"
                    style={{
                      position: "absolute",
                      top: "0.4rem",
                      right: "0.4rem",
                      opacity: 0,
                      transition: "opacity 0.15s",
                      background: "rgba(30,30,40,0.85)",
                      border: "1px solid var(--border)",
                      borderRadius: "4px",
                      color: "var(--text-muted)",
                      cursor: "pointer",
                      padding: "0.2rem 0.35rem",
                      display: "flex",
                      alignItems: "center",
                      gap: "0.25rem",
                      fontSize: "0.72rem",
                    }}
                  >
                    {copied ? (
                      <Check size={11} style={{ color: "var(--approve)" }} />
                    ) : (
                      <Copy size={11} />
                    )}
                    {copied ? "Copied" : "Copy"}
                  </button>
                </div>

                {/* Toolbar */}
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
                    style={{
                      display: "flex",
                      alignItems: "center",
                      gap: "0.3rem",
                      fontSize: "0.8rem",
                      padding: "0.25rem 0.6rem",
                    }}
                    onClick={handleRun}
                    disabled={running || !sqlText.trim()}
                  >
                    <Play size={11} />
                    {running ? "Running…" : "Sample >"}
                  </button>
                  <select
                    value={sampleMode}
                    onChange={(e) => setSampleMode(e.target.value as "first" | "last" | "random")}
                    style={{
                      fontSize: "0.78rem",
                      padding: "0.2rem 0.4rem",
                      background: "var(--bg)",
                      color: "var(--text)",
                      border: "1px solid var(--border)",
                      borderRadius: "3px",
                    }}
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
                    style={{
                      width: "60px",
                      fontSize: "0.78rem",
                      padding: "0.2rem 0.4rem",
                      background: "var(--bg)",
                      color: "var(--text)",
                      border: "1px solid var(--border)",
                      borderRadius: "3px",
                    }}
                    title="Row count"
                  />
                  <select
                    value={role}
                    onChange={(e) => setRole(e.target.value)}
                    style={{
                      fontSize: "0.78rem",
                      padding: "0.2rem 0.4rem",
                      background: "var(--bg)",
                      color: "var(--text)",
                      border: "1px solid var(--border)",
                      borderRadius: "3px",
                    }}
                  >
                    {roles.map((r) => (
                      <option key={r} value={r}>
                        {r}
                      </option>
                    ))}
                  </select>
                  <div style={{ flex: 1 }} />
                  <button
                    className="btn-secondary"
                    style={{ fontSize: "0.78rem", padding: "0.25rem 0.6rem", flexShrink: 0 }}
                    onClick={handleExtractJoins}
                    disabled={!sqlText.trim()}
                  >
                    Extract Joins
                  </button>
                </div>

                {/* Results tabs + content */}
                <div
                  style={{ flex: 1, display: "flex", flexDirection: "column", overflow: "hidden" }}
                >
                  <div
                    style={{
                      display: "flex",
                      alignItems: "center",
                      gap: 0,
                      borderBottom: "1px solid var(--border)",
                      flexShrink: 0,
                      background: "var(--surface)",
                    }}
                  >
                    {(["results", "profile", "candidates", "errors", "history"] as ResultTab[]).map(
                      (tab) => {
                        const count =
                          tab === "results"
                            ? resultRows.length
                            : tab === "profile"
                              ? profile.length
                              : tab === "candidates"
                                ? candidates.length
                                : tab === "errors"
                                  ? errors.length
                                  : history.length;
                        const active = resultTab === tab;
                        return (
                          <button
                            key={tab}
                            onClick={() => setResultTab(tab)}
                            style={{
                              padding: "0.35rem 0.8rem",
                              fontSize: "0.75rem",
                              background: "none",
                              border: "none",
                              borderBottom: active
                                ? "2px solid var(--primary)"
                                : "2px solid transparent",
                              color: active ? "var(--text)" : "var(--text-muted)",
                              cursor: "pointer",
                              textTransform: "capitalize",
                              display: "flex",
                              alignItems: "center",
                              gap: "0.3rem",
                            }}
                          >
                            {tab === "history" ? (
                              <History size={11} />
                            ) : tab === "profile" ? (
                              <BarChart2 size={11} />
                            ) : null}
                            {tab}
                            {count > 0 && (
                              <span
                                style={{
                                  background:
                                    tab === "errors" ? "var(--destructive)" : "var(--primary)",
                                  color: "#fff",
                                  borderRadius: "8px",
                                  fontSize: "0.65rem",
                                  padding: "0 0.35rem",
                                  lineHeight: "1.4",
                                }}
                              >
                                {count}
                              </span>
                            )}
                          </button>
                        );
                      },
                    )}
                    {execMs !== null && (
                      <span
                        style={{
                          marginLeft: "auto",
                          paddingRight: "0.75rem",
                          fontSize: "0.7rem",
                          color: "var(--text-muted)",
                        }}
                      >
                        {execMs}ms
                      </span>
                    )}
                  </div>

                  <div style={{ flex: 1, overflow: "auto" }}>
                    {resultTab === "results" && (
                      <ResultsPanel
                        resultError={resultError}
                        resultRows={resultRows}
                        resultColumns={resultColumns}
                        displayRows={displayRows}
                        page={page}
                        setPage={setPage}
                        sorts={sorts}
                        colWidths={colWidths}
                        filters={filters}
                        setFilters={setFilters}
                        handleSort={handleSort}
                        handleResizeStart={handleResizeStart}
                        handleDownloadCsv={handleDownloadCsv}
                        sqlText={sqlText}
                      />
                    )}
                    {resultTab === "profile" && (
                      <ProfilePanel
                        profile={profile}
                        resultRows={resultRows}
                        handleDownloadProfile={handleDownloadProfile}
                      />
                    )}
                    {resultTab === "candidates" && (
                      <CandidatesPanel
                        candidates={candidates}
                        setCandidates={setCandidates}
                        tableNameSet={tableNameSet}
                        onPromote={onPromote}
                        handlePromote={handlePromote}
                      />
                    )}
                    {resultTab === "errors" && <ErrorsPanel errors={errors} />}
                    {resultTab === "history" && (
                      <HistoryPanel
                        history={history}
                        setSqlText={setSqlText}
                        setRole={setRole}
                        setResultTab={setResultTab}
                      />
                    )}
                  </div>
                </div>
              </>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}
