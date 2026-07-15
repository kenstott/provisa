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
import { useTranslation } from "react-i18next";
import {
  ActionIcon,
  Badge,
  Button,
  Group,
  Loader,
  Modal,
  NumberInput,
  SegmentedControl,
  Select,
  Tabs,
  Text,
  TextInput,
} from "@mantine/core";
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
  const { t } = useTranslation();
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
    <Modal
      opened
      onClose={onClose}
      withCloseButton={false}
      centered
      size="90vw"
      styles={{
        content: {
          height: "90vh",
          maxHeight: "90vh",
          display: "flex",
          flexDirection: "column",
          overflow: "hidden",
        },
        body: {
          padding: 0,
          display: "flex",
          flexDirection: "column",
          flex: 1,
          overflow: "hidden",
        },
      }}
    >
      <>
        {/* Header */}
        <Group
          justify="space-between"
          wrap="nowrap"
          style={{
            padding: "0.75rem 1rem",
            borderBottom: "1px solid var(--border)",
            flexShrink: 0,
          }}
        >
          <Group gap="0.75rem" wrap="nowrap">
            <Text fw={600} size="0.9rem" style={{ letterSpacing: "0.02em" }}>
              {t("sqlModelingModal.title")}
            </Text>
            <Text c="dimmed" size="0.75rem">
              {t("sqlModelingModal.description")}
            </Text>
            <SegmentedControl
              size="xs"
              value={topTab}
              onChange={(v) => setTopTab(v as TopTab)}
              data={[
                { value: "sql", label: t("sqlModelingModal.tabSql") },
                { value: "canvas", label: t("sqlModelingModal.tabCanvas") },
              ]}
              data-testid="sql-modeling-top-tab"
            />
          </Group>
          <ActionIcon
            variant="subtle"
            color="gray"
            aria-label={t("sqlModelingModal.close")}
            onClick={onClose}
            data-testid="sql-modeling-close"
          >
            <X size={14} />
          </ActionIcon>
        </Group>

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
                <Group
                  gap="0.5rem"
                  wrap="nowrap"
                  style={{
                    padding: "0.4rem 0.75rem",
                    borderBottom: "1px solid var(--border)",
                    flexShrink: 0,
                    background: "var(--surface)",
                  }}
                >
                  <TextInput
                    value={nlText}
                    placeholder={t("sqlModelingModal.nlPlaceholder")}
                    aria-label={t("sqlModelingModal.nlPlaceholder")}
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
                    rightSection={nlLoading ? <Loader size="xs" /> : null}
                    disabled={nlLoading}
                    error={!!nlError}
                    title={nlError || undefined}
                    aria-busy={nlLoading}
                    size="xs"
                    style={{ flex: 1 }}
                    data-testid="sql-modeling-nl-input"
                  />
                  <Button
                    size="xs"
                    disabled={!nlText.trim() || nlLoading}
                    loading={nlLoading}
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
                    data-testid="sql-modeling-generate-sql"
                  >
                    {nlLoading ? t("sqlModelingModal.generating") : t("sqlModelingModal.generateSql")}
                  </Button>
                </Group>

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
                  <Button
                    className="copy-sql-btn"
                    onClick={handleCopy}
                    aria-label={t("sqlModelingModal.copySql")}
                    title={t("sqlModelingModal.copySql")}
                    variant="filled"
                    color="gray"
                    size="compact-xs"
                    leftSection={
                      copied ? (
                        <Check size={11} style={{ color: "var(--approve)" }} />
                      ) : (
                        <Copy size={11} />
                      )
                    }
                    style={{
                      position: "absolute",
                      top: "0.4rem",
                      right: "0.4rem",
                      opacity: 0,
                      transition: "opacity 0.15s",
                      background: "rgba(30,30,40,0.85)",
                      border: "1px solid var(--border)",
                      fontSize: "0.72rem",
                    }}
                    data-testid="sql-modeling-copy-sql"
                  >
                    {copied ? t("sqlModelingModal.copied") : t("sqlModelingModal.copy")}
                  </Button>
                </div>

                {/* Toolbar */}
                <Group
                  gap="0.5rem"
                  wrap="nowrap"
                  style={{
                    padding: "0.4rem 0.75rem",
                    borderBottom: "1px solid var(--border)",
                    flexShrink: 0,
                    background: "var(--surface)",
                  }}
                >
                  <Button
                    size="xs"
                    leftSection={<Play size={11} />}
                    onClick={handleRun}
                    loading={running}
                    disabled={running || !sqlText.trim()}
                    data-testid="sql-modeling-run"
                  >
                    {running ? t("sqlModelingModal.runningLabel") : t("sqlModelingModal.run")}
                  </Button>
                  <Select
                    size="xs"
                    aria-label={t("sqlModelingModal.sampleModeLabel")}
                    value={sampleMode}
                    onChange={(v) => v && setSampleMode(v as "first" | "last" | "random")}
                    allowDeselect={false}
                    data={[
                      { value: "first", label: t("sqlModelingModal.sampleModeFirst") },
                      { value: "last", label: t("sqlModelingModal.sampleModeLast") },
                      { value: "random", label: t("sqlModelingModal.sampleModeRandom") },
                    ]}
                    w={100}
                    data-testid="sql-modeling-sample-mode"
                  />
                  <NumberInput
                    size="xs"
                    value={sampleSize}
                    min={1}
                    max={10000}
                    onChange={(v) => setSampleSize(Math.max(1, typeof v === "number" ? v : 100))}
                    aria-label={t("sqlModelingModal.sampleSizeLabel")}
                    title={t("sqlModelingModal.sampleSizeLabel")}
                    w={70}
                    data-testid="sql-modeling-sample-size"
                  />
                  <Select
                    size="xs"
                    aria-label={t("sqlModelingModal.roleLabel")}
                    value={role}
                    onChange={(v) => v && setRole(v)}
                    allowDeselect={false}
                    data={roles.map((r) => ({ value: r, label: r }))}
                    w={120}
                    data-testid="sql-modeling-role"
                  />
                  <div style={{ flex: 1 }} />
                  <Button
                    size="xs"
                    variant="default"
                    onClick={handleExtractJoins}
                    disabled={!sqlText.trim()}
                    data-testid="sql-modeling-extract-joins"
                  >
                    {t("sqlModelingModal.extractJoins")}
                  </Button>
                </Group>

                {/* Results tabs + content */}
                <div
                  style={{ flex: 1, display: "flex", flexDirection: "column", overflow: "hidden" }}
                >
                  <Tabs
                    value={resultTab}
                    onChange={(v) => v && setResultTab(v as ResultTab)}
                    keepMounted={false}
                    variant="outline"
                  >
                    <Group
                      justify="space-between"
                      wrap="nowrap"
                      style={{
                        borderBottom: "1px solid var(--border)",
                        flexShrink: 0,
                        background: "var(--surface)",
                      }}
                    >
                      <Tabs.List style={{ border: "none" }}>
                        {(
                          ["results", "profile", "candidates", "errors", "history"] as ResultTab[]
                        ).map((tab) => {
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
                          return (
                            <Tabs.Tab
                              key={tab}
                              value={tab}
                              leftSection={
                                tab === "history" ? (
                                  <History size={11} />
                                ) : tab === "profile" ? (
                                  <BarChart2 size={11} />
                                ) : undefined
                              }
                              rightSection={
                                count > 0 ? (
                                  <Badge
                                    size="xs"
                                    circle
                                    color={tab === "errors" ? "red" : "var(--primary)"}
                                  >
                                    {count}
                                  </Badge>
                                ) : undefined
                              }
                              data-testid={`sql-modeling-tab-${tab}`}
                            >
                              {t(
                                `sqlModelingModal.tab${tab.charAt(0).toUpperCase()}${tab.slice(1)}`,
                              )}
                            </Tabs.Tab>
                          );
                        })}
                      </Tabs.List>
                      {execMs !== null && (
                        <Text
                          size="0.7rem"
                          c="dimmed"
                          style={{ paddingRight: "0.75rem" }}
                        >
                          {t("sqlModelingModal.execMs", { ms: execMs })}
                        </Text>
                      )}
                    </Group>
                  </Tabs>

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
      </>
    </Modal>
  );
}
