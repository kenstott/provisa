// Copyright (c) 2026 Kenneth Stott
// Canary: e203b774-09b9-4f3a-a172-efc74bdcf20b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import React from "react";
import { useTranslation } from "react-i18next";
import { ActionIcon, Badge, Box, Button, Table, Tabs, Text, TextInput } from "@mantine/core";
import { History, BarChart2, Copy, Check, ChevronsLeft, ChevronLeft, ChevronRight, ChevronsRight } from "lucide-react";
import { COL_MIN, PAGE_SIZE } from "./types";
import type { ResultTab, HistoryEntry, ColumnProfile } from "./types";

interface ResultsPanelProps {
  resultTab: ResultTab;
  setResultTab: React.Dispatch<React.SetStateAction<ResultTab>>;
  running: boolean;
  resultError: string;
  resultRows: Record<string, unknown>[];
  resultColumns: string[];
  sorts: { col: string; dir: "asc" | "desc" }[];
  filters: Record<string, string>;
  setFilters: React.Dispatch<React.SetStateAction<Record<string, string>>>;
  page: number;
  setPage: React.Dispatch<React.SetStateAction<number>>;
  displayRows: Record<string, unknown>[];
  pagedRows: Record<string, unknown>[];
  totalPages: number;
  colWidths: Record<string, number>;
  autoWidths: Record<string, number>;
  copiedResults: boolean;
  profile: ColumnProfile[];
  errors: string[];
  history: HistoryEntry[];
  queryStats: unknown;
  sqlText: string;
  setSqlText: React.Dispatch<React.SetStateAction<string>>;
  setRole: React.Dispatch<React.SetStateAction<string>>;
  handleDownloadCsv: () => void;
  handleCopyResults: () => void;
  handleSort: (col: string) => void;
  handleResizeStart: (col: string, e: React.MouseEvent) => void;
  handleDownloadProfile: () => void;
}

export function ResultsPanel({
  resultTab,
  setResultTab,
  running,
  resultError,
  resultRows,
  resultColumns,
  sorts,
  filters,
  setFilters,
  page,
  setPage,
  displayRows,
  pagedRows,
  totalPages,
  colWidths,
  autoWidths,
  copiedResults,
  profile,
  errors,
  history,
  queryStats,
  sqlText,
  setSqlText,
  setRole,
  handleDownloadCsv,
  handleCopyResults,
  handleSort,
  handleResizeStart,
  handleDownloadProfile,
}: ResultsPanelProps) {
  const { t } = useTranslation();

  const tabLabels: Record<ResultTab, string> = {
    results: t("sqlResultsPanel.tabResults"),
    profile: t("sqlResultsPanel.tabProfile"),
    errors: t("sqlResultsPanel.tabErrors"),
    history: t("sqlResultsPanel.tabHistory"),
    stats: t("sqlResultsPanel.tabStats"),
  };

  return (
    <div style={{ flex: 1, display: "flex", flexDirection: "column", overflow: "hidden" }}>
      <Tabs
        value={resultTab}
        onChange={(value) => value && setResultTab(value as ResultTab)}
        variant="outline"
        keepMounted={false}
        style={{ flexShrink: 0 }}
      >
        <Tabs.List>
          {(["results", "profile", "errors", "history", "stats"] as ResultTab[]).map((tab) => {
            const count =
              tab === "results"
                ? resultRows.length
                : tab === "profile"
                  ? profile.length
                  : tab === "errors"
                    ? errors.length
                    : tab === "stats"
                      ? 0
                      : history.length;
            if (tab === "stats" && !queryStats) return null;
            return (
              <Tabs.Tab
                key={tab}
                value={tab}
                data-testid={`results-tab-${tab}`}
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
                      color={tab === "errors" ? "red" : "blue"}
                    >
                      {count}
                    </Badge>
                  ) : undefined
                }
              >
                {tabLabels[tab]}
              </Tabs.Tab>
            );
          })}
        </Tabs.List>
      </Tabs>

      <div style={{ flex: 1, overflow: "auto" }}>
        {resultTab === "results" &&
          (running ? (
            <div
              style={{
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
                padding: "2rem",
                gap: "0.5rem",
                color: "var(--text-muted)",
                fontSize: "0.85rem",
              }}
            >
              <span className="btn-spinner" style={{ flexShrink: 0 }} />
              {t("sqlResultsPanel.running")}
            </div>
          ) : resultError ? (
            <pre
              style={{
                margin: "0.75rem",
                fontSize: "0.8rem",
                color: "var(--destructive)",
                whiteSpace: "pre-wrap",
                fontFamily: "monospace",
              }}
            >
              {resultError}
            </pre>
          ) : resultRows.length === 0 ? (
            <div
              style={{
                padding: "1.5rem",
                textAlign: "center",
                color: "var(--text-muted)",
                fontSize: "0.85rem",
              }}
            >
              {sqlText.trim() ? t("sqlResultsPanel.noResults") : t("sqlResultsPanel.writeSqlPrompt")}
            </div>
          ) : (
            (() => {
              const displayCols =
                resultColumns.length > 0
                  ? resultColumns
                  : resultRows[0] != null
                    ? Object.keys(resultRows[0] as object)
                    : [];
              return (
                <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
                  <div
                    style={{
                      display: "flex",
                      alignItems: "center",
                      gap: "0.5rem",
                      padding: "0.25rem 0.75rem",
                      borderBottom: "1px solid var(--border)",
                      flexShrink: 0,
                      background: "var(--surface)",
                      fontSize: "0.72rem",
                      color: "var(--text-muted)",
                    }}
                  >
                    <Button
                      variant="default"
                      size="compact-xs"
                      onClick={handleDownloadCsv}
                      data-testid="download-csv-btn"
                    >
                      {t("sqlResultsPanel.downloadCsv")}
                    </Button>
                    <Button
                      variant="default"
                      size="compact-xs"
                      onClick={handleCopyResults}
                      title={t("sqlResultsPanel.copyResultsTitle")}
                      data-testid="copy-results-btn"
                      leftSection={
                        copiedResults ? (
                          <Check size={11} style={{ color: "var(--approve)" }} />
                        ) : (
                          <Copy size={11} />
                        )
                      }
                    >
                      {copiedResults ? t("sqlResultsPanel.copied") : t("sqlResultsPanel.copy")}
                    </Button>
                    <Text component="span" size="xs" c="dimmed">
                      {t("sqlResultsPanel.rowCount", { count: displayRows.length })}
                      {displayRows.length < resultRows.length
                        ? t("sqlResultsPanel.filteredFrom", { count: resultRows.length })
                        : ""}
                    </Text>
                    <div style={{ flex: 1 }} />
                    {totalPages > 1 && (
                      <>
                        <ActionIcon
                          variant="subtle"
                          size="sm"
                          aria-label={t("sqlResultsPanel.firstPage")}
                          onClick={() => setPage(0)}
                          disabled={page === 0}
                        >
                          <ChevronsLeft size={13} />
                        </ActionIcon>
                        <ActionIcon
                          variant="subtle"
                          size="sm"
                          aria-label={t("sqlResultsPanel.previousPage")}
                          onClick={() => setPage((p) => Math.max(0, p - 1))}
                          disabled={page === 0}
                        >
                          <ChevronLeft size={13} />
                        </ActionIcon>
                        <Text component="span" size="xs">
                          {t("sqlResultsPanel.pageIndicator", { page: page + 1, total: totalPages })}
                        </Text>
                        <ActionIcon
                          variant="subtle"
                          size="sm"
                          aria-label={t("sqlResultsPanel.nextPage")}
                          onClick={() => setPage((p) => Math.min(totalPages - 1, p + 1))}
                          disabled={page >= totalPages - 1}
                        >
                          <ChevronRight size={13} />
                        </ActionIcon>
                        <ActionIcon
                          variant="subtle"
                          size="sm"
                          aria-label={t("sqlResultsPanel.lastPage")}
                          onClick={() => setPage(totalPages - 1)}
                          disabled={page >= totalPages - 1}
                        >
                          <ChevronsRight size={13} />
                        </ActionIcon>
                      </>
                    )}
                  </div>
                  <div style={{ flex: 1, overflow: "auto" }}>
                    <table
                      className="data-table sql-results-table"
                      style={{
                        fontSize: "0.78rem",
                        tableLayout: "fixed",
                        width: "max-content",
                        minWidth: "100%",
                      }}
                    >
                      <thead>
                        <tr>
                          {displayCols.map((c) => {
                            const sortIdx = sorts.findIndex((s) => s.col === c);
                            const sortEntry = sortIdx !== -1 ? sorts[sortIdx] : null;
                            return (
                              <th
                                key={c}
                                className={sortEntry ? "col-sorted" : undefined}
                                style={{
                                  width: colWidths[c] ?? autoWidths[c] ?? 140,
                                  minWidth: COL_MIN,
                                  position: "relative",
                                }}
                              >
                                <div className="th-label" onClick={() => handleSort(c)}>
                                  <span
                                    style={{
                                      overflow: "hidden",
                                      textOverflow: "ellipsis",
                                      flex: 1,
                                    }}
                                  >
                                    {c}
                                  </span>
                                  {sortEntry ? (
                                    <span
                                      style={{
                                        display: "flex",
                                        alignItems: "center",
                                        gap: "0.1rem",
                                        flexShrink: 0,
                                        fontSize: "0.62rem",
                                        color: "var(--primary)",
                                      }}
                                    >
                                      {sorts.length > 1 && (
                                        <span style={{ opacity: 0.7 }}>
                                          {sortIdx + 1}
                                        </span>
                                      )}
                                      <span>{sortEntry.dir === "asc" ? "▲" : "▼"}</span>
                                    </span>
                                  ) : (
                                    <span
                                      style={{
                                        fontSize: "0.6rem",
                                        color: "var(--text-muted)",
                                        opacity: 0.3,
                                      }}
                                    >
                                      ⇅
                                    </span>
                                  )}
                                </div>
                                <TextInput
                                  size="xs"
                                  variant="unstyled"
                                  className="th-filter"
                                  aria-label={`${t("sqlResultsPanel.filterPlaceholder")} ${c}`}
                                  value={filters[c] ?? ""}
                                  onChange={(e) => {
                                    setFilters((prev) => ({
                                      ...prev,
                                      [c]: e.currentTarget.value,
                                    }));
                                    setPage(0);
                                  }}
                                  onClick={(e) => e.stopPropagation()}
                                  placeholder={t("sqlResultsPanel.filterPlaceholder")}
                                />
                                <div
                                  role="separator"
                                  aria-label={t("sqlResultsPanel.resizeColumn")}
                                  onMouseDown={(e) => handleResizeStart(c, e)}
                                  style={{
                                    position: "absolute",
                                    right: 0,
                                    top: 0,
                                    bottom: 0,
                                    width: "5px",
                                    cursor: "col-resize",
                                  }}
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
                                <td
                                  key={c}
                                  className={isNum ? "col-num" : undefined}
                                  style={{
                                    overflow: "hidden",
                                    textOverflow: "ellipsis",
                                    whiteSpace: "nowrap",
                                  }}
                                >
                                  {v != null ? (
                                    String(v)
                                  ) : (
                                    <span className="null-val">{t("sqlResultsPanel.nullValue")}</span>
                                  )}
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
          ))}

        {resultTab === "profile" &&
          (profile.length === 0 ? (
            <div
              style={{
                padding: "1.5rem",
                textAlign: "center",
                color: "var(--text-muted)",
                fontSize: "0.85rem",
              }}
            >
              {t("sqlResultsPanel.profileEmpty")}
            </div>
          ) : (
            <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
              <div
                style={{
                  display: "flex",
                  alignItems: "center",
                  padding: "0.25rem 0.75rem",
                  borderBottom: "1px solid var(--border)",
                  flexShrink: 0,
                  background: "var(--surface)",
                }}
              >
                <Button
                  variant="default"
                  size="compact-xs"
                  onClick={handleDownloadProfile}
                  data-testid="download-profile-btn"
                >
                  {t("sqlResultsPanel.downloadJson")}
                </Button>
              </div>
              <div style={{ flex: 1, overflow: "auto" }}>
                <Table className="data-table" style={{ fontSize: "0.75rem" }}>
                  <Table.Thead>
                    <Table.Tr>
                      <Table.Th>{t("sqlResultsPanel.colColumn")}</Table.Th>
                      <Table.Th title={t("sqlResultsPanel.colNullsTitle")}>{t("sqlResultsPanel.colNulls")}</Table.Th>
                      <Table.Th title={t("sqlResultsPanel.colBlanksTitle")}>{t("sqlResultsPanel.colBlanks")}</Table.Th>
                      <Table.Th title={t("sqlResultsPanel.colDistinctTitle")}>{t("sqlResultsPanel.colDistinct")}</Table.Th>
                      <Table.Th title={t("sqlResultsPanel.colConstantTitle")}>{t("sqlResultsPanel.colConstant")}</Table.Th>
                      <Table.Th>{t("sqlResultsPanel.colMin")}</Table.Th>
                      <Table.Th>{t("sqlResultsPanel.colMax")}</Table.Th>
                      <Table.Th title={t("sqlResultsPanel.colMeanTitle")}>{t("sqlResultsPanel.colMean")}</Table.Th>
                      <Table.Th>{t("sqlResultsPanel.colTopValues")}</Table.Th>
                    </Table.Tr>
                  </Table.Thead>
                  <Table.Tbody>
                    {profile.map((p) => {
                      const total = resultRows.length;
                      const nullPct =
                        total > 0 ? Math.round((p.nullCount / total) * 100) : 0;
                      const isHighNull = nullPct >= 50;
                      const isConstant = p.constantValue !== undefined;
                      return (
                        <Table.Tr key={p.col}>
                          <Table.Td style={{ fontFamily: "monospace", fontWeight: 600 }}>
                            {p.col}
                          </Table.Td>
                          <Table.Td>
                            <div style={{ display: "flex", alignItems: "center", gap: "0.4rem" }}>
                              <div
                                style={{
                                  width: 52,
                                  height: 5,
                                  borderRadius: 3,
                                  background: "var(--border)",
                                  position: "relative",
                                  flexShrink: 0,
                                }}
                              >
                                {p.nullCount > 0 && (
                                  <div
                                    style={{
                                      position: "absolute",
                                      left: 0,
                                      top: 0,
                                      bottom: 0,
                                      width: `${nullPct}%`,
                                      borderRadius: 3,
                                      background: isHighNull
                                        ? "var(--destructive)"
                                        : "var(--text-muted)",
                                    }}
                                  />
                                )}
                              </div>
                              <span
                                style={{
                                  color: isHighNull
                                    ? "var(--destructive)"
                                    : p.nullCount > 0
                                      ? "var(--text)"
                                      : "var(--text-muted)",
                                  fontSize: "0.7rem",
                                }}
                              >
                                {p.nullCount > 0 ? `${nullPct}%` : t("sqlResultsPanel.dash")}
                              </span>
                            </div>
                          </Table.Td>
                          <Table.Td
                            style={{
                              color:
                                p.blankCount > 0 ? "var(--text)" : "var(--text-muted)",
                            }}
                          >
                            {p.blankCount > 0 ? (
                              p.blankCount
                            ) : (
                              <span style={{ color: "var(--text-muted)" }}>{t("sqlResultsPanel.dash")}</span>
                            )}
                          </Table.Td>
                          <Table.Td
                            style={{
                              color: isConstant ? "var(--text-muted)" : "var(--text)",
                            }}
                          >
                            {p.distinctCount}
                          </Table.Td>
                          <Table.Td
                            style={{
                              color: isConstant
                                ? "var(--destructive)"
                                : "var(--text-muted)",
                            }}
                          >
                            {isConstant ? (
                              <span title={String(p.constantValue)}>
                                {t("sqlResultsPanel.constantYes", { value: String(p.constantValue).slice(0, 12) })}
                              </span>
                            ) : (
                              <span style={{ color: "var(--text-muted)" }}>{t("sqlResultsPanel.dash")}</span>
                            )}
                          </Table.Td>
                          <Table.Td style={{ fontFamily: "monospace" }}>
                            {p.min !== null ? (
                              String(p.min).slice(0, 16)
                            ) : (
                              <span style={{ color: "var(--text-muted)" }}>{t("sqlResultsPanel.dash")}</span>
                            )}
                          </Table.Td>
                          <Table.Td style={{ fontFamily: "monospace" }}>
                            {p.max !== null ? (
                              String(p.max).slice(0, 16)
                            ) : (
                              <span style={{ color: "var(--text-muted)" }}>{t("sqlResultsPanel.dash")}</span>
                            )}
                          </Table.Td>
                          <Table.Td style={{ fontFamily: "monospace" }}>
                            {p.mean !== null ? (
                              p.mean.toFixed(2)
                            ) : (
                              <span style={{ color: "var(--text-muted)" }}>{t("sqlResultsPanel.dash")}</span>
                            )}
                          </Table.Td>
                          <Table.Td>
                            <div style={{ display: "flex", flexDirection: "column", gap: "0.18rem", minWidth: 140 }}>
                              {p.topValues.map(({ value, count }) => {
                                const barPct = p.topValues[0].count > 0
                                  ? (count / p.topValues[0].count) * 100
                                  : 0;
                                return (
                                  <div key={value} style={{ display: "flex", alignItems: "center", gap: "0.3rem" }}>
                                    <div
                                      style={{
                                        width: 52,
                                        height: 5,
                                        borderRadius: 2,
                                        background: "var(--border)",
                                        position: "relative",
                                        flexShrink: 0,
                                      }}
                                    >
                                      <div
                                        style={{
                                          position: "absolute",
                                          left: 0,
                                          top: 0,
                                          bottom: 0,
                                          width: `${barPct}%`,
                                          borderRadius: 2,
                                          background: "var(--primary)",
                                        }}
                                      />
                                    </div>
                                    <span
                                      style={{
                                        fontFamily: "monospace",
                                        fontSize: "0.68rem",
                                        whiteSpace: "nowrap",
                                        overflow: "hidden",
                                        maxWidth: 110,
                                        textOverflow: "ellipsis",
                                      }}
                                      title={value}
                                    >
                                      {value.slice(0, 22)}
                                    </span>
                                    <span style={{ color: "var(--text-muted)", fontSize: "0.65rem", marginLeft: "auto", flexShrink: 0 }}>
                                      ×{count}
                                    </span>
                                  </div>
                                );
                              })}
                            </div>
                          </Table.Td>
                        </Table.Tr>
                      );
                    })}
                  </Table.Tbody>
                </Table>
              </div>
            </div>
          ))}

        {resultTab === "errors" &&
          (errors.length === 0 ? (
            <div
              style={{
                padding: "1.5rem",
                textAlign: "center",
                color: "var(--text-muted)",
                fontSize: "0.85rem",
              }}
            >
              {t("sqlResultsPanel.errorsEmpty")}
            </div>
          ) : (
            <div style={{ padding: "0.75rem" }}>
              <p
                style={{
                  color: "var(--destructive)",
                  fontSize: "0.8rem",
                  fontWeight: 600,
                  marginBottom: "0.5rem",
                }}
              >
                {t("sqlResultsPanel.errorsHeading")}
              </p>
              <ul
                style={{
                  margin: 0,
                  paddingLeft: "1.25rem",
                  display: "flex",
                  flexDirection: "column",
                  gap: "0.3rem",
                }}
              >
                {errors.map((e, i) => (
                  <li
                    key={i}
                    style={{
                      fontSize: "0.8rem",
                      color: "var(--destructive)",
                      fontFamily: "monospace",
                    }}
                  >
                    {e}
                  </li>
                ))}
              </ul>
            </div>
          ))}

        {resultTab === "history" &&
          (history.length === 0 ? (
            <div
              style={{
                padding: "1.5rem",
                textAlign: "center",
                color: "var(--text-muted)",
                fontSize: "0.85rem",
              }}
            >
              {t("sqlResultsPanel.historyEmpty")}
            </div>
          ) : (
            <Table className="data-table" style={{ fontSize: "0.75rem" }}>
              <Table.Thead>
                <Table.Tr>
                  <Table.Th>{t("sqlResultsPanel.colTime")}</Table.Th>
                  <Table.Th>{t("sqlResultsPanel.colRole")}</Table.Th>
                  <Table.Th>{t("sqlResultsPanel.colDuration")}</Table.Th>
                  <Table.Th>{t("sqlResultsPanel.colRows")}</Table.Th>
                  <Table.Th style={{ width: "50%" }}>{t("sqlResultsPanel.colSql")}</Table.Th>
                  <Table.Th></Table.Th>
                </Table.Tr>
              </Table.Thead>
              <Table.Tbody>
                {history.map((h, i) => {
                  const ts = new Date(h.executedAt);
                  const timeLabel = ts.toLocaleTimeString([], {
                    hour: "2-digit",
                    minute: "2-digit",
                    second: "2-digit",
                  });
                  const dateLabel = ts.toLocaleDateString([], {
                    month: "short",
                    day: "numeric",
                  });
                  const isToday = ts.toDateString() === new Date().toDateString();
                  return (
                    <Table.Tr key={i} style={{ verticalAlign: "top" }}>
                      <Table.Td style={{ whiteSpace: "nowrap", color: "var(--text-muted)" }}>
                        <div>{timeLabel}</div>
                        {!isToday && (
                          <div style={{ fontSize: "0.68rem" }}>{dateLabel}</div>
                        )}
                      </Table.Td>
                      <Table.Td style={{ color: "var(--text-muted)", whiteSpace: "nowrap" }}>
                        {h.role}
                      </Table.Td>
                      <Table.Td
                        style={{
                          whiteSpace: "nowrap",
                          color: h.error ? "var(--destructive)" : "var(--text-muted)",
                        }}
                      >
                        {h.durationMs}ms
                      </Table.Td>
                      <Table.Td
                        style={{
                          whiteSpace: "nowrap",
                          color: h.error ? "var(--destructive)" : "var(--text)",
                        }}
                      >
                        {h.error ? <span title={h.error}>{t("sqlResultsPanel.errorLabel")}</span> : h.rowCount}
                      </Table.Td>
                      <Table.Td>
                        <pre
                          style={{
                            margin: 0,
                            fontSize: "0.72rem",
                            whiteSpace: "pre-wrap",
                            wordBreak: "break-all",
                            color: "var(--text)",
                            maxHeight: "4.5em",
                            overflow: "hidden",
                          }}
                        >
                          {h.sql}
                        </pre>
                      </Table.Td>
                      <Table.Td style={{ whiteSpace: "nowrap" }}>
                        <Button
                          variant="default"
                          size="compact-xs"
                          data-testid={`restore-history-${i}`}
                          onClick={() => {
                            setSqlText(h.sql);
                            setRole(h.role);
                            setResultTab("results");
                          }}
                        >
                          {t("sqlResultsPanel.restore")}
                        </Button>
                      </Table.Td>
                    </Table.Tr>
                  );
                })}
              </Table.Tbody>
            </Table>
          ))}
        {resultTab === "stats" &&
          (() => {
            type StatsSource = {
              field: string;
              source: string;
              strategy: string;
              elapsed_ms: number;
              rows: number;
              cache_hit?: boolean;
              physical_sql?: string;
            };
            const stats = queryStats as {
              total_elapsed_ms?: number;
              sources?: StatsSource[];
            } | null;
            if (!stats) return null;
            return (
              <Box style={{ padding: "0.75rem 1rem", fontSize: "0.8rem" }}>
                <div style={{ marginBottom: "0.5rem", color: "var(--text-muted)" }}>
                  {t("sqlResultsPanel.statsTotal")}{" "}
                  <strong style={{ color: "var(--text)" }}>
                    {t("sqlResultsPanel.statsTotalMs", { ms: stats.total_elapsed_ms })}
                  </strong>
                </div>
                {(stats.sources ?? []).map((s, i) => (
                  <div
                    key={i}
                    style={{
                      marginBottom: "0.75rem",
                      borderLeft: "2px solid var(--primary)",
                      paddingLeft: "0.75rem",
                    }}
                  >
                    <div
                      style={{
                        display: "flex",
                        gap: "1rem",
                        flexWrap: "wrap",
                        marginBottom: "0.25rem",
                      }}
                    >
                      <span>
                        <span style={{ color: "var(--text-muted)" }}>{t("sqlResultsPanel.statsField")}</span>{" "}
                        {s.field}
                      </span>
                      <span>
                        <span style={{ color: "var(--text-muted)" }}>{t("sqlResultsPanel.statsSource")}</span>{" "}
                        {s.source}
                      </span>
                      <span>
                        <span style={{ color: "var(--text-muted)" }}>{t("sqlResultsPanel.statsStrategy")}</span>{" "}
                        {s.strategy}
                      </span>
                      <span>
                        <span style={{ color: "var(--text-muted)" }}>{t("sqlResultsPanel.statsElapsed")}</span>{" "}
                        {t("sqlResultsPanel.statsElapsedMs", { ms: s.elapsed_ms })}
                      </span>
                      <span>
                        <span style={{ color: "var(--text-muted)" }}>{t("sqlResultsPanel.statsRows")}</span> {s.rows}
                      </span>
                      {s.cache_hit && (
                        <span style={{ color: "#4ade80" }}>{t("sqlResultsPanel.statsCacheHit")}</span>
                      )}
                    </div>
                    {s.physical_sql && (
                      <pre
                        style={{
                          margin: "0.25rem 0 0",
                          fontSize: "0.72rem",
                          color: "var(--text-muted)",
                          whiteSpace: "pre-wrap",
                          wordBreak: "break-all",
                          maxHeight: "6em",
                          overflow: "auto",
                          background: "var(--surface)",
                          padding: "0.4rem",
                          borderRadius: "4px",
                        }}
                      >
                        {s.physical_sql}
                      </pre>
                    )}
                  </div>
                ))}
              </Box>
            );
          })()}
      </div>
    </div>
  );
}

// PAGE_SIZE is re-exported for consumers that need it (unused here but kept for tree-shaking)
export { PAGE_SIZE };
