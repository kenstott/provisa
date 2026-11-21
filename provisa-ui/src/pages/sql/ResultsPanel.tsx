// Copyright (c) 2026 Kenneth Stott
// Canary: 5ed860a9-532c-4b85-afe0-69953ed8fa40
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { columnLabel } from "../../lib/columnKeys";
import type { ResultRedirect } from "../../types/resultRedirect";
import { ResultRedirectNotice } from "../../components/ResultRedirectNotice";
import React from "react";
import { useTranslation } from "react-i18next";
import { Badge, Box, Button, Modal, Table, Tabs } from "@mantine/core";
import { History, BarChart2, Maximize2 } from "lucide-react";
import { ResultsGrid } from "./ResultsGrid";
import type { ResultsGridState } from "./useResultsGrid";
import type { ResultTab, HistoryEntry } from "./types";
import type { ExplainResponse } from "../../api/admin";
import { AnalyzePanes } from "./AnalyzePanes";
import { MermaidDiagram } from "../../components/MermaidDiagram";
import "./ResultsPanel.css";
import { Tip } from "../../components/Tip";
import { StatementTabs } from "../../components/StatementTabs";
import { statementLabel } from "./statementResults";
import type { StatementResult } from "./statementResults";

interface ResultsPanelProps {
  resultTab: ResultTab;
  setResultTab: React.Dispatch<React.SetStateAction<ResultTab>>;
  running: boolean;
  /** The last run was stopped before it answered: the results say so, and show nothing it held. */
  stopped?: boolean;
  resultError: string;
  /** The result was landed in the results store and is there, not in the grid. */
  resultRedirect?: ResultRedirect | null;
  resultRows: Record<string, unknown>[];
  resultColumns: string[];
  /** REQ-1937: the rows held are only part of the result, so a filter applies to those loaded. */
  rowsPartial?: boolean;
  /** The tab's statement has been run: no rows held is then its answer, not a tab yet to run. */
  answered?: boolean;
  /** A batch's results, one per statement; the result* props are the one shown. */
  statements: StatementResult[];
  statementIndex: number;
  onStatement: (index: number) => void;
  grid: ResultsGridState;
  errors: string[];
  history: HistoryEntry[];
  queryStats: unknown;
  analyzePlan: ExplainResponse | null;
  analyzeError: string;
  sqlText: string;
  setSqlText: React.Dispatch<React.SetStateAction<string>>;
  setRole: React.Dispatch<React.SetStateAction<string>>;
}

export function ResultsPanel({
  resultTab,
  setResultTab,
  running,
  stopped = false,
  resultError,
  resultRedirect = null,
  resultRows,
  rowsPartial = false,
  answered = false,
  statements,
  statementIndex,
  onStatement,
  grid,
  errors,
  history,
  queryStats,
  analyzePlan,
  analyzeError,
  sqlText,
  setSqlText,
  setRole,
}: ResultsPanelProps) {
  const { t } = useTranslation();
  const { profile, handleDownloadProfile } = grid;
  const [analyzeExpanded, setAnalyzeExpanded] = React.useState(false);
  // A wide or long result set is cramped in the results strip; the expand control reopens the
  // same grid (same filters, sort and groups) in a 90% modal, as the Analyze tab does for plans.
  const [resultsExpanded, setResultsExpanded] = React.useState(false);

  const tabLabels: Record<ResultTab, string> = {
    results: t("sqlResultsPanel.tabResults"),
    profile: t("sqlResultsPanel.tabProfile"),
    errors: t("sqlResultsPanel.tabErrors"),
    history: t("sqlResultsPanel.tabHistory"),
    stats: t("sqlResultsPanel.tabStats"),
    analyze: t("sqlResultsPanel.tabAnalyze"),
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
          {(["results", "profile", "errors", "history", "stats", "analyze"] as ResultTab[]).map(
            (tab) => {
              const count =
                tab === "results"
                  ? resultRows.length
                  : tab === "profile"
                    ? profile.length
                    : tab === "errors"
                      ? errors.length
                      : tab === "stats" || tab === "analyze"
                        ? 0
                        : history.length;
              if (tab === "stats" && !queryStats) return null;
              // REQ-1519: the tab appears only once a plan has been asked for.
              if (tab === "analyze" && !analyzePlan && !analyzeError) return null;
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
                      <Badge size="xs" circle color={tab === "errors" ? "red" : "blue"}>
                        {count}
                      </Badge>
                    ) : undefined
                  }
                >
                  {tabLabels[tab]}
                </Tabs.Tab>
              );
            },
          )}
        </Tabs.List>
      </Tabs>

      {statements.length > 1 &&
        !running &&
        !stopped &&
        (resultTab === "results" || resultTab === "profile") && (
          <StatementTabs
            tabs={statements.map((r) => ({
              name: statementLabel(r.statement),
              rows: r.redirect ? null : r.rows.length,
              failed: r.error !== "",
            }))}
            index={statementIndex}
            onChange={onStatement}
            testId="sql-statement-tab"
          />
        )}
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
          ) : stopped ? (
            <div
              data-testid="sql-results-stopped"
              style={{
                padding: "1.5rem",
                textAlign: "center",
                color: "var(--text-muted)",
                fontSize: "0.85rem",
              }}
            >
              {t("common.stopped")}
            </div>
          ) : resultRedirect ? (
            <ResultRedirectNotice redirect={resultRedirect} />
          ) : resultError ? (
            <pre
              data-testid="sql-results-error"
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
              data-testid="sql-results-empty"
              data-answered={answered ? "" : undefined}
              style={{
                padding: "1.5rem",
                textAlign: "center",
                color: "var(--text-muted)",
                fontSize: "0.85rem",
              }}
            >
              {sqlText.trim()
                ? t("sqlResultsPanel.noResults")
                : t("sqlResultsPanel.writeSqlPrompt")}
            </div>
          ) : (
            <div className="analyze-panes-wrap" style={{ height: "100%" }}>
              <Tip text={t("sqlResultsPanel.resultsExpand")} named>
                <button
                  type="button"
                  className="analyze-expand"
                  onClick={() => setResultsExpanded(true)}
                  aria-label={t("sqlResultsPanel.resultsExpand")}
                  data-testid="results-expand"
                >
                  <Maximize2 size={12} />
                </button>
              </Tip>
              <ResultsGrid
                grid={grid}
                totalRowCount={resultRows.length}
                rowsPartial={rowsPartial}
                // REQ-1441: in the workbench the statement in the editor IS the construction.
                provenance={[{ label: t("tablePreview.provStatement"), value: sqlText }]}
              />
              <Modal
                opened={resultsExpanded}
                onClose={() => setResultsExpanded(false)}
                size="90%"
                title={t("sqlResultsPanel.resultsExpandTitle")}
                styles={{
                  content: { height: "90vh" },
                  body: { height: "calc(90vh - 3.5rem)", overflow: "hidden" },
                }}
              >
                <ResultsGrid
                  grid={grid}
                  totalRowCount={resultRows.length}
                  rowsPartial={rowsPartial}
                  provenance={[{ label: t("tablePreview.provStatement"), value: sqlText }]}
                />
              </Modal>
            </div>
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
                      <Tip text={t("sqlResultsPanel.colNullsTitle")}>
                        <Table.Th>{t("sqlResultsPanel.colNulls")}</Table.Th>
                      </Tip>
                      <Tip text={t("sqlResultsPanel.colBlanksTitle")}>
                        <Table.Th>{t("sqlResultsPanel.colBlanks")}</Table.Th>
                      </Tip>
                      <Tip text={t("sqlResultsPanel.colDistinctTitle")}>
                        <Table.Th>{t("sqlResultsPanel.colDistinct")}</Table.Th>
                      </Tip>
                      <Tip text={t("sqlResultsPanel.colConstantTitle")}>
                        <Table.Th>{t("sqlResultsPanel.colConstant")}</Table.Th>
                      </Tip>
                      <Table.Th>{t("sqlResultsPanel.colMin")}</Table.Th>
                      <Table.Th>{t("sqlResultsPanel.colMax")}</Table.Th>
                      <Tip text={t("sqlResultsPanel.colMeanTitle")}>
                        <Table.Th>{t("sqlResultsPanel.colMean")}</Table.Th>
                      </Tip>
                      <Table.Th>{t("sqlResultsPanel.colTopValues")}</Table.Th>
                    </Table.Tr>
                  </Table.Thead>
                  <Table.Tbody>
                    {profile.map((p) => {
                      const total = resultRows.length;
                      const nullPct = total > 0 ? Math.round((p.nullCount / total) * 100) : 0;
                      const isHighNull = nullPct >= 50;
                      const isConstant = p.constantValue !== undefined;
                      return (
                        <Table.Tr key={columnLabel(p.col)}>
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
                                      insetInlineStart: 0,
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
                              color: p.blankCount > 0 ? "var(--text)" : "var(--text-muted)",
                            }}
                          >
                            {p.blankCount > 0 ? (
                              p.blankCount
                            ) : (
                              <span style={{ color: "var(--text-muted)" }}>
                                {t("sqlResultsPanel.dash")}
                              </span>
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
                              color: isConstant ? "var(--destructive)" : "var(--text-muted)",
                            }}
                          >
                            {isConstant ? (
                              <Tip text={String(p.constantValue)}>
                                <span>
                                  {t("sqlResultsPanel.constantYes", {
                                    value: String(p.constantValue).slice(0, 12),
                                  })}
                                </span>
                              </Tip>
                            ) : (
                              <span style={{ color: "var(--text-muted)" }}>
                                {t("sqlResultsPanel.dash")}
                              </span>
                            )}
                          </Table.Td>
                          <Table.Td style={{ fontFamily: "monospace" }}>
                            {p.min !== null ? (
                              String(p.min).slice(0, 16)
                            ) : (
                              <span style={{ color: "var(--text-muted)" }}>
                                {t("sqlResultsPanel.dash")}
                              </span>
                            )}
                          </Table.Td>
                          <Table.Td style={{ fontFamily: "monospace" }}>
                            {p.max !== null ? (
                              String(p.max).slice(0, 16)
                            ) : (
                              <span style={{ color: "var(--text-muted)" }}>
                                {t("sqlResultsPanel.dash")}
                              </span>
                            )}
                          </Table.Td>
                          <Table.Td style={{ fontFamily: "monospace" }}>
                            {p.mean !== null ? (
                              p.mean.toFixed(2)
                            ) : (
                              <span style={{ color: "var(--text-muted)" }}>
                                {t("sqlResultsPanel.dash")}
                              </span>
                            )}
                          </Table.Td>
                          <Table.Td>
                            <div
                              style={{
                                display: "flex",
                                flexDirection: "column",
                                gap: "0.18rem",
                                minWidth: 140,
                              }}
                            >
                              {p.topValues.map(({ value, count }) => {
                                const barPct =
                                  p.topValues[0].count > 0
                                    ? (count / p.topValues[0].count) * 100
                                    : 0;
                                return (
                                  <div
                                    key={value}
                                    style={{ display: "flex", alignItems: "center", gap: "0.3rem" }}
                                  >
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
                                          insetInlineStart: 0,
                                          top: 0,
                                          bottom: 0,
                                          width: `${barPct}%`,
                                          borderRadius: 2,
                                          background: "var(--primary)",
                                        }}
                                      />
                                    </div>
                                    <Tip text={value}>
                                      <span
                                        style={{
                                          fontFamily: "monospace",
                                          fontSize: "0.68rem",
                                          whiteSpace: "nowrap",
                                          overflow: "hidden",
                                          maxWidth: 110,
                                          textOverflow: "ellipsis",
                                        }}
                                      >
                                        {value.slice(0, 22)}
                                      </span>
                                    </Tip>
                                    <span
                                      style={{
                                        color: "var(--text-muted)",
                                        fontSize: "0.65rem",
                                        marginInlineStart: "auto",
                                        flexShrink: 0,
                                      }}
                                    >
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
                  paddingInlineStart: "1.25rem",
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
                        {!isToday && <div style={{ fontSize: "0.68rem" }}>{dateLabel}</div>}
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
                        {h.error ? (
                          <Tip text={h.error}>
                            <span>{t("sqlResultsPanel.errorLabel")}</span>
                          </Tip>
                        ) : (
                          h.rowCount
                        )}
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
              mermaid?: string;
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
                {stats.mermaid && <MermaidDiagram chart={stats.mermaid} />}
                {(stats.sources ?? []).map((s, i) => (
                  <div
                    key={i}
                    style={{
                      marginBottom: "0.75rem",
                      borderInlineStart: "2px solid var(--primary)",
                      paddingInlineStart: "0.75rem",
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
                        <span style={{ color: "var(--text-muted)" }}>
                          {t("sqlResultsPanel.statsField")}
                        </span>{" "}
                        {s.field}
                      </span>
                      <span>
                        <span style={{ color: "var(--text-muted)" }}>
                          {t("sqlResultsPanel.statsSource")}
                        </span>{" "}
                        {s.source}
                      </span>
                      <span>
                        <span style={{ color: "var(--text-muted)" }}>
                          {t("sqlResultsPanel.statsStrategy")}
                        </span>{" "}
                        {s.strategy}
                      </span>
                      <span>
                        <span style={{ color: "var(--text-muted)" }}>
                          {t("sqlResultsPanel.statsElapsed")}
                        </span>{" "}
                        {t("sqlResultsPanel.statsElapsedMs", { ms: s.elapsed_ms })}
                      </span>
                      <span>
                        <span style={{ color: "var(--text-muted)" }}>
                          {t("sqlResultsPanel.statsRows")}
                        </span>{" "}
                        {s.rows}
                      </span>
                      {s.cache_hit && (
                        <span style={{ color: "#4ade80" }}>
                          {t("sqlResultsPanel.statsCacheHit")}
                        </span>
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

        {/* REQ-1519: the engine's own plan for the routed statement, with the Provisa rewrites
            that produced it drawn onto the same picture. */}
        {resultTab === "analyze" &&
          (analyzeError ? (
            <pre
              style={{
                margin: "0.75rem",
                fontSize: "0.8rem",
                color: "var(--destructive)",
                whiteSpace: "pre-wrap",
                fontFamily: "monospace",
              }}
              data-testid="analyze-error"
            >
              {analyzeError}
            </pre>
          ) : analyzePlan ? (
            <Box
              style={{ padding: "0.75rem 1rem", fontSize: "0.8rem" }}
              data-testid="analyze-panel"
            >
              <div
                style={{
                  display: "flex",
                  gap: "1rem",
                  flexWrap: "wrap",
                  marginBottom: "0.5rem",
                  color: "var(--text-muted)",
                }}
              >
                <span>
                  {t("sqlResultsPanel.analyzeRoute")}{" "}
                  <strong style={{ color: "var(--text)" }}>{analyzePlan.route}</strong>{" "}
                  {analyzePlan.route_reason}
                </span>
                <span>
                  {t("sqlResultsPanel.analyzeDialect")}{" "}
                  <strong style={{ color: "var(--text)" }}>{analyzePlan.dialect}</strong>
                </span>
                <span>
                  {analyzePlan.analyzed
                    ? t("sqlResultsPanel.analyzeMeasured")
                    : t("sqlResultsPanel.analyzeEstimated")}
                </span>
              </div>

              {analyzePlan.optimizations.length > 0 && (
                <div style={{ marginBottom: "0.5rem" }} data-testid="analyze-optimizations">
                  <span style={{ color: "var(--text-muted)" }}>
                    {t("sqlResultsPanel.analyzeOptimizations")}
                  </span>{" "}
                  {analyzePlan.optimizations.map((o) => (
                    <Badge key={o} size="xs" color="green" variant="light" mr="xs">
                      {o}
                    </Badge>
                  ))}
                </div>
              )}

              {/* REQ-1519: the diagram and the operator list are two readings of one tree —
                  side by side so a node in the graph can be found in the list without scrolling. */}
              <div className="analyze-panes-wrap">
                <Tip text={t("sqlResultsPanel.analyzeExpand")} named>
                  <button
                    type="button"
                    className="analyze-expand"
                    onClick={() => setAnalyzeExpanded(true)}
                    aria-label={t("sqlResultsPanel.analyzeExpand")}
                    data-testid="analyze-expand"
                  >
                    <Maximize2 size={12} />
                  </button>
                </Tip>
                <AnalyzePanes plan={analyzePlan} />
              </div>

              <pre
                style={{
                  margin: "0.5rem 0 0",
                  fontSize: "0.72rem",
                  color: "var(--text-muted)",
                  whiteSpace: "pre-wrap",
                  wordBreak: "break-all",
                  maxHeight: "10em",
                  overflow: "auto",
                  background: "var(--surface)",
                  padding: "0.4rem",
                  borderRadius: "4px",
                }}
                data-testid="analyze-sql"
              >
                {analyzePlan.sql}
              </pre>

              {/* REQ-1519: the same two panes at full size — a wide plan is unreadable in the
                  results strip, so the expand control reopens it in a 90% modal. */}
              <Modal
                opened={analyzeExpanded}
                onClose={() => setAnalyzeExpanded(false)}
                size="90%"
                title={t("sqlResultsPanel.analyzeExpandTitle")}
                styles={{
                  content: { height: "90vh" },
                  body: { height: "calc(90vh - 3.5rem)", overflow: "hidden" },
                }}
              >
                <AnalyzePanes plan={analyzePlan} fill />
              </Modal>
            </Box>
          ) : null)}
      </div>
    </div>
  );
}

export { PAGE_SIZE } from "./types";
