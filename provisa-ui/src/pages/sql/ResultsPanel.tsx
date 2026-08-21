// Copyright (c) 2026 Kenneth Stott
// Canary: 11557514-6a05-4a67-ae76-7227c880597f
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import React from "react";
import { useTranslation } from "react-i18next";
import { Badge, Box, Button, Table, Tabs } from "@mantine/core";
import { History, BarChart2 } from "lucide-react";
import { ResultsGrid } from "./ResultsGrid";
import type { ResultsGridState } from "./useResultsGrid";
import type { ResultTab, HistoryEntry } from "./types";
import type { ExplainNodeDto, ExplainResponse } from "../../api/admin";
import { MermaidDiagram } from "../../components/MermaidDiagram";

interface ResultsPanelProps {
  resultTab: ResultTab;
  setResultTab: React.Dispatch<React.SetStateAction<ResultTab>>;
  running: boolean;
  resultError: string;
  resultRows: Record<string, unknown>[];
  resultColumns: string[];
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
  resultError,
  resultRows,
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
              {sqlText.trim()
                ? t("sqlResultsPanel.noResults")
                : t("sqlResultsPanel.writeSqlPrompt")}
            </div>
          ) : (
            <ResultsGrid
              grid={grid}
              totalRowCount={resultRows.length}
              // REQ-1441: in the workbench the statement in the editor IS the construction.
              provenance={[{ label: t("tablePreview.provStatement"), value: sqlText }]}
            />
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
                      <Table.Th title={t("sqlResultsPanel.colNullsTitle")}>
                        {t("sqlResultsPanel.colNulls")}
                      </Table.Th>
                      <Table.Th title={t("sqlResultsPanel.colBlanksTitle")}>
                        {t("sqlResultsPanel.colBlanks")}
                      </Table.Th>
                      <Table.Th title={t("sqlResultsPanel.colDistinctTitle")}>
                        {t("sqlResultsPanel.colDistinct")}
                      </Table.Th>
                      <Table.Th title={t("sqlResultsPanel.colConstantTitle")}>
                        {t("sqlResultsPanel.colConstant")}
                      </Table.Th>
                      <Table.Th>{t("sqlResultsPanel.colMin")}</Table.Th>
                      <Table.Th>{t("sqlResultsPanel.colMax")}</Table.Th>
                      <Table.Th title={t("sqlResultsPanel.colMeanTitle")}>
                        {t("sqlResultsPanel.colMean")}
                      </Table.Th>
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
                              <span title={String(p.constantValue)}>
                                {t("sqlResultsPanel.constantYes", {
                                  value: String(p.constantValue).slice(0, 12),
                                })}
                              </span>
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
                          <span title={h.error}>{t("sqlResultsPanel.errorLabel")}</span>
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
              <div
                style={{ display: "flex", gap: "0.75rem", alignItems: "flex-start" }}
                data-testid="analyze-panes"
              >
                {analyzePlan.plan.length > 0 && (
                  <div style={{ flex: "1 1 50%", minWidth: 0, overflow: "auto" }}>
                    <Table className="data-table" style={{ fontSize: "0.68rem" }}>
                      <Table.Thead>
                        <Table.Tr>
                          <Table.Th>{t("sqlResultsPanel.analyzeColOperator")}</Table.Th>
                          <Table.Th>{t("sqlResultsPanel.analyzeColRows")}</Table.Th>
                          <Table.Th>
                            {analyzePlan.analyzed
                              ? t("sqlResultsPanel.analyzeColActualMs")
                              : t("sqlResultsPanel.analyzeColCost")}
                          </Table.Th>
                          <Table.Th>{t("sqlResultsPanel.analyzeColDetail")}</Table.Th>
                        </Table.Tr>
                      </Table.Thead>
                      <Table.Tbody>
                        {flattenPlan(analyzePlan.plan).map((row, i) => (
                          <Table.Tr key={i}>
                            <Table.Td
                              style={{
                                fontFamily: "monospace",
                                paddingInlineStart: `${0.4 + row.depth * 0.9}rem`,
                              }}
                            >
                              {row.node.op}
                            </Table.Td>
                            <Table.Td>{row.node.rows ?? t("sqlResultsPanel.dash")}</Table.Td>
                            <Table.Td>
                              {(analyzePlan.analyzed ? row.node.actual_ms : row.node.cost) ??
                                t("sqlResultsPanel.dash")}
                            </Table.Td>
                            <Table.Td style={{ color: "var(--text-muted)" }}>
                              {Object.entries(row.node.detail)
                                .map(([k, v]) => `${k}=${v}`)
                                .join("  ")}
                            </Table.Td>
                          </Table.Tr>
                        ))}
                      </Table.Tbody>
                    </Table>
                  </div>
                )}
                {analyzePlan.mermaid && (
                  <div
                    style={{
                      flex: "1 1 50%",
                      minWidth: 0,
                      overflow: "auto",
                      borderInlineStart: "1px solid var(--border)",
                      paddingInlineStart: "0.75rem",
                    }}
                  >
                    <MermaidDiagram chart={analyzePlan.mermaid} />
                  </div>
                )}
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
            </Box>
          ) : null)}
      </div>
    </div>
  );
}

// REQ-1519: the operator tree read top-down, each row carrying its depth so the list reads like
// the diagram.
function flattenPlan(
  nodes: ExplainNodeDto[],
  depth = 0,
): { node: ExplainNodeDto; depth: number }[] {
  return nodes.flatMap((node) => [{ node, depth }, ...flattenPlan(node.children, depth + 1)]);
}

export { PAGE_SIZE } from "./types";
