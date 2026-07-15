// Copyright (c) 2026 Kenneth Stott
// Canary: e2f4e259-305e-44c6-8b2a-d8b8c5facaac
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import React, { useMemo } from "react";
import { useTranslation } from "react-i18next";
import { ActionIcon, Alert, Group, Table, Text, TextInput } from "@mantine/core";
import { PAGE_SIZE, COL_MAX, COL_MIN, CHAR_PX } from "./types";

interface ResultsPanelProps {
  resultError: string;
  resultRows: Record<string, unknown>[];
  resultColumns: string[];
  displayRows: Record<string, unknown>[];
  page: number;
  setPage: React.Dispatch<React.SetStateAction<number>>;
  sorts: { col: string; dir: "asc" | "desc" }[];
  colWidths: Record<string, number>;
  filters: Record<string, string>;
  setFilters: React.Dispatch<React.SetStateAction<Record<string, string>>>;
  handleSort: (col: string) => void;
  handleResizeStart: (col: string, e: React.MouseEvent) => void;
  handleDownloadCsv: () => void;
  sqlText: string;
}

export function ResultsPanel({
  resultError,
  resultRows,
  resultColumns,
  displayRows,
  page,
  setPage,
  sorts,
  colWidths,
  filters,
  setFilters,
  handleSort,
  handleResizeStart,
  handleDownloadCsv,
  sqlText,
}: ResultsPanelProps) {
  const { t } = useTranslation();

  const autoWidths = useMemo(() => {
    const cols = resultColumns.length > 0 ? resultColumns : Object.keys(resultRows[0] ?? {});
    const widths: Record<string, number> = {};
    for (const col of cols) {
      const headerLen = col.length;
      const maxDataLen = resultRows.slice(0, 50).reduce((m, r) => {
        const v = r[col];
        return Math.max(m, v == null ? 4 : String(v).length);
      }, 0);
      widths[col] = Math.min(
        COL_MAX,
        Math.max(COL_MIN, Math.max(headerLen, maxDataLen) * CHAR_PX + 24),
      );
    }
    return widths;
  }, [resultRows, resultColumns]);

  const totalPages = Math.max(1, Math.ceil(displayRows.length / PAGE_SIZE));
  const pagedRows = displayRows.slice(page * PAGE_SIZE, (page + 1) * PAGE_SIZE);

  if (resultError) {
    return (
      <Alert color="red" m="sm" p="xs" data-testid="sql-results-error">
        <Text
          component="pre"
          size="xs"
          c="red"
          style={{ margin: 0, whiteSpace: "pre-wrap", fontFamily: "monospace" }}
        >
          {resultError}
        </Text>
      </Alert>
    );
  }

  if (resultRows.length === 0) {
    return (
      <Text ta="center" c="dimmed" size="sm" p="lg" data-testid="sql-results-empty">
        {sqlText.trim()
          ? t("sqlModelingResultsPanel.noResults")
          : t("sqlModelingResultsPanel.writeSqlPrompt")}
      </Text>
    );
  }

  const displayCols =
    resultColumns.length > 0
      ? resultColumns
      : resultRows[0] != null
        ? Object.keys(resultRows[0] as object)
        : [];

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
      {/* Download + pagination bar */}
      <Group
        gap="xs"
        px="sm"
        py={4}
        wrap="nowrap"
        style={{
          borderBottom: "1px solid var(--border)",
          flexShrink: 0,
          background: "var(--surface)",
          fontSize: "0.72rem",
          color: "var(--text-muted)",
        }}
      >
        <ActionIcon
          variant="default"
          size="sm"
          onClick={handleDownloadCsv}
          aria-label={t("sqlModelingResultsPanel.downloadCsv")}
          data-testid="sql-results-download-csv"
        >
          ↓
        </ActionIcon>
        <Text size="xs" c="dimmed">
          {t("sqlModelingResultsPanel.rowCount", { count: displayRows.length })}
          {displayRows.length < resultRows.length
            ? ` ${t("sqlModelingResultsPanel.filteredFrom", { count: resultRows.length })}`
            : ""}
        </Text>
        <div style={{ flex: 1 }} />
        {totalPages > 1 && (
          <>
            <ActionIcon
              variant="subtle"
              size="sm"
              onClick={() => setPage(0)}
              disabled={page === 0}
              aria-label={t("sqlModelingResultsPanel.firstPage")}
              data-testid="sql-results-first-page"
            >
              «
            </ActionIcon>
            <ActionIcon
              variant="subtle"
              size="sm"
              onClick={() => setPage((p) => Math.max(0, p - 1))}
              disabled={page === 0}
              aria-label={t("sqlModelingResultsPanel.previousPage")}
              data-testid="sql-results-prev-page"
            >
              ‹
            </ActionIcon>
            <Text size="xs" c="dimmed">
              {t("sqlModelingResultsPanel.pageOf", { page: page + 1, total: totalPages })}
            </Text>
            <ActionIcon
              variant="subtle"
              size="sm"
              onClick={() => setPage((p) => Math.min(totalPages - 1, p + 1))}
              disabled={page >= totalPages - 1}
              aria-label={t("sqlModelingResultsPanel.nextPage")}
              data-testid="sql-results-next-page"
            >
              ›
            </ActionIcon>
            <ActionIcon
              variant="subtle"
              size="sm"
              onClick={() => setPage(totalPages - 1)}
              disabled={page >= totalPages - 1}
              aria-label={t("sqlModelingResultsPanel.lastPage")}
              data-testid="sql-results-last-page"
            >
              »
            </ActionIcon>
          </>
        )}
      </Group>
      <div style={{ flex: 1, overflow: "auto" }}>
        <Table
          className="data-table sql-results-table"
          style={{
            fontSize: "0.78rem",
            tableLayout: "fixed",
            width: "max-content",
            minWidth: "100%",
          }}
        >
          <Table.Thead>
            <Table.Tr>
              {displayCols.map((c) => {
                const sortIdx = sorts.findIndex((s) => s.col === c);
                const sortEntry = sortIdx !== -1 ? sorts[sortIdx] : null;
                return (
                  <Table.Th
                    key={c}
                    className={sortEntry ? "col-sorted" : undefined}
                    style={{
                      width: colWidths[c] ?? autoWidths[c] ?? 140,
                      minWidth: COL_MIN,
                      position: "relative",
                    }}
                    aria-sort={
                      sortEntry
                        ? sortEntry.dir === "asc"
                          ? "ascending"
                          : "descending"
                        : undefined
                    }
                  >
                    <button
                      type="button"
                      className="th-label"
                      onClick={() => handleSort(c)}
                      style={{
                        background: "none",
                        border: "none",
                        padding: 0,
                        font: "inherit",
                        color: "inherit",
                        cursor: "pointer",
                        display: "flex",
                        alignItems: "center",
                        width: "100%",
                      }}
                    >
                      <span
                        style={{
                          overflow: "hidden",
                          textOverflow: "ellipsis",
                          flex: 1,
                          textAlign: "left",
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
                            <span style={{ opacity: 0.7 }}>{sortIdx + 1}</span>
                          )}
                          <span
                            aria-label={
                              sortEntry.dir === "asc"
                                ? t("sqlModelingResultsPanel.sortAscending")
                                : t("sqlModelingResultsPanel.sortDescending")
                            }
                          >
                            {sortEntry.dir === "asc" ? "▲" : "▼"}
                          </span>
                        </span>
                      ) : (
                        <span
                          aria-hidden="true"
                          style={{
                            fontSize: "0.6rem",
                            color: "var(--text-muted)",
                            opacity: 0.3,
                          }}
                        >
                          ⇅
                        </span>
                      )}
                    </button>
                    <TextInput
                      className="th-filter"
                      value={filters[c] ?? ""}
                      onChange={(e) => {
                        setFilters((prev) => ({
                          ...prev,
                          [c]: e.target.value,
                        }));
                        setPage(0);
                      }}
                      onClick={(e) => e.stopPropagation()}
                      placeholder={t("sqlModelingResultsPanel.filterPlaceholder")}
                      aria-label={t("sqlModelingResultsPanel.filterColumn", { column: c })}
                      size="xs"
                      variant="unstyled"
                    />
                    <div
                      role="separator"
                      aria-label={t("sqlModelingResultsPanel.resizeColumn", { column: c })}
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
                  </Table.Th>
                );
              })}
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {pagedRows.map((row, i) => (
              <Table.Tr key={i}>
                {displayCols.map((c) => {
                  const v = row[c];
                  const isNum = typeof v === "number";
                  return (
                    <Table.Td
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
                        <span className="null-val">{t("sqlModelingResultsPanel.nullValue")}</span>
                      )}
                    </Table.Td>
                  );
                })}
              </Table.Tr>
            ))}
          </Table.Tbody>
        </Table>
      </div>
    </div>
  );
}
