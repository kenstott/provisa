// Copyright (c) 2026 Kenneth Stott
// Canary: baf9c61c-5a3f-446c-9f8f-d174a71c0bc4
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Sortable/filterable/groupable client-side results grid, driven by
// useResultsGrid. Shared by the SQL workbench results tab, the admin Reports
// viewer, and the table preview modal.

import { useState } from "react";
import { useTranslation } from "react-i18next";
import { ActionIcon, Anchor, Button, Text, TextInput } from "@mantine/core";
import {
  Copy,
  Check,
  ChevronDown,
  ChevronsLeft,
  ChevronLeft,
  ChevronRight,
  ChevronsRight,
  Filter,
  Layers,
} from "lucide-react";
import { COL_MIN } from "./types";
import type { ResultsGridState } from "./useResultsGrid";
import { TraceDetailsModal } from "../../components/telemetry/TraceDetailsModal";
import {
  TELEMETRY_ID_RE,
  telemetryIdKind,
  type TelemetryIdKind,
} from "../../components/telemetry/traceDetails";

interface ResultsGridProps {
  grid: ResultsGridState;
  /** Unfiltered row count (for the "filtered from N" hint). */
  totalRowCount: number;
  /** Show the per-column multi-level group-by toggles (default true). */
  groupable?: boolean;
}

export function ResultsGrid({ grid, totalRowCount, groupable = true }: ResultsGridProps) {
  const { t } = useTranslation();
  // Trace/span ids in any column drill down to the full span record.
  const [detail, setDetail] = useState<{ kind: TelemetryIdKind; id: string } | null>(null);
  const {
    sorts,
    filters,
    setFilters,
    groupBy,
    toggleGroupBy,
    collapsedGroups,
    toggleGroupCollapsed,
    baseColumns,
    page,
    setPage,
    displayColumns,
    displayRows,
    pagedItems,
    totalPages,
    colWidths,
    autoWidths,
    copiedResults,
    handleSort,
    handleResizeStart,
    handleDownloadCsv,
    handleCopyResults,
    serverPaged,
    hasMore,
  } = grid;
  const canGoBack = page > 0;
  const canGoForward = serverPaged ? hasMore : page < totalPages - 1;

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
          {!serverPaged && displayRows.length < totalRowCount && groupBy.length === 0
            ? t("sqlResultsPanel.filteredFrom", { count: totalRowCount })
            : ""}
        </Text>
        <div style={{ flex: 1 }} />
        {(totalPages > 1 || serverPaged) && (
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
              disabled={!canGoBack}
            >
              <ChevronLeft size={13} />
            </ActionIcon>
            <Text component="span" size="xs">
              {serverPaged
                ? t("sqlResultsPanel.pageNumber", { page: page + 1 })
                : t("sqlResultsPanel.pageIndicator", { page: page + 1, total: totalPages })}
            </Text>
            <ActionIcon
              variant="subtle"
              size="sm"
              aria-label={t("sqlResultsPanel.nextPage")}
              onClick={() => setPage((p) => p + 1)}
              disabled={!canGoForward}
            >
              <ChevronRight size={13} />
            </ActionIcon>
            {!serverPaged && (
              <ActionIcon
                variant="subtle"
                size="sm"
                aria-label={t("sqlResultsPanel.lastPage")}
                onClick={() => setPage(totalPages - 1)}
                disabled={!canGoForward}
              >
                <ChevronsRight size={13} />
              </ActionIcon>
            )}
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
              {displayColumns.map((c) => {
                const sortIdx = sorts.findIndex((s) => s.col === c);
                const sortEntry = sortIdx !== -1 ? sorts[sortIdx] : null;
                const isGroupable = groupable && baseColumns.includes(c);
                const groupLevel = groupBy.indexOf(c);
                const isGrouped = groupLevel !== -1;
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
                      {isGroupable && (
                        <ActionIcon
                          component="span"
                          variant="transparent"
                          size="xs"
                          data-testid={`results-group-${c}`}
                          aria-label={
                            isGrouped
                              ? t("sqlResultsPanel.ungroupLevel", { level: groupLevel + 1 })
                              : t("sqlResultsPanel.groupByCol", { label: c })
                          }
                          title={
                            isGrouped
                              ? t("sqlResultsPanel.ungroupLevel", { level: groupLevel + 1 })
                              : t("sqlResultsPanel.groupByCol", { label: c })
                          }
                          onClick={(e) => {
                            e.stopPropagation();
                            toggleGroupBy(c);
                          }}
                          style={{ opacity: isGrouped ? 1 : 0.35, flexShrink: 0 }}
                        >
                          <Layers
                            size={11}
                            color={isGrouped ? "var(--primary, #6366f1)" : undefined}
                            aria-hidden="true"
                          />
                        </ActionIcon>
                      )}
                      {isGroupable && isGrouped && (
                        <Text span fz="0.65rem" c="var(--primary, #6366f1)">
                          {groupLevel + 1}
                        </Text>
                      )}
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
                    {baseColumns.includes(c) && (
                      <TextInput
                        size="xs"
                        variant="unstyled"
                        className="th-filter"
                        leftSection={<Filter size={11} />}
                        leftSectionPointerEvents="none"
                        aria-label={`${t("sqlResultsPanel.filterPlaceholder")} ${c}`}
                        value={filters[c] ?? ""}
                        onChange={(e) => {
                          // Read before the updater: React runs a functional updater on a later
                          // render pass, when the synthetic event has been pooled and
                          // currentTarget is null — typing a filter threw there.
                          const next = e.currentTarget.value;
                          setFilters((prev) => ({ ...prev, [c]: next }));
                          setPage(0);
                        }}
                        onClick={(e) => e.stopPropagation()}
                        placeholder={t("sqlResultsPanel.filterPlaceholder")}
                      />
                    )}
                    <div
                      role="separator"
                      aria-label={t("sqlResultsPanel.resizeColumn")}
                      onMouseDown={(e) => handleResizeStart(c, e)}
                      style={{
                        position: "absolute",
                        insetInlineEnd: 0,
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
            {pagedItems.map((item, i) =>
              item.type === "group" ? (
                <tr
                  key={item.key}
                  className="clickable"
                  data-testid={`group-row-${item.col}-${item.value}`}
                  onClick={() => toggleGroupCollapsed(item.key)}
                  style={{ background: "var(--surface)", cursor: "pointer" }}
                >
                  <td colSpan={displayColumns.length} style={{ fontWeight: 600 }}>
                    <span
                      style={{
                        display: "inline-flex",
                        alignItems: "center",
                        gap: "0.3rem",
                        paddingInlineStart: `${item.level * 16}px`,
                      }}
                    >
                      {collapsedGroups.has(item.key) ? (
                        <ChevronRight size={12} aria-hidden="true" />
                      ) : (
                        <ChevronDown size={12} aria-hidden="true" />
                      )}
                      <span style={{ color: "var(--text-muted)", fontWeight: 400 }}>
                        {item.col}:
                      </span>
                      {item.value}
                      <span style={{ color: "var(--text-muted)", fontWeight: 400 }}>
                        ({item.count})
                      </span>
                    </span>
                  </td>
                </tr>
              ) : (
                <tr key={i}>
                  {displayColumns.map((c) => {
                    const v = item.row[c];
                    const isNum = typeof v === "number";
                    const text = v != null ? String(v) : null;
                    const kind = telemetryIdKind(c);
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
                        {text === null ? (
                          <span className="null-val">{t("sqlResultsPanel.nullValue")}</span>
                        ) : kind !== null && TELEMETRY_ID_RE.test(text) ? (
                          <Anchor
                            component="button"
                            type="button"
                            inherit
                            title={t("traceDetails.openDetails")}
                            onClick={() => setDetail({ kind, id: text })}
                          >
                            {text}
                          </Anchor>
                        ) : (
                          text
                        )}
                      </td>
                    );
                  })}
                </tr>
              ),
            )}
          </tbody>
        </table>
      </div>
      {detail !== null && (
        <TraceDetailsModal
          kind={detail.kind}
          id={detail.id}
          onClose={() => setDetail(null)}
        />
      )}
    </div>
  );
}
