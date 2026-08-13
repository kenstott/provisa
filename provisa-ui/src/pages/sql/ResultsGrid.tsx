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
import { ActionIcon, Anchor, Button, Select, Text, TextInput } from "@mantine/core";
import { notifications } from "@mantine/notifications";
import {
  Copy,
  Check,
  ChevronDown,
  ChevronsLeft,
  ChevronLeft,
  ChevronRight,
  ChevronsRight,
  Filter,
  FilterX,
  Layers,
} from "lucide-react";
import { COL_MIN, PAGE_SIZES } from "./types";
import { formatCell } from "./formatCell";
import { downloadXlsx, type ProvenanceEntry, type XlsxLabels } from "./exportXlsx";
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
  /** REQ-1441: what produced these rows — the relation, its definition, the statement that ran.
      Recorded on the workbook's provenance sheet alongside the reader's own grid choices. */
  provenance?: ProvenanceEntry[];
  /** Base name for the exported workbook, without extension (default "results"). */
  exportName?: string;
  /** REQ-1444: server-paged callers pass a reader for the WHOLE relation under the current
      filter/sort/group choices. Both exports use it instead of the page on screen; without it the
      grid already holds every row and the page is the relation. */
  fetchAllRows?: () => Promise<Record<string, unknown>[]>;
}

export function ResultsGrid({
  grid,
  totalRowCount,
  groupable = true,
  provenance = [],
  exportName = "results",
  fetchAllRows,
}: ResultsGridProps) {
  const { t } = useTranslation();
  // Trace/span ids in any column drill down to the full span record.
  const [detail, setDetail] = useState<{ kind: TelemetryIdKind; id: string } | null>(null);
  const [exporting, setExporting] = useState(false);
  const {
    sorts,
    filters,
    setFilters,
    hasFilters,
    clearFilters,
    groupBy,
    toggleGroupBy,
    collapsedGroups,
    toggleGroupCollapsed,
    baseColumns,
    page,
    setPage,
    pageSize,
    setPageSize,
    displayColumns,
    displayRows,
    pagedItems,
    totalPages,
    colWidths,
    autoWidths,
    copiedResults,
    handleSort,
    handleResizeStart,
    handleCopyResults,
    serverPaged,
    hasMore,
  } = grid;
  const canGoBack = page > 0;
  const canGoForward = serverPaged ? hasMore : page < totalPages - 1;

  // REQ-1441: the caller's facts describe the relation; these describe this export of it.
  // REQ-1444: the export is the whole relation under the current choices, not the page on screen —
  // a server-paged caller reads the remaining pages first, so the scope line says "all rows" and
  // only a caller that cannot read past its page (there is none today) would say otherwise.
  const handleDownloadXlsx = () => runExport(writeXlsx);
  const handleDownloadCsv = () => runExport(writeCsv);

  // REQ-1444: both exports take the same rows — the whole relation when the caller can read past
  // its page, the grid's own rows when it already holds everything.
  const runExport = async (write: (rows: Record<string, unknown>[]) => void | Promise<void>) => {
    setExporting(true);
    try {
      await write(fetchAllRows ? await fetchAllRows() : displayRows);
    } catch (e) {
      notifications.show({ color: "red", message: e instanceof Error ? e.message : String(e) });
    } finally {
      setExporting(false);
    }
  };

  const writeCsv = (rows: Record<string, unknown>[]) => {
    const escape = (v: unknown) => {
      const s = v == null ? "" : String(v);
      return s.includes(",") || s.includes('"') || s.includes("\n")
        ? `"${s.replace(/"/g, '""')}"`
        : s;
    };
    const lines = [displayColumns.map(escape).join(",")];
    for (const row of rows) lines.push(displayColumns.map((c) => escape(row[c])).join(","));
    const url = URL.createObjectURL(new Blob([lines.join("\n")], { type: "text/csv" }));
    const a = document.createElement("a");
    a.href = url;
    a.download = `${exportName}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  };

  const writeXlsx = async (rows: Record<string, unknown>[]) => {
    const none = t("sqlResultsPanel.provNone");
    const entries: ProvenanceEntry[] = [
      ...provenance,
      { label: t("sqlResultsPanel.provExportedAt"), value: new Date().toISOString() },
      {
        label: t("sqlResultsPanel.provScope"),
        value:
          serverPaged && !fetchAllRows
            ? t("sqlResultsPanel.provScopePage", { page: page + 1, pageSize })
            : t("sqlResultsPanel.provScopeAll"),
      },
      { label: t("sqlResultsPanel.provRowsExported"), value: String(rows.length) },
      {
        label: t("sqlResultsPanel.provFilters"),
        value:
          Object.entries(filters)
            .filter(([, v]) => v !== "")
            .map(([c, v]) => `${c} ~ "${v}"`)
            .join("\n") || none,
      },
      {
        label: t("sqlResultsPanel.provSort"),
        value: sorts.map((s) => `${s.col} ${s.dir}`).join(", ") || none,
      },
      { label: t("sqlResultsPanel.provGroupBy"), value: groupBy.join(", ") || none },
    ];
    const labels: XlsxLabels = {
      dataSheet: t("sqlResultsPanel.sheetData"),
      provenanceSheet: t("sqlResultsPanel.sheetProvenance"),
      fact: t("sqlResultsPanel.provFact"),
      detail: t("sqlResultsPanel.provDetail"),
      column: t("sqlResultsPanel.colColumn"),
      writtenAs: t("sqlResultsPanel.provWrittenAs"),
      columnsHeading: t("sqlResultsPanel.provColumnsHeading"),
      typeNumber: t("sqlResultsPanel.provTypeNumber"),
      typeDate: t("sqlResultsPanel.provTypeDate"),
      typeText: t("sqlResultsPanel.provTypeText"),
    };
    await downloadXlsx(`${exportName}.xlsx`, displayColumns, rows, entries, labels);
  };

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
          onClick={() => void handleDownloadCsv()}
          loading={exporting}
          data-testid="download-csv-btn"
        >
          {t("sqlResultsPanel.downloadCsv")}
        </Button>
        <Button
          variant="default"
          size="compact-xs"
          onClick={() => void handleDownloadXlsx()}
          loading={exporting}
          title={t("sqlResultsPanel.downloadXlsxTitle")}
          data-testid="download-xlsx-btn"
        >
          {t("sqlResultsPanel.downloadXlsx")}
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
        {/* REQ-1442: always on the toolbar, disabled when there is nothing to clear — the reader
            who filtered several columns to nothing needs one control, not one per column. */}
        <Button
          variant="default"
          size="compact-xs"
          onClick={clearFilters}
          disabled={!hasFilters}
          title={t("sqlResultsPanel.clearFiltersTitle")}
          data-testid="clear-filters-btn"
          leftSection={<FilterX size={11} />}
        >
          {t("sqlResultsPanel.clearFilters")}
        </Button>
        <Text component="span" size="xs" c="dimmed">
          {t("sqlResultsPanel.rowCount", { count: displayRows.length })}
          {!serverPaged && displayRows.length < totalRowCount && groupBy.length === 0
            ? t("sqlResultsPanel.filteredFrom", { count: totalRowCount })
            : ""}
        </Text>
        <div style={{ flex: 1 }} />
        {/* REQ-1438: rows per page is the reader's choice, offered whether or not the current
            result happens to spill past one page. */}
        <Select
          size="xs"
          w={78}
          aria-label={t("sqlResultsPanel.rowsPerPage")}
          title={t("sqlResultsPanel.rowsPerPage")}
          data={PAGE_SIZES.map((n) => String(n))}
          value={String(pageSize)}
          onChange={(v) => {
            if (v !== null) setPageSize(Number(v));
          }}
          allowDeselect={false}
          data-testid="page-size-select"
        />
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
                          {sorts.length > 1 && <span style={{ opacity: 0.7 }}>{sortIdx + 1}</span>}
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
            {/* REQ-1436: an empty page says so inside the table, under the live header row, so the
                filter that emptied it stays on screen and can be cleared. */}
            {pagedItems.length === 0 && (
              <tr data-testid="results-grid-empty">
                <td
                  colSpan={displayColumns.length}
                  style={{ padding: "1.5rem", textAlign: "center", color: "var(--text-dim)" }}
                >
                  <Text size="sm" c="dimmed">
                    {t("tablePreview.noRows")}
                  </Text>
                </td>
              </tr>
            )}
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
                    // REQ-1437: an instant reaches the grid in the engine's wire form. formatCell
                    // renders it for a reader — no T, no fractional seconds, no zone suffix — and
                    // leaves every other value exactly as it arrived.
                    const text = formatCell(v);
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
        <TraceDetailsModal kind={detail.kind} id={detail.id} onClose={() => setDetail(null)} />
      )}
    </div>
  );
}
