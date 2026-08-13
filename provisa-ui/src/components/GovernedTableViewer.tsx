// Copyright (c) 2026 Kenneth Stott
// Canary: 42ea705d-d2ef-4000-bd8f-bef980d1a96d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1386: server-paged governed viewer over a registered table, shared by the
// Tables preview modal and the admin Reports viewer. The full dataset is NEVER
// loaded: each page is its own governed SELECT * with LIMIT pageSize+1 /
// OFFSET, and filter/sort/group choices are pushed into the query (WHERE /
// ORDER BY) so they act on the whole relation — a billion-row table costs one
// page per view. Native API params are collected first (a path_param is
// required before any query can run). Choices persist per table and restore on
// the next visit. Mount with key=table.id — state is per-table by remount.

import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { ActionIcon, Alert, Button, Group, Loader, Text, TextInput } from "@mantine/core";
import { RefreshCw } from "lucide-react";
import { runSql } from "../api/admin";
import type { RegisteredTable } from "../types/admin";
import { ResultsGrid } from "../pages/sql/ResultsGrid";
import type { ProvenanceEntry } from "../pages/sql/exportXlsx";
import { useResultsGrid } from "../pages/sql/useResultsGrid";
import {
  optionalParamColumns,
  pagedViewerSql,
  requiredParamColumns,
  requiredParamsSatisfied,
} from "./nativeParams";

// REQ-1444: rows per statement while reading the whole relation for an export. Larger than any
// on-screen page size (the reader is a file, not a viewport) and still bounded, so one export is a
// handful of governed statements rather than one unbounded scan.
const EXPORT_CHUNK_SIZE = 5000;

interface PageResult {
  columns: string[];
  rows: Record<string, unknown>[];
  hasMore: boolean;
  error: string;
}

interface GovernedTableViewerProps {
  table: RegisteredTable;
  /** Title + refresh row (the Reports viewer shows it; the modal has its own title). */
  showTitle?: boolean;
}

export function GovernedTableViewer({ table, showTitle = false }: GovernedTableViewerProps) {
  const { t } = useTranslation();
  const requiredCols = requiredParamColumns(table);
  const optionalCols = optionalParamColumns(table);
  const hasParams = requiredCols.length > 0 || optionalCols.length > 0;
  const [paramValues, setParamValues] = useState<Record<string, string>>({});
  // Params the current dataset was activated with; null = blocked on required input.
  const [activeParams, setActiveParams] = useState<Record<string, string> | null>(
    requiredCols.length === 0 ? {} : null,
  );
  const [result, setResult] = useState<PageResult | null>(null);
  // The query key the current `result` answers; loading derives from the gap
  // between it and the live key (no state writes inside the effect body).
  const [fetchedKey, setFetchedKey] = useState<string | null>(null);
  const [refreshTick, setRefreshTick] = useState(0);
  const grid = useResultsGrid(
    result?.rows ?? [],
    result?.columns ?? [],
    `table:${table.schemaName}.${table.alias || table.tableName}`,
    { hasMore: result?.hasMore ?? false },
  );
  const { page, pageSize, sorts, filters, groupBy } = grid;
  const canRun = requiredParamsSatisfied(table, paramValues);

  // One page per query: refetch whenever the page or any pushed-down choice
  // changes. Debounced so filter keystrokes coalesce.
  // REQ-1438: pageSize is part of the key — the LIMIT is pushed into the query, so a resize is a
  // different page of the relation and has to be refetched, not re-sliced.
  const queryKey = JSON.stringify([
    page,
    pageSize,
    sorts,
    filters,
    groupBy,
    activeParams,
    refreshTick,
  ]);
  const loading = activeParams != null && fetchedKey !== queryKey;
  useEffect(() => {
    if (activeParams == null) return;
    let cancelled = false;
    const timer = setTimeout(() => {
      runSql(pagedViewerSql(table, activeParams, filters, sorts, groupBy, page, pageSize)).then(
        (r) => {
          if (cancelled) return;
          setResult({
            columns: r.columns,
            rows: r.rows.slice(0, pageSize),
            hasMore: r.rows.length > pageSize,
            error: r.error ?? "",
          });
          setFetchedKey(queryKey);
        },
      );
    }, 300);
    return () => {
      cancelled = true;
      clearTimeout(timer);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps -- queryKey is the serialized form of every input that must trigger a refetch
  }, [table, queryKey]);

  // REQ-1444: the XLSX export is the whole relation under the current filter/sort/group choices,
  // not the page on screen. Read it the same way the grid reads a page — the same governed
  // statement, one call per chunk with an advancing OFFSET — until a chunk comes back short.
  const fetchAllRows = async (): Promise<Record<string, unknown>[]> => {
    if (activeParams == null) throw new Error("export requested before params were supplied");
    const rows: Record<string, unknown>[] = [];
    for (let chunk = 0; ; chunk++) {
      const r = await runSql(
        pagedViewerSql(table, activeParams, filters, sorts, groupBy, chunk, EXPORT_CHUNK_SIZE),
      );
      if (r.error) throw new Error(r.error);
      rows.push(...r.rows.slice(0, EXPORT_CHUNK_SIZE));
      if (r.rows.length <= EXPORT_CHUNK_SIZE) return rows;
    }
  };

  // REQ-1441: what the exported workbook records about this relation. The statement is the one that
  // actually fetched the exported rows — same call, same arguments — not a reconstruction.
  // REQ-1444: the export issues it once per chunk with OFFSET advancing by EXPORT_CHUNK_SIZE.
  const provenanceFor = (params: Record<string, string>): ProvenanceEntry[] => {
    const entries: ProvenanceEntry[] = [
      {
        label: t("tablePreview.provRelation"),
        value: `${table.domainId}.${table.alias || table.tableName}`,
      },
      { label: t("tablePreview.provRegistered"), value: `${table.schemaName}.${table.tableName}` },
      { label: t("tablePreview.provSource"), value: table.sourceId },
    ];
    if (table.description)
      entries.push({ label: t("tablePreview.provDescription"), value: table.description });
    if (table.apiEndpoint)
      entries.push({ label: t("tablePreview.provEndpoint"), value: table.apiEndpoint });
    if (table.viewSql)
      entries.push({ label: t("tablePreview.provDefinition"), value: table.viewSql });
    const supplied = Object.entries(params).filter(([, v]) => v !== "");
    if (supplied.length > 0)
      entries.push({
        label: t("tablePreview.provParams"),
        value: supplied.map(([k, v]) => `${k} = ${v}`).join("\n"),
      });
    entries.push({
      label: t("tablePreview.provStatement"),
      value: pagedViewerSql(table, params, filters, sorts, groupBy, 0, EXPORT_CHUNK_SIZE),
    });
    return entries;
  };

  return (
    <div style={{ flex: 1, display: "flex", flexDirection: "column", overflow: "hidden" }}>
      {showTitle && (
        <Group px="xs" py={4} gap="xs">
          <Text size="sm" fw={600}>
            {table.alias || table.tableName}
          </Text>
          <ActionIcon
            variant="subtle"
            size="sm"
            aria-label={t("tablePreview.refresh")}
            onClick={() => setRefreshTick((n) => n + 1)}
            data-testid="refresh-report-btn"
          >
            <RefreshCw size={13} />
          </ActionIcon>
        </Group>
      )}
      {hasParams && (
        <Group
          gap="xs"
          mb="xs"
          px={showTitle ? "xs" : 0}
          align="flex-end"
          data-testid="table-preview-params"
        >
          {[...requiredCols, ...optionalCols].map((c) => (
            <TextInput
              key={c.columnName}
              size="xs"
              label={c.alias || c.columnName}
              required={c.nativeFilterType === "path_param"}
              value={paramValues[c.columnName] ?? ""}
              onChange={(e) => {
                // Read before the updater: a functional updater can run on a later render pass,
                // when the synthetic event has been pooled and currentTarget is null.
                const next = e.currentTarget.value;
                setParamValues((prev) => ({ ...prev, [c.columnName]: next }));
              }}
              data-testid={`preview-param-${c.columnName}`}
            />
          ))}
          <Button
            size="compact-sm"
            disabled={!canRun || loading}
            onClick={() => {
              grid.setPage(0);
              setActiveParams({ ...paramValues });
            }}
            data-testid="preview-run-btn"
          >
            {t("tablePreview.run")}
          </Button>
        </Group>
      )}
      {activeParams == null ? (
        <Text size="sm" c="dimmed" ta="center" py="xl" data-testid="table-preview-params-hint">
          {t("tablePreview.paramsRequired")}
        </Text>
      ) : loading && result == null ? (
        <Group justify="center" py="xl">
          <Loader size="sm" />
        </Group>
      ) : result?.error ? (
        <Alert color="red" data-testid="table-preview-error">
          <pre style={{ margin: 0, whiteSpace: "pre-wrap", fontFamily: "monospace" }}>
            {result.error}
          </pre>
        </Alert>
      ) : result != null && result.columns.length === 0 ? (
        // REQ-1436: only a relation with no projection at all gets bare text. An empty PAGE keeps
        // the grid: replacing it took the header row away, and with it the filter inputs, so a
        // filter that matched nothing could no longer be cleared.
        <Text size="sm" c="dimmed" ta="center" py="xl">
          {t("tablePreview.noRows")}
        </Text>
      ) : result != null ? (
        <div style={{ flex: 1, overflow: "hidden", opacity: loading ? 0.6 : 1 }}>
          <ResultsGrid
            grid={grid}
            totalRowCount={result.rows.length}
            provenance={provenanceFor(activeParams)}
            fetchAllRows={fetchAllRows}
            exportName={table.alias || table.tableName}
          />
        </div>
      ) : null}
    </div>
  );
}
