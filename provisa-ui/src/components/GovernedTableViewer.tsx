// Copyright (c) 2026 Kenneth Stott
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
import { useResultsGrid } from "../pages/sql/useResultsGrid";
import { PAGE_SIZE } from "../pages/sql/types";
import {
  optionalParamColumns,
  pagedViewerSql,
  requiredParamColumns,
  requiredParamsSatisfied,
} from "./nativeParams";

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
    `table:${table.domainId || table.schemaName}.${table.alias || table.tableName}`,
    { hasMore: result?.hasMore ?? false },
  );
  const { page, sorts, filters, groupBy } = grid;
  const canRun = requiredParamsSatisfied(table, paramValues);

  // One page per query: refetch whenever the page or any pushed-down choice
  // changes. Debounced so filter keystrokes coalesce.
  const queryKey = JSON.stringify([page, sorts, filters, groupBy, activeParams, refreshTick]);
  const loading = activeParams != null && fetchedKey !== queryKey;
  useEffect(() => {
    if (activeParams == null) return;
    let cancelled = false;
    const timer = setTimeout(() => {
      runSql(pagedViewerSql(table, activeParams, filters, sorts, groupBy, page, PAGE_SIZE)).then(
        (r) => {
          if (cancelled) return;
          setResult({
            columns: r.columns,
            rows: r.rows.slice(0, PAGE_SIZE),
            hasMore: r.rows.length > PAGE_SIZE,
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
              onChange={(e) =>
                setParamValues((prev) => ({ ...prev, [c.columnName]: e.currentTarget.value }))
              }
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
      ) : result != null && result.rows.length === 0 && page === 0 ? (
        <Text size="sm" c="dimmed" ta="center" py="xl">
          {t("tablePreview.noRows")}
        </Text>
      ) : result != null ? (
        <div style={{ flex: 1, overflow: "hidden", opacity: loading ? 0.6 : 1 }}>
          <ResultsGrid grid={grid} totalRowCount={result.rows.length} />
        </div>
      ) : null}
    </div>
  );
}
