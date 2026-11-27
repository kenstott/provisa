// Copyright (c) 2026 Kenneth Stott
// Canary: 6d2f8a41-9c37-4e50-a1b8-5e7c3d9f0a62
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2234 (Review samples): the third step of a source's setup. Each table just registered is a
// tab showing a read-only sample of its first rows, read through the governed query as the
// person's own role: no export, filters, paging or row counts, which belong to Explore. A Query it
// link opens the query explorer for anyone who wants more. The step is done once every table's
// sample has been opened (the first tab is open from the start), or on Done.

import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import type { TFunction } from "i18next";
import { Alert, Anchor, Group, Loader, Stack, Tabs, Text } from "@mantine/core";
import { PreviewRefused, previewTable, runSql } from "../../api/admin";
import { resultRedirectText } from "../../components/resultRedirectText";
import { pagedViewerSql, viewerRequest } from "../../components/nativeParams";
import { useTableAccess, useTables } from "../../hooks/useAdminQueries";
import { QueryPreviewRows } from "../tables/QueryPreviewRows";
import type { Registered } from "../tables/useManyTables";
import type { ListedTable } from "../../types/admin";

/** Rows in a sample. */
export const SAMPLE_ROWS = 20;

interface Sample {
  columns: string[];
  rows: Record<string, unknown>[];
  error: string;
}

/** The first rows of a table, one governed read. A withdrawn table is read by the admin preview,
 * as the table viewer reads it. */
async function readSample(
  table: ListedTable,
  role: string,
  t: TFunction,
  language: string,
): Promise<Sample> {
  const withdrawn = table.withdrawn || table.sourceWithdrawn;
  try {
    const r = withdrawn
      ? await previewTable(
          table.id,
          role,
          viewerRequest(table, {}, [], [], [], 0, SAMPLE_ROWS),
        )
      : await runSql(pagedViewerSql(table, {}, [], [], [], 0, SAMPLE_ROWS), role, false, true);
    // A sample landed in the results store is there, not here: said as where it is.
    const error =
      "redirect" in r && r.redirect
        ? resultRedirectText(t, language, r.redirect)
        : "error" in r && typeof r.error === "string"
          ? r.error
          : "";
    return { columns: r.columns, rows: r.rows.slice(0, SAMPLE_ROWS), error };
  } catch (e) {
    return {
      columns: [],
      rows: [],
      error: e instanceof PreviewRefused || e instanceof Error ? e.message : String(e),
    };
  }
}

function TableSample({
  table,
  role,
  onOpenQuery,
}: {
  table: ListedTable;
  role: string | null;
  onOpenQuery: (graphql: string) => void;
}) {
  const { t, i18n } = useTranslation();
  const { access } = useTableAccess(table.id, role);
  const [sample, setSample] = useState<Sample | null>(null);
  useEffect(() => {
    if (role === null) return;
    let cancelled = false;
    readSample(table, role, t, i18n.language).then((s) => {
      if (!cancelled) setSample(s);
    });
    return () => {
      cancelled = true;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps -- t and the language change with i18n, not the read
  }, [table, role]);
  return (
    <Stack gap="sm" data-testid={`review-samples-panel-${table.tableName}`}>
      {role === null ? (
        <Alert color="red" variant="light">
          {t("sourceSetup.noRole")}
        </Alert>
      ) : sample === null ? (
        <Loader size="sm" />
      ) : sample.error !== "" ? (
        <Alert color="red" variant="light" data-testid="review-samples-error">
          {sample.error}
        </Alert>
      ) : sample.rows.length === 0 ? (
        <Text size="sm" c="dimmed">
          {t("sourceSetup.samplesEmpty")}
        </Text>
      ) : (
        <QueryPreviewRows
          rows={sample.rows}
          columns={sample.columns.map((name) => ({ name }))}
          sourceType="sample"
          title={null}
        />
      )}
      <Group>
        <Anchor
          component="button"
          type="button"
          size="sm"
          disabled={access === null}
          onClick={() => access && onOpenQuery(access.graphql)}
          data-testid="review-samples-query-it"
        >
          {t("sourceSetup.queryIt")}
        </Anchor>
      </Group>
    </Stack>
  );
}

export function ReviewSamplesStep({
  registered,
  role,
  onOpenQuery,
  onAllOpened,
}: {
  registered: Registered;
  role: string | null;
  /** The query explorer opened on a table's query ready to run. */
  onOpenQuery: (graphql: string) => void;
  /** Every table's sample has been opened: the step is done. */
  onAllOpened: () => void;
}) {
  const { t } = useTranslation();
  const { tables } = useTables();
  const shown = registered.tableIds
    .map((id) => tables.find((tbl) => tbl.id === id))
    .filter((tbl): tbl is ListedTable => tbl !== undefined);
  const [chosen, setChosen] = useState<string | null>(null);
  const [opened, setOpened] = useState<Set<string>>(new Set());
  const current = shown.find((tbl) => String(tbl.id) === chosen) ?? shown[0];
  const choose = (value: string | null) => {
    setChosen(value);
    if (value === null) return;
    const next = new Set(opened).add(value).add(String(shown[0].id));
    setOpened(next);
    if (shown.every((tbl) => next.has(String(tbl.id)))) onAllOpened();
  };
  return (
    <Stack gap="md" data-testid="review-samples-step">
      {registered.refused.length > 0 && (
        <Alert color="orange" variant="light" data-testid="review-samples-refused">
          {registered.refused.map((o) => (
            <Text key={o.tableName} size="sm">
              {t("registerManyTables.refused", { table: o.tableName, message: o.result.message })}
            </Text>
          ))}
        </Alert>
      )}
      {current === undefined ? (
        <Text size="sm" c="dimmed">
          {t("sourceSetup.samplesLoading")}
        </Text>
      ) : (
        <Tabs value={String(current.id)} onChange={choose} keepMounted={false}>
          <Tabs.List>
            {shown.map((tbl) => (
              <Tabs.Tab
                key={tbl.id}
                value={String(tbl.id)}
                data-testid={`review-samples-tab-${tbl.tableName}`}
              >
                {tbl.alias || tbl.tableName}
              </Tabs.Tab>
            ))}
          </Tabs.List>
          {shown.map((tbl) => (
            <Tabs.Panel key={tbl.id} value={String(tbl.id)} pt="sm">
              <TableSample table={tbl} role={role} onOpenQuery={onOpenQuery} />
            </Tabs.Panel>
          ))}
        </Tabs>
      )}
    </Stack>
  );
}
