// Copyright (c) 2026 Kenneth Stott
// Canary: ec207bfc-9767-4d1d-a8e0-52ab6ac02755
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2180 (amended 2026-11-24): a table's suggestion count shows as a badge on that table in the
// Tables page, seen by its domain's stewards and owners, and opens that table's suggestions. The
// server answers the counts of the domains whose suggestions go to the caller, so a reader who is
// neither sees no badge.

import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { Alert, Badge, Loader, Table } from "@mantine/core";
import { ModalLayout } from "../../components/modal/ModalLayout";
import { ListEmpty, ListHead, ListRow, ListTable } from "../../components/list/ListTable";
import { tableSuggestions, type Suggestion } from "../../api/productSuggestions";
import { errorText, keyOf, type SuggestedTable } from "./suggestionCounts";

/** The count of a table's suggestions; drawn only when some suggestion names it, and opens
 *  them. */
export function SuggestionsBadge({
  count,
  table,
  onOpen,
}: {
  count: number | undefined;
  table: SuggestedTable;
  onOpen: (table: SuggestedTable) => void;
}) {
  const { t } = useTranslation();
  if (count === undefined) return null;
  return (
    <Badge
      component="button"
      type="button"
      size="xs"
      variant="filled"
      color="orange"
      style={{ cursor: "pointer" }}
      title={t("suggestProduct.badgeTitle", { count })}
      aria-label={t("suggestProduct.badgeTitle", { count })}
      onClick={(e) => {
        // The badge sits in the table's row: opening the suggestions does not select the row.
        e.stopPropagation();
        onOpen(table);
      }}
      data-testid={`table-suggestions-${keyOf(table.domainId, table.tableName)}`}
    >
      {count}
    </Badge>
  );
}

/** The suggestions naming one table, newest first. */
export function TableSuggestionsModal({
  table,
  onClose,
}: {
  table: SuggestedTable | null;
  onClose: () => void;
}) {
  const { t } = useTranslation();
  return (
    <ModalLayout
      opened={table !== null}
      onClose={onClose}
      size="xl"
      title={
        table
          ? t("suggestProduct.listTitle", { table: keyOf(table.domainId, table.tableName) })
          : ""
      }
      data-testid="table-suggestions-modal"
    >
      {table && <SuggestionsList key={keyOf(table.domainId, table.tableName)} table={table} />}
    </ModalLayout>
  );
}

function SuggestionsList({ table }: { table: SuggestedTable }) {
  const { t } = useTranslation();
  const [rows, setRows] = useState<Suggestion[] | null>(null);
  const [problem, setProblem] = useState<string | null>(null);
  useEffect(() => {
    let live = true;
    tableSuggestions(table.domainId, table.tableName).then(
      (found) => {
        if (live) setRows(found);
      },
      (e: unknown) => {
        if (live) setProblem(errorText(e));
      },
    );
    return () => {
      live = false;
    };
  }, [table.domainId, table.tableName]);
  if (problem !== null) {
    return (
      <Alert color="red" data-testid="table-suggestions-problem">
        {problem}
      </Alert>
    );
  }
  if (rows === null) return <Loader size="sm" data-testid="table-suggestions-loading" />;
  return (
    <ListTable testId="table-suggestions-list" minWidth={560}>
      <ListHead
        columns={[
          t("suggestProduct.colWhen"),
          t("suggestProduct.colBy"),
          t("suggestProduct.colTables"),
          t("suggestProduct.colReason"),
        ]}
      />
      <Table.Tbody>
        {rows.length === 0 ? (
          <ListEmpty colSpan={4} testId="table-suggestions-empty">
            {t("suggestProduct.none")}
          </ListEmpty>
        ) : (
          rows.map((s) => (
            <ListRow key={s.id} itemKey={s.id} testId={`table-suggestion-${s.id}`}>
              <Table.Td style={{ whiteSpace: "nowrap" }}>
                {new Date(s.created_at).toLocaleString()}
              </Table.Td>
              <Table.Td>{s.suggested_by}</Table.Td>
              <Table.Td style={{ fontFamily: "monospace" }}>{s.tables.join(", ")}</Table.Td>
              <Table.Td style={{ whiteSpace: "pre-wrap" }}>{s.reason}</Table.Td>
            </ListRow>
          ))
        )}
      </Table.Tbody>
    </ListTable>
  );
}
