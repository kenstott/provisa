// Copyright (c) 2026 Kenneth Stott
// Canary: 1f6a9d3e-7b28-4c51-8e04-b93c2a6d7f18
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { Table } from "@mantine/core";
import { useTranslation } from "react-i18next";
import type { RegisteredTable, TableColumn } from "../../types/admin";
import { ColumnBadge } from "./ColumnBadge";
import { ColumnGlossaryHover } from "./ColumnGlossaryHover";

/** The table editor's column-name cell: the name, its glossary hover and its key/kind badges. */
export function ColumnNameCell({ table, c }: { table: RegisteredTable; c: TableColumn }) {
  const { t } = useTranslation();
  return (
    <Table.Td>
      {/* REQ-1387: glossary term summary card on column-name hover. */}
      <ColumnGlossaryHover tableId={table.id} columnName={c.columnName}>
        <code>{c.columnName}</code>
      </ColumnGlossaryHover>
      {c.nativeFilterType && (
        <ColumnBadge color={c.nativeFilterType === "path_param" ? "yellow" : "blue"}>
          {c.nativeFilterType === "path_param"
            ? t("tableEditForm.pathBadge")
            : t("tableEditForm.queryBadge")}
        </ColumnBadge>
      )}
      {c.isForeignKey && <ColumnBadge color="green">{t("tableEditForm.fkBadge")}</ColumnBadge>}
      {c.isAlternateKey && <ColumnBadge color="yellow">{t("tableEditForm.akBadge")}</ColumnBadge>}
      {/* REQ-1360: metadata-only discoverability badges, gated by the table's own
    enableAggregates/enableGroupBy — never governed/reusable, that stays the
    named metrics: path. */}
      {table.enableAggregates && c.isImplicitMeasure && (
        <ColumnBadge color="grape">{t("tableEditForm.measureBadge")}</ColumnBadge>
      )}
      {table.enableGroupBy && c.isImplicitDimension && (
        <ColumnBadge color="cyan">{t("tableEditForm.dimBadge")}</ColumnBadge>
      )}
    </Table.Td>
  );
}
