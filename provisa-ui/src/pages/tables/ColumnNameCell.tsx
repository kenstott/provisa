// Copyright (c) 2026 Kenneth Stott
// Canary: 1f6a9d3e-7b28-4c51-8e04-b93c2a6d7f18
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { Badge, Table } from "@mantine/core";
import { useTranslation } from "react-i18next";
import type { RegisteredTable, TableColumn } from "../../types/admin";
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
        <Badge
          ml={6}
          size="xs"
          variant="light"
          color={c.nativeFilterType === "path_param" ? "yellow" : "blue"}
          style={{ fontFamily: "monospace" }}
        >
          {c.nativeFilterType === "path_param"
            ? t("tableEditForm.pathBadge")
            : t("tableEditForm.queryBadge")}
        </Badge>
      )}
      {c.isForeignKey && (
        <Badge ml={6} size="xs" variant="light" color="green" style={{ fontFamily: "monospace" }}>
          {t("tableEditForm.fkBadge")}
        </Badge>
      )}
      {c.isAlternateKey && (
        <Badge ml={6} size="xs" variant="light" color="yellow" style={{ fontFamily: "monospace" }}>
          {t("tableEditForm.akBadge")}
        </Badge>
      )}
      {/* REQ-1360: metadata-only discoverability badges, gated by the table's own
    enableAggregates/enableGroupBy — never governed/reusable, that stays the
    named metrics: path. */}
      {table.enableAggregates && c.isImplicitMeasure && (
        <Badge ml={6} size="xs" variant="light" color="grape" style={{ fontFamily: "monospace" }}>
          {t("tableEditForm.measureBadge")}
        </Badge>
      )}
      {table.enableGroupBy && c.isImplicitDimension && (
        <Badge ml={6} size="xs" variant="light" color="cyan" style={{ fontFamily: "monospace" }}>
          {t("tableEditForm.dimBadge")}
        </Badge>
      )}
    </Table.Td>
  );
}
