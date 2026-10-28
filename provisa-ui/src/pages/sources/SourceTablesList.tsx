// Copyright (c) 2026 Kenneth Stott
// Canary: fe9fd245-2bd8-4aa2-a387-482fa91d0470
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { Anchor, Badge, Table, Text } from "@mantine/core";
import { useTranslation } from "react-i18next";
import { Link } from "react-router-dom";
import type { TableRow } from "../../types/admin";

/** The tables registered from a source, each linked to its own panel on the Tables page; a
 *  draft is marked as one. "—" when none is registered. */
export function SourceTablesList({ sourceId, tables }: { sourceId: string; tables: TableRow[] }) {
  const { t } = useTranslation();
  const own = tables.filter((tb) => tb.sourceId === sourceId);
  if (own.length === 0) {
    return (
      <Text size="sm" data-testid="source-tables-none">
        {t("detailFields.empty")}
      </Text>
    );
  }
  return (
    <Table className="data-table" data-testid="source-tables">
      <Table.Thead>
        <Table.Tr>
          <Table.Th>{t("sourceSections.tableName")}</Table.Th>
          <Table.Th>{t("sourceSections.tableDomain")}</Table.Th>
        </Table.Tr>
      </Table.Thead>
      <Table.Tbody>
        {own.map((tb) => (
          <Table.Tr key={tb.id} data-testid={`source-table-${tb.id}`}>
            <Table.Td>
              <Anchor component={Link} to={`/tables?table=${tb.id}`} size="sm">
                {/* As the Tables list names it: its alias, where it has one. */}
                {tb.alias || tb.tableName}
              </Anchor>
              {tb.draft && (
                <Badge size="xs" variant="light" color="gray" ml="xs">
                  {t("sourceSections.draft")}
                </Badge>
              )}
            </Table.Td>
            <Table.Td>{tb.domainId}</Table.Td>
          </Table.Tr>
        ))}
      </Table.Tbody>
    </Table>
  );
}
