// Copyright (c) 2026 Kenneth Stott
// Canary: 322045a8-5045-42e2-9125-d18c12be2c97
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The rows a query source's preview returned, shown under the query in the register-table form.

import { Stack, Table, Text } from "@mantine/core";
import { useTranslation } from "react-i18next";

export function QueryPreviewRows({
  rows,
  columns,
  sourceType,
  title,
}: {
  rows: Record<string, unknown>[];
  columns: { name: string }[];
  sourceType: string;
  /** Heading above the rows; null shows none (a sample carries no row count). Default: the
   * preview heading with its row count. */
  title?: string | null;
}) {
  const { t } = useTranslation();
  return (
    <Stack gap="xs">
      {title !== null && (
        <Text fw={600} fz="sm">
          {title ?? t("registerTableForm.neo4jPreviewRows", { count: rows.length })}
        </Text>
      )}
      <Table.ScrollContainer minWidth={400}>
        <Table striped withTableBorder verticalSpacing="xs" fz="xs">
          <Table.Thead>
            <Table.Tr>
              {columns.map((c) => (
                <Table.Th key={c.name}>{c.name}</Table.Th>
              ))}
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody data-testid={`register-table-${sourceType}-preview-rows`}>
            {rows.map((row, i) => (
              <Table.Tr key={i}>
                {columns.map((c) => (
                  <Table.Td key={c.name}>{String(row[c.name] ?? "")}</Table.Td>
                ))}
              </Table.Tr>
            ))}
          </Table.Tbody>
        </Table>
      </Table.ScrollContainer>
    </Stack>
  );
}
