// Copyright (c) 2026 Kenneth Stott
// Canary: 4b7d1e93-2c58-4a6f-b0e9-8f3a5c2d7e61
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1159 (amended 2026-11-25): a command's output columns are tagged on the Tags page, as a table
// column's tags are put on it: one row per declared output column, its tags edited in place with
// the shared tag control. The command's Columns tab shows them read-only and links here.

import { Alert, Table, Text, Title } from "@mantine/core";
import { useTranslation } from "react-i18next";
import { useCommandOutputColumns } from "../../hooks/useCommandOutputColumns";
import { TagControl } from "../TagControl";
import { EntityLink } from "../EntityLink";

export function CommandColumnTags() {
  const { t } = useTranslation();
  const { columns, error } = useCommandOutputColumns();
  return (
    <div data-testid="command-column-tags">
      <Title order={5} mb="xs">
        {t("tagsTab.commandColumnsTitle")}
      </Title>
      {error !== null && (
        <Alert color="red" data-testid="command-column-tags-error">
          {error}
        </Alert>
      )}
      {columns !== null && columns.length === 0 && (
        <Text size="sm" c="dimmed" data-testid="command-column-tags-empty">
          {t("tagsTab.commandColumnsNone")}
        </Text>
      )}
      {columns !== null && columns.length > 0 && (
        <Table.ScrollContainer minWidth={640}>
          <Table striped withTableBorder verticalSpacing="xs">
            <Table.Thead>
              <Table.Tr>
                <Table.Th>{t("tagsTab.colCommand")}</Table.Th>
                <Table.Th>{t("tagsTab.colOutputColumn")}</Table.Th>
                <Table.Th>{t("tagsTab.colTags")}</Table.Th>
              </Table.Tr>
            </Table.Thead>
            <Table.Tbody>
              {columns.map(({ command, column }) => (
                <Table.Tr key={`${command}\u0000${column}`}>
                  <Table.Td>
                    <EntityLink kind="command" id={command}>
                      {command}
                    </EntityLink>
                  </Table.Td>
                  <Table.Td ff="monospace" fz="sm">
                    {column}
                  </Table.Td>
                  <Table.Td>
                    <TagControl
                      objectType="command_column"
                      commandName={command}
                      columnName={column}
                    />
                  </Table.Td>
                </Table.Tr>
              ))}
            </Table.Tbody>
          </Table>
        </Table.ScrollContainer>
      )}
    </div>
  );
}
