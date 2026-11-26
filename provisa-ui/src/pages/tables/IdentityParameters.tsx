// Copyright (c) 2026 Kenneth Stott
// Canary: efc7d6a5-b58f-43e4-8c33-8344de3d74a6
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1986: a parameterized table says so where it is identified, under its query links: one row
// per parameter, by the name a caller gives it, with its type, whether a read must give it, and
// what it means.

import { Table, Text } from "@mantine/core";
import { useTranslation } from "react-i18next";
import type { TableColumn } from "../../types/admin";
import { isRequiredParam } from "../../components/nativeParams";
import { parameterName } from "./parameters";

export function IdentityParameters({ columns }: { columns: TableColumn[] }) {
  const { t } = useTranslation();
  const parameters = columns.filter((c) => c.nativeFilterType);
  if (parameters.length === 0) return null;
  return (
    <div data-testid="table-identity-parameters">
      <Text size="sm" fw={600} mb={4}>
        {t("parametersPanel.title")}
      </Text>
      <Table className="data-table" mb="md">
        <Table.Thead>
          <Table.Tr>
            <Table.Th>{t("parametersPanel.name")}</Table.Th>
            <Table.Th>{t("parametersPanel.type")}</Table.Th>
            <Table.Th>{t("parametersPanel.required")}</Table.Th>
            <Table.Th>{t("parametersPanel.description")}</Table.Th>
          </Table.Tr>
        </Table.Thead>
        <Table.Tbody>
          {parameters.map((c) => {
            const name = parameterName(c.columnName);
            const required = isRequiredParam(c);
            return (
              <Table.Tr key={c.columnName} data-testid={`table-identity-parameter-${name}`}>
                <Table.Td>
                  <code>{name}</code>
                </Table.Td>
                <Table.Td ff="monospace">{c.dataType ?? ""}</Table.Td>
                <Table.Td data-testid={`table-identity-parameter-${name}-required`}>
                  {required ? t("parametersPanel.required") : t("parametersPanel.optional")}
                </Table.Td>
                <Table.Td className="reasoning-cell">{c.description ?? ""}</Table.Td>
              </Table.Tr>
            );
          })}
        </Table.Tbody>
      </Table>
    </div>
  );
}
