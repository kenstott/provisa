// Copyright (c) 2026 Kenneth Stott
// Canary: ff511d1f-2c93-40ca-84b7-d92d048c2720
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2158: a product member's contract, its output port, as the product's panel shows it: its
// version and status, what it is for, its terms, its declared service levels and the columns it
// promises. Freshness and promises (the product's checks) are read with the member, not here.

import { useTranslation } from "react-i18next";
import { Badge, Group, List, Stack, Table, Text } from "@mantine/core";
import type { DataContract } from "../../types/admin";

function slaText(property: string, value: unknown, unit?: string | null): string {
  const shown = value === null || value === undefined ? "" : String(value);
  return [property, [shown, unit].filter(Boolean).join(" ")].filter(Boolean).join(": ");
}

export function ContractView({ contract }: { contract: DataContract }) {
  const { t } = useTranslation();
  const { terms } = contract;
  const termRows = [
    [t("dataProductsTab.contract.usage"), terms.usage],
    [t("dataProductsTab.contract.limitations"), terms.limitations],
    [t("dataProductsTab.contract.noticePeriod"), terms.noticePeriod],
  ].filter(([, v]) => v);
  return (
    <Stack gap={6} data-testid={`data-contract-${contract.tableId}`}>
      <Group gap="xs" wrap="wrap">
        <Text size="sm" ff="monospace" fw={500}>
          {contract.schemaName}.{contract.tableName}
        </Text>
        <Badge size="sm" variant="light" data-testid={`data-contract-version-${contract.tableId}`}>
          {t("dataProductsTab.contract.versionBadge", { version: contract.version })}
        </Badge>
        <Badge
          size="sm"
          variant="outline"
          color={contract.status === "active" ? "green" : "gray"}
          data-testid={`data-contract-status-${contract.tableId}`}
        >
          {t(`dataProductsTab.contract.status.${contract.status}`)}
        </Badge>
      </Group>
      {contract.description && <Text size="sm">{contract.description}</Text>}
      {termRows.map(([label, value]) => (
        <Text size="sm" key={label}>
          <Text component="span" size="sm" c="var(--text-muted)">
            {label}:
          </Text>{" "}
          {value}
        </Text>
      ))}
      {contract.sla.length > 0 && (
        <List size="sm" data-testid={`data-contract-sla-${contract.tableId}`}>
          {contract.sla.map((s) => (
            <List.Item key={s.property}>{slaText(s.property, s.value, s.unit)}</List.Item>
          ))}
        </List>
      )}
      <Table
        withTableBorder
        striped
        fz="sm"
        data-testid={`data-contract-columns-${contract.tableId}`}
      >
        <Table.Thead>
          <Table.Tr>
            <Table.Th>{t("dataProductsTab.contract.column")}</Table.Th>
            <Table.Th>{t("dataProductsTab.contract.type")}</Table.Th>
            <Table.Th>{t("dataProductsTab.contract.required")}</Table.Th>
          </Table.Tr>
        </Table.Thead>
        <Table.Tbody>
          {contract.columns.map((c) => (
            <Table.Tr key={c.name}>
              <Table.Td ff="monospace">{c.name}</Table.Td>
              <Table.Td ff="monospace">{c.type}</Table.Td>
              <Table.Td>{c.required ? t("dataProductsTab.contract.yes") : ""}</Table.Td>
            </Table.Tr>
          ))}
        </Table.Tbody>
      </Table>
    </Stack>
  );
}
