// Copyright (c) 2026 Kenneth Stott
// Canary: a4f5779c-5720-44fd-8566-2b978a166f45
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1986: a table's parameters -- the values a caller passes to the source to read it. Each is
// listed with its type, how it reaches the source, whether it is opaque (an input to the source's
// call) or a real column of the table, and whether it is required. A parameter the source requires
// is required for good: its switch is on and locked. A steward makes an optional one required, or
// optional again, with the switch. A table that requires a parameter says what it cannot give.

import { Alert, Group, Switch, Table, Text } from "@mantine/core";
import { Info } from "lucide-react";
import { useTranslation } from "react-i18next";
import type { TableColumn } from "../../types/admin";
import { isOpaque, isRequired, parameterName, requiredParameters } from "./parameters";

interface Props {
  columns: TableColumn[];
  /** Given, the switches edit the steward's choice; absent, the panel only shows it. */
  onRequiredChange?: (columnIndex: number, required: boolean) => void;
}

export function ParametersPanel({ columns, onRequiredChange }: Props) {
  const { t } = useTranslation();
  const parameters = columns.map((c, i) => ({ c, i })).filter(({ c }) => c.nativeFilterType);
  if (parameters.length === 0) return null;
  const required = requiredParameters(columns);

  return (
    <div data-testid="parameters-panel">
      <Group gap={6} align="center" className="parameters-panel__heading">
        <Text size="sm" fw={600}>
          {t("parametersPanel.title")}
        </Text>
        <Text size="xs" c="dimmed">
          {t("parametersPanel.summary", {
            count: parameters.length,
            required: parameters.filter(({ c }) => isRequired(c)).length,
          })}
        </Text>
      </Group>
      {required.length > 0 && (
        <Alert
          variant="light"
          color="blue"
          icon={<Info size={16} aria-hidden />}
          mb="sm"
          data-testid="parameters-limits"
        >
          {t("parametersPanel.limits", { parameters: required.join(", ") })}
        </Alert>
      )}
      <Table className="data-table">
        <Table.Thead>
          <Table.Tr>
            <Table.Th>{t("parametersPanel.name")}</Table.Th>
            <Table.Th>{t("parametersPanel.type")}</Table.Th>
            <Table.Th>{t("parametersPanel.reaches")}</Table.Th>
            <Table.Th>{t("parametersPanel.meaning")}</Table.Th>
            <Table.Th>{t("parametersPanel.required")}</Table.Th>
          </Table.Tr>
        </Table.Thead>
        <Table.Tbody>
          {parameters.map(({ c, i }) => {
            const name = parameterName(c.columnName);
            const bySource = Boolean(c.nativeFilterRequired);
            return (
              <Table.Tr key={c.columnName} data-testid={`parameter-${name}`}>
                <Table.Td>
                  <code>{name}</code>
                </Table.Td>
                <Table.Td>{c.dataType ?? ""}</Table.Td>
                <Table.Td>{t(`parametersPanel.kind.${c.nativeFilterType}`)}</Table.Td>
                <Table.Td data-testid={`parameter-${name}-meaning`}>
                  {t(isOpaque(c) ? "parametersPanel.opaque" : "parametersPanel.realColumn")}
                </Table.Td>
                <Table.Td>
                  <Switch
                    size="sm"
                    checked={isRequired(c)}
                    disabled={bySource || onRequiredChange === undefined}
                    aria-label={t("parametersPanel.requiredAria", { name })}
                    description={bySource ? t("parametersPanel.requiredBySource") : undefined}
                    onChange={(e) => onRequiredChange?.(i, e.currentTarget.checked)}
                  />
                </Table.Td>
              </Table.Tr>
            );
          })}
        </Table.Tbody>
      </Table>
    </div>
  );
}
