// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1093: table-level UNIQUE constraints editor. An expandable panel; each row is one
// constraint with a Name field and a multi-select checkbox of the table's columns.

import { useTranslation } from "react-i18next";
import {
  ActionIcon,
  Accordion,
  Button,
  Group,
  MultiSelect,
  Stack,
  Text,
  TextInput,
  Tooltip,
} from "@mantine/core";
import { Info, Plus, X } from "lucide-react";
import type { UniqueConstraint } from "../../types/admin";

interface Props {
  uniques: UniqueConstraint[];
  columns: string[];
  onChange: (uniques: UniqueConstraint[]) => void;
}

export function UniquesPanel({ uniques, columns, onChange }: Props) {
  const { t } = useTranslation();

  const update = (i: number, patch: Partial<UniqueConstraint>) => {
    onChange(uniques.map((u, idx) => (idx === i ? { ...u, ...patch } : u)));
  };
  const remove = (i: number) => onChange(uniques.filter((_, idx) => idx !== i));
  const add = () => onChange([...uniques, { name: "", columns: [] }]);

  return (
    <Accordion variant="contained" mt="md" data-testid="uniques-panel">
      <Accordion.Item value="uniques">
        <Accordion.Control>
          <Group gap={6} align="center">
            <Text size="sm">{t("uniquesPanel.title")}</Text>
            {uniques.length > 0 && (
              <Text size="xs" c="dimmed">
                {t("uniquesPanel.summary", { count: uniques.length })}
              </Text>
            )}
            <Tooltip label={t("uniquesPanel.infoTooltip")} multiline w={320}>
              <ActionIcon
                variant="subtle"
                color="gray"
                size="xs"
                aria-label={t("uniquesPanel.infoTooltip")}
                onClick={(e) => e.stopPropagation()}
              >
                <Info size={12} />
              </ActionIcon>
            </Tooltip>
          </Group>
        </Accordion.Control>
        <Accordion.Panel>
          <Stack gap="sm">
            {uniques.map((u, i) => (
              <Group key={i} gap="sm" align="flex-end" wrap="nowrap" data-testid={`unique-row-${i}`}>
                <TextInput
                  label={t("uniquesPanel.nameLabel")}
                  placeholder={t("uniquesPanel.namePlaceholder")}
                  value={u.name}
                  onChange={(e) => update(i, { name: e.currentTarget.value })}
                  style={{ flex: 1 }}
                  data-testid={`unique-name-${i}`}
                />
                <MultiSelect
                  label={t("uniquesPanel.columnsLabel")}
                  placeholder={t("uniquesPanel.columnsPlaceholder")}
                  data={columns.map((c) => ({ value: c, label: c }))}
                  value={u.columns}
                  onChange={(cols) => update(i, { columns: cols })}
                  searchable
                  style={{ flex: 2 }}
                  data-testid={`unique-columns-${i}`}
                />
                <ActionIcon
                  variant="subtle"
                  color="gray"
                  aria-label={t("uniquesPanel.removeConstraint")}
                  onClick={() => remove(i)}
                  data-testid={`unique-remove-${i}`}
                >
                  <X size={16} />
                </ActionIcon>
              </Group>
            ))}
            <Group>
              <Button
                variant="light"
                size="xs"
                leftSection={<Plus size={14} />}
                onClick={add}
                data-testid="unique-add-button"
              >
                {t("uniquesPanel.addConstraint")}
              </Button>
            </Group>
          </Stack>
        </Accordion.Panel>
      </Accordion.Item>
    </Accordion>
  );
}
