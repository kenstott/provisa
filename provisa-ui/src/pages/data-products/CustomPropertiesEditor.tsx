// Copyright (c) 2026 Kenneth Stott
// Canary: 3c9a1e4d-7b62-4f0a-9e1c-5a8d2f6b0c17
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useTranslation } from "react-i18next";
import { ActionIcon, Group, Stack, Text, TextInput, Tooltip } from "@mantine/core";
import { Plus, Trash2 } from "lucide-react";

interface CustomPropertiesEditorProps {
  value: Record<string, string>;
  onChange: (next: Record<string, string>) => void;
}

// REQ-1660: custom_properties is a JSON dict wire-format, but per user instruction the UI edits
// it as a simple ordered key-value list rather than a raw JSON textarea.
export function CustomPropertiesEditor({ value, onChange }: CustomPropertiesEditorProps) {
  const { t } = useTranslation();
  const entries = Object.entries(value);

  const setEntry = (index: number, key: string, val: string) => {
    const next = entries.map(([k, v], i) => (i === index ? [key, val] : [k, v]));
    onChange(Object.fromEntries(next));
  };

  const removeEntry = (index: number) => {
    onChange(Object.fromEntries(entries.filter((_, i) => i !== index)));
  };

  const addEntry = () => {
    // A blank key is allowed transiently while typing; it's overwritten as soon as the user
    // types a real key since Object.fromEntries keeps only the last "" key.
    onChange({ ...value, "": "" });
  };

  return (
    <Stack gap={4}>
      <Text size="sm" fw={500}>
        {t("dataProductsTab.customPropertiesLabel")}
      </Text>
      <Text size="xs" c="var(--text-muted)">
        {t("dataProductsTab.customPropertiesDesc")}
      </Text>
      {entries.map(([key, val], i) => (
        <Group key={i} gap="xs" wrap="nowrap">
          <TextInput
            flex={1}
            value={key}
            onChange={(e) => setEntry(i, e.target.value, val)}
            placeholder={t("dataProductsTab.customPropertiesKeyPlaceholder")}
            data-testid={`data-product-custom-property-key-${i}`}
          />
          <TextInput
            flex={1}
            value={val}
            onChange={(e) => setEntry(i, key, e.target.value)}
            placeholder={t("dataProductsTab.customPropertiesValuePlaceholder")}
            data-testid={`data-product-custom-property-value-${i}`}
          />
          <Tooltip label={t("dataProductsTab.customPropertiesRemove", { key: key || i })}>
            <ActionIcon
              variant="subtle"
              color="red"
              aria-label={t("dataProductsTab.customPropertiesRemove", { key: key || i })}
              onClick={() => removeEntry(i)}
              data-testid={`data-product-custom-property-remove-${i}`}
            >
              <Trash2 size={14} />
            </ActionIcon>
          </Tooltip>
        </Group>
      ))}
      <Group>
        <ActionIcon
          variant="subtle"
          aria-label={t("dataProductsTab.customPropertiesAdd")}
          onClick={addEntry}
          data-testid="data-product-custom-property-add"
        >
          <Plus size={14} />
        </ActionIcon>
      </Group>
    </Stack>
  );
}
