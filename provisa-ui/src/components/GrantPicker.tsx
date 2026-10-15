// Copyright (c) 2026 Kenneth Stott
// Canary: 1809bfbb-25a8-4436-87c9-82f8bf196066
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A grant list: the roles a reader may hold any one of. "Combine" adds one entry a reader must
// hold every role of -- a role and a claim role (analyst and region__eu), shown as one pill.

import { useState } from "react";
import { useTranslation } from "react-i18next";
import { ActionIcon, Button, Group, Popover, Stack, TagsInput, Text } from "@mantine/core";
import { Combine } from "lucide-react";
import { MultiSelect } from "./MultiSelect";
import { COMPOSITE, compositeEntry, grantLabel } from "../lib/grants";

export function GrantPicker({
  options,
  value,
  onChange,
  ariaLabel,
  label,
  disabled,
  testId,
}: {
  options: { id: string; label: string }[];
  value: string[];
  onChange: (selected: string[]) => void;
  ariaLabel?: string;
  label?: string;
  disabled?: boolean;
  testId?: string;
}) {
  const { t } = useTranslation();
  const [open, setOpen] = useState(false);
  const [parts, setParts] = useState<string[]>([]);

  const known = new Set(options.map((o) => o.id));
  // An entry the role list does not hold (a composite, a claim role) is still a choice to show.
  const shown = [
    ...options,
    ...value
      .filter((v) => !known.has(v))
      .map((v) => ({ id: v, label: v.includes(COMPOSITE) ? grantLabel(v, t) : v })),
  ];
  // A claim role's sub-roles (region__eu) are as many as its values: typed, not listed.
  const suggestions = options.map((o) => o.id);

  const add = () => {
    const entry = compositeEntry(parts);
    if (!value.includes(entry)) onChange([...value, entry]);
    setParts([]);
    setOpen(false);
  };

  return (
    <Group gap={4} wrap="nowrap" align="flex-start" data-testid={testId}>
      <MultiSelect
        options={shown}
        value={value}
        onChange={onChange}
        ariaLabel={ariaLabel}
        label={label}
        disabled={disabled}
      />
      <Popover opened={open} onChange={setOpen} withinPortal position="bottom-end" shadow="md">
        <Popover.Target>
          <ActionIcon
            variant="subtle"
            mt={label ? 24 : 4}
            disabled={disabled}
            aria-label={t("grantPicker.combine")}
            title={t("grantPicker.combine")}
            onClick={() => setOpen((o) => !o)}
            data-testid={testId ? `${testId}-combine` : undefined}
          >
            <Combine size={16} />
          </ActionIcon>
        </Popover.Target>
        <Popover.Dropdown maw={340}>
          <Stack gap="xs">
            <Text size="sm">{t("grantPicker.combineHelp")}</Text>
            {/* Add sits beside the parts: their suggestion list opens below them. */}
            <Group gap="xs" align="flex-end" wrap="nowrap">
              <TagsInput
                style={{ flex: 1 }}
                label={t("grantPicker.parts")}
                placeholder={t("grantPicker.partsPlaceholder")}
                data={suggestions}
                value={parts}
                onChange={setParts}
                comboboxProps={{ withinPortal: false }}
                data-testid={testId ? `${testId}-parts` : undefined}
              />
              <Button
                size="sm"
                onClick={add}
                disabled={parts.length === 0}
                data-testid={testId ? `${testId}-add` : undefined}
              >
                {t("grantPicker.add")}
              </Button>
            </Group>
          </Stack>
        </Popover.Dropdown>
      </Popover>
    </Group>
  );
}
