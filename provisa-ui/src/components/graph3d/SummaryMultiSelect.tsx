// Copyright (c) 2026 Kenneth Stott
// Canary: 3e9b7d52-1a64-4c80-9f25-b6d8a2c4e713
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2128, REQ-2130: a filter picking several values that stays one line high in the graph's
// filter bar: it says how many are chosen (or the one chosen), and lists the choices, ticked,
// in its dropdown.

import { useTranslation } from "react-i18next";
import { CheckIcon, Combobox, Group, InputBase, useCombobox } from "@mantine/core";

export function SummaryMultiSelect({
  label,
  data,
  value,
  onChange,
  testId,
}: {
  /** What the filter chooses; its placeholder and accessible name. */
  label: string;
  data: string[];
  value: string[];
  onChange: (value: string[]) => void;
  testId: string;
}) {
  const { t } = useTranslation();
  const combobox = useCombobox({ onDropdownClose: () => combobox.resetSelectedOption() });
  const toggle = (v: string) =>
    onChange(value.includes(v) ? value.filter((x) => x !== v) : [...value, v]);
  const summary =
    value.length === 0
      ? null
      : value.length === 1
        ? value[0]
        : t("graph3d.selected", { count: value.length });
  return (
    <Combobox store={combobox} onOptionSubmit={toggle} withinPortal>
      <Combobox.Target>
        <InputBase
          component="button"
          type="button"
          size="xs"
          pointer
          rightSection={
            value.length > 0 ? (
              <Combobox.ClearButton onClear={() => onChange([])} />
            ) : (
              <Combobox.Chevron />
            )
          }
          rightSectionPointerEvents={value.length > 0 ? "all" : "none"}
          onClick={() => combobox.toggleDropdown()}
          aria-label={label}
          data-testid={testId}
        >
          {summary ?? <span className="g3d-placeholder">{label}</span>}
        </InputBase>
      </Combobox.Target>
      <Combobox.Dropdown>
        <Combobox.Options mah={260} style={{ overflowY: "auto" }}>
          {data.map((v) => (
            <Combobox.Option
              value={v}
              key={v}
              active={value.includes(v)}
              data-testid={`${testId}-option-${v}`}
            >
              <Group gap={6} wrap="nowrap">
                {value.includes(v) ? <CheckIcon size={10} /> : <span style={{ width: 10 }} />}
                <span>{v}</span>
              </Group>
            </Combobox.Option>
          ))}
        </Combobox.Options>
      </Combobox.Dropdown>
    </Combobox>
  );
}
