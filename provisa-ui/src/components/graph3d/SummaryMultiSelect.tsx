// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2128, REQ-2130: a filter picking several values that stays one line high in the graph's
// filter bar. It says what is chosen -- all of them, the one chosen, or how many of how many -- and
// lists the choices, ticked, in its dropdown, with a search box when the list is long.

import { useState } from "react";
import { useTranslation } from "react-i18next";
import { CheckIcon, Combobox, Group, InputBase, useCombobox } from "@mantine/core";

export interface SummaryOption {
  value: string;
  label: string;
}

export function SummaryMultiSelect({
  label,
  data,
  value,
  onChange,
  testId,
  allText,
  someText,
  searchable = false,
}: {
  /** What the filter chooses; its placeholder and accessible name. */
  label: string;
  /** The choices: a value shown as itself, or a value with the label it is shown by. */
  data: (string | SummaryOption)[];
  value: string[];
  onChange: (value: string[]) => void;
  testId: string;
  /** Said when every choice is chosen ("All domains"). */
  allText: string;
  /** Said when several, not all, are chosen ("2 of 5 domains"). */
  someText: (count: number, total: number) => string;
  searchable?: boolean;
}) {
  const { t } = useTranslation();
  const [search, setSearch] = useState("");
  const combobox = useCombobox({
    onDropdownClose: () => {
      combobox.resetSelectedOption();
      setSearch("");
    },
  });
  const options = data.map((d) => (typeof d === "string" ? { value: d, label: d } : d));
  // A chosen value the choices no longer offer (the graph reloaded without it) is still a choice
  // the reader made: it is named by its value, so it can be seen and cleared.
  const labelOf = (v: string) => options.find((o) => o.value === v)?.label ?? v;
  const toggle = (v: string) =>
    onChange(value.includes(v) ? value.filter((x) => x !== v) : [...value, v]);
  const every = options.length > 0 && options.every((o) => value.includes(o.value));
  const summary =
    value.length === 0
      ? null
      : every
        ? allText
        : value.length === 1
          ? labelOf(value[0])
          : someText(value.length, options.length);
  const listed = search
    ? options.filter((o) => o.label.toLowerCase().includes(search.toLowerCase()))
    : options;
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
        {searchable && (
          <Combobox.Search
            value={search}
            onChange={(e) => setSearch(e.currentTarget.value)}
            placeholder={t("graph3d.search")}
            data-testid={`${testId}-search`}
          />
        )}
        <Combobox.Options mah={260} style={{ overflowY: "auto" }}>
          {listed.map((o) => (
            <Combobox.Option
              value={o.value}
              key={o.value}
              active={value.includes(o.value)}
              data-testid={`${testId}-option-${o.value}`}
            >
              <Group gap={6} wrap="nowrap">
                {value.includes(o.value) ? <CheckIcon size={10} /> : <span style={{ width: 10 }} />}
                <span>{o.label}</span>
              </Group>
            </Combobox.Option>
          ))}
        </Combobox.Options>
      </Combobox.Dropdown>
    </Combobox>
  );
}
