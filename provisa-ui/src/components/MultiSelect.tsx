// Copyright (c) 2026 Kenneth Stott
// Canary: ea9667ae-371a-414f-be7a-2601c4eb9dfd
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { Button, Group, MultiSelect as MantineMultiSelect } from "@mantine/core";
import type { CSSProperties, ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { Tip } from "./Tip";

/** A button beside an option in the open list. */
export interface OptionAction {
  label: string;
  ariaLabel: string;
  onClick: () => void;
  loading?: boolean;
  disabled?: boolean;
}

export interface MultiSelectOption {
  id: string;
  label: string;
  disabled?: boolean;
  reason?: string;
  action?: OptionAction;
  group?: string;
}

/** Mantine's grouped data: options without a group first, then each group in order of appearance. */
function mantineData(options: MultiSelectOption[]) {
  const item = (o: MultiSelectOption) => ({ value: o.id, label: o.label, disabled: o.disabled });
  if (!options.some((o) => o.group)) return options.map(item);
  const groups = new Map<string, MultiSelectOption[]>();
  for (const o of options.filter((x) => x.group)) {
    groups.set(o.group as string, [...(groups.get(o.group as string) ?? []), o]);
  }
  return [
    ...options.filter((o) => !o.group).map(item),
    ...[...groups].map(([group, items]) => ({ group, items: items.map(item) })),
  ];
}

/** Multi-value picker backed by Mantine MultiSelect — provides the ARIA
 *  combobox pattern, keyboard navigation, and searchable options that the
 *  former hand-rolled checkbox-dropdown lacked (REQ-1009, REQ-1013).
 *  Public API is unchanged so existing call sites need no edits. */
export function MultiSelect({
  options,
  disabledHint,
  value,
  onChange,
  className,
  style,
  label,
  ariaLabel,
  placeholder,
  disabled,
  description,
  required,
  withAsterisk,
  "aria-labelledby": labelledBy,
  "aria-description": ariaDescription,
}: {
  /** An option `disabled` is shown and cannot be picked; its `reason` says why, else
   * `disabledHint`, the reason shared by every option that has none of its own. Its `action`, if
   * any, is a button beside it in the list: what can be done with it instead of picking it. An
   * option's `group` lists it under that heading. */
  options: MultiSelectOption[];
  disabledHint?: string;
  value: string[];
  onChange: (selected: string[]) => void;
  className?: string;
  style?: CSSProperties;
  // A form field gives the label with its (?) and the explanation shown under the control.
  label?: ReactNode;
  ariaLabel?: string;
  placeholder?: string;
  disabled?: boolean;
  description?: ReactNode;
  required?: boolean;
  withAsterisk?: boolean;
  "aria-labelledby"?: string;
  "aria-description"?: string;
}) {
  const { t } = useTranslation();
  const reasons = new Map(
    options.flatMap((o) => {
      const why = o.disabled ? (o.reason ?? disabledHint) : undefined;
      return why ? [[o.id, why] as const] : [];
    }),
  );
  const actions = new Map(options.flatMap((o) => (o.action ? [[o.id, o.action] as const] : [])));
  // An option's own content: its label, with its reason on hover when it cannot be picked.
  const labelOf = (value: string, label: string) => {
    const why = reasons.get(value);
    return why ? (
      <Tip text={why}>
        <span data-testid={`option-not-held-${value}`}>{label}</span>
      </Tip>
    ) : (
      label
    );
  };
  return (
    <MantineMultiSelect
      className={className}
      style={style}
      data={mantineData(options)}
      renderOption={
        reasons.size > 0 || actions.size > 0
          ? ({ option }) => {
              const action = actions.get(option.value);
              if (!action) return labelOf(option.value, option.label);
              // The list dims an option that cannot be picked; its action is not dimmed with it.
              return (
                <Group justify="space-between" wrap="nowrap" gap="xs" w="100%">
                  <span className={option.disabled ? "multi-select-option-held" : undefined}>
                    {labelOf(option.value, option.label)}
                  </span>
                  <Button
                    variant="light"
                    size="compact-xs"
                    loading={action.loading}
                    disabled={action.disabled}
                    aria-label={action.ariaLabel}
                    onClick={(e) => {
                      e.stopPropagation();
                      action.onClick();
                    }}
                    data-testid={`option-action-${option.value}`}
                  >
                    {action.label}
                  </Button>
                </Group>
              );
            }
          : undefined
      }
      value={value}
      onChange={onChange}
      label={label}
      description={description}
      required={required}
      withAsterisk={withAsterisk}
      // Callers historically render this with no visible label; supply an
      // accessible name so the combobox is never anonymous to screen readers.
      aria-labelledby={labelledBy}
      aria-description={ariaDescription}
      aria-label={
        labelledBy !== undefined
          ? undefined
          : (ariaLabel ??
            (typeof label === "string" ? label : undefined) ??
            placeholder ??
            t("multiSelect.defaultLabel"))
      }
      placeholder={value.length === 0 ? (placeholder ?? t("multiSelect.all")) : undefined}
      searchable
      clearable={!disabled}
      disabled={disabled}
      size="sm"
      comboboxProps={{ withinPortal: true }}
      // A selection with many values (e.g. every role, in a table cell like
      // RegisterTableForm's per-column Visible To/Writable By) used to wrap pills onto as many
      // lines as needed, inflating the row's height without bound and, on a table with dozens of
      // columns, making the whole page's scroll feel stuck (confirmed live). Cap the pill area and
      // let it scroll internally instead.
      styles={{ pillsList: { maxHeight: 68, overflowY: "auto", flexWrap: "wrap" } }}
    />
  );
}
