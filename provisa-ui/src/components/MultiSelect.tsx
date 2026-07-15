// Copyright (c) 2026 Kenneth Stott
// Canary: ea9667ae-371a-414f-be7a-2601c4eb9dfd
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { MultiSelect as MantineMultiSelect } from "@mantine/core";
import { useTranslation } from "react-i18next";

/** Multi-value picker backed by Mantine MultiSelect — provides the ARIA
 *  combobox pattern, keyboard navigation, and searchable options that the
 *  former hand-rolled checkbox-dropdown lacked (REQ-1009, REQ-1013).
 *  Public API is unchanged so existing call sites need no edits. */
export function MultiSelect({
  options,
  value,
  onChange,
  className,
  label,
  placeholder,
}: {
  options: { id: string; label: string }[];
  value: string[];
  onChange: (selected: string[]) => void;
  className?: string;
  label?: string;
  placeholder?: string;
}) {
  const { t } = useTranslation();
  return (
    <MantineMultiSelect
      className={className}
      data={options.map((o) => ({ value: o.id, label: o.label }))}
      value={value}
      onChange={onChange}
      label={label}
      // Callers historically render this with no visible label; supply an
      // accessible name so the combobox is never anonymous to screen readers.
      aria-label={label ?? placeholder ?? t("multiSelect.defaultLabel")}
      placeholder={value.length === 0 ? (placeholder ?? t("multiSelect.all")) : undefined}
      searchable
      clearable
      size="sm"
      comboboxProps={{ withinPortal: true }}
    />
  );
}
