// Copyright (c) 2026 Kenneth Stott
// Canary: 154765ce-1e79-4785-bf31-0b39e7dbf0d1
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { X } from "lucide-react";
import { ActionIcon, Group, TextInput } from "@mantine/core";
import { useTranslation } from "react-i18next";
import { CopyButton } from "../CopyButton";

export function FilterInput({
  value,
  onChange,
  placeholder,
  testId = "filter-input",
}: {
  value: string;
  onChange: (v: string) => void;
  placeholder?: string;
  /** Overridden where a page's tests already address its filter by another name. */
  testId?: string;
}) {
  const { t } = useTranslation();
  return (
    <div className="search-wrap">
      <TextInput
        type="search"
        aria-label={placeholder ?? t("filterInput.filter")}
        placeholder={placeholder}
        value={value}
        onChange={(e) => onChange(e.target.value)}
        data-testid={testId}
        rightSection={
          <Group gap={2} wrap="nowrap">
            <CopyButton text={value} size={11} />
            <ActionIcon
              type="button"
              variant="transparent"
              aria-label={t("filterInput.clear")}
              data-testid="filter-input-clear"
              onClick={() => onChange("")}
            >
              <X size={11} />
            </ActionIcon>
          </Group>
        }
        rightSectionWidth={54}
      />
    </div>
  );
}
