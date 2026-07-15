// Copyright (c) 2026 Kenneth Stott
// Canary: 9e1b4c67-2d5f-4a3e-8b0d-5c7f2a9e3b18
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState } from "react";
import { Sparkles, X } from "lucide-react";
import { ActionIcon, Group, Stack, Textarea, Tooltip } from "@mantine/core";
import { useTranslation } from "react-i18next";
import { CopyButton } from "../../components/CopyButton";

export function DescriptionField({
  value,
  onChange,
  placeholder,
  rows = 2,
  onGenerate,
  generating,
}: {
  value: string;
  onChange: (v: string) => void;
  placeholder?: string;
  rows?: number;
  onGenerate?: () => void;
  generating?: boolean;
}) {
  const { t } = useTranslation();
  const [focused, setFocused] = useState(false);
  return (
    <Stack gap={4} className="desc-field">
      <Textarea
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder={placeholder}
        rows={rows}
        onFocus={() => setFocused(true)}
        onBlur={() => setFocused(false)}
        styles={{
          input: focused
            ? { height: 300, transition: "height 0.15s ease" }
            : { transition: "height 0.15s ease" },
        }}
      />
      <Group gap={4} justify="flex-end" className="desc-field-toolbar">
        <CopyButton text={value} size={11} />
        {onGenerate && (
          <Tooltip label={t("descriptionField.generateWithAi")}>
            <ActionIcon
              type="button"
              variant="transparent"
              aria-label={t("descriptionField.generateWithAi")}
              data-testid="description-field-generate"
              onClick={onGenerate}
              disabled={generating}
            >
              <Sparkles size={11} />
            </ActionIcon>
          </Tooltip>
        )}
        <Tooltip label={t("descriptionField.clear")}>
          <ActionIcon
            type="button"
            variant="transparent"
            aria-label={t("descriptionField.clear")}
            data-testid="description-field-clear"
            onClick={() => onChange("")}
          >
            <X size={11} />
          </ActionIcon>
        </Tooltip>
      </Group>
    </Stack>
  );
}
