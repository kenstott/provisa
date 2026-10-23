// Copyright (c) 2026 Kenneth Stott
// Canary: ccd1c7f9-e0d0-4f0a-8bbb-53d9d902fcda
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A regex mask entry's Pattern and Replacement, written in the universal regex dialect Provisa
// translates for each engine. Beside them, the presets the server offers: picking one fills both
// fields, which stay free to edit, and one with a number to choose (keep the first N and last N)
// takes it first. Under them, the server's verdict as they are typed: each construct outside the
// universal dialect, named in red, or a quiet note that the mask works on every engine.

import { useState } from "react";
import { useTranslation } from "react-i18next";
import { ActionIcon, Group, Menu, NumberInput, Stack, Text, TextInput } from "@mantine/core";
import { WandSparkles } from "lucide-react";
import { Tip } from "./Tip";
import type { MaskPatternVerdict, MaskPreset } from "../api/admin";
import { filledTemplate } from "../lib/masks";

const DEFAULT_COUNT = 2;

export function RegexMaskFields({
  entry,
  pattern,
  replace,
  onChange,
  presets,
  verdicts,
  checked,
  disabled,
  testId,
}: {
  /** The entry's index, for the labels. */
  entry: number;
  pattern: string;
  replace: string;
  onChange: (patch: { pattern?: string; replace?: string }) => void;
  presets: MaskPreset[];
  /** The server's verdicts on this entry's fields. */
  verdicts: MaskPatternVerdict[];
  /** Whether the server has checked the list as it stands. */
  checked: boolean;
  disabled?: boolean;
  testId: string;
}) {
  const { t } = useTranslation();
  const [counts, setCounts] = useState<Record<string, number>>({});
  const countOf = (param: string) => counts[param] ?? DEFAULT_COUNT;
  const pick = (p: MaskPreset) => {
    const values = Object.fromEntries((p.params ?? []).map((name) => [name, countOf(name)]));
    const filled = p.pattern_template ? filledTemplate(p.pattern_template, values) : p.pattern;
    onChange({ pattern: filled ?? "", replace: p.replace });
  };
  const said = (v: MaskPatternVerdict) =>
    t("maskList.notUniversal", {
      construct: t(`maskList.construct.${v.construct}`),
      position: v.position + 1,
    });
  const refused = verdicts.filter((v) => !v.ok);
  const warned = verdicts.filter((v) => v.ok);
  const field = (name: "pattern" | "replace") => refused.find((v) => v.field === name);
  return (
    <Stack gap={2} style={{ minWidth: 0, flex: 1 }}>
      <Group gap={6} wrap="nowrap" align="flex-start">
        <TextInput
          aria-label={t("maskList.pattern")}
          placeholder={t("maskList.pattern")}
          value={pattern}
          onChange={(e) => onChange({ pattern: e.currentTarget.value })}
          error={field("pattern") ? said(field("pattern")!) : undefined}
          disabled={disabled}
          style={{ flex: 1, minWidth: 0 }}
          data-testid={`${testId}-pattern`}
        />
        <TextInput
          aria-label={t("maskList.replace")}
          placeholder={t("maskList.replace")}
          value={replace}
          onChange={(e) => onChange({ replace: e.currentTarget.value })}
          error={field("replace") ? said(field("replace")!) : undefined}
          disabled={disabled}
          style={{ flex: 1, minWidth: 0 }}
          data-testid={`${testId}-replace`}
        />
        <Menu position="bottom-end" withinPortal closeOnItemClick>
          <Menu.Target>
            <Tip text={t("maskList.presets")}>
              <ActionIcon
                variant="subtle"
                mt={4}
                aria-label={t("maskList.presetsAria", { n: entry + 1 })}
                disabled={disabled || presets.length === 0}
                data-testid={`${testId}-presets`}
              >
                <WandSparkles size={14} />
              </ActionIcon>
            </Tip>
          </Menu.Target>
          <Menu.Dropdown>
            <Menu.Label>{t("maskList.universalHelp")}</Menu.Label>
            {presets.map((p) => (
              <div key={p.id}>
                <Tip
                  position="left"
                  text={t("maskList.presetExample", {
                    from: p.example.input,
                    to: p.example.output,
                  })}
                >
                  <Menu.Item onClick={() => pick(p)} data-testid={`${testId}-preset-${p.id}`}>
                    {t(`maskList.preset.${p.id}`)}
                  </Menu.Item>
                </Tip>
                {(p.params ?? []).length > 0 && (
                  // The numbers the preset is filled with, set before its label is picked.
                  <Group gap={6} px="sm" pb={4} wrap="nowrap">
                    {(p.params ?? []).map((name) => (
                      <NumberInput
                        key={name}
                        size="xs"
                        w={110}
                        min={0}
                        allowDecimal={false}
                        label={t(`maskList.presetParam.${name}`)}
                        value={countOf(name)}
                        onChange={(v) =>
                          setCounts((c) => ({ ...c, [name]: typeof v === "number" ? v : 0 }))
                        }
                        data-testid={`${testId}-preset-${p.id}-${name}`}
                      />
                    ))}
                  </Group>
                )}
              </div>
            ))}
          </Menu.Dropdown>
        </Menu>
      </Group>
      {warned.map((v, k) => (
        <Text key={k} size="xs" c="orange" data-testid={`${testId}-warning`}>
          {said(v)}
        </Text>
      ))}
      {checked && refused.length === 0 && pattern !== "" && (
        <Tip text={t("maskList.universalHelp")}>
          <Text size="xs" c="dimmed" w="fit-content" data-testid={`${testId}-universal`}>
            {t("maskList.universal")}
          </Text>
        </Tip>
      )}
    </Stack>
  );
}
