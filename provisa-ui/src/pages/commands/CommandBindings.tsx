// Copyright (c) 2026 Kenneth Stott
// Canary: 0d7c3e85-6a19-4b2f-9e41-7b5a2c8d1f63
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A command's command bindings: its name in each language. A language left alone offers the
// command under its own name; a binding renames it there or leaves it out. SQL is always offered,
// since every language compiles to it.

import { Checkbox, Table, Tabs, Text, TextInput } from "@mantine/core";
import { useTranslation } from "react-i18next";
import type { LanguageBinding } from "../../api/actions";
import {
  BINDING_LANGUAGES,
  COMMAND_SECTIONS,
  type BindingLanguage,
  type CommandSection,
  type LanguageBindings,
} from "./bindingLanguages";


/** The command's sections, one tab each, the same in its read view and its form. */
export function CommandSectionTabs({
  value,
  onChange,
}: {
  value: CommandSection;
  onChange: (section: CommandSection) => void;
}) {
  const { t } = useTranslation();
  return (
    <Tabs value={value} onChange={(v) => v && onChange(v as CommandSection)} mb="sm">
      <Tabs.List aria-label={t("commandBindings.sectionsLabel")}>
        {COMMAND_SECTIONS.map((section) => (
          <Tabs.Tab key={section} value={section} data-testid={`command-section-${section}`}>
            {t(`commandBindings.section_${section}`)}
          </Tabs.Tab>
        ))}
      </Tabs.List>
    </Tabs>
  );
}

/** Each language's command binding, as the read view shows it. */
export function CommandBindingsRead({
  name,
  bindings,
}: {
  name: string;
  bindings: LanguageBindings;
}) {
  const { t } = useTranslation();
  return (
    <Table data-testid="command-bindings-read">
      <Table.Thead>
        <Table.Tr>
          <Table.Th>{t("commandBindings.language")}</Table.Th>
          <Table.Th>{t("commandBindings.name")}</Table.Th>
        </Table.Tr>
      </Table.Thead>
      <Table.Tbody>
        {BINDING_LANGUAGES.map((language) => {
          const binding = bindings[language];
          return (
            <Table.Tr key={language} data-testid={`command-binding-read-${language}`}>
              <Table.Td>{t(`commandBindings.lang_${language}`)}</Table.Td>
              <Table.Td>
                {binding?.exclude ? (
                  <Text size="sm" c="dimmed">
                    {t("commandBindings.notOffered")}
                  </Text>
                ) : (
                  <Text size="sm" ff="monospace">
                    {binding?.name || name}
                  </Text>
                )}
              </Table.Td>
            </Table.Tr>
          );
        })}
      </Table.Tbody>
    </Table>
  );
}

/** Each language's command binding, as the form edits it. */
export function CommandBindingsFields({
  name,
  bindings,
  onChange,
}: {
  name: string;
  bindings: LanguageBindings;
  onChange: (bindings: LanguageBindings) => void;
}) {
  const { t } = useTranslation();
  const set = (language: BindingLanguage, binding: LanguageBinding) => {
    const next = { ...bindings };
    if (!binding.exclude && !binding.name) delete next[language];
    else next[language] = binding;
    onChange(next);
  };
  return (
    <>
      <Text size="sm" c="dimmed" mb="xs">
        {t("commandBindings.hint")}
      </Text>
      <Table data-testid="command-bindings-fields">
        <Table.Thead>
          <Table.Tr>
            <Table.Th>{t("commandBindings.language")}</Table.Th>
            <Table.Th>{t("commandBindings.name")}</Table.Th>
            <Table.Th>{t("commandBindings.leaveOut")}</Table.Th>
          </Table.Tr>
        </Table.Thead>
        <Table.Tbody>
          {BINDING_LANGUAGES.map((language) => {
            const binding = bindings[language] ?? {};
            const label = t(`commandBindings.lang_${language}`);
            return (
              <Table.Tr key={language}>
                <Table.Td>{label}</Table.Td>
                <Table.Td>
                  <TextInput
                    aria-label={t("commandBindings.nameIn", { language: label })}
                    value={binding.name ?? ""}
                    placeholder={name}
                    disabled={!!binding.exclude}
                    onChange={(e) => set(language, { name: e.currentTarget.value || undefined })}
                    data-testid={`command-binding-name-${language}`}
                  />
                </Table.Td>
                <Table.Td>
                  {language === "sql" ? (
                    <Text size="sm" c="dimmed">
                      {t("commandBindings.sqlAlways")}
                    </Text>
                  ) : (
                    <Checkbox
                      aria-label={t("commandBindings.leaveOutOf", { language: label })}
                      checked={!!binding.exclude}
                      onChange={(e) =>
                        set(
                          language,
                          e.currentTarget.checked ? { exclude: true } : { name: binding.name },
                        )
                      }
                      data-testid={`command-binding-exclude-${language}`}
                    />
                  )}
                </Table.Td>
              </Table.Tr>
            );
          })}
        </Table.Tbody>
      </Table>
    </>
  );
}
