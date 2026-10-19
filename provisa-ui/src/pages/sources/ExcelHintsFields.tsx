// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// How the Excel reader finds the tables of a files source's workbooks: each hint shows Auto-detect
// until the operator gives it a number. Folded away, since every hint has a default.

import {
  ActionIcon,
  Button,
  Divider,
  Group,
  NumberInput,
  Select,
  Stack,
  Text,
  TextInput,
} from "@mantine/core";
import { Trash2 } from "lucide-react";
import { useTranslation } from "react-i18next";
import { FieldGroup, FormField, FormFold, FormFolds } from "../../components/form/FormLayout";
import { Tip } from "../../components/Tip";
import {
  COUNT_HINTS,
  excelHintsOf,
  repeatedNames,
  TABLE_HINTS,
  WORKBOOK_HINTS,
  withExcelHints,
  type CountHint,
  type ExcelHints,
  type HintEntry,
  type HintScope,
} from "./excelHints";

interface Props {
  fields: Record<string, string>;
  setFields: (fields: Record<string, string>) => void;
}

const AUTO = Object.fromEntries(COUNT_HINTS.map((h) => [h.key, h.auto])) as Record<
  CountHint,
  number | null
>;

export function ExcelHintsFields({ fields, setFields }: Props) {
  const { t } = useTranslation();
  const hints = excelHintsOf(fields);
  const set = (next: ExcelHints) => setFields(withExcelHints(fields, next));
  const repeated = repeatedNames(hints);

  const countInput = (
    key: CountHint,
    value: number | undefined,
    onChange: (value: number | undefined) => void,
    testId: string,
  ) => (
    <FormField size="short" key={key} help={t(`excelHints.help.${key}`)}>
      <NumberInput
        label={t(`excelHints.hint.${key}`)}
        placeholder={
          AUTO[key] === null
            ? t("excelHints.auto")
            : t("excelHints.autoValue", { value: AUTO[key] })
        }
        min={1}
        allowDecimal={false}
        allowNegative={false}
        value={value ?? ""}
        onChange={(v) => onChange(typeof v === "number" && v >= 1 ? v : undefined)}
        data-testid={testId}
      />
    </FormField>
  );

  const setCount = (
    counts: Partial<Record<CountHint, number>>,
    key: CountHint,
    value: number | undefined,
  ) => {
    const next = { ...counts };
    if (value === undefined) delete next[key];
    else next[key] = value;
    return next;
  };

  const changeEntry = (i: number, change: Partial<HintEntry>) =>
    set({
      ...hints,
      entries: hints.entries.map((e, j) => (j === i ? { ...e, ...change } : e)),
    });

  return (
    <FormField size="full">
      <FormFolds id="files-excel-hints">
        <FormFold value="excel" title={t("excelHints.title")} note={t("excelHints.note")}>
          <FieldGroup>
            {WORKBOOK_HINTS.map((key) =>
              countInput(
                key,
                hints.counts[key],
                (v) => set({ ...hints, counts: setCount(hints.counts, key, v) }),
                `excel-hint-${key}`,
              ),
            )}
          </FieldGroup>
          <FormField size="full">
            <Stack gap="xs" data-testid="excel-hint-entries">
              <Text fw={600} fz="sm">
                {t("excelHints.entriesTitle")}
              </Text>
              <Text size="sm" c="dimmed">
                {t("excelHints.entriesNote")}
              </Text>
              {hints.entries.map((entry, i) => {
                const allowed = entry.scope === "table" ? TABLE_HINTS : WORKBOOK_HINTS;
                const twice = repeated.has(`${entry.scope}:${entry.name.trim()}`);
                return (
                  <Stack key={i} gap={4} data-testid={`excel-hint-entry-${i}`}>
                    <Divider />
                    <Group gap="xs" align="flex-end" wrap="nowrap">
                      <FieldGroup>
                        <FormField size="short">
                          <Select
                            label={t("excelHints.scope")}
                            value={entry.scope}
                            data={[
                              { value: "workbook", label: t("excelHints.scopeWorkbook") },
                              { value: "table", label: t("excelHints.scopeTable") },
                            ]}
                            onChange={(v) =>
                              changeEntry(i, {
                                scope: (v ?? entry.scope) as HintScope,
                                counts: Object.fromEntries(
                                  Object.entries(entry.counts).filter(([k]) =>
                                    (v === "table" ? TABLE_HINTS : WORKBOOK_HINTS).includes(
                                      k as CountHint,
                                    ),
                                  ),
                                ),
                                range: v === "table" ? entry.range : undefined,
                              })
                            }
                            allowDeselect={false}
                            comboboxProps={{ withinPortal: true }}
                            data-testid={`excel-hint-scope-${i}`}
                          />
                        </FormField>
                        <FormField
                          size="medium"
                          help={
                            entry.scope === "table"
                              ? t("excelHints.tableNameHelp")
                              : t("excelHints.workbookNameHelp")
                          }
                        >
                          <TextInput
                            label={
                              entry.scope === "table"
                                ? t("excelHints.tableName")
                                : t("excelHints.workbookName")
                            }
                            placeholder={
                              entry.scope === "table" ? "report__summary" : "report.xlsx"
                            }
                            value={entry.name}
                            error={twice ? t("excelHints.repeated") : undefined}
                            onChange={(e) => changeEntry(i, { name: e.currentTarget.value })}
                            data-testid={`excel-hint-name-${i}`}
                          />
                        </FormField>
                      </FieldGroup>
                      <Tip text={t("excelHints.remove")} named>
                        <ActionIcon
                          variant="subtle"
                          aria-label={t("excelHints.remove")}
                          onClick={() =>
                            set({ ...hints, entries: hints.entries.filter((_e, j) => j !== i) })
                          }
                          data-testid={`excel-hint-remove-${i}`}
                        >
                          <Trash2 size={14} />
                        </ActionIcon>
                      </Tip>
                    </Group>
                    <FieldGroup>
                      {allowed.map((key) =>
                        countInput(
                          key,
                          entry.counts[key],
                          (v) => changeEntry(i, { counts: setCount(entry.counts, key, v) }),
                          `excel-hint-${i}-${key}`,
                        ),
                      )}
                      {entry.scope === "table" && (
                        <FormField size="medium" help={t("excelHints.help.range")}>
                          <TextInput
                            label={t("excelHints.hint.range")}
                            placeholder={t("excelHints.rangePlaceholder")}
                            value={entry.range ?? ""}
                            onChange={(e) => changeEntry(i, { range: e.currentTarget.value })}
                            data-testid={`excel-hint-${i}-range`}
                          />
                        </FormField>
                      )}
                    </FieldGroup>
                  </Stack>
                );
              })}
              <div>
                <Button
                  variant="light"
                  onClick={() =>
                    set({
                      ...hints,
                      entries: [...hints.entries, { scope: "table", name: "", counts: {} }],
                    })
                  }
                  data-testid="excel-hint-add"
                >
                  {t("excelHints.add")}
                </Button>
              </div>
            </Stack>
          </FormField>
        </FormFold>
      </FormFolds>
    </FormField>
  );
}
