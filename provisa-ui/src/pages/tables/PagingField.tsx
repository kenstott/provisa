// Copyright (c) 2026 Kenneth Stott
// Canary: f4ea2739-dc1d-4953-86a5-39c0da8fdc0b
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-318: the table's Paging section. A paged REST endpoint: its paging type and the parameters
// that type reads (each placeholder is the name used when the field is left empty), page size and
// max pages. A connection table: max rows only — its page arguments come from its schema — which
// may lower the operator's graphql_remote.max_rows, never raise it.

import { useTranslation } from "react-i18next";
import { Group, NumberInput, Select, Text, TextInput } from "@mantine/core";
import type { Paging, PagingKind, PagingType } from "../../types/admin";
import { FieldLabel } from "./FieldLabel";
import { NO_PAGING, pagingProblem } from "./paging";

interface PagingFieldProps {
  kind: PagingKind;
  paging: Paging | null;
  onChange: (paging: Paging) => void;
  ceilingRows: number | null;
  // REQ-316: where a REST answer's rows are is set when the table is registered and shown,
  // not edited, afterwards (the table's columns are those of the objects there).
  rowsFieldEditable?: boolean;
}

const TYPES: PagingType[] = ["offset", "page_number", "cursor", "link_header"];

// Which declared fields each paging type reads (provisa/api_source/caller.py), with the name the
// caller sends when a parameter field is left empty.
const READS: Record<PagingType, { field: keyof Paging; fallback?: string }[]> = {
  offset: [
    { field: "pageParam", fallback: "offset" },
    { field: "pageSizeParam", fallback: "limit" },
    { field: "pageSize" },
  ],
  page_number: [
    { field: "pageParam", fallback: "page" },
    { field: "pageSizeParam", fallback: "per_page" },
    { field: "pageSize" },
  ],
  cursor: [
    { field: "cursorParam", fallback: "cursor" },
    { field: "cursorField", fallback: "next_cursor" },
  ],
  link_header: [],
};

const NUMERIC = new Set<keyof Paging>(["pageSize", "maxPages", "maxRows"]);

export function PagingField({
  kind,
  paging,
  onChange,
  ceilingRows,
  rowsFieldEditable = false,
}: PagingFieldProps) {
  const { t } = useTranslation();
  const staged = paging ?? NO_PAGING;
  const problem = pagingProblem(kind, paging, ceilingRows);
  const error = problem ? t(problem.key, problem.params) : undefined;
  const set = (patch: Partial<Paging>) => onChange({ ...staged, ...patch });
  const number = (field: keyof Paging) => (v: string | number) =>
    set({ [field]: v === "" ? null : Number(v) } as Partial<Paging>);

  if (kind === "connection") {
    return (
      <div data-testid="paging-field">
        <FieldLabel
          text={t("tableEditForm.pagingLabel")}
          help={t("tableEditForm.pagingConnectionHelp")}
        />
        <NumberInput
          aria-label={t("tableEditForm.pagingMaxRows")}
          label={t("tableEditForm.pagingMaxRows")}
          min={1}
          allowDecimal={false}
          allowNegative={false}
          value={staged.maxRows ?? ""}
          placeholder={
            ceilingRows === null
              ? undefined
              : t("tableEditForm.pagingMaxRowsDefault", { ceiling: ceilingRows })
          }
          onChange={number("maxRows")}
          error={error}
        />
      </div>
    );
  }

  // Changing the type keeps only what the new type reads.
  const pickType = (value: string | null) => {
    const type = value as PagingType | null;
    const kept: Partial<Paging> = { maxPages: staged.maxPages, rowsField: staged.rowsField };
    for (const { field } of type ? READS[type] : []) kept[field] = staged[field] as never;
    onChange({ ...NO_PAGING, ...kept, type });
  };

  return (
    <div data-testid="paging-field">
      <FieldLabel
        text={t("tableEditForm.pagingLabel")}
        help={t("tableEditForm.pagingEndpointHelp")}
      />
      <Select
        aria-label={t("tableEditForm.pagingType")}
        label={t("tableEditForm.pagingType")}
        data={TYPES.map((type) => ({ value: type, label: t(`tableEditForm.pagingTypes.${type}`) }))}
        value={staged.type}
        onChange={pickType}
        clearable
        placeholder={t("tableEditForm.pagingNone")}
        comboboxProps={{ withinPortal: true }}
        error={error}
      />
      {staged.type !== null && (
        <Group gap="xs" mt={4} align="flex-start" grow>
          {READS[staged.type].map(({ field, fallback }) =>
            NUMERIC.has(field) ? (
              <NumberInput
                key={field}
                aria-label={t(`tableEditForm.pagingFields.${field}`)}
                label={t(`tableEditForm.pagingFields.${field}`)}
                min={1}
                allowDecimal={false}
                allowNegative={false}
                value={(staged[field] as number | null) ?? ""}
                placeholder="100"
                onChange={number(field)}
              />
            ) : (
              <TextInput
                key={field}
                aria-label={t(`tableEditForm.pagingFields.${field}`)}
                label={t(`tableEditForm.pagingFields.${field}`)}
                value={(staged[field] as string | null) ?? ""}
                placeholder={fallback}
                onChange={(e) =>
                  set({ [field]: e.currentTarget.value || null } as Partial<Paging>)
                }
              />
            ),
          )}
          <NumberInput
            aria-label={t("tableEditForm.pagingFields.maxPages")}
            label={t("tableEditForm.pagingFields.maxPages")}
            min={1}
            allowDecimal={false}
            allowNegative={false}
            value={staged.maxPages ?? ""}
            placeholder="10"
            onChange={number("maxPages")}
          />
        </Group>
      )}
      {staged.type !== null && (
        <Text size="xs" c="dimmed" data-testid="paging-cut-note">
          {t("tableEditForm.pagingCutNote")}
        </Text>
      )}
      {(rowsFieldEditable || staged.rowsField !== null) && (
        <TextInput
          mt={4}
          aria-label={t("tableEditForm.pagingFields.rowsField")}
          label={t("tableEditForm.pagingFields.rowsField")}
          description={t(
            rowsFieldEditable
              ? "tableEditForm.pagingRowsFieldHelp"
              : "tableEditForm.pagingRowsFieldFixed",
          )}
          value={staged.rowsField ?? ""}
          readOnly={!rowsFieldEditable}
          onChange={(e) => set({ rowsField: e.currentTarget.value || null })}
        />
      )}
    </div>
  );
}
