// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useTranslation } from "react-i18next";
import { Select, Stack, Text } from "@mantine/core";
import type { RegisteredTable } from "../../types/admin";
import { tableRef, type RelForm } from "./relationship-types";
import { FormField, FormLayout } from "../form/FormLayout";

interface ParametersPanelProps {
  form: RelForm;
  setForm: (f: RelForm) => void;
  tables: RegisteredTable[];
  testIdPrefix: string;
}

const NF_PREFIX = "_nf_";

/**
 * REQ-2099: the target's parameters this relationship feeds, each from a column of the source.
 *
 * Shown only when the target table takes parameters (a code table's function arguments): a join
 * over the relationship then calls the target once per distinct set of the source's values. A
 * parameter left blank is not fed, and a read gives it as any read does.
 */
export function ParametersPanel({ form, setForm, tables, testIdPrefix }: ParametersPanelProps) {
  const { t } = useTranslation();

  const target = tables.find((tbl) => tableRef(tbl) === form.targetTableId);
  const source = tables.find((tbl) => tableRef(tbl) === form.sourceTableId);
  const parameters = (target?.columns ?? [])
    .filter((c) => c.nativeFilterType !== null)
    .map((c) => c.columnName.replace(NF_PREFIX, ""));
  if (parameters.length === 0) return null;

  const sourceOptions = (source?.columns ?? [])
    .filter((c) => c.nativeFilterType === null)
    .map((c) => ({ value: c.columnName, label: c.columnName }));
  const fed = Object.fromEntries(form.parameters.map((p) => [p.parameter, p.sourceColumn]));

  const setFed = (parameter: string, sourceColumn: string | null) => {
    const others = form.parameters.filter((p) => p.parameter !== parameter);
    setForm({
      ...form,
      parameters: sourceColumn ? [...others, { parameter, sourceColumn }] : others,
    });
  };

  return (
    <Stack gap="xs" data-testid={`${testIdPrefix}-parameters-panel`}>
      <Text fw={600} fz="sm">
        {t("relationshipRow.parametersFed")}
      </Text>
      <Text c="dimmed" fz="0.78rem">
        {t("relationshipRow.parametersFedHelp")}
      </Text>
      <FormLayout>
        {parameters.map((parameter) => (
          <FormField key={parameter} size="medium">
            <Select
              label={parameter}
              data={sourceOptions}
              value={fed[parameter] ?? null}
              placeholder={t("relationshipRow.parameterNotFed")}
              onChange={(v) => setFed(parameter, v)}
              clearable
              data-testid={`${testIdPrefix}-parameter-${parameter}`}
            />
          </FormField>
        ))}
      </FormLayout>
    </Stack>
  );
}
