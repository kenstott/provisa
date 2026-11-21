// Copyright (c) 2026 Kenneth Stott
// Canary: abd48c25-60bf-4332-8483-f4ef939ed139
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1954: a ServiceNow table's import set — offered only when the instance has an active
// transform map into the table. The same field on Register Table and in the table's editor. Bound, its inserts go through the map's import set; unbound (the
// default), they are written to the table directly. Updates and deletes are always direct.

import { useEffect, useMemo } from "react";
import { useQuery } from "@apollo/client/react";
import { Select } from "@mantine/core";
import { useTranslation } from "react-i18next";
import { AvailableImportSets } from "../../hooks/admin.graphql";
import { FormField } from "../../components/form/FormLayout";

interface ImportSet {
  name: string;
  importSet: string;
}

export function ImportSetField({
  sourceId,
  schemaName,
  tableName,
  value,
  onChange,
}: {
  sourceId: string;
  schemaName: string;
  tableName: string;
  value: string | null;
  onChange: (importSet: string | null) => void;
}) {
  const { t } = useTranslation();
  const { data } = useQuery<{ availableImportSets: ImportSet[] }>(AvailableImportSets, {
    variables: { sourceId, schemaName, tableName },
    skip: !tableName,
    fetchPolicy: "no-cache",
  });
  const maps = useMemo(() => data?.availableImportSets ?? [], [data]);
  // A binding chosen for another table is not this one's. Judged once the instance has answered:
  // a stored binding is not dropped while its table's maps are still being asked for.
  const answered = data !== undefined;
  useEffect(() => {
    if (answered && value !== null && !maps.some((m) => m.importSet === value)) onChange(null);
  }, [answered, maps, value, onChange]);
  if (maps.length === 0) return null;
  return (
    <FormField help={t("registerTableForm.importSetHint")} size="medium">
      <Select
        label={t("registerTableForm.importSetLabel")}
        value={value ?? ""}
        onChange={(v) => onChange(v || null)}
        allowDeselect={false}
        comboboxProps={{ withinPortal: true }}
        data={[
          { value: "", label: t("registerTableForm.importSetNone") },
          ...maps.map((m) => ({
            value: m.importSet,
            label: t("registerTableForm.importSetOption", { name: m.name, importSet: m.importSet }),
          })),
        ]}
        data-testid="register-table-import-set"
      />
    </FormField>
  );
}
