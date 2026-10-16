// Copyright (c) 2026 Kenneth Stott
// Canary: d7004522-f6fd-4f00-b44d-3eb2e216b74e
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { MaskEntry } from "../../lib/masks";
import { Stack, Text } from "@mantine/core";
import { useTranslation } from "react-i18next";
import { FormField } from "../../components/form/FormLayout";
import type { Role } from "../../types/auth";
import { ColumnsTable } from "./ColumnsTable";
import type { ColumnForm } from "./types";

/** Register Table's columns: the heading (with their loading) and the table of them. */
export function RegisterColumnsField({
  columns,
  loadingColumns,
  updateCol,
  roles,
  irTypes,
  sourceType,
}: {
  columns: ColumnForm[];
  loadingColumns: boolean;
  updateCol: (
    i: number,
    key: keyof ColumnForm,
    value: string | boolean | string[] | MaskEntry[],
  ) => void;
  roles: Role[];
  irTypes: string[];
  sourceType: string;
}) {
  const { t } = useTranslation();
  return (
    <FormField size="full">
      <Stack gap="xs">
        <Text fw={600} fz="sm">
          {t("registerTableForm.columnsLabel")}{" "}
          {loadingColumns && t("registerTableForm.columnsLoading")}
        </Text>
        {columns.length > 0 && (
          <ColumnsTable
            columns={columns}
            updateCol={updateCol}
            roles={roles}
            irTypes={irTypes}
            sourceType={sourceType}
          />
        )}
      </Stack>
    </FormField>
  );
}
