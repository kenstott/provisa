// Copyright (c) 2026 Kenneth Stott
// Canary: 419ff11d-2f29-426a-ba50-c90e3635bb56
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1865: whether a table is replicated row by row, fetched by its key as statements name its
// rows. Offered only on a source whose type fetches rows by key (Source.rowFetchByKeys); a table
// whose source cannot fetch it so is refused by name when it is saved.

import { Checkbox } from "@mantine/core";
import { useTranslation } from "react-i18next";
import { FormField } from "../../components/form/FormLayout";

export function RowMaterializeField({
  checked,
  onChange,
  testId,
}: {
  checked: boolean;
  onChange: (checked: boolean) => void;
  testId: string;
}) {
  const { t } = useTranslation();
  return (
    <FormField size="long" help={t("rowMaterialize.help")}>
      <Checkbox
        label={t("rowMaterialize.label")}
        checked={checked}
        onChange={(e) => onChange(e.currentTarget.checked)}
        data-testid={testId}
      />
    </FormField>
  );
}
