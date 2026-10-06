// Copyright (c) 2026 Kenneth Stott
// Canary: d29e1303-c5f7-42e2-9653-27cb6d7cf68c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { Checkbox, TextInput } from "@mantine/core";
import { useTranslation } from "react-i18next";
import type { TableColumn } from "../../types/admin";

// REQ-1494: a column's kind of fake as the operator writes it ("email()", "categories((a, b))")
// and whether it is stable. The server checks the declaration when the table is saved.
export function ColumnFakeField({
  col,
  onChange,
}: {
  col: TableColumn;
  onChange: (key: "fake" | "fakeStable", value: string | boolean) => void;
}) {
  const { t } = useTranslation();
  return (
    <>
      <TextInput
        aria-label={t("tableEditForm.fakeHeader")}
        value={col.fake || ""}
        onChange={(e) => onChange("fake", e.target.value)}
        placeholder={t("tableEditForm.fakePlaceholder")}
        data-testid={`table-edit-col-fake-${col.columnName}`}
      />
      <Checkbox
        mt={4}
        size="xs"
        label={t("tableEditForm.fakeStable")}
        checked={!!col.fakeStable}
        onChange={(e) => onChange("fakeStable", e.currentTarget.checked)}
        data-testid={`table-edit-col-fake-stable-${col.columnName}`}
      />
    </>
  );
}
