// Copyright (c) 2026 Kenneth Stott
// Canary: 5336777a-52bc-492b-9ce5-9004b5be798c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1934: a Data Profiler source's tables are the result relations it produces per member; its
// catalog lists them, and the register step picks one.

import { useTranslation } from "react-i18next";
import type { ProfilerCatalogEntry } from "../../api/profiler";
import { FormField, RequiredMark } from "../../components/form/FormLayout";
import { Input } from "@mantine/core";
import type { RequiredFields } from "../../components/form/useRequiredFields";

export function ProfilerResultSelect({
  catalog,
  value,
  onChange,
  required,
}: {
  /** null while the catalog loads. */
  catalog: ProfilerCatalogEntry[] | null;
  value: string;
  onChange: (tableName: string) => void;
  /** The form's required fields: the result picked is its table name, required (REQ-1968). */
  required: RequiredFields<"tableName">;
}) {
  const { t } = useTranslation();
  return (
    <FormField size="medium">
      <label>
        {t("registerTableForm.profilerResultLabel")}
        <RequiredMark />
        <select
          className="form-select"
          value={value}
          onChange={(e) => onChange(e.target.value)}
          {...required.native("tableName")}
          disabled={catalog == null}
          data-testid="register-table-profiler-select"
        >
          <option value="">
            {catalog == null
              ? t("registerTableForm.schemaLoading")
              : catalog.length === 0
                ? t("registerTableForm.profilerNoMembers")
                : t("registerTableForm.profilerResultPlaceholder")}
          </option>
          {(catalog ?? []).flatMap((entry) =>
            entry.tables.map((tb) => (
              <option key={tb.tableName} value={tb.tableName}>
                {t("registerTableForm.profilerResultOption", {
                  member: entry.member,
                  kind: tb.kind,
                })}
              </option>
            )),
          )}
        </select>
        <Input.Error>{required.errorOf("tableName")}</Input.Error>
      </label>
    </FormField>
  );
}
