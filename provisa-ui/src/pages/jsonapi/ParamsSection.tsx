// Copyright (c) 2026 Kenneth Stott
// Canary: db8f9d80-d246-4825-9f4c-897064979e20
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1986/REQ-1991: the JSON:API Explorer's Parameters section — one input per parameter of the
// selected table, a required one marked. Split out of JsonApiPage.tsx to keep that file under the
// max-lines gate.

import type { Dispatch, SetStateAction } from "react";
import { TextInput } from "@mantine/core";
import { useTranslation } from "react-i18next";
import type { ListedColumn } from "../../types/admin";
import { isRequiredParam, parameterName } from "../../components/nativeParams";

export function ParamsSection({
  params,
  values,
  setValues,
}: {
  params: ListedColumn[];
  values: Record<string, string>;
  setValues: Dispatch<SetStateAction<Record<string, string>>>;
}) {
  const { t } = useTranslation();
  if (params.length === 0) return null;
  return (
    <>
      <div className="jsonapi-section-divider">{t("jsonApiPage.parametersSectionTitle")}</div>
      {params.map((c) => (
        <TextInput
          key={c.columnName}
          size="xs"
          label={parameterName(c)}
          withAsterisk={isRequiredParam(c)}
          value={values[c.columnName] ?? ""}
          onChange={(e) => {
            const value = e.currentTarget.value;
            setValues((prev) => ({ ...prev, [c.columnName]: value }));
          }}
          data-testid={`jsonapi-param-${parameterName(c)}`}
        />
      ))}
    </>
  );
}
