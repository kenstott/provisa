// Copyright (c) 2026 Kenneth Stott
// Canary: 6a2f8d14-b3e7-4c95-9d01-e5b7c3a8f264
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Register Table's description: the same field, and the same AI describe, as the table editor's,
// written from what the form holds -- the table need not be registered to be described.

import { useState } from "react";
import { useTranslation } from "react-i18next";
import { FormField } from "../../components/form/FormLayout";
import { DescriptionField } from "./DescriptionField";
import { FieldLabel } from "./FieldLabel";

export function RegisterDescriptionField({
  value,
  onChange,
  describe,
  setError,
}: {
  value: string;
  onChange: (v: string) => void;
  /** Absent until the form names the table (source, schema and name): nothing to describe yet. */
  describe?: () => Promise<string>;
  setError: (e: string | null) => void;
}) {
  const { t } = useTranslation();
  const [generating, setGenerating] = useState(false);
  return (
    <FormField size="full">
      <div data-testid="register-table-description-field">
        <FieldLabel
          text={t("registerTableForm.descriptionLabel")}
          help={t("tableEditForm.descriptionHelp")}
        />
        <DescriptionField
          value={value}
          onChange={onChange}
          placeholder={t("registerTableForm.descriptionPlaceholder")}
          rows={2}
          generating={generating}
          onGenerate={
            describe &&
            (async () => {
              setGenerating(true);
              try {
                const desc = await describe();
                if (desc) onChange(desc);
              } catch (err) {
                setError(err instanceof Error ? err.message : String(err));
              } finally {
                setGenerating(false);
              }
            })
          }
        />
      </div>
    </FormField>
  );
}
