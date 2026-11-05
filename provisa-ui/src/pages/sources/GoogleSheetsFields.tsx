// Copyright (c) 2026 Kenneth Stott
// Canary: 3f8c1a52-7d94-4e0b-b6a2-9e5d1c7f4b18
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A Google Sheets source -- the service account's credentials file and the metadata sheet.

import { TextInput } from "@mantine/core";
import { useTranslation } from "react-i18next";
import { FormField } from "../../components/form/FormLayout";

interface SheetForm {
  database: string;
}

interface Props<F extends SheetForm> {
  form: F;
  setForm: (form: F) => void;
  fields: Record<string, string>;
  setFields: (fields: Record<string, string>) => void;
}

export function GoogleSheetsFields<F extends SheetForm>({
  form,
  setForm,
  fields,
  setFields,
}: Props<F>) {
  const { t } = useTranslation();
  return (
    <>
      <FormField section="authentication" size="long">
        <TextInput
          required
          label={t("sourceFormFieldsExtended.credentialsJsonPath")}
          value={fields.credentials_json ?? ""}
          onChange={(e) => setFields({ ...fields, credentials_json: e.target.value })}
          placeholder={t("sourceFormFieldsExtended.credentialsJsonPathPlaceholder")}
          data-testid="google-sheets-credentials-input"
        />
      </FormField>
      <FormField size="medium">
        <TextInput
          label={t("sourceFormFieldsExtended.metadataSheetId")}
          value={form.database}
          onChange={(e) => setForm({ ...form, database: e.target.value })}
          placeholder={t("sourceFormFieldsExtended.metadataSheetIdPlaceholder")}
          data-testid="google-sheets-sheet-id-input"
        />
      </FormField>
    </>
  );
}
