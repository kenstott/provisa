// Copyright (c) 2026 Kenneth Stott
// Canary: 4a1d7e93-2c58-4b06-9f3e-8b5c0d6e2a17
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2280: the sign-in fields of a files source on the onedrive:// or gdrive:// transport.

import { PasswordInput, Select, TextInput } from "@mantine/core";
import { useTranslation } from "react-i18next";
import { FieldGroup, FormField } from "../../components/form/FormLayout";
import {
  GDRIVE_AUTH,
  GDRIVE_OAUTH,
  GDRIVE_SUBJECT,
  gdriveAuth,
  gdriveAuthKinds,
  REFRESH_TOKEN,
} from "./cloudDrive";
import type { SourceFormFieldsProps } from "./SourceFormFields";

export function CloudDriveFields({
  needsAuth,
  form,
  setForm,
  fields: authFields,
  setFields: setAuthFields,
}: {
  needsAuth: string | false;
  fields: SourceFormFieldsProps["authFields"];
  setFields: SourceFormFieldsProps["setAuthFields"];
} & Pick<SourceFormFieldsProps, "form" | "setForm">) {
  const { t } = useTranslation();
  return (
    <>
      {/* REQ-2280: OneDrive signs in as the SharePoint source does. */}
      {needsAuth === "onedrive" && (
        <FieldGroup>
          <FormField section="authentication" size="medium">
            <TextInput
              required
              label={t("sourceFormFieldsExtended.tenantId")}
              value={form.database}
              onChange={(e) => setForm({ ...form, database: e.target.value })}
              data-testid="files-onedrive-tenant-input"
            />
          </FormField>
          <FormField section="authentication" size="medium">
            <TextInput
              required
              label={t("sourceFormFieldsExtended.clientId")}
              value={form.username}
              onChange={(e) => setForm({ ...form, username: e.target.value })}
              data-testid="files-onedrive-client-id-input"
            />
          </FormField>
          <FormField section="authentication" size="medium">
            <PasswordInput
              required
              label={t("sourceFormFieldsExtended.clientSecret")}
              value={form.password}
              onChange={(e) => setForm({ ...form, password: e.target.value })}
              data-testid="files-onedrive-client-secret-input"
            />
          </FormField>
        </FieldGroup>
      )}
      {/* REQ-2280: Google Drive signs in as the Google Sheets source does. */}
      {needsAuth === "gdrive" && (
        <>
          <FormField section="authentication" size="medium">
            <Select
              label={t("sourceFormFieldsExtended.authMethod")}
              value={gdriveAuth(authFields)}
              onChange={(v) => {
                setAuthFields({
                  ...authFields,
                  [GDRIVE_AUTH]: v ?? "",
                  [REFRESH_TOKEN]: "",
                  [GDRIVE_SUBJECT]: "",
                });
                setForm({ ...form, username: "", password: "" });
              }}
              data={gdriveAuthKinds().map((kind) => ({
                value: kind,
                label:
                  kind === GDRIVE_OAUTH
                    ? t("sourceFormFieldsExtended.gdriveAuth_oauth")
                    : t("sourceFormFieldsExtended.gdriveAuth_service_account"),
              }))}
              allowDeselect={false}
              data-testid="files-gdrive-auth-select"
            />
          </FormField>
          {gdriveAuth(authFields) === GDRIVE_OAUTH ? (
            <FieldGroup>
              <FormField section="authentication" size="medium">
                <TextInput
                  required
                  label={t("sourceFormFieldsExtended.clientId")}
                  value={form.username}
                  onChange={(e) => setForm({ ...form, username: e.target.value })}
                  data-testid="files-gdrive-client-id-input"
                />
              </FormField>
              <FormField section="authentication" size="medium">
                <PasswordInput
                  required
                  label={t("sourceFormFieldsExtended.clientSecret")}
                  value={form.password}
                  onChange={(e) => setForm({ ...form, password: e.target.value })}
                  data-testid="files-gdrive-client-secret-input"
                />
              </FormField>
              <FormField section="authentication" size="medium">
                <PasswordInput
                  required
                  label={t("sourceFormFieldsExtended.gdriveRefreshToken")}
                  value={authFields[REFRESH_TOKEN] ?? ""}
                  onChange={(e) =>
                    setAuthFields({ ...authFields, [REFRESH_TOKEN]: e.target.value })
                  }
                  data-testid="files-gdrive-refresh-token-input"
                />
              </FormField>
            </FieldGroup>
          ) : (
            <FieldGroup>
              <FormField
                section="authentication"
                size="long"
                help={t("sourceFormFieldsExtended.gdriveServiceAccountKeyHint")}
              >
                <PasswordInput
                  required
                  label={t("sourceFormFieldsExtended.gdriveServiceAccountKey")}
                  value={form.password}
                  onChange={(e) => setForm({ ...form, password: e.target.value })}
                  data-testid="files-gdrive-key-input"
                />
              </FormField>
              {/* REQ-2280: domain-wide delegation, the service account acting as this user. */}
              <FormField
                section="authentication"
                size="medium"
                help={t("sourceFormFieldsExtended.gdriveSubjectHint")}
              >
                <TextInput
                  label={t("sourceFormFieldsExtended.gdriveSubject")}
                  value={authFields[GDRIVE_SUBJECT] ?? ""}
                  onChange={(e) =>
                    setAuthFields({ ...authFields, [GDRIVE_SUBJECT]: e.target.value })
                  }
                  data-testid="files-gdrive-subject-input"
                />
              </FormField>
            </FieldGroup>
          )}
        </>
      )}
    </>
  );
}
