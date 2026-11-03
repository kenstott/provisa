// Copyright (c) 2026 Kenneth Stott
// Canary: 8c41f0a3-7d29-4e65-b2a8-3f9e06d1c7b4
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2197: a QuickBooks Online source — the company (realm id) and whether it is a sandbox
// company, the Intuit app's client id and secret, and the refresh token that approves Provisa to
// read the company. Each secret may be a ${secret:…} reference.

import { PasswordInput, Select, TextInput } from "@mantine/core";
import { useTranslation } from "react-i18next";
import {
  QUICKBOOKS_ENVIRONMENTS,
  quickbooksEnvironment,
  quickbooksRealmIdValid,
} from "./quickbooks";
import { FieldGroup, FormField } from "../../components/form/FormLayout";

interface ConnectionForm {
  database: string;
  username: string;
  password: string;
}

interface Props<F extends ConnectionForm> {
  form: F;
  setForm: (form: F) => void;
  fields: Record<string, string>;
  setFields: (fields: Record<string, string>) => void;
}

const ENVIRONMENT_LABEL = { sandbox: "qbSandbox", production: "qbProduction" } as const;

export function QuickBooksFields<F extends ConnectionForm>({
  form,
  setForm,
  fields,
  setFields,
}: Props<F>) {
  const { t } = useTranslation();
  return (
    <>
      <FieldGroup>
        <FormField help={t("sourceFormFieldsExtended.qbRealmIdHint")} size="medium">
          <TextInput
            required
            label={t("sourceFormFieldsExtended.qbRealmId")}
            value={form.database}
            onChange={(e) => setForm({ ...form, database: e.currentTarget.value })}
            error={
              form.database && !quickbooksRealmIdValid(form.database)
                ? t("sourceFormFieldsExtended.qbRealmIdInvalid")
                : undefined
            }
            placeholder="9341453456789012"
            data-testid="quickbooks-realm-id-input"
          />
        </FormField>
        <FormField hint={t("sourceFormFieldsExtended.qbEnvironmentHint")} size="medium">
          <Select
            label={t("sourceFormFieldsExtended.qbEnvironment")}
            value={quickbooksEnvironment(fields)}
            onChange={(v) => setFields({ ...fields, environment: v ?? "sandbox" })}
            data={QUICKBOOKS_ENVIRONMENTS.map((v) => ({
              value: v,
              label: t(`sourceFormFieldsExtended.${ENVIRONMENT_LABEL[v]}`),
            }))}
            allowDeselect={false}
            data-testid="quickbooks-environment-select"
          />
        </FormField>
      </FieldGroup>
      <FieldGroup>
        <FormField
          section="authentication"
          hint={t("sourceFormFieldsExtended.qbClientIdHint")}
          size="medium"
        >
          <TextInput
            required
            label={t("sourceFormFieldsExtended.qbClientId")}
            value={form.username}
            onChange={(e) => setForm({ ...form, username: e.currentTarget.value })}
            data-testid="quickbooks-client-id-input"
          />
        </FormField>
        <FormField
          section="authentication"
          hint={t("sourceFormFieldsExtended.qbClientSecretHint")}
          size="medium"
        >
          <PasswordInput
            required
            label={t("sourceFormFieldsExtended.qbClientSecret")}
            value={form.password}
            onChange={(e) => setForm({ ...form, password: e.currentTarget.value })}
            placeholder="${secret:quickbooks_client_secret}"
            data-testid="quickbooks-client-secret-input"
          />
        </FormField>
      </FieldGroup>
      <FormField
        section="authentication"
        help={t("sourceFormFieldsExtended.qbRefreshTokenHint")}
        size="long"
      >
        <PasswordInput
          required
          label={t("sourceFormFieldsExtended.qbRefreshToken")}
          value={fields.refresh_token ?? ""}
          onChange={(e) => setFields({ ...fields, refresh_token: e.currentTarget.value })}
          placeholder="${secret:quickbooks_refresh_token}"
          data-testid="quickbooks-refresh-token-input"
        />
      </FormField>
    </>
  );
}
