// Copyright (c) 2026 Kenneth Stott
// Canary: 9f2d6b40-1e85-4c37-a7d9-0b3c8e5f1a62
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1946: a Salesforce source — the org's login URL, one credential set, and an optional API
// version. Each secret may be a ${secret:…} reference.

import { PasswordInput, Select, TextInput } from "@mantine/core";
import { useTranslation } from "react-i18next";
import {
  SALESFORCE_AUTH_TYPES,
  salesforceAuthType,
  salesforceLoginUrlValid,
  salesforceUsesConnectedApp,
  type SalesforceAuthType,
} from "./salesforce";

interface ConnectionForm {
  host: string;
  username: string;
  password: string;
}

interface Props<F extends ConnectionForm> {
  form: F;
  setForm: (form: F) => void;
  fields: Record<string, string>;
  setFields: (fields: Record<string, string>) => void;
}

const AUTH_LABEL: Record<SalesforceAuthType, string> = {
  CLIENT_CREDENTIALS: "clientCredentials",
  USERNAME_PASSWORD: "usernamePassword",
  ACCESS_TOKEN: "sfAccessTokenAuth",
};

export function SalesforceFields<F extends ConnectionForm>({
  form,
  setForm,
  fields,
  setFields,
}: Props<F>) {
  const { t } = useTranslation();
  const type = salesforceAuthType(fields);
  const set = (key: string) => (e: React.ChangeEvent<HTMLInputElement>) =>
    setFields({ ...fields, [key]: e.currentTarget.value });
  return (
    <>
      <TextInput
        required
        label={t("sourceFormFieldsExtended.sfLoginUrl")}
        description={t("sourceFormFieldsExtended.sfLoginUrlHelp")}
        value={form.host}
        onChange={(e) => setForm({ ...form, host: e.currentTarget.value })}
        error={
          form.host && !salesforceLoginUrlValid(form.host)
            ? t("sourceFormFieldsExtended.sfLoginUrlInvalid")
            : undefined
        }
        placeholder="https://acme.my.salesforce.com"
        style={{ gridColumn: "1 / -1" }}
        data-testid="salesforce-login-url-input"
      />
      <Select
        label={t("sourceFormFieldsExtended.authType")}
        value={type}
        onChange={(v) => setFields({ ...fields, auth_type: v ?? "CLIENT_CREDENTIALS" })}
        data={SALESFORCE_AUTH_TYPES.map((v) => ({
          value: v,
          label: t(`sourceFormFieldsExtended.${AUTH_LABEL[v]}`),
        }))}
        allowDeselect={false}
        data-testid="salesforce-auth-type-select"
      />
      {salesforceUsesConnectedApp(type) && (
        <>
          <TextInput
            required
            label={t("sourceFormFieldsExtended.sfConsumerKey")}
            value={form.username}
            onChange={(e) => setForm({ ...form, username: e.currentTarget.value })}
            data-testid="salesforce-consumer-key-input"
          />
          <PasswordInput
            required
            label={t("sourceFormFieldsExtended.sfConsumerSecret")}
            value={form.password}
            onChange={(e) => setForm({ ...form, password: e.currentTarget.value })}
            placeholder="${secret:salesforce_consumer_secret}"
            data-testid="salesforce-consumer-secret-input"
          />
        </>
      )}
      {type === "USERNAME_PASSWORD" && (
        <>
          <TextInput
            required
            label={t("sourceFormFieldsExtended.sfUsername")}
            value={fields.sf_username ?? ""}
            onChange={set("sf_username")}
            placeholder="user@acme.com"
            data-testid="salesforce-username-input"
          />
          <PasswordInput
            required
            label={t("sourceFormFieldsExtended.sfPassword")}
            value={fields.sf_password ?? ""}
            onChange={set("sf_password")}
            placeholder="${secret:salesforce_password}"
            data-testid="salesforce-password-input"
          />
          <PasswordInput
            label={t("sourceFormFieldsExtended.sfSecurityToken")}
            description={t("sourceFormFieldsExtended.sfSecurityTokenHelp")}
            value={fields.security_token ?? ""}
            onChange={set("security_token")}
            placeholder="${secret:salesforce_security_token}"
            data-testid="salesforce-security-token-input"
          />
        </>
      )}
      {type === "ACCESS_TOKEN" && (
        <>
          <PasswordInput
            required
            label={t("sourceFormFieldsExtended.sfAccessToken")}
            value={fields.access_token ?? ""}
            onChange={set("access_token")}
            placeholder="${secret:salesforce_access_token}"
            data-testid="salesforce-access-token-input"
          />
          <TextInput
            required
            label={t("sourceFormFieldsExtended.sfInstanceUrl")}
            value={fields.instance_url ?? ""}
            onChange={set("instance_url")}
            placeholder="https://acme.my.salesforce.com"
            data-testid="salesforce-instance-url-input"
          />
        </>
      )}
      <TextInput
        label={t("sourceFormFieldsExtended.sfApiVersion")}
        description={t("sourceFormFieldsExtended.sfApiVersionHelp")}
        value={fields.api_version ?? ""}
        onChange={set("api_version")}
        placeholder="v61.0"
        data-testid="salesforce-api-version-input"
      />
    </>
  );
}
