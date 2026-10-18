// Copyright (c) 2026 Kenneth Stott
// Canary: 5ec41494-ad26-4f19-abfa-29abb0e5e16d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2069: a Jira source — the site's address, the Atlassian account's email and its API token.
// The token may be a ${secret:…} reference.

import { PasswordInput, TextInput } from "@mantine/core";
import { useTranslation } from "react-i18next";
import { jiraSiteUrlValid } from "./jira";
import { FieldGroup, FormField } from "../../components/form/FormLayout";

interface ConnectionForm {
  host: string;
  username: string;
  password: string;
}

interface Props<F extends ConnectionForm> {
  form: F;
  setForm: (form: F) => void;
}

export function JiraFields<F extends ConnectionForm>({ form, setForm }: Props<F>) {
  const { t } = useTranslation();
  return (
    <>
      <FormField hint={t("sourceFormFieldsExtended.siteAddressHint")} size="long">
        <TextInput
          required
          label={t("sourceFormFieldsExtended.siteAddress")}
          value={form.host}
          onChange={(e) => setForm({ ...form, host: e.currentTarget.value })}
          error={
            form.host && !jiraSiteUrlValid(form.host)
              ? t("sourceFormFieldsExtended.siteAddressInvalid")
              : undefined
          }
          placeholder="https://your-site.atlassian.net"
          data-testid="jira-site-input"
        />
      </FormField>
      <FieldGroup>
        <FormField
          section="authentication"
          hint={t("sourceFormFieldsExtended.accountEmailHint")}
          size="medium"
        >
          <TextInput
            required
            label={t("sourceFormFieldsExtended.accountEmail")}
            value={form.username}
            onChange={(e) => setForm({ ...form, username: e.currentTarget.value })}
            data-testid="jira-email-input"
          />
        </FormField>
        <FormField
          section="authentication"
          hint={t("sourceFormFieldsExtended.apiTokenHint")}
          size="medium"
        >
          <PasswordInput
            required
            label={t("sourceFormFieldsExtended.apiToken")}
            value={form.password}
            onChange={(e) => setForm({ ...form, password: e.currentTarget.value })}
            data-testid="jira-token-input"
          />
        </FormField>
      </FieldGroup>
    </>
  );
}
