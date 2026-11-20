// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1923, REQ-2265: what a branded source's form asks for: the setup fields its brand
// declares (BRAND_SETUP) and the credential its auth type states (BRAND_AUTH).

import { PasswordInput, TextInput } from "@mantine/core";
import { useTranslation } from "react-i18next";
import { FormField } from "../../components/form/FormLayout";
import { BRAND_AUTH, BRAND_SETUP } from "./constants";

export interface BrandFieldsProps {
  brand: string;
  brandSetup: Record<string, string>;
  setBrandSetup: (setup: Record<string, string>) => void;
  authFields: Record<string, string>;
  setAuthFields: (fields: Record<string, string>) => void;
}

export function BrandFields({
  brand,
  brandSetup,
  setBrandSetup,
  authFields,
  setAuthFields,
}: BrandFieldsProps) {
  const { t } = useTranslation();
  return (
    <>
      {/* REQ-1923: a brand reached at a per-customer address asks where (BRAND_SETUP). */}
      {(BRAND_SETUP[brand] ?? []).map((field) => (
        <FormField key={field.name} hint={t(field.hintKey)} size="medium">
          <TextInput
            required={field.required}
            label={t(field.labelKey)}
            value={brandSetup[field.name] ?? ""}
            onChange={(e) => setBrandSetup({ ...brandSetup, [field.name]: e.target.value })}
            placeholder={field.placeholder}
            data-testid={`brand-setup-${field.name}-input`}
          />
        </FormField>
      ))}
      {/* REQ-1923: a brand asks for the credential it states (BRAND_AUTH), never one by default. */}
      {BRAND_AUTH[brand] === "token" && (
        <FormField
          section="authentication"
          hint={t("sourceFormFieldsExtended.accessTokenHint")}
          size="long"
        >
          <PasswordInput
            required
            label={t("sourceFormFieldsExtended.accessToken")}
            value={authFields.token ?? ""}
            onChange={(e) => setAuthFields({ ...authFields, token: e.target.value })}
            data-testid="brand-token-input"
          />
        </FormField>
      )}
      {/* REQ-2265: a brand whose auth is OAuth client credentials asks for the client's ID and
          secret; the token endpoint is the brand's. */}
      {BRAND_AUTH[brand] === "client_credentials" && (
        <>
          <FormField section="authentication" size="medium">
            <TextInput
              required
              label={t("sourceFormFieldsExtended.clientId")}
              value={authFields.client_id ?? ""}
              onChange={(e) => setAuthFields({ ...authFields, client_id: e.target.value })}
              data-testid="brand-client-id-input"
            />
          </FormField>
          <FormField
            section="authentication"
            hint={t("sourceFormFieldsExtended.accessTokenHint")}
            size="long"
          >
            <PasswordInput
              required
              label={t("sourceFormFieldsExtended.clientSecret")}
              value={authFields.client_secret ?? ""}
              onChange={(e) => setAuthFields({ ...authFields, client_secret: e.target.value })}
              data-testid="brand-client-secret-input"
            />
          </FormField>
        </>
      )}
    </>
  );
}
