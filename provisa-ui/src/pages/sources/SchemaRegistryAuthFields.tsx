// Copyright (c) 2026 Kenneth Stott
// Canary: 853a9caa-12f7-42cf-9fbc-d9bb73936778
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1951: how a source's CDC schema registry is reached. Each method shows exactly the fields
// it needs; the CA bundle path is optional with every method.

import { useTranslation } from "react-i18next";
import { PasswordInput, Select, Stack, TextInput } from "@mantine/core";
import type { CdcState } from "./SourceFormFields";
import {
  REGISTRY_AUTH_METHODS,
  METHOD_FIELDS,
  ALL_FIELDS,
  type RegistryAuthMethod,
  type RegistryFieldKey,
} from "./schemaRegistryAuth";

interface SchemaRegistryAuthFieldsProps {
  cdc: CdcState;
  setCdc: (v: CdcState) => void;
}

const FIELD_UI: Record<
  RegistryFieldKey,
  { label: string; testId: string; secret: boolean; path: boolean }
> = {
  schemaRegistryUsername: {
    label: "registryUsername",
    testId: "cdc-registry-username-input",
    secret: false,
    path: false,
  },
  schemaRegistryPassword: {
    label: "registryPassword",
    testId: "cdc-registry-password-input",
    secret: true,
    path: false,
  },
  schemaRegistryToken: {
    label: "registryToken",
    testId: "cdc-registry-token-input",
    secret: true,
    path: false,
  },
  schemaRegistryClientCert: {
    label: "registryClientCert",
    testId: "cdc-registry-client-cert-input",
    secret: false,
    path: true,
  },
  schemaRegistryClientKey: {
    label: "registryClientKey",
    testId: "cdc-registry-client-key-input",
    secret: false,
    path: true,
  },
};

export function SchemaRegistryAuthFields({ cdc, setCdc }: SchemaRegistryAuthFieldsProps) {
  const { t } = useTranslation();
  const method = cdc.schemaRegistryAuth as RegistryAuthMethod;
  const methodLabels: Record<RegistryAuthMethod, string> = {
    none: t("sourceFormFieldsExtended.registryAuthNone"),
    basic: t("sourceFormFieldsExtended.registryAuthBasic"),
    bearer: t("sourceFormFieldsExtended.registryAuthBearer"),
    mtls: t("sourceFormFieldsExtended.registryAuthMtls"),
  };
  const pathPlaceholder = t("sourceFormFieldsExtended.registryPathPlaceholder");
  return (
    <Stack gap="xs">
      <Select
        label={t("sourceFormFieldsExtended.registryAuth")}
        value={cdc.schemaRegistryAuth}
        onChange={(v) => {
          if (v) setCdc({ ...cdc, schemaRegistryAuth: v });
        }}
        data={REGISTRY_AUTH_METHODS.map((m) => ({ value: m, label: methodLabels[m] }))}
        allowDeselect={false}
        data-testid="cdc-registry-auth-select"
      />
      {ALL_FIELDS.filter((key) => METHOD_FIELDS[method].includes(key)).map((key) => {
        const ui = FIELD_UI[key];
        const Input = ui.secret ? PasswordInput : TextInput;
        return (
          <Input
            key={key}
            label={t(`sourceFormFieldsExtended.${ui.label}`)}
            value={cdc[key]}
            onChange={(e) => setCdc({ ...cdc, [key]: e.target.value })}
            placeholder={ui.path ? pathPlaceholder : undefined}
            data-testid={ui.testId}
          />
        );
      })}
      <TextInput
        label={t("sourceFormFieldsExtended.registryCa")}
        value={cdc.schemaRegistryCa}
        onChange={(e) => setCdc({ ...cdc, schemaRegistryCa: e.target.value })}
        placeholder={pathPlaceholder}
        data-testid="cdc-registry-ca-input"
      />
    </Stack>
  );
}
