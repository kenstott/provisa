// Copyright (c) 2026 Kenneth Stott
// Canary: 8c7d1ead-1a52-4dcc-91ed-c222dadc9e42
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-990: an Iceberg source's catalog, read when a SingleStore store lands the table by pipeline
// (a workspace with enable_iceberg_ingest). Each value may be a ${secret:…} reference.

import { Alert, PasswordInput, Select, TextInput } from "@mantine/core";
import { useTranslation } from "react-i18next";
import {
  CATALOG_PASSWORD_IN_CONFIG,
  ICEBERG_CATALOG_FIELDS,
  ICEBERG_CATALOG_TYPES,
  type IcebergCatalogType,
} from "./icebergCatalog";

interface Props {
  fields: Record<string, string>;
  setFields: (fields: Record<string, string>) => void;
}

const LABEL: Record<string, string> = {
  iceberg_table_id: "icebergTableId",
  iceberg_catalog_uri: "icebergCatalogUri",
  iceberg_catalog_name: "icebergCatalogName",
  iceberg_catalog_warehouse: "icebergCatalogWarehouse",
  iceberg_catalog_user: "icebergCatalogUser",
  iceberg_catalog_password: "icebergCatalogPassword",
  iceberg_catalog_role: "icebergCatalogRole",
};

export function IcebergCatalogFields({ fields, setFields }: Props) {
  const { t } = useTranslation();
  const type = fields.iceberg_catalog_type as IcebergCatalogType | undefined;
  const input = (key: string, required: boolean) => {
    const props = {
      key,
      label: t(`sourceFormFields.${LABEL[key]}`),
      required,
      value: fields[key] ?? "",
      onChange: (e: React.ChangeEvent<HTMLInputElement>) =>
        setFields({ ...fields, [key]: e.currentTarget.value }),
      "data-testid": `iceberg-${key}`,
    };
    return key === "iceberg_catalog_password" ? (
      <PasswordInput {...props} placeholder="${secret:catalog_password}" />
    ) : (
      <TextInput {...props} />
    );
  };
  return (
    <>
      <Select
        label={t("sourceFormFields.icebergCatalogType")}
        description={t("sourceFormFields.icebergCatalogTypeHelp")}
        data={ICEBERG_CATALOG_TYPES.map((v) => ({ value: v, label: v }))}
        value={type ?? null}
        onChange={(v) => setFields({ ...fields, iceberg_catalog_type: v ?? "" })}
        clearable
        data-testid="iceberg-catalog-type"
      />
      {type && (
        <>
          {input("iceberg_table_id", true)}
          {ICEBERG_CATALOG_FIELDS[type].map((f) => input(f.key, f.required))}
          {CATALOG_PASSWORD_IN_CONFIG.has(type) && (
            <Alert
              color="yellow"
              style={{ gridColumn: "1 / -1" }}
              data-testid="iceberg-password-exposure"
            >
              {t("sourceFormFields.icebergCatalogPasswordExposure")}
            </Alert>
          )}
        </>
      )}
    </>
  );
}
