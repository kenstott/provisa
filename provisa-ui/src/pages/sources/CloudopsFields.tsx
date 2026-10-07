// Copyright (c) 2026 Kenneth Stott
// Canary: 7d4a2f95-6b18-4c03-9e7a-0f3d1b8c5e26
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1947: a cloud inventory source — the credentials and scope of one or more of Azure, AWS
// and GCP. Each secret may be a ${secret:…} reference.

import { Alert, Divider, PasswordInput, TextInput } from "@mantine/core";
import { useTranslation } from "react-i18next";
import {
  CLOUDOPS_CLOUD_ORDER,
  CLOUDOPS_CLOUDS,
  cloudopsMissing,
  cloudopsNamedClouds,
  type CloudopsCloud,
} from "./cloudops";

interface Props {
  fields: Record<string, string>;
  setFields: (fields: Record<string, string>) => void;
}

const CLOUD_LABEL: Record<CloudopsCloud, string> = {
  azure: "Azure",
  aws: "AWS",
  gcp: "GCP",
};

export function CloudopsFields({ fields, setFields }: Props) {
  const { t } = useTranslation();
  const named = cloudopsNamedClouds(fields);
  const missing = new Set(cloudopsMissing(fields));
  return (
    <>
      <Alert
        color={named.length ? "gray" : "yellow"}
        style={{ gridColumn: "1 / -1" }}
        data-testid="cloudops-clouds-note"
      >
        {named.length
          ? t("sourceFormFieldsExtended.coCloudsNamed", {
              clouds: named.map((c) => CLOUD_LABEL[c]).join(", "),
            })
          : t("sourceFormFieldsExtended.coNameACloud")}
      </Alert>
      {CLOUDOPS_CLOUD_ORDER.map((cloud) => (
        <div key={cloud} style={{ display: "contents" }}>
          <Divider label={CLOUD_LABEL[cloud]} labelPosition="left" style={{ gridColumn: "1 / -1" }} />
          {CLOUDOPS_CLOUDS[cloud].map((f) => {
            const props = {
              label: t(`sourceFormFieldsExtended.${f.label}`),
              required: f.required && named.includes(cloud),
              value: fields[f.key] ?? "",
              onChange: (e: React.ChangeEvent<HTMLInputElement>) =>
                setFields({ ...fields, [f.key]: e.currentTarget.value }),
              placeholder: f.placeholder,
              error: missing.has(f.key) ? t("sourceFormFieldsExtended.coRequiredWithCloud") : undefined,
              "data-testid": `cloudops-${f.key}`,
            };
            return f.secret ? (
              <PasswordInput key={f.key} {...props} />
            ) : (
              <TextInput key={f.key} {...props} />
            );
          })}
        </div>
      ))}
      <Divider style={{ gridColumn: "1 / -1" }} />
      <TextInput
        label={t("sourceFormFieldsExtended.coCacheTtlMinutes")}
        description={t("sourceFormFieldsExtended.coCacheTtlMinutesHelp")}
        value={fields.cache_ttl_minutes ?? ""}
        onChange={(e) => setFields({ ...fields, cache_ttl_minutes: e.currentTarget.value })}
        placeholder="5"
        data-testid="cloudops-cache_ttl_minutes"
      />
    </>
  );
}
