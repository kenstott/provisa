// Copyright (c) 2026 Kenneth Stott
// Canary: 1c6e8b47-3a90-4f52-8d17-e5b2c9a0f364
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1947: a cloud inventory source's clouds. Everything rides in the source's mapping; a cloud
// is named by filling its fields, and each cloud is all or nothing.

export type CloudopsCloud = "azure" | "aws" | "gcp";

export interface CloudopsField {
  key: string;
  label: string; // i18n key under sourceFormFieldsExtended
  required: boolean;
  secret?: boolean;
  placeholder?: string;
}

export const CLOUDOPS_CLOUDS: Record<CloudopsCloud, CloudopsField[]> = {
  azure: [
    { key: "azure_tenant_id", label: "coAzureTenantId", required: true },
    { key: "azure_client_id", label: "coAzureClientId", required: true },
    {
      key: "azure_client_secret",
      label: "coAzureClientSecret",
      required: true,
      secret: true,
      placeholder: "${secret:azure_client_secret}",
    },
    {
      key: "azure_subscription_ids",
      label: "coAzureSubscriptionIds",
      required: true,
      placeholder: "sub-1,sub-2",
    },
  ],
  aws: [
    { key: "aws_access_key_id", label: "coAwsAccessKeyId", required: true },
    {
      key: "aws_secret_access_key",
      label: "coAwsSecretAccessKey",
      required: true,
      secret: true,
      placeholder: "${secret:aws_secret_access_key}",
    },
    { key: "aws_region", label: "coAwsRegion", required: true, placeholder: "us-east-1" },
    {
      key: "aws_account_ids",
      label: "coAwsAccountIds",
      required: true,
      placeholder: "111111111111,222222222222",
    },
    {
      key: "aws_role_arn",
      label: "coAwsRoleArn",
      required: false,
      placeholder: "arn:aws:iam::111111111111:role/CloudOpsRole",
    },
  ],
  gcp: [
    {
      key: "gcp_credentials_path",
      label: "coGcpCredentialsPath",
      required: true,
      placeholder: "/etc/provisa/gcp-key.json",
    },
    { key: "gcp_project_ids", label: "coGcpProjectIds", required: true, placeholder: "proj-1,proj-2" },
  ],
};

export const CLOUDOPS_CLOUD_ORDER: CloudopsCloud[] = ["azure", "aws", "gcp"];
const CACHE_TTL = "cache_ttl_minutes";
const ALL_KEYS = [
  ...CLOUDOPS_CLOUD_ORDER.flatMap((c) => CLOUDOPS_CLOUDS[c].map((f) => f.key)),
  CACHE_TTL,
];

/** The clouds the fields name: a cloud is named when any of its required fields is filled. */
export function cloudopsNamedClouds(fields: Record<string, string>): CloudopsCloud[] {
  return CLOUDOPS_CLOUD_ORDER.filter((c) =>
    CLOUDOPS_CLOUDS[c].some((f) => f.required && fields[f.key]?.trim()),
  );
}

/** The required fields still empty, of the clouds the fields name. */
export function cloudopsMissing(fields: Record<string, string>): string[] {
  return cloudopsNamedClouds(fields).flatMap((c) =>
    CLOUDOPS_CLOUDS[c].filter((f) => f.required && !fields[f.key]?.trim()).map((f) => f.key),
  );
}

export function cloudopsMappingJson(fields: Record<string, string>): string {
  const mapping: Record<string, string> = {};
  for (const key of ALL_KEYS) {
    const value = fields[key]?.trim();
    if (value) mapping[key] = value;
  }
  return JSON.stringify(mapping);
}

export function cloudopsFieldsFromMapping(mappingJson: string): Record<string, string> {
  const mapping = JSON.parse(mappingJson) as Record<string, unknown>;
  const fields: Record<string, string> = {};
  for (const key of ALL_KEYS) {
    if (mapping[key] !== undefined && mapping[key] !== "") fields[key] = String(mapping[key]);
  }
  return fields;
}
