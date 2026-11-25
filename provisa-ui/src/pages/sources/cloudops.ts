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

import { fieldsWhen, fieldsWithoutChoice, isRequired, isSecret, whenChoices } from "./descriptor";

// REQ-2114: the clouds, each cloud's keys, and which are required or secret are the descriptor's
// (provisa/api/admin/source_types.py CLOUDOPS_CLOUDS); the labels and placeholders are the form's.
const CLOUDOPS = "cloudops";
const MAPPING = "mappingJson";

export type CloudopsCloud = string;

export interface CloudopsField {
  key: string;
  label: string; // i18n key under sourceFormFieldsExtended
  required: boolean;
  secret?: boolean;
  placeholder?: string;
}

/** How the form shows each key: its label (an i18n key) and its placeholder. */
const SHOWN: Record<string, { label: string; placeholder?: string }> = {
  azure_tenant_id: { label: "coAzureTenantId" },
  azure_client_id: { label: "coAzureClientId" },
  azure_client_secret: {
    label: "coAzureClientSecret",
    placeholder: "${secret:azure_client_secret}",
  },
  azure_subscription_ids: { label: "coAzureSubscriptionIds", placeholder: "sub-1,sub-2" },
  aws_access_key_id: { label: "coAwsAccessKeyId" },
  aws_secret_access_key: {
    label: "coAwsSecretAccessKey",
    placeholder: "${secret:aws_secret_access_key}",
  },
  aws_region: { label: "coAwsRegion", placeholder: "us-east-1" },
  aws_account_ids: { label: "coAwsAccountIds", placeholder: "111111111111,222222222222" },
  aws_role_arn: {
    label: "coAwsRoleArn",
    placeholder: "arn:aws:iam::111111111111:role/CloudOpsRole",
  },
  gcp_credentials_path: { label: "coGcpCredentialsPath", placeholder: "/etc/provisa/gcp-key.json" },
  gcp_project_ids: { label: "coGcpProjectIds", placeholder: "proj-1,proj-2" },
};

function shown(key: string): { label: string; placeholder?: string } {
  const found = SHOWN[key];
  if (!found) throw new Error(`cloudops key ${key} has no label on the form`);
  return found;
}

export const CLOUDOPS_CLOUD_ORDER: CloudopsCloud[] = whenChoices(CLOUDOPS, MAPPING, "cloud");

export const CLOUDOPS_CLOUDS: Record<CloudopsCloud, CloudopsField[]> = Object.fromEntries(
  CLOUDOPS_CLOUD_ORDER.map((cloud) => [
    cloud,
    fieldsWhen(CLOUDOPS, MAPPING, "cloud", cloud).map((f) => ({
      key: f.name,
      required: isRequired(f),
      ...(isSecret(f) ? { secret: true } : {}),
      ...shown(f.name),
    })),
  ]),
);

const ALL_KEYS = [
  ...CLOUDOPS_CLOUD_ORDER.flatMap((c) => CLOUDOPS_CLOUDS[c].map((f) => f.key)),
  // The settings that belong to no cloud (the cache TTL).
  ...fieldsWithoutChoice(CLOUDOPS, MAPPING).map((f) => f.name),
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
