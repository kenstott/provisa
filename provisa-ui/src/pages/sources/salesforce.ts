// Copyright (c) 2026 Kenneth Stott
// Canary: 4e7a1c93-5b20-4d8f-9a36-c1d0e2f7b854
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1946: a Salesforce source's credential set and API version. The login URL, consumer key
// and consumer secret ride in the source's host, username and password; the rest is its mapping.
// Only the chosen credential set's fields are saved.

export const SALESFORCE_AUTH_TYPES = [
  "CLIENT_CREDENTIALS",
  "USERNAME_PASSWORD",
  "ACCESS_TOKEN",
] as const;
export type SalesforceAuthType = (typeof SALESFORCE_AUTH_TYPES)[number];

export const SALESFORCE_AUTH_FIELDS: Record<SalesforceAuthType, string[]> = {
  CLIENT_CREDENTIALS: [],
  USERNAME_PASSWORD: ["sf_username", "sf_password", "security_token"],
  ACCESS_TOKEN: ["access_token", "instance_url"],
};

export function salesforceAuthType(fields: Record<string, string>): SalesforceAuthType {
  const type = fields.auth_type as SalesforceAuthType | undefined;
  return type && SALESFORCE_AUTH_TYPES.includes(type) ? type : "CLIENT_CREDENTIALS";
}

/** Whether the chosen credential set carries the connected app's consumer key and secret. */
export function salesforceUsesConnectedApp(type: SalesforceAuthType): boolean {
  return type !== "ACCESS_TOKEN";
}

/** Whether a login URL is one the server takes: the full https:// My Domain URL, or a
 * ${secret:…} / ${env:…} reference to it (checked when the source is used). */
export function salesforceLoginUrlValid(loginUrl: string): boolean {
  const url = loginUrl.trim();
  return url.startsWith("${") || /^https:\/\/\S+$/.test(url);
}

export function salesforceMappingJson(fields: Record<string, string>): string {
  const type = salesforceAuthType(fields);
  const mapping: Record<string, string> = { auth_type: type };
  for (const key of SALESFORCE_AUTH_FIELDS[type]) {
    if (fields[key]) mapping[key] = fields[key];
  }
  if (fields.api_version?.trim()) mapping.api_version = fields.api_version.trim();
  return JSON.stringify(mapping);
}

export function salesforceFieldsFromMapping(mappingJson: string): Record<string, string> {
  const mapping = JSON.parse(mappingJson) as Record<string, string>;
  const fields: Record<string, string> = {};
  for (const key of ["auth_type", "api_version", ...Object.values(SALESFORCE_AUTH_FIELDS).flat()]) {
    if (mapping[key]) fields[key] = mapping[key];
  }
  return fields;
}
