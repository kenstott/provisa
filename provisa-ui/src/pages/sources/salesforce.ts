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

import { fieldChoices, fieldDefault, fieldsWhen, mappingKey } from "./descriptor";

// REQ-2114: the credential sets, the keys each saves, and the default set are the descriptor's
// (provisa/api/admin/source_types.py SALESFORCE_AUTH).
const SALESFORCE = "salesforce";
const MAPPING = "mappingJson";
const AUTH_TYPE = mappingKey(SALESFORCE, "auth_type");
const API_VERSION = mappingKey(SALESFORCE, "api_version");

export type SalesforceAuthType = "CLIENT_CREDENTIALS" | "USERNAME_PASSWORD" | "ACCESS_TOKEN";
export const SALESFORCE_AUTH_TYPES = fieldChoices(
  SALESFORCE,
  MAPPING,
  AUTH_TYPE,
) as readonly SalesforceAuthType[];
const DEFAULT_AUTH_TYPE = fieldDefault(SALESFORCE, MAPPING, AUTH_TYPE) as SalesforceAuthType;

export const SALESFORCE_AUTH_FIELDS: Record<SalesforceAuthType, string[]> = Object.fromEntries(
  SALESFORCE_AUTH_TYPES.map((type) => [
    type,
    fieldsWhen(SALESFORCE, MAPPING, AUTH_TYPE, type).map((f) => f.name),
  ]),
) as Record<SalesforceAuthType, string[]>;

export function salesforceAuthType(fields: Record<string, string>): SalesforceAuthType {
  const type = fields[AUTH_TYPE] as SalesforceAuthType | undefined;
  return type && SALESFORCE_AUTH_TYPES.includes(type) ? type : DEFAULT_AUTH_TYPE;
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
  const mapping: Record<string, string> = { [AUTH_TYPE]: type };
  for (const key of SALESFORCE_AUTH_FIELDS[type]) {
    if (fields[key]) mapping[key] = fields[key];
  }
  if (fields[API_VERSION]?.trim()) mapping[API_VERSION] = fields[API_VERSION].trim();
  return JSON.stringify(mapping);
}

export function salesforceFieldsFromMapping(mappingJson: string): Record<string, string> {
  const mapping = JSON.parse(mappingJson) as Record<string, string>;
  const fields: Record<string, string> = {};
  for (const key of [AUTH_TYPE, API_VERSION, ...Object.values(SALESFORCE_AUTH_FIELDS).flat()]) {
    if (mapping[key]) fields[key] = mapping[key];
  }
  return fields;
}
