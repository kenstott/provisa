// Copyright (c) 2026 Kenneth Stott
// Canary: 5e2d9c71-4a08-4b3f-96e1-c07b8f3a2d45
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2197: a QuickBooks Online source. The Intuit app's client id and client secret ride in the
// source's username and password, the company's realm id in its database (as a Salesforce
// source's connected app does); sandbox or production and the refresh token are its mapping.

import { fieldChoices, fieldDefault, mappingKey } from "./descriptor";

export const QUICKBOOKS = "quickbooks";
// REQ-2114: the environments, the default one and the mapping keys are the descriptor's
// (provisa/api/admin/source_types.py, from provisa/quickbooks/settings.py).
const MAPPING = "mappingJson";
const ENVIRONMENT = mappingKey(QUICKBOOKS, "environment");
const REFRESH_TOKEN = mappingKey(QUICKBOOKS, "refresh_token");

export type QuickBooksEnvironment = "sandbox" | "production";
export const QUICKBOOKS_ENVIRONMENTS = fieldChoices(
  QUICKBOOKS,
  MAPPING,
  ENVIRONMENT,
) as readonly QuickBooksEnvironment[];
const DEFAULT_ENVIRONMENT = fieldDefault(QUICKBOOKS, MAPPING, ENVIRONMENT) as QuickBooksEnvironment;

export function quickbooksEnvironment(fields: Record<string, string>): QuickBooksEnvironment {
  const value = fields[ENVIRONMENT] as QuickBooksEnvironment | undefined;
  return value && QUICKBOOKS_ENVIRONMENTS.includes(value) ? value : DEFAULT_ENVIRONMENT;
}

/** Whether a realm id is one the server takes: the company's number, or a ${secret:…} /
 * ${env:…} reference to it (checked when the source is used). */
export function quickbooksRealmIdValid(realmId: string): boolean {
  const value = realmId.trim();
  return value.startsWith("${") || /^\d{1,32}$/.test(value);
}

export function quickbooksMappingJson(fields: Record<string, string>): string {
  const mapping: Record<string, string> = { [ENVIRONMENT]: quickbooksEnvironment(fields) };
  if (fields[REFRESH_TOKEN]?.trim()) mapping[REFRESH_TOKEN] = fields[REFRESH_TOKEN].trim();
  return JSON.stringify(mapping);
}

export function quickbooksFieldsFromMapping(mappingJson: string): Record<string, string> {
  const mapping = JSON.parse(mappingJson) as Record<string, string>;
  const fields: Record<string, string> = {};
  for (const key of [ENVIRONMENT, REFRESH_TOKEN]) {
    if (mapping[key]) fields[key] = mapping[key];
  }
  return fields;
}
