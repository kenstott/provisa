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

export const QUICKBOOKS = "quickbooks";
export const QUICKBOOKS_ENVIRONMENTS = ["sandbox", "production"] as const;
export type QuickBooksEnvironment = (typeof QUICKBOOKS_ENVIRONMENTS)[number];

export function quickbooksEnvironment(fields: Record<string, string>): QuickBooksEnvironment {
  const value = fields.environment as QuickBooksEnvironment | undefined;
  return value && QUICKBOOKS_ENVIRONMENTS.includes(value) ? value : "sandbox";
}

/** Whether a realm id is one the server takes: the company's number, or a ${secret:…} /
 * ${env:…} reference to it (checked when the source is used). */
export function quickbooksRealmIdValid(realmId: string): boolean {
  const value = realmId.trim();
  return value.startsWith("${") || /^\d{1,32}$/.test(value);
}

export function quickbooksMappingJson(fields: Record<string, string>): string {
  const mapping: Record<string, string> = { environment: quickbooksEnvironment(fields) };
  if (fields.refresh_token?.trim()) mapping.refresh_token = fields.refresh_token.trim();
  return JSON.stringify(mapping);
}

export function quickbooksFieldsFromMapping(mappingJson: string): Record<string, string> {
  const mapping = JSON.parse(mappingJson) as Record<string, string>;
  const fields: Record<string, string> = {};
  for (const key of ["environment", "refresh_token"]) {
    if (mapping[key]) fields[key] = mapping[key];
  }
  return fields;
}
