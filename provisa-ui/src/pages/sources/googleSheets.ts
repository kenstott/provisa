// Copyright (c) 2026 Kenneth Stott
// Canary: 0287c075-f710-46d9-9e99-61c85c1c47c9
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The Trino gsheets catalog needs the service-account key file's path beside the metadata sheet
// id (which rides in `database`), so it travels in the mapping like the other connectors' extra
// options.

import { mappingKey } from "./descriptor";

export const GOOGLE_SHEETS = "google_sheets";
// REQ-2114: the mapping key is the descriptor's (provisa/api/admin/source_types.py).
const CREDENTIALS_JSON = mappingKey(GOOGLE_SHEETS, "credentials_json");

/** The mapping a form saves. The field is required, so the form is not submitted without it. */
export function googleSheetsMapping(fields: Record<string, string>): Record<string, string> {
  return { [CREDENTIALS_JSON]: fields[CREDENTIALS_JSON] ?? "" };
}

/** A saved source's mapping into the form's fields. */
export function googleSheetsFields(mapping: Record<string, string>): Record<string, string> {
  return { [CREDENTIALS_JSON]: mapping[CREDENTIALS_JSON] };
}
