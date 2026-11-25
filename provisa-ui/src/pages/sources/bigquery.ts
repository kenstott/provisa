// Copyright (c) 2026 Kenneth Stott
// Canary: e72e4cab-a24e-4ec9-ac7f-da8516458c07
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A BigQuery source signs in with a service-account key file, saved as its federation hints, or
// with the application default credential, which saves none
// (provisa/executor/drivers/bigquery.py).

import { fieldsWhen, hintKey } from "./descriptor";
import type { AuthSeed } from "./descriptorPick";

export const BIGQUERY = "bigquery";
/** The sign-in by a service-account key file. */
export const BIGQUERY_SERVICE_ACCOUNT = "service_account";
/** The sign-in by the application default credential. */
export const BIGQUERY_APPLICATION_DEFAULT = "application_default";
// REQ-2114: the hint key is the descriptor's (provisa/api/admin/source_types.py).
const CREDENTIALS_PATH = hintKey(BIGQUERY, "credentials_path");
/** The form's field for the key file's path. */
const CREDENTIALS_JSON = "credentials_json";

/** The hints a form saves: the key file's path, for a service-account sign-in. */
export function bigqueryHints(
  authType: string,
  fields: Record<string, string>,
): Record<string, string> {
  const keys = fieldsWhen(BIGQUERY, "federationHintsJson", "auth", authType).map((f) => f.name);
  return keys.includes(CREDENTIALS_PATH) && fields[CREDENTIALS_JSON]
    ? { [CREDENTIALS_PATH]: fields[CREDENTIALS_JSON] }
    : {};
}

/** A saved source's hints into the form: a key file's path is a service-account sign-in. */
export function bigquerySeed(hints: Record<string, unknown>): AuthSeed {
  const path = hints[CREDENTIALS_PATH];
  return path
    ? { authType: BIGQUERY_SERVICE_ACCOUNT, authFields: { [CREDENTIALS_JSON]: String(path) } }
    : { authType: BIGQUERY_APPLICATION_DEFAULT, authFields: {} };
}
