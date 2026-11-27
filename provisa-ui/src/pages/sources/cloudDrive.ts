// Copyright (c) 2026 Kenneth Stott
// Canary: 9b4e2c71-5a0d-4f38-b6e1-d27c8a3f5e90
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2280: the OneDrive and Google Drive transports of a files source. onedrive:// signs in
// with the source's tenant id (database), client id (username) and client secret (password);
// gdrive:// with a service account key (password), acting as the Workspace user gdrive_subject
// names when the domain delegated to it, or an OAuth client (username, password) and its refresh
// token, as the source's mapping says (provisa/file_source/cloud_drive.py).

import { fieldChoices, fieldDefault, mappingKey } from "./descriptor";

export const ONEDRIVE_TRANSPORT = "onedrive://";
export const GDRIVE_TRANSPORT = "gdrive://";

const FILES = "files";
export const GDRIVE_AUTH = mappingKey(FILES, "gdrive_auth");
export const REFRESH_TOKEN = mappingKey(FILES, "refresh_token");
/** The Workspace user a service account acts as, by domain-wide delegation (optional). */
export const GDRIVE_SUBJECT = mappingKey(FILES, "gdrive_subject");
/** The mapping keys only a gdrive:// source holds. */
export const GDRIVE_KEYS: readonly string[] = [GDRIVE_AUTH, REFRESH_TOKEN, GDRIVE_SUBJECT];

/** How a gdrive:// source signs in, as the descriptor offers it. */
export const gdriveAuthKinds = (): string[] => fieldChoices(FILES, "mappingJson", "gdrive_auth");
export const GDRIVE_OAUTH = "oauth";

/** The form field holding the gdrive:// sign-in kind: the one chosen, else the descriptor's. */
export function gdriveAuth(fields: Record<string, string>): string {
  return fields[GDRIVE_AUTH] || String(fieldDefault(FILES, "mappingJson", "gdrive_auth"));
}

/** The drive keys a files source's mapping holds for `transport`: none but on gdrive://. */
export function driveMapping(
  transport: string,
  fields: Record<string, string>,
): Record<string, string> {
  if (transport !== GDRIVE_TRANSPORT) return {};
  const auth = gdriveAuth(fields);
  if (auth === GDRIVE_OAUTH) {
    return { [GDRIVE_AUTH]: auth, [REFRESH_TOKEN]: fields[REFRESH_TOKEN] ?? "" };
  }
  const subject = fields[GDRIVE_SUBJECT]?.trim();
  return subject ? { [GDRIVE_AUTH]: auth, [GDRIVE_SUBJECT]: subject } : { [GDRIVE_AUTH]: auth };
}

/** The form's drive fields from a saved mapping. */
export function driveFieldsFromMapping(
  mapping: Record<string, unknown> | null,
): Record<string, string> {
  const out: Record<string, string> = {};
  for (const key of GDRIVE_KEYS) {
    const value = mapping?.[key];
    if (typeof value === "string") out[key] = value;
  }
  return out;
}
