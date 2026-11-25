// Copyright (c) 2026 Kenneth Stott
// Canary: b911fa0d-0b54-419f-b902-b6c2c99f946d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A SharePoint source's sign-in beside its site URL (host), tenant (database) and client
// (username / password): how it signs in, and that sign-in's own fields, saved in its mapping
// (provisa/federation/trino_connectors.py reads them).

import { fieldChoices, fieldDefault, fieldsWhen, mappingKeysOf } from "./descriptor";

export const SHAREPOINT = "sharepoint";
// REQ-2114: the keys, the sign-ins and the default one are the descriptor's
// (provisa/api/admin/source_types.py).
const K = mappingKeysOf(SHAREPOINT, [
  "auth_type",
  "certificate_path",
  "certificate_password",
  "sp_username",
  "sp_password",
] as const);
export const SHAREPOINT_AUTH_TYPES = fieldChoices(SHAREPOINT, "mappingJson", K.auth_type);

/** The form's SharePoint sign-in. */
export interface SharepointSeed {
  authType: string;
  certPath: string;
  certPassword: string;
  username: string;
  password: string;
}

/** The sign-in of a new SharePoint source: the default one, no field filled. */
export const EMPTY_SHAREPOINT: SharepointSeed = {
  authType: fieldDefault(SHAREPOINT, "mappingJson", K.auth_type) as string,
  certPath: "",
  certPassword: "",
  username: "",
  password: "",
};

/** The mapping a form saves: the sign-in, and the fields the descriptor states for it. */
export function sharepointMapping(sp: SharepointSeed): Record<string, string> {
  const values: Record<string, string> = {
    [K.certificate_path]: sp.certPath,
    [K.certificate_password]: sp.certPassword,
    [K.sp_username]: sp.username,
    [K.sp_password]: sp.password,
  };
  return {
    [K.auth_type]: sp.authType,
    ...Object.fromEntries(
      fieldsWhen(SHAREPOINT, "mappingJson", K.auth_type, sp.authType).map((f) => [
        f.name,
        values[f.name],
      ]),
    ),
  };
}

/** A saved source's mapping into the form; a field it does not hold is a new source's. */
export function sharepointFromMapping(mapping: Record<string, string> | null): SharepointSeed {
  if (!mapping) return EMPTY_SHAREPOINT;
  return {
    authType: mapping[K.auth_type] ?? EMPTY_SHAREPOINT.authType,
    certPath: mapping[K.certificate_path] ?? EMPTY_SHAREPOINT.certPath,
    certPassword: mapping[K.certificate_password] ?? EMPTY_SHAREPOINT.certPassword,
    username: mapping[K.sp_username] ?? EMPTY_SHAREPOINT.username,
    password: mapping[K.sp_password] ?? EMPTY_SHAREPOINT.password,
  };
}
