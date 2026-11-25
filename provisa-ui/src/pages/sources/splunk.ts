// Copyright (c) 2026 Kenneth Stott
// Canary: fbbf6707-78d6-458f-953f-83dfffe4242a
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A Splunk source signs in with a bearer token (the connector's default, `use_token: true`) or a
// username and password; both ride in the standard username and password fields. Whether it
// checks the server's certificate is saved beside it in the mapping
// (provisa/federation/trino_connectors.py reads both).

import { mappingKey } from "./descriptor";

export const SPLUNK = "splunk";
// REQ-2114: the mapping keys are the descriptor's (provisa/api/admin/source_types.py).
const USE_TOKEN = mappingKey(SPLUNK, "use_token");
const DISABLE_SSL_VALIDATION = mappingKey(SPLUNK, "disable_ssl_validation");

/** The form's Splunk settings. */
export interface SplunkSeed {
  disableSsl: boolean;
  authMode: "token" | "userpass";
}

/** The settings of a new Splunk source: a token, the certificate checked. */
export const EMPTY_SPLUNK: SplunkSeed = { disableSsl: false, authMode: "token" };

/** The mapping a form saves. */
export function splunkMapping(splunk: SplunkSeed): Record<string, boolean> {
  return {
    [USE_TOKEN]: splunk.authMode === "token",
    ...(splunk.disableSsl ? { [DISABLE_SSL_VALIDATION]: true } : {}),
  };
}

/** A saved source's mapping into the form; the connector signs in with a token unless told not
 * to. */
export function splunkFromMapping(mapping: Record<string, unknown> | null): SplunkSeed {
  return {
    disableSsl: !!mapping?.[DISABLE_SSL_VALIDATION],
    authMode: mapping?.[USE_TOKEN] === false ? "userpass" : "token",
  };
}
