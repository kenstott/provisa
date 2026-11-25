// Copyright (c) 2026 Kenneth Stott
// Canary: 328a381d-bb36-48bd-b6ac-07e05ecb86a4
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2138: a sparql source's reasoner and its OWL axioms travel in the source's mapping
// (provisa/sparql/reasoning.py: "reasoner", "axioms"); the form holds them in its fields.
// Its credential -- the service identity every call to its endpoint is sent as -- rides in the
// source's standard username and password fields, as entered (a value or a ${secret:...}
// reference, resolved at each call), and its kind in the mapping's "auth"
// (provisa/sparql/source.py: sparql_auth).

import { fieldChoices, mappingKey } from "./descriptor";

// REQ-2114: the mapping keys, the reasoners and the credential kinds are the descriptor's
// (provisa/api/admin/source_types.py, from provisa/sparql/reasoning.py and source.py).
const SPARQL = "sparql";
const REASONER = mappingKey(SPARQL, "reasoner");
const AXIOMS = mappingKey(SPARQL, "axioms");
const AUTH = mappingKey(SPARQL, "auth");

/** The reasoners a sparql source may name. */
export const SPARQL_REASONERS: readonly string[] = fieldChoices(SPARQL, "mappingJson", REASONER);

/** The credential kinds a sparql source may declare; "none" declares none. */
const SPARQL_AUTH_KINDS = new Set(fieldChoices(SPARQL, "mappingJson", AUTH));

export function sparqlMappingJson(fields: Record<string, string>, authType: string): string {
  const reasoner = fields[REASONER]?.trim();
  return JSON.stringify({
    ...(reasoner ? { [REASONER]: reasoner, [AXIOMS]: fields[AXIOMS] ?? "" } : {}),
    ...(SPARQL_AUTH_KINDS.has(authType) ? { [AUTH]: authType } : {}),
  });
}

/** The source's username and password for the credential the form declares. */
export function sparqlCredentials(
  authType: string,
  fields: Record<string, string>,
): { username: string; password: string } {
  if (authType === "bearer") return { username: "", password: fields.token ?? "" };
  if (authType === "basic")
    return { username: fields.username ?? "", password: fields.password ?? "" };
  return { username: "", password: "" };
}

export function sparqlFieldsFromMapping(mappingJson: string): Record<string, string> {
  const mapping = JSON.parse(mappingJson) as Record<string, unknown>;
  const reasoner = mapping[REASONER];
  if (typeof reasoner !== "string") return {};
  return {
    [REASONER]: reasoner,
    [AXIOMS]: typeof mapping[AXIOMS] === "string" ? (mapping[AXIOMS] as string) : "",
  };
}

/** The auth mode and credential fields a saved sparql source opens with, as it holds them. */
export function sparqlCredentialFromSource(
  mappingJson: string | null | undefined,
  username: string | null | undefined,
  passwordRef: string | null | undefined,
): { authType: string; fields: Record<string, string> } {
  const kind = mappingJson ? (JSON.parse(mappingJson) as Record<string, unknown>)[AUTH] : undefined;
  if (kind === "bearer") return { authType: "bearer", fields: { token: passwordRef ?? "" } };
  if (kind === "basic")
    return {
      authType: "basic",
      fields: { username: username ?? "", password: passwordRef ?? "" },
    };
  return { authType: "none", fields: {} };
}
