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

/** The reasoners a sparql source may name. */
export const SPARQL_REASONERS = ["hermit"] as const;

/** The credential kinds a sparql source may declare; "none" declares none. */
const SPARQL_AUTH_KINDS = new Set(["bearer", "basic"]);

export function sparqlMappingJson(fields: Record<string, string>, authType: string): string {
  const reasoner = fields.reasoner?.trim();
  return JSON.stringify({
    ...(reasoner ? { reasoner, axioms: fields.axioms ?? "" } : {}),
    ...(SPARQL_AUTH_KINDS.has(authType) ? { auth: authType } : {}),
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
  if (typeof mapping.reasoner !== "string") return {};
  return {
    reasoner: mapping.reasoner,
    axioms: typeof mapping.axioms === "string" ? mapping.axioms : "",
  };
}

/** The auth mode and credential fields a saved sparql source opens with, as it holds them. */
export function sparqlCredentialFromSource(
  mappingJson: string | null | undefined,
  username: string | null | undefined,
  passwordRef: string | null | undefined,
): { authType: string; fields: Record<string, string> } {
  const kind = mappingJson ? (JSON.parse(mappingJson) as Record<string, unknown>).auth : undefined;
  if (kind === "bearer") return { authType: "bearer", fields: { token: passwordRef ?? "" } };
  if (kind === "basic")
    return {
      authType: "basic",
      fields: { username: username ?? "", password: passwordRef ?? "" },
    };
  return { authType: "none", fields: {} };
}
