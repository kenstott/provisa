// Copyright (c) 2026 Kenneth Stott
// Canary: 49a5d73c-cadc-4048-80e5-d25b1e0bd18c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A source's Duplicate: the create form prefilled from a registered source (rule 2 of
// components/list/__tests__/panelDuplicateGuard.test.ts). It holds what the source's edit form
// holds, under the id "<id>-copy", with one exception: a credential. A credential field holds what
// was entered, resolved when the source is used; one that holds a reference (`${secret:NAME}`,
// `${env:NAME}`, ...) is carried as the same reference, and one that holds a value is left empty,
// the form saying it must be entered. The source's tables are not part of the form, so none is
// copied.

import type { RemoteSeed, SourceSeed } from "./sourceSeed";

/** A whole field that names a secret, as the server resolves one (provisa/core/secrets.py). */
const REFERENCE = /^\$\{\w+:[^}]+\}$/;

/** The keys of a source's settings that hold a credential. */
const CREDENTIAL =
  /(password|passwd|secret|token|api_?key|^access_key$|account_key|passphrase|private_key|client_key|pairing_code|credentials_json|sas)/i;

export const isCredentialKey = (key: string) => CREDENTIAL.test(key);
export const isReference = (value: string) => REFERENCE.test(value.trim());

/** What a credential field of the duplicate holds: the same reference, or nothing. */
function carried(value: string, dropped: { any: boolean }): string {
  if (value === "" || isReference(value)) return value;
  dropped.any = true;
  return "";
}

/** A source's settings with each credential that is not a reference left empty. */
export function carryReferences(
  fields: Record<string, string>,
  dropped: { any: boolean },
): Record<string, string> {
  return Object.fromEntries(
    Object.entries(fields).map(([k, v]) =>
      isCredentialKey(k) && typeof v === "string" ? [k, carried(v, dropped)] : [k, v],
    ),
  );
}

/** The id a duplicate is offered under: editable, and refused on save if it exists. */
export const duplicateId = (id: string) => `${id}-copy`;

/**
 * The create form's state for a duplicate of the source `seed` was read from, and whether a
 * credential was left empty (the form then says it must be entered).
 */
export function duplicateSeed(seed: SourceSeed): { seed: SourceSeed; credentialsLeft: boolean } {
  const dropped = { any: false };
  const out: SourceSeed = {
    ...seed,
    form: {
      ...seed.form,
      id: duplicateId(seed.form.id),
      password: carried(seed.form.password, dropped),
    },
    authFields: carryReferences(seed.authFields, dropped),
    sharepoint: {
      ...seed.sharepoint,
      password: carried(seed.sharepoint.password, dropped),
      certPassword: carried(seed.sharepoint.certPassword, dropped),
    },
    cdc: {
      ...seed.cdc,
      schemaRegistryPassword: carried(seed.cdc.schemaRegistryPassword, dropped),
      schemaRegistryToken: carried(seed.cdc.schemaRegistryToken, dropped),
      schemaRegistryClientKey: carried(seed.cdc.schemaRegistryClientKey, dropped),
    },
  };
  return { seed: out, credentialsLeft: dropped.any };
}

/**
 * A remote API source's registration, as its duplicate's form holds it: its auth and set-up
 * fields carried as references or left empty. A dbt Cloud token is never sent back, so a dbt
 * Cloud source's duplicate always asks for it.
 */
export function duplicateRemote(r: RemoteSeed, dropped: { any: boolean }): RemoteSeed {
  if (r.kind === "dbt") {
    if (r.spec === "cloud") dropped.any = true;
    return r;
  }
  const auth = r.auth ? carryReferences(r.auth, dropped) : r.auth;
  if (r.kind === "graphql") return { ...r, auth, setup: carryReferences(r.setup, dropped) };
  return { ...r, auth };
}
