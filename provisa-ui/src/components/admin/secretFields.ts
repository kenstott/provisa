// Copyright (c) 2026 Kenneth Stott
// Canary: c24df54c-040d-4af6-bbfc-647f7fd9ccc9
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1575: the settings surfaces render forms over credentials the server will not give back.
// A secret field therefore arrives EMPTY and stays out of the saved payload until it is typed in —
// absent means "leave what is stored", and a field the operator empties on purpose is present and
// empty, which is how a credential is cleared. `secret_set` is the only thing the server says about
// a stored value, and it is what these helpers turn into a legible form.

export interface SecretAwareField {
  config_key: string;
  required: boolean;
  secret?: boolean;
}

/** Seed a form's values from the server config, leaving every secret field empty. */
export function seedFields(
  fields: SecretAwareField[],
  stored: Record<string, unknown> | undefined,
): Record<string, string> {
  const out: Record<string, string> = {};
  for (const f of fields) {
    if (f.secret) continue; // never seeded — the server did not send it and must not be told it back
    out[f.config_key] = String(stored?.[f.config_key] ?? "");
  }
  return out;
}

/** Whether a required field is unsatisfied: a stored secret counts as filled without retyping. */
export function missingRequired(
  fields: SecretAwareField[],
  values: Record<string, string> | undefined,
  isSet: Record<string, boolean> | undefined,
): boolean {
  return fields.some((f) => {
    if (!f.required) return false;
    const typed = (values?.[f.config_key] ?? "").trim();
    if (typed) return false;
    return !(f.secret && isSet?.[f.config_key] && values?.[f.config_key] === undefined);
  });
}

/** The placeholder a secret field shows: what is on file, never any part of it. */
export function secretPlaceholder(
  f: SecretAwareField,
  isSet: Record<string, boolean> | undefined,
  labels: { set: string; unset: string },
): string | undefined {
  if (!f.secret) return undefined;
  return isSet?.[f.config_key] ? labels.set : labels.unset;
}
