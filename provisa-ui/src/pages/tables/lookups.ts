// Copyright (c) 2026 Kenneth Stott
// Canary: 3f6c2d8e-9a41-4b7f-8e25-c1d0a7b94e63
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1986: a lookup is a table that needs a parameter to be read (a GraphQL root field with a
// required id, an OpenAPI operation with a path parameter). The Register Table picker lists it
// apart and names what it needs.

import type { TFunction } from "i18next";
import { isRequiredParam } from "../../components/nativeParams";

/** The mark a lookup's choice starts with: a native option shows text alone. */
export const LOOKUP_GLYPH = "\u{1F511}";

const NF_PREFIX = "_nf_";

interface ParameterColumn {
  columnName: string;
  nativeFilterType?: string | null;
  nativeFilterRequired?: boolean | null;
  requiredBySteward?: boolean;
}

/** The parameters a read of an offered table must give: those its source requires, as the list
 * names them, and, once it is registered, those a steward made required for it. */
export function requiredParametersOf(
  offered: { requiredParameters?: string[] | null },
  registered?: { columns: ParameterColumn[] } | null,
): string[] {
  const names = [...(offered.requiredParameters ?? [])];
  for (const c of registered?.columns ?? []) {
    if (!isRequiredParam(c)) continue;
    const name = c.columnName.startsWith(NF_PREFIX)
      ? c.columnName.slice(NF_PREFIX.length)
      : c.columnName;
    if (!names.includes(name)) names.push(name);
  }
  return names;
}

/** "needs id", "needs owner, name": what a lookup must be given. */
export function lookupNeeds(t: TFunction, required: string[]): string {
  return t("registerTableForm.lookupNeeds", { params: required.join(", ") });
}
