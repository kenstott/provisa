// Copyright (c) 2026 Kenneth Stott
// Canary: e6781a36-34b2-45af-ba15-6914cf324021
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2078: the derived roles' rows in the roles list, as its search and its detail read them
// (REQ-2277 rule 9: a derived role is open only while the list shows its row).

import type { DeclaredDerivedRole as DerivedRole } from "../../api/admin";
import { derivedId } from "../../components/list/useDetailParam";
import { SEPARATOR } from "../../hooks/useDerivedRoles";

/** The texts the roles list's search finds a derived role, and every value role under it, by. */
export function derivedRoleTexts(d: DerivedRole): string[] {
  return [d.attribute, ...d.values.map((v) => `${d.attribute}${SEPARATOR}${v}`)];
}

/** A row of the derived roles in the roles list: a parent, or one of its value roles. */
export interface DerivedItem {
  /** Its `derived` id (derivedId). */
  id: string;
  /** As its row shows it. */
  name: string;
  role: DerivedRole;
}

/** Every derived role's rows: each parent, then its value roles. */
export function derivedItems(roles: readonly DerivedRole[]): DerivedItem[] {
  return roles.flatMap((d) => [
    { id: derivedId(d.attribute, null), name: d.attribute, role: d },
    ...d.values.map((v) => ({
      id: derivedId(d.attribute, v),
      name: `${d.attribute}${SEPARATOR}${v}`,
      role: d,
    })),
  ]);
}
