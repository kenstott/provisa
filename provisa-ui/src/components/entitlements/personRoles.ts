// Copyright (c) 2026 Kenneth Stott
// Canary: 41c7e9a2-6b3d-4f58-9e17-d2a8c05b6f39
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2219: the roles a person holds, each with where it came from (GET
// /admin/entitlements/roles): the role grants an entitlement list is set on.

import { useEffect, useState } from "react";
import type { TFunction } from "i18next";
import { fetchPersonRoles, type HeldRole } from "../../api/entitlements";

/** A role as the selector names it: its id and where the person holds it from. */
export function roleLabel(t: TFunction, role: HeldRole): string {
  return t("entitlements.roleHeld", {
    role: role.role_id,
    origins: role.origins.map((o) => t(`entitlements.roleOrigin.${o}`)).join(", "),
  });
}

/** The roles `principal` holds, read whenever it changes; null for no principal, or until they
 *  arrive. */
export function usePersonRoles(
  principal: string | null,
  onError: (e: unknown) => void,
): HeldRole[] | null {
  const [held, setHeld] = useState<{ principal: string; roles: HeldRole[] } | null>(null);
  useEffect(() => {
    if (principal === null) return;
    let live = true;
    fetchPersonRoles(principal)
      .then((r) => live && setHeld({ principal, roles: r.roles }))
      .catch((e) => live && onError(e));
    return () => {
      live = false;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps -- onError may be re-created each render, so depending on it refetches in a loop; the read is keyed on the principal alone
  }, [principal]);
  return principal !== null && held?.principal === principal ? held.roles : null;
}
