// Copyright (c) 2026 Kenneth Stott
// Canary: 8c3e5a91-2d47-4b6f-a0e8-93f1c7d24b56
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// What a row of the Roles list shows, which the list's filter searches: kept here, beside nothing
// else, so the rows (SecurityPage, DerivedRoles, CapabilityNames) and the filter read one source.

import type { TFunction } from "i18next";
import type { DeclaredDerivedRole } from "../../api/admin";
import type { Role } from "../../types/auth";
import { NO_REGION } from "../../components/admin/ResidencyGrant";
import { SEPARATOR } from "../../hooks/useDerivedRoles";
import { matchesQuery } from "../../lib/searchItems";

const RESIDENCY = "data_residency";

/** A capability as a role's row names it: data residency with the regions it grants. */
export function capabilityLabel(
  capability: string,
  regions: readonly string[],
  t: TFunction,
): string {
  if (capability !== RESIDENCY || regions.length === 0) return capability;
  const region = (value: string) => (value === NO_REGION ? t("residencyGrant.noRegion") : value);
  return `${capability}: ${regions.map(region).join(", ")}`;
}

/** Every text a plain role's row shows. */
export function roleRowTexts(r: Role, t: TFunction): string[] {
  return [
    r.id,
    ...(r.detailsHidden
      ? [t("securityPage.detailsHidden")]
      : [
          ...r.capabilities.map((c) => capabilityLabel(c, r.residencyValues, t)),
          r.domain_access.join(", ") || t("securityPage.noDomains"),
        ]),
    r.description,
  ];
}

/** Whether a derived role's rows -- the parent and each value role under it -- hold ``search``. */
export function derivedRoleMatches(d: DeclaredDerivedRole, search: string, t: TFunction): boolean {
  const shown = [t("securityPage.derivedNoRights"), t("securityPage.derivedNoDomains")];
  return (
    matchesQuery([d.attribute, t("securityPage.derivedBadge"), ...shown, d.description], search) ||
    d.values.some((v) =>
      matchesQuery(
        [
          `${d.attribute}${SEPARATOR}${v}`,
          ...shown,
          t("securityPage.valueRoleHeld", { attribute: d.attribute, value: v }),
        ],
        search,
      ),
    )
  );
}
