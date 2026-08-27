// Copyright (c) 2026 Kenneth Stott
// Canary: 5f1c2b90-3a44-4b0e-9a7d-2c6f1b8e40d1
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1559: Admin navigates down a left-hand rail rather than across the horizontal subnav every
// other section uses. The entry list, the rights gating it and the commercial/installed filtering
// are the same ones NavBar applies — only the direction changes, so no surface appears here that
// the person may not open.

import { NavLink, useLocation } from "react-router-dom";
import { useTranslation } from "react-i18next";
import { CapabilityGate } from "./CapabilityGate";
import { useAuth } from "../context/AuthContext";
import { NAV_GROUPS, activeGroupId, labelKeyFor, type NavGroup } from "./navGroups";

// The rail IS the admin group; a build whose NAV_GROUPS lost it has nothing to render, so this
// fails at import rather than degrading to an empty rail.
function adminGroup(): NavGroup {
  const group = NAV_GROUPS.find((g) => g.id === "admin");
  if (!group) throw new Error("NAV_GROUPS carries no admin group");
  return group;
}

const ADMIN_GROUP = adminGroup();

export function AdminRail() {
  const { t } = useTranslation();
  const location = useLocation();
  const { billing, capabilities } = useAuth();

  if (activeGroupId(location.pathname) !== "admin") return null;

  return (
    <nav className="admin-rail" data-testid="admin-rail" aria-label={t("navBar.groupAdmin")}>
      {ADMIN_GROUP.items
        .filter((item) => !(item.commercial && !billing) && !(item.installedOnly && billing))
        .map((item) =>
          item.comingSoon ? (
            <span key={item.to} className="admin-rail-coming-soon">
              {t("navBar.comingSoon", { label: t(item.labelKey) })}
            </span>
          ) : (
            <CapabilityGate
              key={item.to}
              capability={item.capability}
              strict={item.strict}
              orCapability={item.orCapability}
              navigable
            >
              <NavLink to={item.to}>{t(labelKeyFor(item, capabilities))}</NavLink>
            </CapabilityGate>
          ),
        )}
    </nav>
  );
}
