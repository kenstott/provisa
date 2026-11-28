// Copyright (c) 2026 Kenneth Stott
// Canary: 985f9a48-bf14-4869-98fe-86ab974ffe64
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2277 (amended 2026-11-27): the navbar Domains picker lists the business domains first,
// alphabetically, then a divider and a System label over Provisa's internal domains.

import { Checkbox, Menu, Stack } from "@mantine/core";
import { useTranslation } from "react-i18next";
import { isInternalDomain } from "../lib/internalDomains";

interface Props {
  domains: readonly string[];
  checkedDomains: ReadonlySet<string>;
  onToggle: (domainId: string) => void;
}

function byName(a: string, b: string): number {
  return a.localeCompare(b);
}

export function NavBarDomainList({ domains, checkedDomains, onToggle }: Props) {
  const { t } = useTranslation();
  const business = domains.filter((d) => !isInternalDomain(d)).sort(byName);
  const system = domains.filter(isInternalDomain).sort(byName);

  const boxes = (ids: string[]) => (
    <Stack gap={4} px="sm" pb="xs">
      {ids.map((d) => (
        <Checkbox
          key={d}
          label={d}
          data-testid={`navbar-domain-item-${d}`}
          checked={checkedDomains.has(d)}
          onChange={() => onToggle(d)}
        />
      ))}
    </Stack>
  );

  return (
    <>
      <Menu.Label>{t("navBar.domainsLabel")}</Menu.Label>
      {business.length > 0 && boxes(business)}
      {system.length > 0 && (
        <>
          {business.length > 0 && <Menu.Divider data-testid="navbar-domain-system-divider" />}
          <Menu.Label data-testid="navbar-domain-system-label">
            {t("navBar.domainsSystem")}
          </Menu.Label>
          {boxes(system)}
        </>
      )}
    </>
  );
}
