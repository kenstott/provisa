// Copyright (c) 2026 Kenneth Stott
// Canary: 3dd42c41-7fd7-4305-ab55-54f4a1f94502
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { Button, Menu } from "@mantine/core";
import { Check, ChevronDown } from "lucide-react";
import { useTranslation } from "react-i18next";
import { useAuth } from "../context/AuthContext";

export function DomainSelector() {
  const { t } = useTranslation();
  const { selectedDomain, availableDomains, selectDomain } = useAuth();

  const label = selectedDomain ?? t("domainSelector.all");

  function handleSelect(domain: string | null) {
    selectDomain(domain);
  }

  return (
    <Menu position="bottom-start" withinPortal transitionProps={{ duration: 0 }}>
      <Menu.Target>
        <Button
          variant="default"
          size="compact-sm"
          rightSection={<ChevronDown size={14} aria-hidden />}
          data-testid="domain-selector-trigger"
        >
          {t("domainSelector.label", { domain: label })}
        </Button>
      </Menu.Target>
      <Menu.Dropdown>
        <Menu.Item
          aria-label={t("domainSelector.all")}
          aria-current={selectedDomain === null ? "true" : undefined}
          leftSection={selectedDomain === null ? <Check size={14} aria-hidden /> : undefined}
          onClick={() => handleSelect(null)}
        >
          {t("domainSelector.all")}
        </Menu.Item>
        {availableDomains.map((d) => (
          <Menu.Item
            key={d}
            aria-label={d}
            aria-current={selectedDomain === d ? "true" : undefined}
            leftSection={selectedDomain === d ? <Check size={14} aria-hidden /> : undefined}
            onClick={() => handleSelect(d)}
          >
            {d}
          </Menu.Item>
        ))}
      </Menu.Dropdown>
    </Menu>
  );
}
