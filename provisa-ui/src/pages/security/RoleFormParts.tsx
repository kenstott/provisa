// Copyright (c) 2026 Kenneth Stott
// Canary: 5397601b-c80d-4309-8a4b-c3a8c683e829
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useTranslation } from "react-i18next";
import { MultiSelect, Text } from "@mantine/core";
import { alsoReached } from "../../lib/domainTree";
import type { TreeDomain } from "../../lib/domainTree";
import { FormField } from "../../components/form/FormLayout";
import { CapabilityOption } from "./CapabilityOption";
import type { Capability } from "../../types/auth";

// Every right a role may be given but data_residency, which its own control carries, with what it
// covers (components/admin/ResidencyGrant). A platform right is defined by a platform administrator.
const ALL_CAPABILITIES: Capability[] = [
  "catalog_read",
  "glossary_read",
  "data_product_read",
  "usage",
  "query_development",
  "full_results",
  "write",
  "no_aggregations",
  "create_view",
  "approve_view",
  "create_relationship",
  "approve_relationship",
  "ignore_relationships",
  "source_registration",
  "table_registration",
  "access_config",
  "column_grant",
  "masking_config",
  "sensitive_data",
  "view_governance",
  "user_management",
  "glossary_rw",
  "org_glossary_rw",
  "data_product_rw",
  "dq_read",
  "dq_rw",
  "org_settings",
  "observability",
  "environment_switch",
  "environment_owned_management",
  "environment_management",
  "environment_data",
  "platform_settings",
  "cross_org",
];

/**
 * A role is always one or more domains, or all: "All Domains" is the only way to say all, and
 * the server refuses a role saved with none. The editor therefore never lets an empty picker
 * look like "unrestricted" — it says what is required and withholds Save until it is met.
 *
 * Two messages, for two states. A form with no domain chosen yet states the requirement. An
 * EXISTING role that has ended up with none (the server's backstop: such a role reads no data)
 * is told so, because that is a fact about the role as it stands, not about the form.
 */
export function DomainsNote({ savedWithNone }: { savedWithNone: boolean }) {
  const { t } = useTranslation();
  return savedWithNone ? (
    <Text size="sm" c="orange" role="note" data-testid="role-no-domains-note">
      {t("securityPage.noDomainsNote")}
    </Text>
  ) : (
    <Text size="sm" c="red" role="alert" data-testid="role-domains-required">
      {t("securityPage.domainsRequired")}
    </Text>
  );
}

/** REQ-1363: a role that lists a domain reaches every domain under it. The picker shows what was
 *  chosen; this says what else that choice reaches, so a grant on a business unit is not mistaken
 *  for a grant on one domain. Nothing is rendered when the choice reaches no more. */
export function SubtreeNote({ listed, domains }: { listed: string[]; domains: TreeDomain[] }) {
  const { t } = useTranslation();
  const reached = alsoReached(listed, domains);
  if (listed.includes("*") || reached.length === 0) return null;
  return (
    <Text size="sm" c="dimmed" role="note" data-testid="role-domain-subtree">
      {t("securityPage.alsoReaches", { count: reached.length, domains: reached.join(", ") })}
    </Text>
  );
}

export function CapabilityGrid({
  value,
  held,
  onToggle,
  label,
}: {
  value: Capability[];
  /** What the role's parents hold; null for a role with none. Anything else is shown disabled. */
  held: ReadonlySet<string> | null;
  onToggle: (cap: Capability) => void;
  label: string;
}) {
  const offered = (cap: Capability) => held === null || held.has(cap);
  return (
    // One dropdown, a line of the role form to itself, shaped as Domain Access is.
    <FormField size="long">
      <MultiSelect
        label={label}
        aria-label={label}
        data={ALL_CAPABILITIES.map((cap) => ({ value: cap, label: cap, disabled: !offered(cap) }))}
        value={value}
        // The picker reports the whole selection; the form's contract is one toggle at a time.
        onChange={(next) => {
          const changed = [
            ...next.filter((c) => !value.includes(c as Capability)),
            ...value.filter((c) => !next.includes(c)),
          ];
          for (const cap of changed) onToggle(cap as Capability);
        }}
        renderOption={({ option, checked }) => (
          <CapabilityOption
            cap={option.value as Capability}
            checked={!!checked}
            offered={offered(option.value as Capability)}
          />
        )}
        searchable
        clearable={false}
        comboboxProps={{ withinPortal: true }}
        placeholder={label}
        size="sm"
        data-testid="capability-grid"
      />
    </FormField>
  );
}
