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
import { Checkbox, Group, Text } from "@mantine/core";
import type { Capability } from "../../types/auth";

const ALL_CAPABILITIES: Capability[] = [
  "source_registration",
  "table_registration",
  "create_relationship",
  "access_config",
  "query_development",
  "approve_view",
  "full_results",
  "usage",
  "read_restricted",
  "approve_relationship",
  "create_view",
  "column_grant",
  "user_management",
  "masking_config",
  // REQ-1590: granted and revoked here like any other right — read opens the glossary, rw curates it.
  "glossary_read",
  "glossary_rw",
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

export function CapabilityGrid({
  value,
  onToggle,
  label,
}: {
  value: Capability[];
  onToggle: (cap: Capability) => void;
  label: string;
}) {
  return (
    <Checkbox.Group label={label} value={value} data-testid="capability-grid">
      <Group gap="sm" mt="xs" style={{ rowGap: "0.35rem" }}>
        {ALL_CAPABILITIES.map((cap) => (
          <Checkbox
            key={cap}
            label={cap}
            checked={value.includes(cap)}
            onChange={() => onToggle(cap)}
            size="sm"
          />
        ))}
      </Group>
    </Checkbox.Group>
  );
}
