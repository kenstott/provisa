// Copyright (c) 2026 Kenneth Stott
// Canary: 5b7b5561-5176-4926-8c7f-8e755fd39eb6
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useTranslation } from "react-i18next";
import { Text } from "@mantine/core";
import type { Role } from "../../types/auth";
import { DetailFields } from "../../components/detail/DetailFields";

/**
 * Every value of a role, read-only, under the labels its form gives them. An empty value is
 * shown as such, never left out; a role that lists no domain says what that means, as its row
 * does.
 */
export function RoleRead({ role: r }: { role: Role }) {
  const { t } = useTranslation();
  const list = (values: readonly string[] | undefined) =>
    values && values.length > 0 ? values.join(", ") : null;
  const domain = (d: string) => (d === "*" ? t("securityPage.allDomains") : d);
  const sessionVars = r.sessionVars ?? [];
  return (
    <div className="detail-groups" data-testid={`role-read-${r.id}`}>
      <DetailFields
        testId="role-read-fields"
        fields={[
          { label: t("securityPage.roleId"), value: r.id },
          // REQ-2080: what the role is for.
          { label: t("securityPage.roleDescription"), value: r.description },
          { label: t("securityPage.capabilities"), value: list(r.capabilities) },
          { label: t("securityPage.demonstrated"), value: list(r.demonstrated) },
          { label: t("residencyGrant.values"), value: list(r.residencyValues) },
          {
            label: t("securityPage.domainAccess"),
            value:
              r.domain_access.length > 0
                ? r.domain_access.map(domain).join(", ")
                : t("securityPage.noDomains"),
          },
          {
            label: t("securityPage.parentRole"),
            value: list(r.parents),
            testId: `role-parent-${r.id}`,
          },
          // REQ-2155: Provisa's own, extended with child roles and never edited. Said only of one.
          ...(r.standard
            ? [
                {
                  label: t("securityPage.standardRole"),
                  value: t("securityPage.standardRoleNote"),
                  testId: `role-standard-${r.id}`,
                },
              ]
            : []),
          {
            // REQ-1682: the role's own constant session values, one to a line.
            label: t("roleSessionValues.label"),
            value:
              sessionVars.length > 0
                ? sessionVars.map((v) => (
                    <span key={v.name} className="role-session-value">
                      <code>{v.name}</code> = {v.value}
                    </span>
                  ))
                : null,
            testId: `role-session-values-${r.id}`,
          },
        ]}
      />
      <Text fw={700} c="dimmed" fz="0.75rem" tt="uppercase" mb="xs">
        {t("securityPage.limitsHeading")}
      </Text>
      <DetailFields
        testId="role-read-limits"
        fields={[
          {
            label: t("securityPage.rateReqPerSec"),
            value: r.rateLimit?.requestsPerSecond,
          },
          {
            label: t("securityPage.maxQueryComplexity"),
            value: r.rateLimit?.maxQueryComplexity,
          },
          { label: t("securityPage.maxQueryTimeMs"), value: r.rateLimit?.maxQueryTimeMs },
          {
            // REQ-2077: a grant of the role unused this long is removed.
            label: t("securityPage.inactivityDays"),
            value: r.inactivityDays ?? t("securityPage.inactivityNever"),
            testId: `role-inactivity-${r.id}`,
          },
        ]}
      />
    </div>
  );
}
