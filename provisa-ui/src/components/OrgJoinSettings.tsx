// Copyright (c) 2026 Kenneth Stott
// Canary: 6b0c7f31-4a0e-4f2a-9a7f-2f2e1c9a4d55
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1569: the auto-join rule, after the org exists.
//
// The rule is written once at onboarding and then decides, silently and forever, who walks into
// the org without an invitation. The org_admin who owns that has to be able to read it back and
// change it — a company changes domains, acquires another, or discovers the rule admits more than
// it meant to. Every refusal here (REQ-1268 compilability, REQ-1477 public domains, REQ-1567
// breadth, REQ-1567 duplicate claim) comes from the server; this form carries the answer to the
// breadth question back on the second attempt, exactly as the onboarding form does.

import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { Alert, Button, Checkbox, Group, Stack, Text, TextInput } from "@mantine/core";
import { notifications } from "@mantine/notifications";
import { OrgError, fetchOrgSettings, updateOrgSettings } from "../api/admin";

export function OrgJoinSettings({
  orgId,
  onError,
}: {
  orgId: string;
  onError: (e: unknown) => void;
}) {
  const { t } = useTranslation();
  const [emailRule, setEmailRule] = useState("");
  const [autoJoin, setAutoJoin] = useState(false);
  const [autoJoinRole, setAutoJoinRole] = useState("");
  const [breadth, setBreadth] = useState<string | null>(null);
  const [accepted, setAccepted] = useState(false);
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    fetchOrgSettings(orgId)
      .then((s) => {
        setEmailRule(s.email_rule ?? "");
        setAutoJoin(s.auto_join);
        setAutoJoinRole(s.auto_join_role ?? "");
      })
      .catch(onError);
    // eslint-disable-next-line react-hooks/exhaustive-deps -- onError is re-created each render, so depending on it refetches in a loop; the read is keyed on the org alone
  }, [orgId]);

  // A different rule admits a different set of people, so an acceptance of the old one is void.
  const editRule = (value: string) => {
    setEmailRule(value);
    setBreadth(null);
    setAccepted(false);
  };

  const save = async () => {
    setSaving(true);
    try {
      await updateOrgSettings(orgId, {
        emailRule: emailRule.trim() === "" ? null : emailRule.trim(),
        autoJoin,
        autoJoinRole: autoJoinRole.trim() === "" ? null : autoJoinRole.trim(),
        riskAcknowledged: accepted,
      });
      setBreadth(null);
      notifications.show({ color: "green", message: t("orgJoin.saved") });
    } catch (e) {
      if (e instanceof OrgError && e.code === "orgs.auto_join_breadth_unacknowledged") {
        setBreadth(e.message);
      } else {
        onError(e);
      }
    } finally {
      setSaving(false);
    }
  };

  return (
    <Stack gap="sm" maw={480} data-testid="org-join-settings">
      <Text c="dimmed" size="sm">
        {t("orgJoin.help")}
      </Text>
      <TextInput
        label={t("orgJoin.emailRuleLabel")}
        description={t("orgJoin.emailRuleDesc")}
        placeholder="@acme\\.com$"
        value={emailRule}
        onChange={(e) => editRule(e.currentTarget.value)}
        data-testid="org-join-email-rule"
      />
      <Checkbox
        label={t("orgJoin.autoJoinLabel")}
        description={t("orgJoin.autoJoinDesc")}
        checked={autoJoin}
        onChange={(e) => {
          setAutoJoin(e.currentTarget.checked);
          setBreadth(null);
          setAccepted(false);
        }}
        data-testid="org-join-auto-join"
      />
      {autoJoin && (
        <TextInput
          label={t("orgJoin.autoJoinRoleLabel")}
          description={t("orgJoin.autoJoinRoleDesc")}
          value={autoJoinRole}
          onChange={(e) => setAutoJoinRole(e.currentTarget.value)}
          data-testid="org-join-auto-join-role"
        />
      )}
      {breadth && (
        <Alert color="yellow" data-testid="org-join-breadth-warning">
          <Stack gap="xs">
            <Text size="sm">{breadth}</Text>
            <Checkbox
              label={t("orgJoin.acceptRisk")}
              checked={accepted}
              onChange={(e) => setAccepted(e.currentTarget.checked)}
              data-testid="org-join-accept-risk"
            />
          </Stack>
        </Alert>
      )}
      <Group>
        <Button onClick={save} loading={saving} data-testid="org-join-save">
          {t("orgJoin.save")}
        </Button>
      </Group>
    </Stack>
  );
}
