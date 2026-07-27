// Copyright (c) 2026 Kenneth Stott
// Canary: 7c1e2b95-4d38-4f0a-8e21-9a6b3f0c1d47
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  Alert,
  Button,
  Group,
  Select,
  Stack,
  Table,
  Text,
  Title,
} from "@mantine/core";
import { notifications } from "@mantine/notifications";
import { createInvite, fetchInvites, revokeInvite } from "../api/admin";
import type { OrgInvite } from "../api/admin";
import { useRoles } from "../hooks/useAdminQueries";
import { useAuth } from "../context/AuthContext";

// REQ-1266: org_admin self-service team management. The org_admin invites people into their
// active org and picks the role each invitee is granted on redemption (the invite carries
// role_id → grant_org_role in the org's tenant plane). Invite create/list/revoke are scoped to
// the caller's active org server-side (_require_org_admin / _administered_org_scope), so this page
// only ever surfaces the current org's invites. Roles are shaped at Security → Roles.
export function TeamPage() {
  const { t } = useTranslation();
  const { activeOrgId } = useAuth();
  const { roles } = useRoles();
  const [invites, setInvites] = useState<OrgInvite[]>([]);
  const [roleId, setRoleId] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [copiedToken, setCopiedToken] = useState<string | null>(null);

  useEffect(() => {
    fetchInvites()
      .then(setInvites)
      .catch((e) => setError(e instanceof Error ? e.message : String(e)));
  }, []);

  const roleOptions = roles.map((r) => ({ value: r.id, label: r.id }));

  const inviteUrl = (token: string) => `${window.location.origin}/register?invite=${token}`;

  const handleCreate = async () => {
    if (!activeOrgId || !roleId) return;
    setError(null);
    try {
      const invite = await createInvite(activeOrgId, roleId);
      setInvites(await fetchInvites());
      const url = inviteUrl(invite.token);
      await navigator.clipboard.writeText(url);
      notifications.show({ color: "green", message: t("teamPage.inviteCreated", { url }) });
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  const handleCopy = async (token: string) => {
    await navigator.clipboard.writeText(inviteUrl(token));
    setCopiedToken(token);
    setTimeout(() => setCopiedToken(null), 2000);
  };

  const handleRevoke = async (token: string) => {
    await revokeInvite(token);
    setInvites((prev) => prev.filter((i) => i.token !== token));
  };

  return (
    <Stack gap="lg" p="md" maw={860} data-testid="team-page">
      <div>
        <Title order={2}>{t("teamPage.title")}</Title>
        <Text c="dimmed" size="sm">
          {t("teamPage.subtitle")}
        </Text>
      </div>

      {error && (
        <Alert color="red" data-testid="team-error">
          {error}
        </Alert>
      )}

      <Stack gap="sm" maw={480}>
        <Title order={4}>{t("teamPage.inviteHeading")}</Title>
        <Text c="dimmed" size="sm">
          {t("teamPage.inviteHelp")}
        </Text>
        <Select
          label={t("teamPage.roleLabel")}
          description={t("teamPage.roleDesc")}
          placeholder={t("teamPage.rolePlaceholder")}
          data={roleOptions}
          value={roleId}
          onChange={setRoleId}
          data-testid="team-invite-role"
        />
        <Button
          onClick={handleCreate}
          disabled={!activeOrgId || !roleId}
          style={{ alignSelf: "flex-start" }}
          data-testid="team-invite-create"
        >
          {t("teamPage.generateInvite")}
        </Button>
      </Stack>

      <Table.ScrollContainer minWidth={640}>
        <Table striped highlightOnHover withTableBorder verticalSpacing="xs">
          <Table.Thead>
            <Table.Tr>
              <Table.Th>{t("teamPage.colToken")}</Table.Th>
              <Table.Th>{t("teamPage.colRole")}</Table.Th>
              <Table.Th>{t("teamPage.colExpires")}</Table.Th>
              <Table.Th>{t("teamPage.colStatus")}</Table.Th>
              <Table.Th>{t("teamPage.colActions")}</Table.Th>
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {invites.length === 0 && (
              <Table.Tr>
                <Table.Td colSpan={5} ta="center" c="dimmed">
                  {t("teamPage.noInvites")}
                </Table.Td>
              </Table.Tr>
            )}
            {invites.map((inv) => (
              <Table.Tr key={inv.token}>
                <Table.Td>
                  <Text ff="monospace" span>
                    {inv.token.slice(0, 8)}…
                  </Text>
                </Table.Td>
                <Table.Td>{inv.role_id ?? "—"}</Table.Td>
                <Table.Td>{new Date(inv.expires_at).toLocaleDateString()}</Table.Td>
                <Table.Td>
                  {inv.used_at
                    ? t("teamPage.usedStatus", { date: new Date(inv.used_at).toLocaleDateString() })
                    : t("teamPage.activeStatus")}
                </Table.Td>
                <Table.Td>
                  <Group gap="xs">
                    {!inv.used_at && (
                      <Button size="compact-xs" variant="default" onClick={() => handleCopy(inv.token)}>
                        {copiedToken === inv.token ? t("teamPage.copiedButton") : t("teamPage.copyButton")}
                      </Button>
                    )}
                    {!inv.used_at && (
                      <Button
                        size="compact-xs"
                        color="red"
                        variant="light"
                        onClick={() => handleRevoke(inv.token)}
                      >
                        {t("teamPage.revokeButton")}
                      </Button>
                    )}
                  </Group>
                </Table.Td>
              </Table.Tr>
            ))}
          </Table.Tbody>
        </Table>
      </Table.ScrollContainer>
    </Stack>
  );
}
