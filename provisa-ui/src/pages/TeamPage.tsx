// Copyright (c) 2026 Kenneth Stott
// Canary: 7c1e2b95-4d38-4f0a-8e21-9a6b3f0c1d47
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useCallback, useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  Alert,
  Button,
  Group,
  Modal,
  Select,
  Stack,
  Table,
  Text,
  TextInput,
  Title,
} from "@mantine/core";
import { notifications } from "@mantine/notifications";
import {
  createInvite,
  deleteOrg,
  exportOrgConfig,
  fetchInvites,
  fetchOrgMembers,
  grantOrgAdmin,
  removeOrgMember,
  revokeInvite,
  revokeOrgAdmin,
} from "../api/admin";
import type { OrgInvite, OrgMember } from "../api/admin";
import { OrgBrandingSettings } from "../components/OrgBrandingSettings";
import { useRoles } from "../hooks/useAdminQueries";
import { useAuth } from "../context/AuthContext";

// REQ-1266: org_admin self-service team management. The org_admin invites people into their
// active org and picks the role each invitee is granted on redemption (the invite carries
// role_id → grant_org_role in the org's tenant plane). Invite create/list/revoke are scoped to
// the caller's active org server-side (_require_org_admin / _administered_org_scope), so this page
// only ever surfaces the current org's invites. Roles are shaped at Security → Roles.
export function TeamPage() {
  const { t } = useTranslation();
  const { activeOrgId, userId, multitenancy } = useAuth();
  const { roles } = useRoles();
  const [invites, setInvites] = useState<OrgInvite[]>([]);
  const [members, setMembers] = useState<OrgMember[]>([]);
  const [roleId, setRoleId] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [copiedToken, setCopiedToken] = useState<string | null>(null);
  // REQ-1300: deletion is unrecoverable, so it is gated behind an explicit modal in which the
  // org_admin retypes the org id. Nothing about it is reachable by a single mis-click.
  const [deleteOpen, setDeleteOpen] = useState(false);
  const [confirmText, setConfirmText] = useState("");
  const [exported, setExported] = useState(false);

  const reportError = (e: unknown) => setError(e instanceof Error ? e.message : String(e));

  const loadMembers = useCallback(async () => {
    if (!activeOrgId) return;
    await fetchOrgMembers(activeOrgId).then(setMembers).catch(reportError);
  }, [activeOrgId]);

  useEffect(() => {
    fetchInvites().then(setInvites).catch(reportError);
  }, []);

  useEffect(() => {
    if (!activeOrgId) return;
    fetchOrgMembers(activeOrgId).then(setMembers).catch(reportError);
  }, [activeOrgId]);

  // REQ-1302: the server refuses the removal or demotion that would leave the org with no admin.
  // The page mirrors that rule so the last admin sees a disabled control instead of an error.
  const adminCount = members.filter((m) => m.is_org_admin).length;

  const handleRemoveMember = async (member: OrgMember) => {
    if (!activeOrgId) return;
    setError(null);
    try {
      await removeOrgMember(activeOrgId, member.user_id);
      await loadMembers();
    } catch (e) {
      reportError(e);
    }
  };

  const handleToggleAdmin = async (member: OrgMember) => {
    if (!activeOrgId) return;
    setError(null);
    try {
      if (member.is_org_admin) await revokeOrgAdmin(activeOrgId, member.user_id);
      else await grantOrgAdmin(activeOrgId, member.user_id);
      await loadMembers();
    } catch (e) {
      reportError(e);
    }
  };

  // REQ-1304: the download is offered inside the deletion flow, because taking the configuration
  // with you is only useful before the org that holds it is gone.
  const handleExport = async () => {
    if (!activeOrgId) return;
    setError(null);
    try {
      const yaml = await exportOrgConfig(activeOrgId);
      const blob = new Blob([yaml], { type: "application/x-yaml" });
      const url = URL.createObjectURL(blob);
      const a = document.createElement("a");
      a.href = url;
      a.download = `${activeOrgId}-config.yaml`;
      a.click();
      URL.revokeObjectURL(url);
      setExported(true);
    } catch (e) {
      reportError(e);
    }
  };

  const handleDeleteOrg = async () => {
    if (!activeOrgId) return;
    setError(null);
    try {
      await deleteOrg(activeOrgId, confirmText);
      // The org the session was pointed at no longer exists, so a reload is the only coherent next
      // state: it re-runs the identity bootstrap, which routes to onboarding or a remaining org.
      window.location.assign("/");
    } catch (e) {
      reportError(e);
    }
  };

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

      <Stack gap="sm">
        <Title order={4}>{t("teamPage.membersHeading")}</Title>
        <Table.ScrollContainer minWidth={640}>
          <Table
            striped
            highlightOnHover
            withTableBorder
            verticalSpacing="xs"
            data-testid="team-members"
          >
            <Table.Thead>
              <Table.Tr>
                <Table.Th>{t("teamPage.colPerson")}</Table.Th>
                <Table.Th>{t("teamPage.colProvider")}</Table.Th>
                <Table.Th>{t("teamPage.colOrgAdmin")}</Table.Th>
                <Table.Th>{t("teamPage.colActions")}</Table.Th>
              </Table.Tr>
            </Table.Thead>
            <Table.Tbody>
              {members.length === 0 && (
                <Table.Tr>
                  <Table.Td colSpan={4} ta="center" c="dimmed">
                    {t("teamPage.noMembers")}
                  </Table.Td>
                </Table.Tr>
              )}
              {members.map((m) => {
                const lastAdmin = m.is_org_admin && adminCount <= 1;
                return (
                  <Table.Tr key={m.user_id} data-testid={`team-member-${m.user_id}`}>
                    <Table.Td>
                      <Text size="sm">{m.display_name ?? m.email ?? m.user_id}</Text>
                      {m.email && m.display_name && (
                        <Text size="xs" c="dimmed">
                          {m.email}
                        </Text>
                      )}
                    </Table.Td>
                    <Table.Td>{m.provider ?? "—"}</Table.Td>
                    <Table.Td>{m.is_org_admin ? t("teamPage.yes") : t("teamPage.no")}</Table.Td>
                    <Table.Td>
                      <Group gap="xs">
                        <Button
                          size="compact-xs"
                          variant="default"
                          disabled={lastAdmin}
                          title={lastAdmin ? t("teamPage.lastAdminHint") : undefined}
                          onClick={() => handleToggleAdmin(m)}
                          data-testid={`team-toggle-admin-${m.user_id}`}
                        >
                          {m.is_org_admin ? t("teamPage.demote") : t("teamPage.promote")}
                        </Button>
                        <Button
                          size="compact-xs"
                          color="red"
                          variant="light"
                          disabled={lastAdmin || m.user_id === userId}
                          title={
                            m.user_id === userId
                              ? t("teamPage.selfRemoveHint")
                              : lastAdmin
                                ? t("teamPage.lastAdminHint")
                                : undefined
                          }
                          onClick={() => handleRemoveMember(m)}
                          data-testid={`team-remove-${m.user_id}`}
                        >
                          {t("teamPage.removeButton")}
                        </Button>
                      </Group>
                    </Table.Td>
                  </Table.Tr>
                );
              })}
            </Table.Tbody>
          </Table>
        </Table.ScrollContainer>
      </Stack>

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
                      <Button
                        size="compact-xs"
                        variant="default"
                        onClick={() => handleCopy(inv.token)}
                      >
                        {copiedToken === inv.token
                          ? t("teamPage.copiedButton")
                          : t("teamPage.copyButton")}
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

      {/* REQ-1486: the org's own presentation, edited by the same org_admin who runs the team. */}
      {activeOrgId && <OrgBrandingSettings orgId={activeOrgId} onError={reportError} />}

      {multitenancy && (
        <>
          <Stack gap="sm" maw={520}>
            <Title order={4}>{t("teamPage.dangerHeading")}</Title>
            <Text c="dimmed" size="sm">
              {t("teamPage.dangerHelp")}
            </Text>
            <Button
              color="red"
              variant="outline"
              style={{ alignSelf: "flex-start" }}
              disabled={!activeOrgId}
              onClick={() => {
                setConfirmText("");
                setExported(false);
                setDeleteOpen(true);
              }}
              data-testid="team-delete-org"
            >
              {t("teamPage.deleteOrgButton")}
            </Button>
          </Stack>

          <Modal
            opened={deleteOpen}
            onClose={() => setDeleteOpen(false)}
            title={t("teamPage.deleteModalTitle")}
            transitionProps={{ duration: 0 }}
          >
            <Stack gap="sm" data-testid="team-delete-modal">
              <Alert color="red">{t("teamPage.deleteWarning", { org: activeOrgId })}</Alert>
              <Button
                variant="default"
                onClick={handleExport}
                style={{ alignSelf: "flex-start" }}
                data-testid="team-export-config"
              >
                {exported ? t("teamPage.downloadedButton") : t("teamPage.downloadButton")}
              </Button>
              <TextInput
                label={t("teamPage.confirmLabel", { org: activeOrgId })}
                value={confirmText}
                onChange={(e) => setConfirmText(e.currentTarget.value)}
                data-testid="team-delete-confirm"
              />
              <Button
                color="red"
                disabled={confirmText !== activeOrgId}
                onClick={handleDeleteOrg}
                style={{ alignSelf: "flex-start" }}
                data-testid="team-delete-submit"
              >
                {t("teamPage.deleteConfirmButton")}
              </Button>
            </Stack>
          </Modal>
        </>
      )}
    </Stack>
  );
}
