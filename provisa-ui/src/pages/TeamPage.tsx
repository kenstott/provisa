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
  Accordion,
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
import { OrgJoinSettings } from "../components/OrgJoinSettings";
import { useSearchParams } from "react-router-dom";
import { useLocalStorage } from "../components/graph/graph-persistence";
import { useRoles } from "../hooks/useAdminQueries";
import { useAuth } from "../context/AuthContext";
import { inviteUrl } from "../lib/authHost";

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
  // REQ-1287/REQ-1310: addressing the invitation is what makes it a message rather than a link the
  // org_admin has to carry themselves. Empty means a shareable link, which is still a valid choice.
  const [inviteEmail, setInviteEmail] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [copiedToken, setCopiedToken] = useState<string | null>(null);
  // REQ-1300: deletion is unrecoverable, so it is gated behind an explicit modal in which the
  // org_admin retypes the org id. Nothing about it is reachable by a single mis-click.
  const [deleteOpen, setDeleteOpen] = useState(false);
  const [confirmText, setConfirmText] = useState("");
  const [exported, setExported] = useState(false);
  // Which sections are open, kept across visits. Branding is absent from the default because it is
  // set once and then rarely touched, unlike the member and invite lists.
  const [openSections, setOpenSections] = useLocalStorage<string[]>("provisa.team.sections", [
    "members",
    "invites",
    "danger",
  ]);

  // ?section=<value> opens that accordion item — how another page links straight to a section a
  // returning visitor has collapsed (the org-address modal links to branding this way).
  const [searchParams] = useSearchParams();
  const requestedSection = searchParams.get("section");
  useEffect(() => {
    if (!requestedSection) return;
    setOpenSections((open) =>
      open.includes(requestedSection) ? open : [...open, requestedSection],
    );
  }, [requestedSection, setOpenSections]);

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

  const handleCreate = async () => {
    if (!activeOrgId || !roleId) return;
    setError(null);
    const email = inviteEmail.trim();
    try {
      const invite = await createInvite(activeOrgId, roleId, 7, email === "" ? undefined : email);
      setInvites(await fetchInvites());
      const url = inviteUrl(activeOrgId, invite.token);
      await navigator.clipboard.writeText(url);
      setInviteEmail("");
      // REQ-1310: the server reports what became of the message, and a failed send is the moment
      // the org_admin has to know they must pass the link on themselves — so it is said here
      // rather than logged.
      if (invite.delivery === "sent") {
        notifications.show({
          color: "green",
          message: t("teamPage.inviteSent", { email, url }),
        });
      } else if (invite.delivery !== undefined && invite.delivery.startsWith("failed")) {
        setError(t("teamPage.inviteNotSent", { email, reason: invite.delivery.slice(8), url }));
      } else if (invite.delivery === "saas_only" && email !== "") {
        setError(t("teamPage.inviteNoMail", { email, url }));
      } else {
        notifications.show({ color: "green", message: t("teamPage.inviteCreated", { url }) });
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  const handleCopy = async (orgId: string, token: string) => {
    await navigator.clipboard.writeText(inviteUrl(orgId, token));
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

      <Accordion
        multiple
        variant="separated"
        value={openSections}
        onChange={setOpenSections}
        data-testid="team-sections"
      >
        <Accordion.Item value="members">
          <Accordion.Control data-testid="team-section-members">
            <Title order={4}>{t("teamPage.membersHeading")}</Title>
          </Accordion.Control>
          <Accordion.Panel>
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
          </Accordion.Panel>
        </Accordion.Item>

        <Accordion.Item value="invites">
          <Accordion.Control data-testid="team-section-invites">
            <Title order={4}>{t("teamPage.inviteHeading")}</Title>
          </Accordion.Control>
          <Accordion.Panel>
            <Stack gap="sm" maw={480}>
              <Text c="dimmed" size="sm">
                {t("teamPage.inviteHelp")}
              </Text>
              <TextInput
                label={t("teamPage.emailLabel")}
                description={t("teamPage.emailDesc")}
                placeholder="person@acme.com"
                type="email"
                value={inviteEmail}
                onChange={(e) => setInviteEmail(e.currentTarget.value)}
                data-testid="team-invite-email"
              />
              {/* The button sits beside the role, bottom-aligned with its input: picking the role
                  is the last thing done before creating the link, and it is the field the button
                  is disabled on. */}
              <Group align="flex-end" gap="sm" wrap="nowrap">
                <Select
                  label={t("teamPage.roleLabel")}
                  description={t("teamPage.roleDesc")}
                  placeholder={t("teamPage.rolePlaceholder")}
                  data={roleOptions}
                  value={roleId}
                  onChange={setRoleId}
                  data-testid="team-invite-role"
                  style={{ flex: 1 }}
                />
                <Button
                  onClick={handleCreate}
                  disabled={!activeOrgId || !roleId}
                  data-testid="team-invite-create"
                >
                  {inviteEmail.trim() === ""
                    ? t("teamPage.generateInvite")
                    : t("teamPage.sendInvite")}
                </Button>
              </Group>
            </Stack>

            <Table.ScrollContainer minWidth={640} mt="md">
              <Table striped highlightOnHover withTableBorder verticalSpacing="xs">
                <Table.Thead>
                  <Table.Tr>
                    <Table.Th>{t("teamPage.colToken")}</Table.Th>
                    <Table.Th>{t("teamPage.colEmail")}</Table.Th>
                    <Table.Th>{t("teamPage.colRole")}</Table.Th>
                    <Table.Th>{t("teamPage.colExpires")}</Table.Th>
                    <Table.Th>{t("teamPage.colStatus")}</Table.Th>
                    <Table.Th>{t("teamPage.colActions")}</Table.Th>
                  </Table.Tr>
                </Table.Thead>
                <Table.Tbody>
                  {invites.length === 0 && (
                    <Table.Tr>
                      <Table.Td colSpan={6} ta="center" c="dimmed">
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
                      <Table.Td>{inv.email ?? "—"}</Table.Td>
                      <Table.Td>{inv.role_id ?? "—"}</Table.Td>
                      <Table.Td>{new Date(inv.expires_at).toLocaleDateString()}</Table.Td>
                      <Table.Td>
                        {inv.used_at
                          ? t("teamPage.usedStatus", {
                              date: new Date(inv.used_at).toLocaleDateString(),
                            })
                          : t("teamPage.activeStatus")}
                      </Table.Td>
                      <Table.Td>
                        <Group gap="xs">
                          {!inv.used_at && (
                            <Button
                              size="compact-xs"
                              variant="default"
                              onClick={() => handleCopy(inv.org_id, inv.token)}
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
          </Accordion.Panel>
        </Accordion.Item>

        {/* REQ-1569: the rule that decides who joins without an invitation, read and edited by the
            org_admin who owns it. */}
        {activeOrgId && (
          <Accordion.Item value="joining">
            <Accordion.Control data-testid="team-section-joining">
              <Title order={4}>{t("orgJoin.heading")}</Title>
            </Accordion.Control>
            <Accordion.Panel>
              <OrgJoinSettings orgId={activeOrgId} onError={reportError} />
            </Accordion.Panel>
          </Accordion.Item>
        )}

        {/* REQ-1486: the org's own presentation, edited by the same org_admin who runs the team. */}
        {activeOrgId && (
          <Accordion.Item value="branding">
            <Accordion.Control data-testid="team-section-branding">
              <Title order={4}>{t("orgBranding.heading")}</Title>
            </Accordion.Control>
            <Accordion.Panel>
              <OrgBrandingSettings orgId={activeOrgId} onError={reportError} />
            </Accordion.Panel>
          </Accordion.Item>
        )}

        {multitenancy && (
          <Accordion.Item value="danger">
            <Accordion.Control data-testid="team-section-danger">
              <Title order={4}>{t("teamPage.dangerHeading")}</Title>
            </Accordion.Control>
            <Accordion.Panel>
              <Stack gap="sm" maw={520}>
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
            </Accordion.Panel>
          </Accordion.Item>
        )}
      </Accordion>

      {multitenancy && (
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
      )}
    </Stack>
  );
}
