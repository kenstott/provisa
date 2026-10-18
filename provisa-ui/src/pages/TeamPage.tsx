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
  NumberInput,
  Select,
  Stack,
  Switch,
  Table,
  Text,
  TextInput,
  Title,
} from "@mantine/core";
import { notifications } from "@mantine/notifications";
import {
  ENV_POLICY_NONE,
  ENV_POLICY_PER_VISITOR,
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
import { FormChecks, FormField, FormFooter, FormLayout } from "../components/form/FormLayout";
import { OrgJoinSettings } from "../components/OrgJoinSettings";
import { useSearchParams } from "react-router-dom";
import { CollapsibleItem, Collapsibles } from "../components/collapsible/Collapsible";
import { useRoles } from "../hooks/useAdminQueries";
import { useAuth } from "../context/AuthContext";
import { inviteUrl } from "../lib/authHost";
import { ListTable, ListHead, ListRow, ListEmpty, ListItems } from "../components/list/ListTable";
import { useListSortGroup, type ListColumn } from "../components/list/useListSortGroup";
import { Tip } from "../components/Tip";
import { SplitPane } from "../components/list/SplitPane";
import { MemberAttributes } from "../components/admin/MemberAttributes";
import { SuspendedAccess } from "../components/admin/SuspendedAccess";

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
  // The member whose attributes the panel beside the list shows.
  const [selectedMemberId, setSelectedMemberId] = useState<string | null>(null);
  const [roleId, setRoleId] = useState<string | null>(null);
  // REQ-1287/REQ-1310: addressing the invitation is what makes it a message rather than a link the
  // org_admin has to carry themselves. Empty means a shareable link, which is still a valid choice.
  const [inviteEmail, setInviteEmail] = useState("");
  // REQ-1594: an open link admits everyone who holds it instead of the one person it was minted
  // for. Off is the single-use invitation, which is what an invitation meant before this existed.
  const [openLink, setOpenLink] = useState(false);
  // REQ-1595/REQ-1600: what the redeemer is given to work in. `per_visitor` deploys a private
  // environment per redeemer and reaps it once it has gone `envTtl` seconds without a request --
  // which is how an org_admin hands out a look at their real model without handing over the org.
  const [envPolicy, setEnvPolicy] = useState(ENV_POLICY_NONE);
  const [envTtl, setEnvTtl] = useState("3600");
  // REQ-1696: how long the link stays redeemable is the org_admin's call, made per invitation. A
  // fixed week was cutting off links that were meant to outlive it.
  const [expiresInDays, setExpiresInDays] = useState<number | string>(7);
  const [error, setError] = useState<string | null>(null);
  const [copiedToken, setCopiedToken] = useState<string | null>(null);
  // REQ-1300: deletion is unrecoverable, so it is gated behind an explicit modal in which the
  // org_admin retypes the org id. Nothing about it is reachable by a single mis-click.
  const [deleteOpen, setDeleteOpen] = useState(false);
  const [confirmText, setConfirmText] = useState("");
  const [exported, setExported] = useState(false);

  // ?section=<value> opens that section — how another page links straight to a section a
  // returning visitor has collapsed (the org-address modal links to branding this way).
  const [searchParams] = useSearchParams();
  const requestedSection = searchParams.get("section");

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

  // REQ-1594: an invitation is spent when it has no redemptions left, which "used_at is set" stopped
  // describing once a link could admit more than one person. A null ceiling never arrives there.
  const isSpent = (inv: OrgInvite) => inv.max_uses !== null && inv.uses >= inv.max_uses;

  const ttlOptions = [
    { value: "900", label: t("teamPage.envTtl15m") },
    { value: "3600", label: t("teamPage.envTtl1h") },
    { value: "28800", label: t("teamPage.envTtl8h") },
    { value: "86400", label: t("teamPage.envTtl24h") },
  ];

  // What each invitation hands its redeemer, said in the row rather than left to the token.
  const envSummary = (inv: OrgInvite) => {
    if (inv.env_policy === ENV_POLICY_PER_VISITOR) {
      const ttl = ttlOptions.find((o) => o.value === String(inv.env_ttl_seconds));
      return t("teamPage.envPerVisitorBadge", {
        ttl: ttl ? ttl.label : `${inv.env_ttl_seconds}s`,
      });
    }
    if (inv.env_name !== null) return t("teamPage.envSharedBadge", { name: inv.env_name });
    return "—";
  };

  // REQ-1940: sort and group are the shared list mechanism.
  const inviteStatus = (inv: OrgInvite) =>
    isSpent(inv)
      ? t("teamPage.spentStatus")
      : inv.used_at
        ? t("teamPage.usedStatus", { date: new Date(inv.used_at).toLocaleDateString() })
        : t("teamPage.activeStatus");
  const memberColumns: ListColumn<OrgMember>[] = [
    {
      key: "person",
      label: t("teamPage.colPerson"),
      sortValue: (m) => m.display_name ?? m.email ?? m.user_id,
    },
    {
      key: "provider",
      label: t("teamPage.colProvider"),
      sortValue: (m) => m.provider ?? "—",
      groupValue: (m) => m.provider ?? "—",
    },
    {
      key: "orgAdmin",
      label: t("teamPage.colOrgAdmin"),
      sortValue: (m) => (m.is_org_admin ? t("teamPage.yes") : t("teamPage.no")),
      groupValue: (m) => (m.is_org_admin ? t("teamPage.yes") : t("teamPage.no")),
    },
  ];
  const inviteColumns: ListColumn<OrgInvite>[] = [
    { key: "token", label: t("teamPage.colToken"), sortValue: (i) => i.token },
    { key: "email", label: t("teamPage.colEmail"), sortValue: (i) => i.email ?? "—" },
    {
      key: "role",
      label: t("teamPage.colRole"),
      sortValue: (i) => i.role_id ?? "—",
      groupValue: (i) => i.role_id ?? "—",
    },
    {
      key: "expires",
      label: t("teamPage.colExpires"),
      sortValue: (i) => new Date(i.expires_at).getTime(),
    },
    { key: "uses", label: t("teamPage.colUses"), sortValue: (i) => i.uses },
    {
      key: "environment",
      label: t("teamPage.colEnvironment"),
      sortValue: (i) => envSummary(i),
      groupValue: (i) => envSummary(i),
    },
    {
      key: "status",
      label: t("teamPage.colStatus"),
      sortValue: (i) => inviteStatus(i),
      groupValue: (i) => inviteStatus(i),
    },
  ];
  const memberSortGroup = useListSortGroup(members, memberColumns, "team-members");
  const inviteSortGroup = useListSortGroup(invites, inviteColumns, "team-invites");

  const handleCreate = async () => {
    if (!activeOrgId || !roleId) return;
    setError(null);
    const email = inviteEmail.trim();
    try {
      const invite = await createInvite(activeOrgId, {
        roleId,
        expiresInDays: Number(expiresInDays),
        email: email === "" ? undefined : email,
        // REQ-1594: null is unlimited, and unlimited is the only ceiling an open link can carry.
        maxUses: openLink ? null : 1,
        envPolicy,
        // REQ-1595: the TTL is what `per_visitor` is missing without, and meaningless with any
        // other policy -- the server rejects the pairing either way round, so it is sent to match.
        envTtlSeconds: envPolicy === ENV_POLICY_PER_VISITOR ? Number(envTtl) : null,
      });
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
    <Stack gap="lg" p="md" data-testid="team-page">
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

      <Collapsibles id="team.sections" reveal={requestedSection} testId="team-sections">
        <CollapsibleItem
          value="members"
          controlTestId="team-section-members"
          title={t("teamPage.membersHeading")}
        >
          <SplitPane
            testId="team-members-split"
            storageKey="provisa.team.detailWidth"
            title={(() => {
              const m = members.find((m) => m.user_id === selectedMemberId);
              return m ? (m.display_name ?? m.email ?? m.user_id) : "";
            })()}
            onClose={() => setSelectedMemberId(null)}
            labels={{
              panel: t("teamPage.memberPanel"),
              resize: t("teamPage.resizeMemberPanel"),
              expand: t("teamPage.expandMemberPanel"),
              restore: t("teamPage.restoreMemberPanel"),
              close: t("teamPage.closeMemberPanel"),
              backToList: t("teamPage.backToMembers"),
            }}
            list={
              <ListTable minWidth={640} testId="team-members">
                <ListHead
                  sortGroup={memberSortGroup}
                  columns={[
                    { col: "person" },
                    { col: "provider" },
                    { col: "orgAdmin" },
                    t("teamPage.colActions"),
                  ]}
                />
                <Table.Tbody>
                  {members.length === 0 && (
                    <ListEmpty colSpan={4}>{t("teamPage.noMembers")}</ListEmpty>
                  )}
                  <ListItems
                    state={memberSortGroup}
                    colSpan={4}
                    rowKey={(m) => m.user_id}
                    render={(m) => {
                      const lastAdmin = m.is_org_admin && adminCount <= 1;
                      return (
                        <ListRow
                          testId={`team-member-${m.user_id}`}
                          selected={m.user_id === selectedMemberId}
                          onClick={() =>
                            setSelectedMemberId(m.user_id === selectedMemberId ? null : m.user_id)
                          }
                        >
                          <Table.Td>
                            <Text size="sm">{m.display_name ?? m.email ?? m.user_id}</Text>
                            {m.email && m.display_name && (
                              <Text size="xs" c="dimmed">
                                {m.email}
                              </Text>
                            )}
                          </Table.Td>
                          <Table.Td>{m.provider ?? "—"}</Table.Td>
                          <Table.Td>
                            {m.is_org_admin ? t("teamPage.yes") : t("teamPage.no")}
                          </Table.Td>
                          <Table.Td onClick={(e) => e.stopPropagation()}>
                            <Group gap="xs">
                              <Tip text={lastAdmin ? t("teamPage.lastAdminHint") : undefined}>
                                <Button
                                  size="compact-xs"
                                  variant="default"
                                  disabled={lastAdmin}
                                  onClick={() => handleToggleAdmin(m)}
                                  data-testid={`team-toggle-admin-${m.user_id}`}
                                >
                                  {m.is_org_admin ? t("teamPage.demote") : t("teamPage.promote")}
                                </Button>
                              </Tip>
                              <Tip
                                text={
                                  m.user_id === userId
                                    ? t("teamPage.selfRemoveHint")
                                    : lastAdmin
                                      ? t("teamPage.lastAdminHint")
                                      : undefined
                                }
                              >
                                <Button
                                  size="compact-xs"
                                  color="red"
                                  variant="light"
                                  disabled={lastAdmin || m.user_id === userId}
                                  onClick={() => handleRemoveMember(m)}
                                  data-testid={`team-remove-${m.user_id}`}
                                >
                                  {t("teamPage.removeButton")}
                                </Button>
                              </Tip>
                            </Group>
                          </Table.Td>
                        </ListRow>
                      );
                    }}
                  />
                </Table.Tbody>
              </ListTable>
            }
          >
            {selectedMemberId === null ? null : (
              <MemberAttributes key={selectedMemberId} userId={selectedMemberId} />
            )}
          </SplitPane>
        </CollapsibleItem>

        <CollapsibleItem
          value="invites"
          controlTestId="team-section-invites"
          title={t("teamPage.inviteHeading")}
        >
          <FormLayout data-testid="team-invite-form" style={{ marginBottom: "1rem" }}>
            <Text c="dimmed" size="sm">
              {t("teamPage.inviteHelp")}
            </Text>
            <FormField size="long" hint={t("teamPage.emailDesc")}>
              <TextInput
                label={t("teamPage.emailLabel")}
                placeholder="person@acme.com"
                type="email"
                value={inviteEmail}
                onChange={(e) => setInviteEmail(e.currentTarget.value)}
                data-testid="team-invite-email"
              />
            </FormField>
            <FormField size="medium" hint={t("teamPage.roleDesc")}>
              <Select
                label={t("teamPage.roleLabel")}
                placeholder={t("teamPage.rolePlaceholder")}
                data={roleOptions}
                value={roleId}
                onChange={setRoleId}
                data-testid="team-invite-role"
              />
            </FormField>
            {/* REQ-1594/REQ-1595: who the link admits, and what it gives them: both are answered
                  about the invitation as a whole, not per invitee. */}
            <FormField size="short" help={t("teamPage.expiresDesc")}>
              <NumberInput
                label={t("teamPage.expiresLabel")}
                min={1}
                max={36500}
                allowDecimal={false}
                value={expiresInDays}
                onChange={setExpiresInDays}
                data-testid="team-invite-expires-days"
              />
            </FormField>
            <FormField size="medium" help={t("teamPage.envPolicyDesc")}>
              <Select
                label={t("teamPage.envPolicyLabel")}
                data={[
                  { value: ENV_POLICY_NONE, label: t("teamPage.envPolicyNone") },
                  { value: ENV_POLICY_PER_VISITOR, label: t("teamPage.envPolicyPerVisitor") },
                ]}
                value={envPolicy}
                onChange={(v) => setEnvPolicy(v ?? ENV_POLICY_NONE)}
                allowDeselect={false}
                data-testid="team-invite-env-policy"
              />
            </FormField>
            {envPolicy === ENV_POLICY_PER_VISITOR && (
              <FormField size="short">
                <Select
                  label={t("teamPage.envTtlLabel")}
                  data={ttlOptions}
                  value={envTtl}
                  onChange={(v) => setEnvTtl(v ?? "3600")}
                  allowDeselect={false}
                  data-testid="team-invite-env-ttl"
                />
              </FormField>
            )}
            <FormChecks>
              <FormField size="long" help={t("teamPage.openLinkDesc")}>
                <Switch
                  label={t("teamPage.openLinkLabel")}
                  checked={openLink}
                  onChange={(e) => setOpenLink(e.currentTarget.checked)}
                  data-testid="team-invite-open-link"
                />
              </FormField>
            </FormChecks>
            {/* The form's one action: it creates the link (or sends it, with an address). */}
            <FormFooter
              action={
                <Button
                  onClick={handleCreate}
                  disabled={!activeOrgId || !roleId || !(Number(expiresInDays) >= 1)}
                  data-testid="team-invite-create"
                >
                  {inviteEmail.trim() === ""
                    ? t("teamPage.generateInvite")
                    : t("teamPage.sendInvite")}
                </Button>
              }
            />
          </FormLayout>

          {/* The table keeps every column; what its card leaves too little room for scrolls. */}
          <div className="card-table-scroll">
            <ListTable minWidth={640} testId="team-invites">
              <ListHead
                sortGroup={inviteSortGroup}
                columns={[
                  { col: "token" },
                  { col: "email" },
                  { col: "role" },
                  { col: "expires" },
                  { col: "uses" },
                  { col: "environment" },
                  { col: "status" },
                  t("teamPage.colActions"),
                ]}
              />
              <Table.Tbody>
                {invites.length === 0 && (
                  <ListEmpty colSpan={8}>{t("teamPage.noInvites")}</ListEmpty>
                )}
                <ListItems
                  state={inviteSortGroup}
                  colSpan={8}
                  rowKey={(inv) => inv.token}
                  render={(inv) => (
                    <ListRow>
                      <Table.Td>
                        <Text ff="monospace" span>
                          {inv.token.slice(0, 8)}…
                        </Text>
                      </Table.Td>
                      <Table.Td>{inv.email ?? "—"}</Table.Td>
                      <Table.Td>{inv.role_id ?? "—"}</Table.Td>
                      <Table.Td>{new Date(inv.expires_at).toLocaleDateString()}</Table.Td>
                      <Table.Td>
                        {inv.max_uses === null
                          ? t("teamPage.usesUnlimited", { used: inv.uses })
                          : t("teamPage.usesOf", { used: inv.uses, max: inv.max_uses })}
                      </Table.Td>
                      <Table.Td>{envSummary(inv)}</Table.Td>
                      <Table.Td>
                        {isSpent(inv)
                          ? t("teamPage.spentStatus")
                          : inv.used_at
                            ? t("teamPage.usedStatus", {
                                date: new Date(inv.used_at).toLocaleDateString(),
                              })
                            : t("teamPage.activeStatus")}
                      </Table.Td>
                      <Table.Td>
                        <Group gap="xs">
                          {!isSpent(inv) && (
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
                          {!isSpent(inv) && (
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
                    </ListRow>
                  )}
                />
              </Table.Tbody>
            </ListTable>
          </div>
        </CollapsibleItem>

        {/* REQ-2076: credentials suspended for naming a role that was not held, and the events
            that suspended them. Only an administrator lifts one. */}
        {activeOrgId && (
          <CollapsibleItem
            value="suspended"
            defaultOpen={false}
            controlTestId="team-section-suspended"
            title={t("suspendedAccess.heading")}
          >
            <SuspendedAccess orgId={activeOrgId} onError={reportError} />
          </CollapsibleItem>
        )}

        {/* REQ-1569: the rule that decides who joins without an invitation, read and edited by the
            org_admin who owns it. */}
        {activeOrgId && (
          <CollapsibleItem
            value="joining"
            defaultOpen={false}
            controlTestId="team-section-joining"
            title={t("orgJoin.heading")}
          >
            <OrgJoinSettings orgId={activeOrgId} onError={reportError} />
          </CollapsibleItem>
        )}

        {/* REQ-1486: the org's own presentation, edited by the same org_admin who runs the team. */}
        {activeOrgId && (
          <CollapsibleItem
            value="branding"
            defaultOpen={false}
            controlTestId="team-section-branding"
            title={t("orgBranding.heading")}
          >
            <OrgBrandingSettings orgId={activeOrgId} onError={reportError} />
          </CollapsibleItem>
        )}

        {multitenancy && (
          <CollapsibleItem
            value="danger"
            controlTestId="team-section-danger"
            title={t("teamPage.dangerHeading")}
          >
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
          </CollapsibleItem>
        )}
      </Collapsibles>

      {multitenancy && (
        <Modal
          opened={deleteOpen}
          onClose={() => setDeleteOpen(false)}
          title={t("teamPage.deleteModalTitle")}
          transitionProps={{ duration: 0 }}
        >
          <FormLayout data-testid="team-delete-modal">
            <Alert color="red">{t("teamPage.deleteWarning", { org: activeOrgId })}</Alert>
            <FormField size="long">
              <TextInput
                label={t("teamPage.confirmLabel", { org: activeOrgId })}
                value={confirmText}
                onChange={(e) => setConfirmText(e.currentTarget.value)}
                data-testid="team-delete-confirm"
              />
            </FormField>
            {/* Delete takes Save's place, then the shared Cancel; the download is the row's other
                action. */}
            <FormFooter
              onCancel={() => setDeleteOpen(false)}
              action={
                <Button
                  color="red"
                  disabled={confirmText !== activeOrgId}
                  onClick={handleDeleteOrg}
                  data-testid="team-delete-submit"
                >
                  {t("teamPage.deleteConfirmButton")}
                </Button>
              }
            >
              <Button variant="default" onClick={handleExport} data-testid="team-export-config">
                {exported ? t("teamPage.downloadedButton") : t("teamPage.downloadButton")}
              </Button>
            </FormFooter>
          </FormLayout>
        </Modal>
      )}
    </Stack>
  );
}
