// Copyright (c) 2026 Kenneth Stott
// Canary: f09d73b4-de5f-4d5a-a125-c26597327e3c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState, useEffect } from "react";
import { useTranslation } from "react-i18next";
import {
  ActionIcon,
  Badge,
  Button,
  Group,
  Pagination,
  Select,
  Stack,
  Table,
  Tabs,
  Text,
  TextInput,
} from "@mantine/core";
import { notifications } from "@mantine/notifications";
import { Building2, Pencil, Trash2, X } from "lucide-react";
import {
  fetchOrgs,
  createOrg,
  deleteOrg,
  fetchOrgMembers,
  addOrgMember,
  removeOrgMember,
  fetchInvites,
  createInvite,
  revokeInvite,
  fetchOrgRoles,
  ORG_TIERS,
  setOrgTier,
} from "../../api/admin";
import type { Org, OrgMember, OrgInvite, OrgTier } from "../../api/admin";
import { FilterInput } from "./FilterInput";
import { inviteUrl } from "../../lib/authHost";
import { PageTitle } from "../PageTitle";
import { Tip } from "../Tip";
import { DetailFields } from "../detail/DetailFields";
import { ListRow } from "../list/ListTable";
import { PanelDelete } from "../list/PanelDelete";
import { SortGroupTable } from "../list/SortGroupTable";
import { PanelActions, SplitPane } from "../list/SplitPane";
import { useRememberedDetail } from "../list/useDetailParam";
import { detailListOf, searchNarrowing, shownBy } from "../list/leftOut";
import { useUrlFilter, useUrlTab } from "../list/useUrlFilter";
import { FormField, FormFooter, FormIdentifiers, FormLayout } from "../form/FormLayout";
import { useRequiredFields } from "../form/useRequiredFields";
import { PageActions } from "../PageActions";

const PAGE_SIZE = 50;

const VIEWS = ["orgs", "invites"] as const;
type View = (typeof VIEWS)[number];

/** The form open in the panel: a new org, a new invite link, or the open org's tier. */
type Form = "org" | "invite" | "tier" | null;

/**
 * REQ-2277: the object management anatomy -- the header (title, filter, Add), the orgs and the
 * invite links as the page's two tabs, and a row opening its panel (?org=<id>, ?invite=<token>):
 * an org's tier, members and delete; an invite link's copy and revoke. Each Add opens its form in
 * the same panel.
 */
export function OrgsTab({ title }: { title: string }) {
  const { t } = useTranslation();
  const [orgs, setOrgs] = useState<Org[] | null>(null);
  const [orgInvites, setOrgInvites] = useState<OrgInvite[] | null>(null);
  const [view, setView] = useUrlTab<View>("view", VIEWS);
  const [search, setSearch] = useUrlFilter();
  // REQ-1594: an invitation is spent when it has no redemptions left. "used_at is set" stopped
  // describing that once a link could admit more than the one person it was minted for.
  const isSpent = (inv: OrgInvite) => inv.max_uses !== null && inv.uses >= inv.max_uses;

  const tierLabel = (tier: OrgTier | null) =>
    tier === null ? t("orgsTab.tierNotSet") : t(`orgsTab.tier_${tier}`);
  const statusOf = (inv: OrgInvite) =>
    isSpent(inv)
      ? t("orgsTab.spentStatus")
      : inv.used_at
        ? t("orgsTab.usedStatus", { date: new Date(inv.used_at).toLocaleDateString() })
        : t("orgsTab.activeStatus");
  // REQ-2277 rule 9: an org or an invite is open only while its tab's list, under the search,
  // shows its row; the other tab's list is not shown.
  const orgNarrowings = [
    searchNarrowing<Org>(
      t,
      search,
      () => setSearch(""),
      // Every text an organization's row shows.
      (o) => [o.id, o.name, tierLabel(o.tier)],
    ),
  ];
  const inviteNarrowings = [
    searchNarrowing<OrgInvite>(
      t,
      search,
      () => setSearch(""),
      // Every text an invite's row shows.
      (i) => [
        i.org_name,
        i.role_id,
        i.token,
        i.created_by,
        new Date(i.expires_at).toLocaleDateString(),
        statusOf(i),
      ],
    ),
  ];
  const orgDetail = useRememberedDetail(
    "orgs",
    detailListOf(
      t,
      view === "orgs" ? orgs : null,
      (o) => o.id,
      (o) => o.name,
      orgNarrowings,
    ),
  );
  const inviteDetail = useRememberedDetail(
    "orgInvites",
    detailListOf(
      t,
      view === "invites" ? orgInvites : null,
      (i) => i.token,
      (i) => i.org_name,
      inviteNarrowings,
    ),
  );
  const [form, setForm] = useState<Form>(null);
  const [saving, setSaving] = useState(false);
  const [problem, setProblem] = useState<string | null>(null);
  const [newOrgId, setNewOrgId] = useState("");
  const [newOrgName, setNewOrgName] = useState("");
  const [draftTier, setDraftTier] = useState<OrgTier | null>(null);
  const [orgMembers, setOrgMembers] = useState<Record<string, OrgMember[]>>({});
  const [addMemberUserId, setAddMemberUserId] = useState("");
  const [inviteOrgId, setInviteOrgId] = useState<string | null>(null);
  // The role the invitation confers. Defaulted to org_admin here because this tab's invitations
  // exist to give a new org an administrator — the server's own default is analyst (REQ-1314).
  const [inviteRoleId, setInviteRoleId] = useState<string>("org_admin");
  const [inviteRoles, setInviteRoles] = useState<string[]>([]);
  const [copiedToken, setCopiedToken] = useState<string | null>(null);
  const [orgPage, setOrgPage] = useState(1);
  const [invitePage, setInvitePage] = useState(1);
  const [orgsGrouped, setOrgsGrouped] = useState(false);
  const [invitesGrouped, setInvitesGrouped] = useState(false);
  const orgRequired = useRequiredFields({ id: newOrgId, name: newOrgName });
  const inviteRequired = useRequiredFields({ org: inviteOrgId });
  const memberRequired = useRequiredFields({ user: addMemberUserId });

  useEffect(() => {
    fetchOrgs()
      .then(setOrgs)
      .catch((e: unknown) => {
        setOrgs([]);
        notifications.show({ color: "red", message: (e as Error).message });
      });
    fetchInvites()
      .then(setOrgInvites)
      .catch((e: unknown) => {
        setOrgInvites([]);
        notifications.show({ color: "red", message: (e as Error).message });
      });
  }, []);

  // The open org's members, read when its panel opens.
  const openOrgId = orgDetail.id;
  useEffect(() => {
    if (openOrgId === null || orgMembers[openOrgId] !== undefined) return;
    fetchOrgMembers(openOrgId)
      .then((members) => setOrgMembers((prev) => ({ ...prev, [openOrgId]: members })))
      .catch((e: unknown) => notifications.show({ color: "red", message: (e as Error).message }));
  }, [openOrgId, orgMembers]);

  const closeForm = () => {
    setForm(null);
    setProblem(null);
  };
  const openForm = (next: Form) => {
    setProblem(null);
    if (next === "org") {
      setNewOrgId("");
      setNewOrgName("");
    }
    if (next === "invite") {
      setInviteOrgId(null);
      setInviteRoles([]);
      setInviteRoleId("org_admin");
    }
    setForm(next);
  };

  /** Runs a form's save, keeping its refusal at the form. */
  const run = async (act: () => Promise<void>) => {
    setSaving(true);
    setProblem(null);
    try {
      await act();
      setForm(null);
    } catch (e) {
      setProblem((e as Error).message);
    } finally {
      setSaving(false);
    }
  };

  const handleCreateOrg = () => {
    if (!orgRequired.validate()) return;
    const id = newOrgId.trim();
    const name = newOrgName.trim();
    return run(async () => {
      await createOrg(id, name);
      setOrgs(await fetchOrgs());
      notifications.show({ color: "green", message: t("orgsTab.created", { name }) });
      orgDetail.open(id);
    });
  };

  const handleDeleteOrg = async (id: string) => {
    // REQ-1300: the server refuses deletion unless the caller repeats the org id back, so the
    // platform admin confirms it here the same way the org's own admin does on the team page.
    if (window.prompt(t("orgsTab.deleteConfirmPrompt", { id })) !== id) return;
    await deleteOrg(id, id);
    setOrgs(await fetchOrgs());
    orgDetail.close({ only: id });
    notifications.show({ message: t("orgsTab.deleted", { id }) });
  };

  // REQ-1066: the tier is stored with the org and read by every tier-gated feature.
  const handleSetTier = (orgId: string) =>
    run(async () => {
      await setOrgTier(orgId, draftTier);
      setOrgs((prev) => (prev ?? []).map((o) => (o.id === orgId ? { ...o, tier: draftTier } : o)));
    });

  const handleAddOrgMember = async (oid: string) => {
    if (!memberRequired.validate()) return;
    await addOrgMember(oid, addMemberUserId.trim());
    const members = await fetchOrgMembers(oid);
    setOrgMembers((prev) => ({ ...prev, [oid]: members }));
    setAddMemberUserId("");
  };

  const handleRemoveOrgMember = async (oid: string, userId: string) => {
    await removeOrgMember(oid, userId);
    setOrgMembers((prev) => ({
      ...prev,
      [oid]: (prev[oid] ?? []).filter((m) => m.user_id !== userId),
    }));
  };

  const handleCreateInvite = () => {
    if (!inviteRequired.validate() || inviteOrgId === null) return;
    const orgId = inviteOrgId.trim();
    return run(async () => {
      const invite = await createInvite(orgId, { roleId: inviteRoleId });
      setOrgInvites(await fetchInvites());
      const url = inviteUrl(orgId, invite.token);
      await navigator.clipboard.writeText(url);
      notifications.show({ color: "green", message: t("orgsTab.inviteCreated", { url }) });
      inviteDetail.open(invite.token);
    });
  };

  // The roles on offer are the selected org's own — role ids are per-org rows, and the server
  // refuses an invitation naming one the org does not have (REQ-1313).
  const handleSelectInviteOrg = async (orgId: string | null) => {
    setInviteOrgId(orgId);
    if (!orgId) {
      setInviteRoles([]);
      return;
    }
    try {
      setInviteRoles((await fetchOrgRoles(orgId)).map((r) => r.id));
    } catch (e) {
      setProblem((e as Error).message);
    }
  };

  const handleRevokeInvite = async (token: string) => {
    await revokeInvite(token);
    setOrgInvites((prev) => (prev ?? []).filter((i) => i.token !== token));
    inviteDetail.close({ only: token });
  };

  const handleCopyInvite = async (orgId: string, token: string) => {
    await navigator.clipboard.writeText(inviteUrl(orgId, token));
    setCopiedToken(token);
    setTimeout(() => setCopiedToken(null), 2000);
  };

  const allOrgs = orgs ?? [];
  const allInvites = orgInvites ?? [];
  const filteredOrgs = shownBy(allOrgs, orgNarrowings);
  const orgTotalPages = Math.max(1, Math.ceil(filteredOrgs.length / PAGE_SIZE));
  const orgSafePage = Math.min(orgPage, orgTotalPages);
  const filteredInvites = shownBy(allInvites, inviteNarrowings);
  const invTotalPages = Math.max(1, Math.ceil(filteredInvites.length / PAGE_SIZE));
  const invSafePage = Math.min(invitePage, invTotalPages);
  const orgSelectData = allOrgs.map((o) => ({ value: o.id, label: `${o.name} (${o.id})` }));

  const openOrg = view === "orgs" ? (allOrgs.find((o) => o.id === orgDetail.id) ?? null) : null;
  const openInvite =
    view === "invites" ? (allInvites.find((i) => i.token === inviteDetail.id) ?? null) : null;

  const orgForm = (
    <FormLayout data-testid="org-create-form">
      <FormIdentifiers>
        <FormField help={t("orgsTab.orgIdDesc")} size="medium">
          <TextInput
            label={t("orgsTab.orgIdLabel")}
            placeholder={t("orgsTab.orgIdPlaceholder")}
            value={newOrgId}
            onChange={(e) => setNewOrgId(e.currentTarget.value)}
            {...orgRequired.field("id")}
          />
        </FormField>
        <FormField size="medium">
          <TextInput
            label={t("orgsTab.orgNameLabel")}
            placeholder={t("orgsTab.orgNamePlaceholder")}
            value={newOrgName}
            onChange={(e) => setNewOrgName(e.currentTarget.value)}
            {...orgRequired.field("name")}
          />
        </FormField>
      </FormIdentifiers>
      <FormFooter
        panel
        onSave={() => void handleCreateOrg()}
        onCancel={closeForm}
        saving={saving}
        problem={problem ?? undefined}
        saveProps={{ "data-testid": "org-create-save" }}
      />
    </FormLayout>
  );

  const inviteForm = (
    <FormLayout data-testid="invite-create-form">
      <FormField size="full">
        <Text size="sm" c="dimmed">
          {t("orgsTab.invitesIntro")}
        </Text>
      </FormField>
      <FormField size="medium">
        <Select
          label={t("orgsTab.orgSelectLabel")}
          placeholder={t("orgsTab.orgSelectPlaceholder")}
          data={orgSelectData}
          value={inviteOrgId}
          onChange={(v) => void handleSelectInviteOrg(v)}
          data-testid="invite-org"
          {...inviteRequired.field("org")}
        />
      </FormField>
      <FormField help={t("orgsTab.inviteRoleDescription")} size="medium">
        <Select
          label={t("orgsTab.inviteRoleLabel")}
          data={inviteRoles}
          value={inviteRoleId}
          onChange={(v) => v && setInviteRoleId(v)}
          disabled={!inviteOrgId}
        />
      </FormField>
      <FormFooter
        panel
        onCancel={closeForm}
        problem={problem ?? undefined}
        action={
          <Button
            onClick={() => void handleCreateInvite()}
            loading={saving}
            data-testid="invite-generate"
          >
            {t("orgsTab.generateInvite")}
          </Button>
        }
      />
    </FormLayout>
  );

  const tierForm = (org: Org) => (
    <FormLayout data-testid="org-tier-form">
      <FormField size="medium">
        <Select
          label={t("orgsTab.colTier")}
          aria-label={t("orgsTab.tierFor", { name: org.name })}
          data-testid="org-tier"
          placeholder={t("orgsTab.tierNotSet")}
          clearable
          data={ORG_TIERS.map((tier) => ({ value: tier, label: t(`orgsTab.tier_${tier}`) }))}
          value={draftTier}
          onChange={(v) => setDraftTier(v as OrgTier | null)}
        />
      </FormField>
      <FormFooter
        panel
        onSave={() => void handleSetTier(org.id)}
        onCancel={closeForm}
        saving={saving}
        problem={problem ?? undefined}
        saveProps={{ "data-testid": "org-tier-save" }}
      />
    </FormLayout>
  );

  const orgView = (org: Org) => {
    const members = orgMembers[org.id] ?? [];
    return (
      <Stack gap="md" data-testid={`org-detail-${org.id}`}>
        <PanelActions>
          <fieldset className="panel-action-set">
            <Group gap="xs">
              <Tip text={t("orgsTab.editTier", { name: org.name })} named>
                <ActionIcon
                  aria-label={t("orgsTab.editTier", { name: org.name })}
                  data-testid={`org-edit-${org.id}`}
                  onClick={(e) => {
                    e.stopPropagation();
                    setDraftTier(org.tier);
                    openForm("tier");
                  }}
                >
                  <Pencil size={14} />
                </ActionIcon>
              </Tip>
              {org.id !== "root" && (
                <Tip text={t("orgsTab.deleteOrg", { name: org.name })} named>
                  <ActionIcon
                    color="red"
                    aria-label={t("orgsTab.deleteOrg", { name: org.name })}
                    data-testid={`org-delete-${org.id}`}
                    onClick={() => void handleDeleteOrg(org.id)}
                  >
                    <Trash2 size={14} />
                  </ActionIcon>
                </Tip>
              )}
            </Group>
          </fieldset>
        </PanelActions>
        <DetailFields
          testId={`org-read-${org.id}`}
          fields={[
            { label: t("orgsTab.colId"), value: org.id },
            { label: t("orgsTab.colName"), value: org.name },
            { label: t("orgsTab.colTier"), value: tierLabel(org.tier) },
          ]}
        />
        <Stack gap="xs" data-testid={`org-members-${org.id}`}>
          <Text fw={600} fz="sm">
            {t("orgsTab.membersHeading")}
          </Text>
          <Group gap="xs">
            {members.length === 0 && (
              <Text c="dimmed" fz="sm">
                {t("orgsTab.noMembers")}
              </Text>
            )}
            {members.map((m) => {
              const label = m.display_name ?? m.email ?? m.user_id;
              return (
                <Badge
                  key={m.user_id}
                  variant="light"
                  rightSection={
                    <ActionIcon
                      variant="transparent"
                      size="xs"
                      color="red"
                      aria-label={t("orgsTab.removeMember", { member: label })}
                      onClick={() => void handleRemoveOrgMember(org.id, m.user_id)}
                    >
                      <X size={12} />
                    </ActionIcon>
                  }
                >
                  {label}
                </Badge>
              );
            })}
          </Group>
          <FormLayout>
            <FormField size="medium">
              <TextInput
                label={t("orgsTab.userIdPlaceholder")}
                value={addMemberUserId}
                onChange={(e) => setAddMemberUserId(e.currentTarget.value)}
                data-testid="org-member-user"
                {...memberRequired.field("user")}
              />
            </FormField>
            <FormFooter
              action={
                <Button
                  onClick={() => void handleAddOrgMember(org.id)}
                  data-testid="org-member-add"
                >
                  {t("orgsTab.addMemberButton")}
                </Button>
              }
            />
          </FormLayout>
        </Stack>
      </Stack>
    );
  };

  const inviteView = (inv: OrgInvite) => (
    <Stack gap="sm" data-testid={`invite-detail-${inv.token}`}>
      {!isSpent(inv) && (
        <PanelActions>
          <fieldset className="panel-action-set">
            <Group gap="xs">
              <Button
                data-testid={`invite-copy-${inv.token}`}
                onClick={() => void handleCopyInvite(inv.org_id, inv.token)}
              >
                {copiedToken === inv.token ? t("orgsTab.copiedButton") : t("orgsTab.copyButton")}
              </Button>
              <PanelDelete
                itemKey={inv.token}
                title={t("orgsTab.revokeInvite", { org: inv.org_name })}
                consequence={t("orgsTab.revokeConfirm", { org: inv.org_name })}
                ariaLabel={t("orgsTab.revokeInvite", { org: inv.org_name })}
                confirmLabel={t("orgsTab.revokeButton")}
                testId={`invite-revoke-${inv.token}`}
                onDelete={() => handleRevokeInvite(inv.token)}
              />
            </Group>
          </fieldset>
        </PanelActions>
      )}
      <DetailFields
        testId={`invite-read-${inv.token}`}
        fields={[
          { label: t("orgsTab.colOrg"), value: inv.org_name },
          { label: t("orgsTab.colRole"), value: inv.role_id ?? "" },
          { label: t("orgsTab.colToken"), value: `${inv.token.slice(0, 8)}…` },
          { label: t("orgsTab.colCreatedBy"), value: inv.created_by },
          {
            label: t("orgsTab.colExpires"),
            value: new Date(inv.expires_at).toLocaleDateString(),
          },
          { label: t("orgsTab.colStatus"), value: statusOf(inv) },
        ]}
      />
    </Stack>
  );

  const panelTitle =
    form === "org"
      ? t("orgsTab.addOrgTitle")
      : form === "invite"
        ? t("orgsTab.addInviteTitle")
        : openOrg !== null
          ? openOrg.name
          : openInvite !== null
            ? openInvite.org_name
            : "";
  const panelBody =
    form === "org"
      ? orgForm
      : form === "invite"
        ? inviteForm
        : openOrg !== null
          ? form === "tier"
            ? tierForm(openOrg)
            : orgView(openOrg)
          : openInvite !== null
            ? inviteView(openInvite)
            : null;
  const creating = view === "orgs" ? form === "org" : form === "invite";

  const orgList = (
    <>
      <SortGroupTable
        testPrefix="orgs"
        testId="orgs-list"
        emptyTestId="orgs-empty"
        minWidth={480}
        rows={filteredOrgs}
        columns={[
          { key: "id", label: t("orgsTab.colId"), sortValue: (o) => o.id },
          { key: "name", label: t("orgsTab.colName"), sortValue: (o) => o.name },
          {
            key: "tier",
            label: t("orgsTab.colTier"),
            sortValue: (o) => o.tier ?? "",
            groupValue: (o) => tierLabel(o.tier),
          },
        ]}
        headers={[{ col: "id" }, { col: "name" }, { col: "tier" }]}
        colSpan={3}
        rowKey={(org) => org.id}
        // A list a filter emptied says so, never that there are none.
        empty={allOrgs.length === 0 ? t("orgsTab.empty") : t("orgsTab.noMatch")}
        page={{ index: orgSafePage - 1, size: PAGE_SIZE }}
        onGroupedChange={setOrgsGrouped}
        render={(org) => (
          <ListRow
            key={org.id}
            testId={`org-row-${org.id}`}
            selected={form !== "org" && org.id === orgDetail.id}
            onClick={() => {
              closeForm();
              if (orgDetail.id === org.id) orgDetail.close();
              else orgDetail.open(org.id);
            }}
          >
            <Table.Td>{org.id}</Table.Td>
            <Table.Td>{org.name}</Table.Td>
            <Table.Td data-testid={`org-tier-${org.id}`}>{tierLabel(org.tier)}</Table.Td>
          </ListRow>
        )}
      />
      {orgTotalPages > 1 && !orgsGrouped && (
        <Group justify="flex-end">
          <Pagination
            total={orgTotalPages}
            value={orgSafePage}
            onChange={setOrgPage}
            size="sm"
            aria-label={t("orgsTab.orgPagination")}
          />
        </Group>
      )}
    </>
  );

  const inviteList = (
    <>
      <SortGroupTable
        testPrefix="org-invites"
        testId="org-invites-list"
        emptyTestId="org-invites-empty"
        minWidth={640}
        rows={filteredInvites}
        columns={[
          {
            key: "org",
            label: t("orgsTab.colOrg"),
            sortValue: (i) => i.org_name ?? "",
            groupValue: (i) => i.org_name ?? "",
          },
          {
            key: "role",
            label: t("orgsTab.colRole"),
            sortValue: (i) => i.role_id ?? "",
            groupValue: (i) => i.role_id ?? "",
          },
          { key: "token", label: t("orgsTab.colToken"), sortValue: (i) => i.token },
          {
            key: "createdBy",
            label: t("orgsTab.colCreatedBy"),
            sortValue: (i) => i.created_by ?? "",
            groupValue: (i) => i.created_by ?? "",
          },
          {
            key: "expires",
            label: t("orgsTab.colExpires"),
            sortValue: (i) => new Date(i.expires_at).getTime(),
          },
          {
            key: "status",
            label: t("orgsTab.colStatus"),
            sortValue: statusOf,
            groupValue: statusOf,
          },
        ]}
        headers={[
          { col: "org" },
          { col: "role" },
          { col: "token" },
          { col: "createdBy" },
          { col: "expires" },
          { col: "status" },
        ]}
        colSpan={6}
        rowKey={(inv) => inv.token}
        empty={allInvites.length === 0 ? t("orgsTab.noInvites") : t("orgsTab.noInvitesMatch")}
        page={{ index: invSafePage - 1, size: PAGE_SIZE }}
        onGroupedChange={setInvitesGrouped}
        render={(inv) => (
          <ListRow
            key={inv.token}
            testId={`invite-row-${inv.token}`}
            selected={form !== "invite" && inv.token === inviteDetail.id}
            onClick={() => {
              closeForm();
              if (inviteDetail.id === inv.token) inviteDetail.close();
              else inviteDetail.open(inv.token);
            }}
          >
            <Table.Td>{inv.org_name}</Table.Td>
            <Table.Td>
              <Badge variant="light" color={inv.role_id === "org_admin" ? "blue" : "gray"}>
                {inv.role_id}
              </Badge>
            </Table.Td>
            <Table.Td>
              <Text ff="monospace" span>
                {inv.token.slice(0, 8)}…
              </Text>
            </Table.Td>
            <Table.Td>{inv.created_by}</Table.Td>
            <Table.Td>{new Date(inv.expires_at).toLocaleDateString()}</Table.Td>
            <Table.Td>{statusOf(inv)}</Table.Td>
          </ListRow>
        )}
      />
      {invTotalPages > 1 && !invitesGrouped && (
        <Group justify="flex-end">
          <Pagination
            total={invTotalPages}
            value={invSafePage}
            onChange={setInvitePage}
            size="sm"
            aria-label={t("orgsTab.invitePagination")}
          />
        </Group>
      )}
    </>
  );

  return (
    <>
      <div className="page-header">
        <PageTitle icon={Building2}>{title}</PageTitle>
        <FilterInput
          value={search}
          onChange={(v) => {
            setSearch(v);
            setOrgPage(1);
            setInvitePage(1);
          }}
          placeholder={t(
            view === "orgs" ? "orgsTab.orgsFilterPlaceholder" : "orgsTab.invitesFilterPlaceholder",
          )}
        />
        <PageActions>
          {view === "orgs" ? (
            <Button
              variant={creating ? "outline" : "filled"}
              onClick={() => (creating ? closeForm() : openForm("org"))}
              aria-label={creating ? t("orgsTab.closeFormLabel") : undefined}
              data-testid="org-create-toggle"
            >
              {creating ? <X size={14} /> : t("orgsTab.addOrg")}
            </Button>
          ) : (
            <Button
              variant={creating ? "outline" : "filled"}
              onClick={() => (creating ? closeForm() : openForm("invite"))}
              aria-label={creating ? t("orgsTab.closeFormLabel") : undefined}
              data-testid="invite-create-toggle"
            >
              {creating ? <X size={14} /> : t("orgsTab.addInvite")}
            </Button>
          )}
        </PageActions>
      </div>
      <Tabs
        value={view}
        onChange={(v) => {
          if (v === null) return;
          closeForm();
          setView(v as View);
        }}
        mb="sm"
      >
        <Tabs.List>
          <Tabs.Tab value="orgs" data-testid="orgs-view-orgs">
            {t("orgsTab.orgsHeading")}
          </Tabs.Tab>
          <Tabs.Tab value="invites" data-testid="orgs-view-invites">
            {t("orgsTab.invitesHeading")}
          </Tabs.Tab>
        </Tabs.List>
      </Tabs>
      <SplitPane
        outOfList={view === "orgs" ? orgDetail.outOfList : inviteDetail.outOfList}
        testId="orgs-split"
        storageKey="provisa.orgs.detailWidth"
        title={panelTitle}
        onClose={() => {
          closeForm();
          if (view === "orgs") orgDetail.close();
          else inviteDetail.close();
        }}
        closeOnEscape={form === null}
        labels={{
          panel: t(view === "orgs" ? "orgsTab.orgsHeading" : "orgsTab.invitesHeading"),
          resize: t("tablesPage.resizeDetail"),
          expand: t("tablesPage.expandDetail"),
          restore: t("tablesPage.restoreDetail"),
          close: t("tablesPage.closeDetail"),
          backToList: t("tablesPage.backToList"),
        }}
        list={view === "orgs" ? orgList : inviteList}
      >
        {panelBody}
      </SplitPane>
    </>
  );
}
