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
  Collapse,
  Group,
  Pagination,
  Select,
  Stack,
  Table,
  Text,
  TextInput,
  Title,
} from "@mantine/core";
import { notifications } from "@mantine/notifications";
import { ChevronDown, ChevronRight, Trash2, X } from "lucide-react";
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
} from "../../api/admin";
import type { Org, OrgMember, OrgInvite } from "../../api/admin";
import { FilterInput } from "./FilterInput";

const PAGE_SIZE = 50;

export function OrgsTab() {
  const { t } = useTranslation();
  const [orgs, setOrgs] = useState<Org[]>([]);
  const [newOrgId, setNewOrgId] = useState("");
  const [newOrgName, setNewOrgName] = useState("");
  const [showCreateOrg, setShowCreateOrg] = useState(false);
  const [expandedOrgId, setExpandedOrgId] = useState<string | null>(null);
  const [orgMembers, setOrgMembers] = useState<Record<string, OrgMember[]>>({});
  const [addMemberUserId, setAddMemberUserId] = useState("");
  const [orgSearch, setOrgSearch] = useState("");
  const [orgPage, setOrgPage] = useState(1);
  const [orgInvites, setOrgInvites] = useState<OrgInvite[]>([]);
  const [inviteOrgId, setInviteOrgId] = useState<string | null>(null);
  const [showInviteForm, setShowInviteForm] = useState(false);
  const [inviteSearch, setInviteSearch] = useState("");
  const [copiedToken, setCopiedToken] = useState<string | null>(null);
  const [invitePage, setInvitePage] = useState(1);

  useEffect(() => {
    fetchOrgs()
      .then(setOrgs)
      .catch(() => setOrgs([]));
    fetchInvites()
      .then(setOrgInvites)
      .catch(() => setOrgInvites([]));
  }, []);

  const handleCreateOrg = async () => {
    if (!newOrgId.trim() || !newOrgName.trim()) return;
    const name = newOrgName.trim();
    await createOrg(newOrgId.trim(), name);
    setOrgs(await fetchOrgs());
    setNewOrgId("");
    setNewOrgName("");
    setShowCreateOrg(false);
    notifications.show({ color: "green", message: t("orgsTab.created", { name }) });
  };

  const handleDeleteOrg = async (id: string) => {
    await deleteOrg(id);
    setOrgs(await fetchOrgs());
    notifications.show({ message: t("orgsTab.deleted", { id }) });
  };

  const handleExpandOrg = async (id: string) => {
    if (expandedOrgId === id) {
      setExpandedOrgId(null);
      return;
    }
    setExpandedOrgId(id);
    if (!orgMembers[id]) {
      const members = await fetchOrgMembers(id);
      setOrgMembers((prev) => ({ ...prev, [id]: members }));
    }
  };

  const handleAddOrgMember = async (oid: string) => {
    if (!addMemberUserId.trim()) return;
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

  const handleCreateInvite = async () => {
    if (!inviteOrgId || !inviteOrgId.trim()) return;
    const invite = await createInvite(inviteOrgId.trim());
    setOrgInvites(await fetchInvites());
    const url = `${window.location.origin}/register?invite=${invite.token}`;
    await navigator.clipboard.writeText(url);
    notifications.show({ color: "green", message: t("orgsTab.inviteCreated", { url }) });
    setInviteOrgId(null);
    setShowInviteForm(false);
  };

  const handleRevokeInvite = async (token: string) => {
    await revokeInvite(token);
    setOrgInvites((prev) => prev.filter((i) => i.token !== token));
  };

  const handleCopyInvite = async (token: string) => {
    const url = `${window.location.origin}/register?invite=${token}`;
    await navigator.clipboard.writeText(url);
    setCopiedToken(token);
    setTimeout(() => setCopiedToken(null), 2000);
  };

  const q = orgSearch.toLowerCase();
  const filteredOrgs = orgs.filter(
    (o) => o.id.toLowerCase().includes(q) || o.name.toLowerCase().includes(q),
  );
  const orgTotalPages = Math.max(1, Math.ceil(filteredOrgs.length / PAGE_SIZE));
  const orgSafePage = Math.min(orgPage, orgTotalPages);
  const pagedOrgs = filteredOrgs.slice((orgSafePage - 1) * PAGE_SIZE, orgSafePage * PAGE_SIZE);

  const iq = inviteSearch.toLowerCase();
  const filteredInvites = orgInvites.filter(
    (i) =>
      (i.org_name ?? "").toLowerCase().includes(iq) ||
      i.token.toLowerCase().includes(iq) ||
      (i.created_by ?? "").toLowerCase().includes(iq),
  );
  const invTotalPages = Math.max(1, Math.ceil(filteredInvites.length / PAGE_SIZE));
  const invSafePage = Math.min(invitePage, invTotalPages);
  const pagedInvites = filteredInvites.slice((invSafePage - 1) * PAGE_SIZE, invSafePage * PAGE_SIZE);

  const orgSelectData = orgs.map((o) => ({ value: o.id, label: `${o.name} (${o.id})` }));

  return (
    <Stack gap="md">
      <Group justify="space-between" wrap="wrap">
        <Title order={3}>{t("orgsTab.orgsHeading")}</Title>
        <FilterInput
          value={orgSearch}
          onChange={(v) => { setOrgSearch(v); setOrgPage(1); }}
          placeholder={t("orgsTab.orgsFilterPlaceholder")}
        />
        <Button
          variant={showCreateOrg ? "default" : "filled"}
          onClick={() => setShowCreateOrg((v) => !v)}
          data-testid="org-create-toggle"
        >
          {showCreateOrg ? t("orgsTab.closeForm") : t("orgsTab.addOrg")}
        </Button>
      </Group>

      {showCreateOrg && (
        <Stack gap="sm" maw={480}>
          <TextInput
            label={t("orgsTab.orgIdLabel")}
            placeholder={t("orgsTab.orgIdPlaceholder")}
            value={newOrgId}
            onChange={(e) => setNewOrgId(e.currentTarget.value)}
          />
          <TextInput
            label={t("orgsTab.orgNameLabel")}
            placeholder={t("orgsTab.orgNamePlaceholder")}
            value={newOrgName}
            onChange={(e) => setNewOrgName(e.currentTarget.value)}
          />
          <Button
            onClick={handleCreateOrg}
            disabled={!newOrgId.trim() || !newOrgName.trim()}
            style={{ alignSelf: "flex-start" }}
          >
            {t("orgsTab.createButton")}
          </Button>
        </Stack>
      )}

      <Table.ScrollContainer minWidth={640}>
        <Table striped highlightOnHover withTableBorder verticalSpacing="xs">
          <Table.Thead>
            <Table.Tr>
              <Table.Th>{t("orgsTab.colId")}</Table.Th>
              <Table.Th>{t("orgsTab.colName")}</Table.Th>
              <Table.Th>{t("orgsTab.colMembers")}</Table.Th>
              <Table.Th>{t("orgsTab.colActions")}</Table.Th>
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {filteredOrgs.length === 0 && (
              <Table.Tr>
                <Table.Td colSpan={4} ta="center" c="dimmed">
                  {t("orgsTab.empty")}
                </Table.Td>
              </Table.Tr>
            )}
            {pagedOrgs.map((org) => {
              const expanded = expandedOrgId === org.id;
              return (
                <Table.Tr key={org.id}>
                  <Table.Td>{org.id}</Table.Td>
                  <Table.Td>{org.name}</Table.Td>
                  <Table.Td>
                    <Button
                      variant="subtle"
                      size="compact-xs"
                      leftSection={expanded ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
                      aria-expanded={expanded}
                      aria-label={t("orgsTab.expandOrg", { name: org.name })}
                      onClick={() => handleExpandOrg(org.id)}
                    >
                      {expanded ? t("orgsTab.hideButton") : t("orgsTab.membersButton")}
                    </Button>
                    <Collapse in={expanded}>
                      <Stack gap="xs" pl="md" py="sm">
                        <Text fw={600} fz="sm">
                          {t("orgsTab.membersHeading")}
                        </Text>
                        <Group gap="xs">
                          {(orgMembers[org.id] ?? []).length === 0 && (
                            <Text c="dimmed" fz="sm">
                              {t("orgsTab.noMembers")}
                            </Text>
                          )}
                          {(orgMembers[org.id] ?? []).map((m) => {
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
                                    onClick={() => handleRemoveOrgMember(org.id, m.user_id)}
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
                        <Group gap="xs" align="flex-end">
                          <TextInput
                            aria-label={t("orgsTab.userIdPlaceholder")}
                            placeholder={t("orgsTab.userIdPlaceholder")}
                            size="xs"
                            value={addMemberUserId}
                            onChange={(e) => setAddMemberUserId(e.currentTarget.value)}
                          />
                          <Button
                            size="xs"
                            onClick={() => handleAddOrgMember(org.id)}
                            disabled={!addMemberUserId.trim()}
                          >
                            {t("orgsTab.addMemberButton")}
                          </Button>
                        </Group>
                      </Stack>
                    </Collapse>
                  </Table.Td>
                  <Table.Td>
                    {org.id !== "root" && (
                      <ActionIcon
                        variant="subtle"
                        color="red"
                        aria-label={t("orgsTab.deleteOrg", { name: org.name })}
                        onClick={() => handleDeleteOrg(org.id)}
                      >
                        <Trash2 size={14} />
                      </ActionIcon>
                    )}
                  </Table.Td>
                </Table.Tr>
              );
            })}
          </Table.Tbody>
        </Table>
      </Table.ScrollContainer>
      {orgTotalPages > 1 && (
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

      <Group justify="space-between" wrap="wrap" mt="lg">
        <Title order={3}>{t("orgsTab.invitesHeading")}</Title>
        <FilterInput
          value={inviteSearch}
          onChange={(v) => { setInviteSearch(v); setInvitePage(1); }}
          placeholder={t("orgsTab.invitesFilterPlaceholder")}
        />
        <Button
          variant={showInviteForm ? "default" : "filled"}
          onClick={() => setShowInviteForm((v) => !v)}
          data-testid="invite-create-toggle"
        >
          {showInviteForm ? t("orgsTab.closeForm") : t("orgsTab.addInvite")}
        </Button>
      </Group>

      {showInviteForm && (
        <Stack gap="sm" maw={480}>
          <Select
            label={t("orgsTab.orgSelectLabel")}
            placeholder={t("orgsTab.orgSelectPlaceholder")}
            data={orgSelectData}
            value={inviteOrgId}
            onChange={setInviteOrgId}
          />
          <Button
            onClick={handleCreateInvite}
            disabled={!inviteOrgId}
            style={{ alignSelf: "flex-start" }}
          >
            {t("orgsTab.generateInvite")}
          </Button>
        </Stack>
      )}

      <Table.ScrollContainer minWidth={640}>
        <Table striped highlightOnHover withTableBorder verticalSpacing="xs">
          <Table.Thead>
            <Table.Tr>
              <Table.Th>{t("orgsTab.colOrg")}</Table.Th>
              <Table.Th>{t("orgsTab.colToken")}</Table.Th>
              <Table.Th>{t("orgsTab.colCreatedBy")}</Table.Th>
              <Table.Th>{t("orgsTab.colExpires")}</Table.Th>
              <Table.Th>{t("orgsTab.colStatus")}</Table.Th>
              <Table.Th>{t("orgsTab.colActions")}</Table.Th>
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {filteredInvites.length === 0 && (
              <Table.Tr>
                <Table.Td colSpan={6} ta="center" c="dimmed">
                  {t("orgsTab.noInvites")}
                </Table.Td>
              </Table.Tr>
            )}
            {pagedInvites.map((inv) => (
              <Table.Tr key={inv.token}>
                <Table.Td>{inv.org_name}</Table.Td>
                <Table.Td>
                  <Text ff="monospace" span>{inv.token.slice(0, 8)}…</Text>
                </Table.Td>
                <Table.Td>{inv.created_by}</Table.Td>
                <Table.Td>{new Date(inv.expires_at).toLocaleDateString()}</Table.Td>
                <Table.Td>
                  {inv.used_at
                    ? t("orgsTab.usedStatus", { date: new Date(inv.used_at).toLocaleDateString() })
                    : t("orgsTab.activeStatus")}
                </Table.Td>
                <Table.Td>
                  <Group gap="xs">
                    {!inv.used_at && (
                      <Button size="compact-xs" variant="default" onClick={() => handleCopyInvite(inv.token)}>
                        {copiedToken === inv.token ? t("orgsTab.copiedButton") : t("orgsTab.copyButton")}
                      </Button>
                    )}
                    {!inv.used_at && (
                      <Button
                        size="compact-xs"
                        color="red"
                        variant="light"
                        aria-label={t("orgsTab.revokeInvite", { org: inv.org_name })}
                        onClick={() => handleRevokeInvite(inv.token)}
                      >
                        {t("orgsTab.revokeButton")}
                      </Button>
                    )}
                  </Group>
                </Table.Td>
              </Table.Tr>
            ))}
          </Table.Tbody>
        </Table>
      </Table.ScrollContainer>
      {invTotalPages > 1 && (
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
    </Stack>
  );
}
