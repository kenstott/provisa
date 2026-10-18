// Copyright (c) 2026 Kenneth Stott
// Canary: fa264cd2-9acb-4b3b-b896-f4c690da4a02
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
  Button,
  Collapse,
  Group,
  Modal,
  Pagination,
  Select,
  Stack,
  Table,
  Text,
  TextInput,
  PasswordInput,
  Title,
} from "@mantine/core";
import { DateInput } from "@mantine/dates";
import { notifications } from "@mantine/notifications";
import { ChevronDown, ChevronRight, Plus, Trash2, X } from "lucide-react";
import {
  fetchLocalUsers,
  createLocalUser,
  deleteLocalUser,
  fetchUserAssignments,
  addUserAssignment,
  removeUserAssignment,
} from "../../api/admin";
import type { LocalUser, UserAssignment } from "../../api/admin";
import { useCapability } from "../../hooks/useCapability";
import { ListRow } from "../list/ListTable";
import { useCut } from "../list/useCut";
import { LIST_COLUMN_CAP_PX } from "../list/listCellLimits";
import { Tip } from "../Tip";
import { SortGroupTable } from "../list/SortGroupTable";
import { FormField, FormFooter, FormIdentifiers, FormLayout } from "../form/FormLayout";
import { tomorrow, untilEndOf } from "../../lib/grantUntil";

const PAGE_SIZE = 50;

/**
 * A user's row opens and closes on its name. The name asks for no more than a list column's cap;
 * a longer one is cut with an ellipsis and shown whole in the hint.
 */
function UserExpandButton({
  username,
  expanded,
  onClick,
}: {
  username: string;
  expanded: boolean;
  onClick: () => void;
}) {
  const { t } = useTranslation();
  const { ref, cut } = useCut<HTMLSpanElement>(username);
  return (
    <Tip text={cut ? username : false}>
      <Button
        variant="subtle"
        size="compact-sm"
        maw={LIST_COLUMN_CAP_PX}
        leftSection={expanded ? <ChevronDown size={14} /> : <ChevronRight size={14} />}
        aria-expanded={expanded}
        aria-label={t("localUsers.expandUser", { username })}
        onClick={onClick}
        styles={{
          inner: { minWidth: 0 },
          label: { fontFamily: "monospace", minWidth: 0, overflow: "hidden" },
        }}
        data-testid={`expand-user-${username}`}
        data-cut={cut || undefined}
      >
        <span
          ref={ref}
          style={{
            display: "block",
            overflow: "hidden",
            textOverflow: "ellipsis",
            whiteSpace: "nowrap",
          }}
        >
          {username}
        </span>
      </Button>
    </Tip>
  );
}

interface LocalUsersTabProps {
  allRoles: string[];
  allDomains: string[];
}

export function LocalUsersTab({ allRoles, allDomains }: LocalUsersTabProps) {
  const { t, i18n } = useTranslation();
  const [localUsers, setLocalUsers] = useState<LocalUser[]>([]);
  const [creating, setCreating] = useState(false);
  const [newUsername, setNewUsername] = useState("");
  const [newPassword, setNewPassword] = useState("");
  const [newEmail, setNewEmail] = useState("");
  const [newDisplayName, setNewDisplayName] = useState("");
  const [expandedUserId, setExpandedUserId] = useState<string | null>(null);
  const [userAssignments, setUserAssignments] = useState<Record<string, UserAssignment[]>>({});
  const [assignRole, setAssignRole] = useState<string | null>(null);
  const [assignDomain, setAssignDomain] = useState<string>("*");
  const [assignUntil, setAssignUntil] = useState<string | null>(null);
  const day = (value: string | null) =>
    value ? new Date(value).toLocaleDateString(i18n.language) : t("localUsers.none");
  const [userPage, setUserPage] = useState(1);

  useEffect(() => {
    fetchLocalUsers()
      .then(setLocalUsers)
      .catch(() => setLocalUsers([]));
  }, []);

  const closeCreate = () => {
    setCreating(false);
    setNewUsername("");
    setNewPassword("");
    setNewEmail("");
    setNewDisplayName("");
  };

  const handleAddUser = async () => {
    if (!newUsername.trim() || !newPassword.trim()) return;
    try {
      await createLocalUser({
        username: newUsername.trim(),
        password: newPassword,
        email: newEmail.trim() || undefined,
        display_name: newDisplayName.trim() || undefined,
      });
      const updated = await fetchLocalUsers();
      setLocalUsers(updated);
      const created = newUsername.trim();
      setNewUsername("");
      setNewPassword("");
      setNewEmail("");
      setNewDisplayName("");
      setCreating(false);
      notifications.show({
        color: "green",
        message: t("localUsers.created", { username: created }),
      });
    } catch (e: unknown) {
      notifications.show({
        color: "red",
        message: e instanceof Error ? e.message : t("localUsers.createFailed"),
      });
    }
  };

  // The holder of the cross-org right deletes the ACCOUNT, everywhere; an org administrator
  // removes the person from THIS org. The server decides by the same right; the page says which
  // before the person confirms, and again from the server's answer afterwards.
  const deletesAccount = useCapability("cross_org");
  const [removing, setRemoving] = useState<{ id: string; username: string } | null>(null);
  const [removeBusy, setRemoveBusy] = useState(false);

  const handleDeleteUser = async () => {
    if (removing === null) return;
    const { id: userId, username } = removing;
    setRemoveBusy(true);
    try {
      const scope = await deleteLocalUser(userId);
      setLocalUsers((prev) => prev.filter((u) => u.id !== userId));
      if (expandedUserId === userId) setExpandedUserId(null);
      notifications.show({
        message: t(scope === "account" ? "localUsers.deleted" : "localUsers.removedFromOrg", {
          username,
        }),
      });
    } catch (e: unknown) {
      notifications.show({
        color: "red",
        autoClose: false,
        message: e instanceof Error ? e.message : t("localUsers.removeFailed"),
      });
    } finally {
      setRemoveBusy(false);
      setRemoving(null);
    }
  };

  const handleExpandUser = async (userId: string) => {
    if (expandedUserId === userId) {
      setExpandedUserId(null);
      return;
    }
    setExpandedUserId(userId);
    setAssignRole(allRoles[0] ?? null);
    setAssignDomain("*");
    setAssignUntil(null);
    if (!userAssignments[userId]) {
      const rows = await fetchUserAssignments(userId);
      setUserAssignments((prev) => ({ ...prev, [userId]: rows }));
    }
  };

  const handleAddAssignment = async (userId: string) => {
    if (!assignRole) return;
    try {
      await addUserAssignment(
        userId,
        assignRole,
        assignDomain || "*",
        assignUntil ? untilEndOf(assignUntil) : null,
      );
      setAssignUntil(null);
      const rows = await fetchUserAssignments(userId);
      setUserAssignments((prev) => ({ ...prev, [userId]: rows }));
    } catch (e: unknown) {
      notifications.show({
        color: "red",
        message: e instanceof Error ? e.message : t("localUsers.assignmentFailed"),
      });
    }
  };

  const handleRemoveAssignment = async (userId: string, assignmentId: number) => {
    await removeUserAssignment(userId, assignmentId);
    setUserAssignments((prev) => ({
      ...prev,
      [userId]: (prev[userId] ?? []).filter((a) => a.id !== assignmentId),
    }));
  };

  const [grouped, setGrouped] = useState(false);
  const totalPages = Math.max(1, Math.ceil(localUsers.length / PAGE_SIZE));
  const domainOptions = [
    { value: "*", label: t("localUsers.allDomains") },
    ...allDomains.map((d) => ({ value: d, label: d })),
  ];

  return (
    <Stack gap="md">
      {creating ? (
        <>
          <Title order={4}>{t("localUsers.createHeading")}</Title>
          <FormLayout>
            <FormIdentifiers>
              <FormField size="medium">
                <TextInput
                  label={t("localUsers.username")}
                  placeholder={t("localUsers.usernameRequired")}
                  required
                  value={newUsername}
                  onChange={(e) => setNewUsername(e.currentTarget.value)}
                />
              </FormField>
            </FormIdentifiers>
            <FormField size="medium">
              <PasswordInput
                label={t("localUsers.password")}
                placeholder={t("localUsers.passwordRequired")}
                required
                value={newPassword}
                onChange={(e) => setNewPassword(e.currentTarget.value)}
              />
            </FormField>
            <FormField size="medium">
              <TextInput
                label={t("localUsers.email")}
                placeholder={t("localUsers.emailOptional")}
                value={newEmail}
                onChange={(e) => setNewEmail(e.currentTarget.value)}
              />
            </FormField>
            <FormField size="medium">
              <TextInput
                label={t("localUsers.displayName")}
                placeholder={t("localUsers.displayNameOptional")}
                value={newDisplayName}
                onChange={(e) => setNewDisplayName(e.currentTarget.value)}
              />
            </FormField>
            <FormFooter
              onSave={handleAddUser}
              onCancel={closeCreate}
              saveProps={{
                "data-testid": "local-user-save",
                disabled: !newUsername.trim() || !newPassword.trim(),
              }}
            />
          </FormLayout>
        </>
      ) : (
        <FormFooter>
          <Button
            leftSection={<Plus size={14} />}
            onClick={() => setCreating(true)}
            data-testid="local-user-add"
          >
            {t("localUsers.createButton")}
          </Button>
        </FormFooter>
      )}

      <SortGroupTable
        testPrefix="local-users"
        minWidth={640}
        rows={localUsers}
        columns={[
          { key: "username", label: t("localUsers.colUsername"), sortValue: (u) => u.username },
          { key: "email", label: t("localUsers.colEmail"), sortValue: (u) => u.email ?? "" },
          {
            key: "displayName",
            label: t("localUsers.colDisplayName"),
            sortValue: (u) => u.display_name ?? "",
          },
          {
            key: "active",
            label: t("localUsers.colActive"),
            sortValue: (u) => String(u.is_active),
            groupValue: (u) => String(u.is_active),
          },
        ]}
        headers={[
          { col: "username" },
          { col: "email" },
          { col: "displayName" },
          { col: "active" },
          t("localUsers.actions"),
        ]}
        colSpan={5}
        rowKey={(u) => u.id}
        empty={t("localUsers.empty")}
        page={{ index: userPage - 1, size: PAGE_SIZE }}
        onGroupedChange={setGrouped}
        render={(u) => {
          const expanded = expandedUserId === u.id;
          return (
            <ListRow key={u.id}>
              <Table.Td>
                <UserExpandButton
                  username={u.username}
                  expanded={expanded}
                  onClick={() => handleExpandUser(u.id)}
                />
                <Collapse in={expanded}>
                  <Stack gap="xs" pl="md" py="sm">
                    <Text fw={600} fz="sm">
                      {t("localUsers.assignmentsHeading")}
                    </Text>
                    {(userAssignments[u.id] ?? []).length === 0 ? (
                      <Text c="dimmed" fz="sm">
                        {t("localUsers.noAssignments")}
                      </Text>
                    ) : (
                      <Table withRowBorders={false} verticalSpacing={2} fz="sm" w="auto">
                        <Table.Thead>
                          <Table.Tr>
                            <Table.Th>{t("localUsers.roleLabel")}</Table.Th>
                            <Table.Th>{t("localUsers.domainLabel")}</Table.Th>
                            <Table.Th>{t("localUsers.colUntil")}</Table.Th>
                            <Table.Th>{t("localUsers.colLastUsed")}</Table.Th>
                            <Table.Th />
                          </Table.Tr>
                        </Table.Thead>
                        <Table.Tbody>
                          {(userAssignments[u.id] ?? []).map((a) => (
                            <Table.Tr
                              key={a.id}
                              data-testid={`assignment-${a.role_id}-${a.domain_id}`}
                            >
                              <Table.Td ff="monospace">{a.role_id}</Table.Td>
                              <Table.Td ff="monospace">{a.domain_id}</Table.Td>
                              <Table.Td>
                                {a.expires_at ? day(a.expires_at) : t("localUsers.noExpiry")}
                              </Table.Td>
                              <Table.Td>
                                {a.last_used_at ? day(a.last_used_at) : t("localUsers.neverUsed")}
                              </Table.Td>
                              <Table.Td>
                                <ActionIcon
                                  variant="subtle"
                                  size="sm"
                                  color="red"
                                  aria-label={t("localUsers.removeAssignment", {
                                    role: a.role_id,
                                    domain: a.domain_id,
                                  })}
                                  onClick={() => handleRemoveAssignment(u.id, a.id)}
                                >
                                  <X size={12} />
                                </ActionIcon>
                              </Table.Td>
                            </Table.Tr>
                          ))}
                        </Table.Tbody>
                      </Table>
                    )}
                    <Group gap="xs" align="flex-end">
                      <Select
                        label={t("localUsers.roleLabel")}
                        size="xs"
                        data={allRoles}
                        value={assignRole}
                        onChange={setAssignRole}
                        allowDeselect={false}
                      />
                      <Select
                        label={t("localUsers.domainLabel")}
                        size="xs"
                        data={domainOptions}
                        value={assignDomain}
                        onChange={(v) => setAssignDomain(v ?? "*")}
                        allowDeselect={false}
                      />
                      <DateInput
                        label={t("localUsers.untilLabel")}
                        placeholder={t("localUsers.untilPlaceholder")}
                        size="xs"
                        valueFormat="YYYY-MM-DD"
                        clearable
                        minDate={tomorrow()}
                        value={assignUntil}
                        onChange={setAssignUntil}
                        popoverProps={{ withinPortal: true }}
                        data-testid="assignment-until"
                      />
                      <Button
                        size="xs"
                        onClick={() => handleAddAssignment(u.id)}
                        disabled={!assignRole}
                      >
                        {t("localUsers.add")}
                      </Button>
                    </Group>
                  </Stack>
                </Collapse>
              </Table.Td>
              <Table.Td>{u.email || t("localUsers.none")}</Table.Td>
              <Table.Td>{u.display_name || t("localUsers.none")}</Table.Td>
              <Table.Td>{u.is_active ? t("localUsers.yes") : t("localUsers.no")}</Table.Td>
              <Table.Td>
                <ActionIcon
                  variant="subtle"
                  color="red"
                  aria-label={t(
                    deletesAccount ? "localUsers.deleteUser" : "localUsers.removeFromOrg",
                    { username: u.username },
                  )}
                  data-testid={`remove-user-${u.username}`}
                  onClick={() => setRemoving({ id: u.id, username: u.username })}
                >
                  <Trash2 size={14} />
                </ActionIcon>
              </Table.Td>
            </ListRow>
          );
        }}
      />
      {totalPages > 1 && !grouped && (
        <Group justify="flex-end">
          <Pagination total={totalPages} value={userPage} onChange={setUserPage} size="sm" />
        </Group>
      )}

      <Modal
        opened={removing !== null}
        onClose={() => setRemoving(null)}
        title={t(
          deletesAccount ? "localUsers.confirmDeleteTitle" : "localUsers.confirmRemoveTitle",
        )}
        centered
        closeOnClickOutside={!removeBusy}
        closeOnEscape={!removeBusy}
        data-testid="remove-user-modal"
      >
        <Stack gap="md">
          <Text>
            {t(deletesAccount ? "localUsers.confirmDeleteBody" : "localUsers.confirmRemoveBody", {
              username: removing?.username,
            })}
          </Text>
          <FormFooter
            onCancel={() => setRemoving(null)}
            cancelProps={{ disabled: removeBusy }}
            action={
              <Button
                color="red"
                onClick={handleDeleteUser}
                loading={removeBusy}
                data-testid="remove-user-confirm"
              >
                {t(
                  deletesAccount
                    ? "localUsers.confirmDeleteButton"
                    : "localUsers.confirmRemoveButton",
                )}
              </Button>
            }
          />
        </Stack>
      </Modal>
    </Stack>
  );
}
