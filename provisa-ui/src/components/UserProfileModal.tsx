// Copyright (c) 2026 Kenneth Stott
// Canary: e7f2a1b3-c4d5-6789-abcd-ef0123456789
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import {
  Alert,
  Badge,
  Button,
  Checkbox,
  Group,
  Modal,
  Stack,
  Table,
  Text,
  TextInput,
  Title,
} from "@mantine/core";
import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { useAuth } from "../context/AuthContext";
import { deleteAccount, leaveOrg, updateProfile } from "../api/admin";
import { getEmailPreferences, updateEmailPreferences } from "../api/email";
import { PersonalAccessTokens } from "./PersonalAccessTokens";

interface Props {
  onClose: () => void;
}

export function UserProfileModal({ onClose }: Props) {
  const { t } = useTranslation();
  const {
    displayName,
    email,
    userId,
    givenName,
    familyName,
    devMode,
    availableRoles,
    assignments,
    capabilities,
    orgMemberships,
    activeOrgId,
    refresh,
  } = useAuth();
  const [first, setFirst] = useState(givenName ?? "");
  const [last, setLast] = useState(familyName ?? "");
  const [saving, setSaving] = useState(false);
  const [saveError, setSaveError] = useState<string | null>(null);
  // REQ-1306/REQ-1307: leaving an org and deleting the account are the person's own acts, so they
  // live on their profile rather than under an admin page they may not be able to reach.
  const [membershipError, setMembershipError] = useState<string | null>(null);
  const [confirmDelete, setConfirmDelete] = useState("");
  const [emailOptIn, setEmailOptIn] = useState(true);
  const [emailLoading, setEmailLoading] = useState(true);
  const dirty = first !== (givenName ?? "") || last !== (familyName ?? "");

  useEffect(() => {
    getEmailPreferences()
      .then((pref) => setEmailOptIn(pref.email_opt_in))
      .catch(() => setEmailOptIn(true))
      .finally(() => setEmailLoading(false));
  }, []);

  async function handleEmailOptInChange(value: boolean) {
    setEmailOptIn(value);
    try {
      await updateEmailPreferences({ email_opt_in: value });
    } catch (e) {
      setEmailOptIn(!value);
      setMembershipError(e instanceof Error ? e.message : String(e));
    }
  }

  async function handleLeave(orgId: string) {
    setMembershipError(null);
    try {
      await leaveOrg(orgId);
      // The org list the session was built from has changed; a reload re-runs identity bootstrap.
      window.location.assign("/");
    } catch (e) {
      setMembershipError(e instanceof Error ? e.message : String(e));
    }
  }

  async function handleDeleteAccount() {
    setMembershipError(null);
    try {
      await deleteAccount(confirmDelete);
      window.location.assign("/");
    } catch (e) {
      setMembershipError(e instanceof Error ? e.message : String(e));
    }
  }

  async function handleSave() {
    setSaving(true);
    setSaveError(null);
    try {
      await updateProfile({ given_name: first.trim() || null, family_name: last.trim() || null });
      await refresh();
    } catch (e) {
      setSaveError(e instanceof Error ? e.message : t("userProfileModal.saveError"));
    } finally {
      setSaving(false);
    }
  }

  return (
    <Modal
      opened
      onClose={onClose}
      title={t("userProfileModal.title")}
      size={560}
      centered
      data-testid="user-profile-modal"
    >
      <Stack gap="lg">
        <section>
          <Title
            order={4}
            tt="uppercase"
            fz="0.75rem"
            c="dimmed"
            fw={600}
            mb="xs"
            style={{ letterSpacing: "0.05em" }}
          >
            {t("userProfileModal.identity")}
          </Title>
          <Group grow gap="sm" mb="sm">
            <TextInput
              label={t("userProfileModal.firstName")}
              value={first}
              onChange={(e) => setFirst(e.currentTarget.value)}
              data-testid="profile-first-name"
            />
            <TextInput
              label={t("userProfileModal.lastName")}
              value={last}
              onChange={(e) => setLast(e.currentTarget.value)}
              data-testid="profile-last-name"
            />
          </Group>
          <Group justify="flex-end" mb="sm">
            {saveError && (
              <Text fz="0.8rem" c="var(--reject)" data-testid="profile-save-error">
                {saveError}
              </Text>
            )}
            <Button
              size="xs"
              onClick={handleSave}
              loading={saving}
              disabled={!dirty || saving}
              data-testid="profile-save"
            >
              {saving ? t("userProfileModal.saving") : t("userProfileModal.save")}
            </Button>
          </Group>
          <Table withRowBorders={false} verticalSpacing={4} fz="0.85rem">
            <Table.Tbody>
              {displayName && (
                <Table.Tr>
                  <Table.Td c="dimmed" style={{ width: "max-content" }}>
                    {t("userProfileModal.name")}
                  </Table.Td>
                  <Table.Td>{displayName}</Table.Td>
                </Table.Tr>
              )}
              {email && (
                <Table.Tr>
                  <Table.Td c="dimmed">{t("userProfileModal.email")}</Table.Td>
                  <Table.Td>{email}</Table.Td>
                </Table.Tr>
              )}
              {userId && (
                <Table.Tr>
                  <Table.Td c="dimmed">{t("userProfileModal.userId")}</Table.Td>
                  <Table.Td ff="monospace">{userId}</Table.Td>
                </Table.Tr>
              )}
              {activeOrgId && (
                <Table.Tr>
                  <Table.Td c="dimmed">{t("userProfileModal.org")}</Table.Td>
                  <Table.Td>
                    {orgMemberships.find((m) => m.org_id === activeOrgId)?.org_name ?? activeOrgId}
                  </Table.Td>
                </Table.Tr>
              )}
              {devMode && (
                <Table.Tr>
                  <Table.Td c="dimmed">{t("userProfileModal.mode")}</Table.Td>
                  <Table.Td>
                    <Badge size="xs" color="gray" variant="filled">
                      {t("userProfileModal.devBadge")}
                    </Badge>
                  </Table.Td>
                </Table.Tr>
              )}
            </Table.Tbody>
          </Table>
        </section>

        <section>
          <Title
            order={4}
            tt="uppercase"
            fz="0.75rem"
            c="dimmed"
            fw={600}
            mb="xs"
            style={{ letterSpacing: "0.05em" }}
          >
            {t("userProfileModal.emailPreferences")}
          </Title>
          {!emailLoading && (
            <Checkbox
              label={t("userProfileModal.receiveEmails")}
              checked={emailOptIn}
              onChange={(e) => handleEmailOptInChange(e.currentTarget.checked)}
              description={t("userProfileModal.receiveEmailsHelp")}
              mb="md"
            />
          )}
        </section>

        <section>
          <Title
            order={4}
            tt="uppercase"
            fz="0.75rem"
            c="dimmed"
            fw={600}
            mb="xs"
            style={{ letterSpacing: "0.05em" }}
          >
            {t("userProfileModal.rolesAndDomainAccess")}
          </Title>
          {availableRoles.length === 0 ? (
            <Text fz="0.85rem" c="dimmed">
              {t("userProfileModal.noRolesAssigned")}
            </Text>
          ) : (
            <Table fz="0.82rem" withTableBorder={false}>
              <Table.Thead>
                <Table.Tr>
                  <Table.Th c="dimmed" fw={500}>
                    {t("userProfileModal.role")}
                  </Table.Th>
                  <Table.Th c="dimmed" fw={500}>
                    {t("userProfileModal.domains")}
                  </Table.Th>
                </Table.Tr>
              </Table.Thead>
              <Table.Tbody>
                {availableRoles.map((role) => {
                  const domains = assignments
                    .filter((a) => a.role_id === role.id)
                    .map((a) => a.domain_id);
                  return (
                    <Table.Tr key={role.id}>
                      <Table.Td ff="monospace">{role.id}</Table.Td>
                      <Table.Td>
                        {domains.length === 0 ? (
                          <Text component="span" c="dimmed">
                            —
                          </Text>
                        ) : domains.includes("*") ? (
                          <Text component="span" c="var(--approve)">
                            {t("userProfileModal.allDomains")}
                          </Text>
                        ) : (
                          domains.join(", ")
                        )}
                      </Table.Td>
                    </Table.Tr>
                  );
                })}
              </Table.Tbody>
            </Table>
          )}
        </section>

        <section>
          <Title
            order={4}
            tt="uppercase"
            fz="0.75rem"
            c="dimmed"
            fw={600}
            mb="xs"
            style={{ letterSpacing: "0.05em" }}
          >
            {t("userProfileModal.capabilities")}
          </Title>
          {capabilities.length === 0 ? (
            <Text fz="0.85rem" c="dimmed">
              {t("userProfileModal.noCapabilities")}
            </Text>
          ) : (
            <Group gap="xs">
              {capabilities.map((cap) => (
                <Badge
                  key={cap}
                  size="sm"
                  variant="outline"
                  color="gray"
                  ff="monospace"
                  tt="none"
                  fw={400}
                >
                  {cap}
                </Badge>
              ))}
            </Group>
          )}
        </section>

        <section data-testid="profile-memberships">
          <Title
            order={4}
            tt="uppercase"
            fz="0.75rem"
            c="dimmed"
            fw={600}
            mb="xs"
            style={{ letterSpacing: "0.05em" }}
          >
            {t("userProfileModal.membershipsHeading")}
          </Title>
          {membershipError && (
            <Alert color="red" mb="xs" data-testid="profile-membership-error">
              {membershipError}
            </Alert>
          )}
          {orgMemberships.length === 0 ? (
            <Text fz="0.85rem" c="dimmed">
              {t("userProfileModal.noMemberships")}
            </Text>
          ) : (
            <Table fz="0.82rem" withTableBorder={false}>
              <Table.Tbody>
                {orgMemberships.map((m) => (
                  <Table.Tr key={m.org_id}>
                    <Table.Td>{m.org_name}</Table.Td>
                    <Table.Td ta="right">
                      <Button
                        size="compact-xs"
                        variant="default"
                        onClick={() => handleLeave(m.org_id)}
                        data-testid={`profile-leave-${m.org_id}`}
                      >
                        {t("userProfileModal.leaveOrg")}
                      </Button>
                    </Table.Td>
                  </Table.Tr>
                ))}
              </Table.Tbody>
            </Table>
          )}
          <Text fz="0.8rem" c="dimmed" mt="xs">
            {t("userProfileModal.leaveHelp")}
          </Text>
        </section>

        <section data-testid="profile-tokens">
          <Title
            order={4}
            tt="uppercase"
            fz="0.75rem"
            c="dimmed"
            fw={600}
            mb="xs"
            style={{ letterSpacing: "0.05em" }}
          >
            {t("userProfileModal.patHeading")}
          </Title>
          <PersonalAccessTokens />
        </section>

        <section data-testid="profile-delete-account">
          <Title
            order={4}
            tt="uppercase"
            fz="0.75rem"
            c="dimmed"
            fw={600}
            mb="xs"
            style={{ letterSpacing: "0.05em" }}
          >
            {t("userProfileModal.deleteAccountHeading")}
          </Title>
          <Text fz="0.85rem" c="dimmed" mb="xs">
            {t("userProfileModal.deleteAccountHelp")}
          </Text>
          <Group gap="xs" align="flex-end">
            <TextInput
              label={t("userProfileModal.deleteAccountConfirmLabel", { userId })}
              value={confirmDelete}
              onChange={(e) => setConfirmDelete(e.currentTarget.value)}
              data-testid="profile-delete-confirm"
            />
            <Button
              color="red"
              size="xs"
              disabled={!userId || confirmDelete !== userId}
              onClick={handleDeleteAccount}
              data-testid="profile-delete-submit"
            >
              {t("userProfileModal.deleteAccountButton")}
            </Button>
          </Group>
        </section>
      </Stack>
    </Modal>
  );
}
