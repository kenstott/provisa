// Copyright (c) 2026 Kenneth Stott
// Canary: 0f4a1d7e-6c92-4b3a-9d15-8e7b2c40af31
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState } from "react";
import { useTranslation } from "react-i18next";
import { useNavigate } from "react-router-dom";
import { Button, Group, List, Modal, Stack, Text, Title } from "@mantine/core";

/** Set by LoginPage when POST /auth/claim-bootstrap reports the slot was taken by this sign-in. */
export const CLAIMED_ADMIN_FLAG = "provisa_claimed_platform_admin";

/**
 * REQ-1294: claiming the platform-admin slot is a one-time, irreversible act that happens behind a
 * provider redirect — the user clicks "Sign in with Google" and lands in the app with no statement
 * of what they now are or what to do next. This modal is that statement: it names the role and
 * gives the ONE next step that exists at this point — create an organization.
 *
 * It deliberately does NOT give invite instructions. Invitations are org-scoped: a person is
 * invited into an organization with a role in that organization. A freshly claimed platform admin
 * holds no membership and the deployment holds no org, so "go invite someone" names a screen with
 * nothing on it. Inviting is org-admin work, and it comes after an org exists.
 *
 * The flag is read once at mount (not on every render) and cleared when the modal is dismissed, so
 * the disclosure survives the post-claim navigation but is never shown twice.
 */
export function PlatformAdminWelcomeModal() {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const [opened, setOpened] = useState(
    () => localStorage.getItem(CLAIMED_ADMIN_FLAG) === "1",
  );

  const close = () => {
    localStorage.removeItem(CLAIMED_ADMIN_FLAG);
    setOpened(false);
  };

  return (
    <Modal
      opened={opened}
      onClose={close}
      size="lg"
      centered
      title={<Title order={4}>{t("platformAdminWelcome.title")}</Title>}
    >
      {/* The test id sits on the body, not the Modal root: Mantine keeps the root element mounted
          while closed, so a root-level id would report the modal as present after dismissal. */}
      <Stack gap="md" data-testid="platform-admin-welcome">
        <Text>{t("platformAdminWelcome.body")}</Text>
        <div>
          <Text fw={600} mb="xs">
            {t("platformAdminWelcome.orgHeading")}
          </Text>
          <Text mb="xs">{t("platformAdminWelcome.orgBody")}</Text>
          <List type="ordered" spacing="xs">
            <List.Item>{t("platformAdminWelcome.orgStep1")}</List.Item>
            <List.Item>{t("platformAdminWelcome.orgStep2")}</List.Item>
            <List.Item>{t("platformAdminWelcome.orgStep3")}</List.Item>
          </List>
        </div>
        <Group justify="flex-end">
          <Button variant="subtle" onClick={close} data-testid="platform-admin-welcome-dismiss">
            {t("platformAdminWelcome.dismiss")}
          </Button>
          <Button
            data-testid="platform-admin-welcome-orgs"
            onClick={() => {
              close();
              navigate("/admin/orgs");
            }}
          >
            {t("platformAdminWelcome.goToOrgs")}
          </Button>
        </Group>
      </Stack>
    </Modal>
  );
}
