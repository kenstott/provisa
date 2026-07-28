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
 * of what they now are or what to do next. This modal is that statement, and it says exactly three
 * things:
 *
 *   1. They are this deployment's platform_admin AND the administrator of the root org (REQ-1296),
 *      which is already seeded with the demo assets — so there is data behind this modal.
 *   2. Backup platform_admins start with an invitation to join root, then a platform_admin role
 *      assignment in root (REQ-1298). The bootstrap slot is single-use and never reopens, so this
 *      is the only path to a second administrator, and a one-account deployment is a fragile one.
 *   3. The site is now open for org creation: from the next sign-in onward people create their own
 *      org and administer it themselves.
 *
 * The invite step it names is into ROOT specifically — that is platform work. Inviting people into
 * some other org is that org's admin's job, not this user's.
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
            {t("platformAdminWelcome.backupHeading")}
          </Text>
          <Text mb="xs">{t("platformAdminWelcome.backupBody")}</Text>
          <List type="ordered" spacing="xs">
            <List.Item>{t("platformAdminWelcome.backupStep1")}</List.Item>
            <List.Item>{t("platformAdminWelcome.backupStep2")}</List.Item>
            <List.Item>{t("platformAdminWelcome.backupStep3")}</List.Item>
          </List>
        </div>
        <div>
          <Text fw={600} mb="xs">
            {t("platformAdminWelcome.openHeading")}
          </Text>
          <Text>{t("platformAdminWelcome.openBody")}</Text>
        </div>
        <Group justify="flex-end">
          <Button variant="subtle" onClick={close} data-testid="platform-admin-welcome-dismiss">
            {t("platformAdminWelcome.dismiss")}
          </Button>
          <Button
            data-testid="platform-admin-welcome-team"
            onClick={() => {
              close();
              navigate("/team");
            }}
          >
            {t("platformAdminWelcome.goToTeam")}
          </Button>
        </Group>
      </Stack>
    </Modal>
  );
}
