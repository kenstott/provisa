// Copyright (c) 2026 Kenneth Stott
// Canary: f5fce682-708d-4b2e-919e-4ebbd73979a2
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1576, REQ-1923: Admin › Email holds two different things, each under its own heading so
// neither is taken for the other:
//
// - the mail Provisa SENDS (invitations and other notifications): the deployment's, opened by
//   platform_settings;
// - the mail platforms the organisation's sources CONNECT TO (Google Workspace, Microsoft 365):
//   the organisation's, opened by org_settings.
//
// A caller sees the half whose right they hold, and only that half.

import { Divider, Stack, Text, Title } from "@mantine/core";
import { useTranslation } from "react-i18next";
import { useAuth } from "../../context/AuthContext";
import { hasCapability } from "../../lib/capabilities";
import { MailPlatformsSection } from "./MailPlatformsSection";
import { MailTab } from "./MailTab";

export function EmailTab() {
  const { t } = useTranslation();
  const { capabilities } = useAuth();
  const sends = hasCapability(capabilities, "platform_settings");
  const connects = hasCapability(capabilities, "org_settings");
  return (
    <Stack gap="xl">
      {connects && (
        <section data-testid="email-mail-platforms">
          <Title order={3}>{t("mailPlatforms.platformsTitle")}</Title>
          <Text size="sm" c="dimmed" mb="md">
            {t("mailPlatforms.platformsHelp")}
          </Text>
          <MailPlatformsSection />
        </section>
      )}
      {sends && connects && <Divider />}
      {sends && (
        <section data-testid="email-outgoing">
          <Title order={3}>{t("mailPlatforms.sentTitle")}</Title>
          <Text size="sm" c="dimmed" mb="md">
            {t("mailPlatforms.sentHelp")}
          </Text>
          <MailTab />
        </section>
      )}
    </Stack>
  );
}
