// Copyright (c) 2026 Kenneth Stott
// Canary: 6b2f1e04-2a7c-4c0e-9d3a-7f1b5c8e9a42
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { Center, Paper, Stack, Text, Title, Group } from "@mantine/core";
import { useTranslation } from "react-i18next";
import { LoginPage } from "./LoginPage";

interface LandingPageProps {
  onLoginSuccess: (token: string) => void;
  authDisabled?: boolean;
}

// Public entry page for unauthenticated visitors (REQ-1266). Rendered standalone —
// above the app shell — so a signed-out visitor lands on a branded page with the
// sign-in card, not the full Sources/Tables/Explore chrome with a bare button. The
// auth card reuses LoginPage verbatim so there is one provider-branch implementation
// (firebase → Google chooser; basic → login/register + invite flow).
export function LandingPage({ onLoginSuccess, authDisabled }: LandingPageProps) {
  const { t } = useTranslation();
  return (
    <Center mih="100vh" p="md">
      <Stack align="center" gap="lg" style={{ width: "100%", maxWidth: 420 }}>
        <Group gap="sm" align="center">
          <svg
            viewBox="0 0 100 100"
            width="40"
            height="40"
            role="img"
            aria-hidden="true"
          >
            <g fill="currentColor">
              <rect x="30" y="18" width="15" height="64" rx="7" />
              <circle cx="52" cy="35" r="22" />
            </g>
            <circle cx="52" cy="35" r="10.5" fill="var(--surface)" />
            <circle cx="52" cy="35" r="4.5" fill="#10B981" />
          </svg>
          <Title order={1}>{t("navBar.brand")}</Title>
        </Group>
        <Text c="dimmed" ta="center">
          {t("landingPage.tagline")}
        </Text>
        <Paper withBorder radius="md" p="lg" style={{ width: "100%" }}>
          <LoginPage onLoginSuccess={onLoginSuccess} authDisabled={authDisabled} />
        </Paper>
      </Stack>
    </Center>
  );
}
