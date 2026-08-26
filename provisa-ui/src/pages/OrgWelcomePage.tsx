// Copyright (c) 2026 Kenneth Stott
// Canary: 3a9e51c7-0b64-4d2f-8e17-6c25b0d9f483
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
import { Box, Button, Divider, Group, List, Stack, Text, ThemeIcon, Title } from "@mantine/core";
import { PartyPopper, ShieldCheck, UserPlus } from "lucide-react";
import { useAuth } from "../context/AuthContext";
import { OrgAddressModal } from "../components/OrgAddressModal";
import { isOrgSubdomainHost, orgFromHost, orgOrigin } from "../lib/authHost";

// REQ-1276: where the org create lands. Onboarding runs on the control plane, but the org it built
// lives at its own host, so the last step of the create is a document load to that host — and this
// is the page it loads. Being a route inside the shell rather than a phase of the onboarding page
// is what puts `{org}.provisa.dev` in the address bar: the welcome is read at the address it is
// welcoming you to, and the links off it are ordinary in-app navigation from there.
export function OrgWelcomePage() {
  const { t } = useTranslation();
  const { activeOrgId, orgMemberships } = useAuth();
  const navigate = useNavigate();

  // The org this document is bound to. On an org host the Host IS the binding; a deployment whose
  // host names no org (a desktop install) binds by the selected membership instead.
  const onOrgHost = isOrgSubdomainHost();
  const orgId = onOrgHost ? orgFromHost() : activeOrgId;
  const membership = orgMemberships.find((m) => m.org_id === orgId);
  // The membership gate holds an account with none of them on onboarding, and this route renders
  // inside the shell — so a miss here is a broken binding, not a case to paper over with a name.
  if (!membership) throw new Error(`welcome: no membership for org ${orgId}`);

  // The address the org answers at, told only where the URL bar is not already showing it: this
  // page is also rendered on the control plane when the create ends with something to report,
  // and there the org's own host is still news. Null where the host names no org at all.
  const address = onOrgHost ? null : orgOrigin(membership.org_id);
  const [addressShown, setAddressShown] = useState(true);

  // Leaving the welcome enters the org, which on the control plane means leaving the control plane:
  // a document load, so the org binds by Host and the identity bootstrap re-runs against it.
  const enter = (path: string) => {
    if (address) window.location.assign(`${address}${path}`);
    else navigate(path);
  };

  return (
    <Box maw={560} mx="auto" my={80} data-testid="org-welcome">
      {address && (
        <OrgAddressModal
          url={address}
          opened={addressShown}
          onClose={() => setAddressShown(false)}
        />
      )}
      <Stack gap="lg">
        <Group gap="sm">
          <ThemeIcon size="xl" radius="xl" color="green" variant="light">
            <PartyPopper size={22} />
          </ThemeIcon>
          <div>
            <Title order={2}>{t("onboardOrg.welcomeTitle", { name: membership.org_name })}</Title>
            <Text c="dimmed" size="sm">
              {t("onboardOrg.welcomeAdmin")}
            </Text>
          </div>
        </Group>

        <Divider />

        <Group gap="sm" align="flex-start" wrap="nowrap">
          <ThemeIcon size="lg" radius="md" variant="light">
            <UserPlus size={18} />
          </ThemeIcon>
          <div>
            <Text fw={600}>{t("onboardOrg.welcomeInviteHeading")}</Text>
            <Text c="dimmed" size="sm">
              {t("onboardOrg.welcomeInviteBody")}
            </Text>
          </div>
        </Group>

        <Group gap="sm" align="flex-start" wrap="nowrap">
          <ThemeIcon size="lg" radius="md" variant="light">
            <ShieldCheck size={18} />
          </ThemeIcon>
          <div>
            <Text fw={600}>{t("onboardOrg.welcomeGrantHeading")}</Text>
            <Text c="dimmed" size="sm">
              {t("onboardOrg.welcomeGrantBody")}
            </Text>
            <List size="sm" c="dimmed" mt={4}>
              <List.Item>{t("onboardOrg.welcomeGrantStep1")}</List.Item>
              <List.Item>{t("onboardOrg.welcomeGrantStep2")}</List.Item>
            </List>
          </div>
        </Group>

        <Divider />

        <Group>
          <Button data-testid="onboard-welcome-invite" onClick={() => enter("/team")}>
            {t("onboardOrg.welcomeInviteButton")}
          </Button>
          <Button
            variant="default"
            data-testid="onboard-welcome-roles"
            onClick={() => enter("/security/roles")}
          >
            {t("onboardOrg.welcomeRolesButton")}
          </Button>
          <Button
            variant="subtle"
            data-testid="onboard-welcome-continue"
            onClick={() => enter("/query")}
          >
            {t("onboardOrg.welcomeContinueButton")}
          </Button>
        </Group>
      </Stack>
    </Box>
  );
}
