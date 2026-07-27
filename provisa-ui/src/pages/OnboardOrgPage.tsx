// Copyright (c) 2026 Kenneth Stott
// Canary: 6f2a7d41-9c83-4e15-a0b6-2d7e91f4c8ab
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useState } from "react";
import type { FormEvent } from "react";
import { useTranslation } from "react-i18next";
import { useNavigate } from "react-router-dom";
import {
  Alert,
  Box,
  Button,
  Checkbox,
  Divider,
  Group,
  List,
  Loader,
  SegmentedControl,
  Stack,
  Text,
  TextInput,
  ThemeIcon,
  Title,
} from "@mantine/core";
import { PartyPopper, ShieldCheck, UserPlus } from "lucide-react";
import type { PendingInvite } from "../api/admin";
import { createOrg, fetchMyInvites, fetchOrgStatus, redeemInvite } from "../api/admin";
import { useAuth } from "../context/AuthContext";

// REQ-1266: a member-less authenticated user either self-creates an org OR joins an existing one
// with an invite code. Create returns immediately with provisioning_state="provisioning"; we poll
// /status until ready/failed. Join redeems the invite (server grants membership synchronously).
// Both then refetch identity (so the new membership clears the onboarding gate) and route in.
export function OnboardOrgPage() {
  const { t } = useTranslation();
  const { selectOrg, refresh } = useAuth();
  const navigate = useNavigate();
  const [mode, setMode] = useState<"create" | "join">("create");
  const [id, setId] = useState("");
  const [name, setName] = useState("");
  const [includeDemo, setIncludeDemo] = useState(false);
  const [emailRule, setEmailRule] = useState("");
  const [autoJoin, setAutoJoin] = useState(false);
  const [autoJoinRole, setAutoJoinRole] = useState("");
  const [invite, setInvite] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [phase, setPhase] = useState<"form" | "provisioning" | "joining" | "welcome">("form");
  // REQ-1287: an invited user should be TOLD they were invited, not asked to produce a token they
  // may never have kept. Invites addressed to this identity's email are offered as one-click joins;
  // the token field stays for link invites, which are addressed to nobody.
  const [pendingInvites, setPendingInvites] = useState<PendingInvite[]>([]);

  useEffect(() => {
    let cancelled = false;
    fetchMyInvites()
      .then((invites) => {
        if (cancelled) return;
        setPendingInvites(invites);
      })
      .catch(() => {
        // A failed lookup must not block org creation — the token field still works. Nothing to
        // show, so leave the list empty rather than reporting an error the user cannot act on.
        if (!cancelled) setPendingInvites([]);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  const acceptInvite = async (token: string) => {
    setError(null);
    setPhase("joining");
    try {
      const { org_id } = await redeemInvite(token);
      selectOrg(org_id);
      await refresh();
      navigate("/query");
    } catch (err) {
      setError(err instanceof Error ? err.message : t("onboardOrg.joinFailed"));
      setPhase("form");
    }
  };

  const handleCreate = async (e: FormEvent) => {
    e.preventDefault();
    setError(null);
    setPhase("provisioning");
    try {
      const created = await createOrg(id, name, includeDemo, {
        emailRule: emailRule.trim() || null,
        autoJoin,
        autoJoinRole: autoJoinRole.trim() || null,
      });
      let state = created.provisioning_state;
      // Bounded poll — the background provisioning task flips the row.
      for (let i = 0; i < 300 && state === "provisioning"; i++) {
        await new Promise((r) => setTimeout(r, 1000));
        const status = await fetchOrgStatus(id);
        state = status.provisioning_state;
        if (state === "failed") {
          throw new Error(status.provisioning_error || t("onboardOrg.provisionFailed"));
        }
      }
      if (state !== "ready") throw new Error(t("onboardOrg.provisionTimeout"));
      // Bind the new org + refresh identity so the org_admin grant resolves BEFORE the welcome
      // screen offers links into /team (user_management) and /security/roles (access_config).
      selectOrg(id);
      await refresh();
      setPhase("welcome");
    } catch (err) {
      setError(err instanceof Error ? err.message : t("onboardOrg.createFailed"));
      setPhase("form");
    }
  };

  const handleJoin = async (e: FormEvent) => {
    e.preventDefault();
    setError(null);
    setPhase("joining");
    try {
      const { org_id } = await redeemInvite(invite.trim());
      selectOrg(org_id);
      await refresh();
      navigate("/query");
    } catch (err) {
      setError(err instanceof Error ? err.message : t("onboardOrg.joinFailed"));
      setPhase("form");
    }
  };

  if (phase === "welcome") {
    return (
      <Box maw={560} mx="auto" my={80} data-testid="onboard-org-welcome">
        <Stack gap="lg">
          <Group gap="sm">
            <ThemeIcon size="xl" radius="xl" color="green" variant="light">
              <PartyPopper size={22} />
            </ThemeIcon>
            <div>
              <Title order={2}>{t("onboardOrg.welcomeTitle", { name })}</Title>
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
            <Button data-testid="onboard-welcome-invite" onClick={() => navigate("/team")}>
              {t("onboardOrg.welcomeInviteButton")}
            </Button>
            <Button
              variant="default"
              data-testid="onboard-welcome-roles"
              onClick={() => navigate("/security/roles")}
            >
              {t("onboardOrg.welcomeRolesButton")}
            </Button>
            <Button
              variant="subtle"
              data-testid="onboard-welcome-continue"
              onClick={() => navigate("/query")}
            >
              {t("onboardOrg.welcomeContinueButton")}
            </Button>
          </Group>
        </Stack>
      </Box>
    );
  }

  return (
    <Box maw={480} mx="auto" my={80} data-testid="onboard-org-page">
      <Title order={2}>{t("onboardOrg.title")}</Title>
      <Text c="dimmed" size="sm" mb="lg">
        {t("onboardOrg.subtitle")}
      </Text>

      {phase === "provisioning" ? (
        <Stack gap="md" align="center" data-testid="onboard-org-provisioning">
          <Loader />
          <Text>{t("onboardOrg.provisioning")}</Text>
        </Stack>
      ) : phase === "joining" ? (
        <Stack gap="md" align="center" data-testid="onboard-org-joining">
          <Loader />
          <Text>{t("onboardOrg.joining")}</Text>
        </Stack>
      ) : (
        <Stack gap="lg">
          {pendingInvites.length > 0 && (
            <Alert
              variant="light"
              color="blue"
              icon={<UserPlus size={18} />}
              title={t("onboardOrg.pendingInvitesTitle")}
              data-testid="onboard-pending-invites"
            >
              <Stack gap="sm">
                <Text size="sm">{t("onboardOrg.pendingInvitesBody")}</Text>
                {pendingInvites.map((inv) => (
                  <Group key={inv.token} justify="space-between" wrap="nowrap">
                    <Text size="sm" fw={500}>
                      {inv.org_name}
                    </Text>
                    <Button
                      size="xs"
                      data-testid={`onboard-accept-invite-${inv.org_id}`}
                      onClick={() => void acceptInvite(inv.token)}
                    >
                      {t("onboardOrg.acceptInvite")}
                    </Button>
                  </Group>
                ))}
              </Stack>
            </Alert>
          )}
          <SegmentedControl
            fullWidth
            data-testid="onboard-org-mode"
            value={mode}
            onChange={(v) => {
              setMode(v as "create" | "join");
              setError(null);
            }}
            data={[
              { label: t("onboardOrg.modeCreate"), value: "create" },
              { label: t("onboardOrg.modeJoin"), value: "join" },
            ]}
          />

          {mode === "create" ? (
            <form onSubmit={handleCreate}>
              <Stack gap="md">
                <TextInput
                  id="onboard-org-id"
                  data-testid="onboard-org-id"
                  label={t("onboardOrg.orgIdLabel")}
                  description={t("onboardOrg.orgIdDesc")}
                  value={id}
                  onChange={(e) => setId(e.currentTarget.value)}
                  required
                  pattern="[A-Za-z_][A-Za-z0-9_]*"
                />
                <TextInput
                  id="onboard-org-name"
                  data-testid="onboard-org-name"
                  label={t("onboardOrg.orgNameLabel")}
                  value={name}
                  onChange={(e) => setName(e.currentTarget.value)}
                  required
                />
                <Checkbox
                  data-testid="onboard-org-demo"
                  label={t("onboardOrg.includeDemoLabel")}
                  description={t("onboardOrg.includeDemoDesc")}
                  checked={includeDemo}
                  onChange={(e) => setIncludeDemo(e.currentTarget.checked)}
                />
                <TextInput
                  id="onboard-org-email-rule"
                  data-testid="onboard-org-email-rule"
                  label={t("onboardOrg.emailRuleLabel")}
                  description={t("onboardOrg.emailRuleDesc")}
                  placeholder="@acme\.com$"
                  value={emailRule}
                  onChange={(e) => setEmailRule(e.currentTarget.value)}
                />
                <Checkbox
                  data-testid="onboard-org-auto-join"
                  label={t("onboardOrg.autoJoinLabel")}
                  description={t("onboardOrg.autoJoinDesc")}
                  checked={autoJoin}
                  onChange={(e) => setAutoJoin(e.currentTarget.checked)}
                />
                {autoJoin && (
                  <TextInput
                    id="onboard-org-auto-join-role"
                    data-testid="onboard-org-auto-join-role"
                    label={t("onboardOrg.autoJoinRoleLabel")}
                    description={t("onboardOrg.autoJoinRoleDesc")}
                    placeholder="analyst"
                    value={autoJoinRole}
                    onChange={(e) => setAutoJoinRole(e.currentTarget.value)}
                    required
                  />
                )}
                {error && (
                  <Alert variant="light" color="red" data-testid="onboard-org-error">
                    {error}
                  </Alert>
                )}
                <Button type="submit" data-testid="onboard-org-submit">
                  {t("onboardOrg.createButton")}
                </Button>
              </Stack>
            </form>
          ) : (
            <form onSubmit={handleJoin}>
              <Stack gap="md">
                <TextInput
                  id="onboard-org-invite"
                  data-testid="onboard-org-invite"
                  label={t("onboardOrg.inviteLabel")}
                  description={t("onboardOrg.inviteDesc")}
                  value={invite}
                  onChange={(e) => setInvite(e.currentTarget.value)}
                  required
                />
                {error && (
                  <Alert variant="light" color="red" data-testid="onboard-org-error">
                    {error}
                  </Alert>
                )}
                <Button type="submit" data-testid="onboard-org-join-submit">
                  {t("onboardOrg.joinButton")}
                </Button>
              </Stack>
            </form>
          )}
        </Stack>
      )}
    </Box>
  );
}
