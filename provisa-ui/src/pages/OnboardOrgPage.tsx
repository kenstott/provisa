// Copyright (c) 2026 Kenneth Stott
// Canary: 6f2a7d41-9c83-4e15-a0b6-2d7e91f4c8ab
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState } from "react";
import type { FormEvent } from "react";
import { useTranslation } from "react-i18next";
import { useNavigate } from "react-router-dom";
import {
  Alert,
  Box,
  Button,
  Checkbox,
  Loader,
  SegmentedControl,
  Stack,
  Text,
  TextInput,
  Title,
} from "@mantine/core";
import { createOrg, fetchOrgStatus, redeemInvite } from "../api/admin";
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
  const [invite, setInvite] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [phase, setPhase] = useState<"form" | "provisioning" | "joining">("form");

  const handleCreate = async (e: FormEvent) => {
    e.preventDefault();
    setError(null);
    setPhase("provisioning");
    try {
      const created = await createOrg(id, name, includeDemo);
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
      selectOrg(id);
      await refresh();
      navigate("/query");
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
