// Copyright (c) 2026 Kenneth Stott
// Canary: 4e7c9a02-1b6d-4f38-8a55-c0d3e7b91f24
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { lazy, Suspense, useEffect, useState } from "react";
import type { ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { Box, Button, Group, Stack, Text, Title } from "@mantine/core";
import { useAuth } from "../context/AuthContext";
import { fetchBootstrapStatus } from "../api/admin";
import { clearSessionState } from "../lib/session";
import { storedToken } from "../lib/sessionToken";
import { PageLoading } from "./PageLoading";
import { JoinNotice } from "./JoinNotice";

const OnboardOrgPage = lazy(() =>
  import("../pages/OnboardOrgPage").then((m) => ({ default: m.OnboardOrgPage })),
);

/**
 * REQ-1266: gates the app shell behind org membership. An authenticated user with no org
 * memberships is routed to self-service org creation; everyone else falls through to the shell.
 * Waits for the AuthProvider bootstrap (loading) so the gate keys off the settled membership set.
 */
export function OnboardGate({
  children,
  onSessionExpired,
}: {
  children: ReactNode;
  onSessionExpired: () => void;
}) {
  const { t } = useTranslation();
  const {
    orgMemberships,
    assignments,
    availableRoles,
    loading,
    authEnabled,
    userId,
    identityErrorStatus,
    refresh,
  } = useAuth();
  const token = storedToken();
  // REQ-1286: an unresolved identity has two causes that must NOT share a screen.
  //
  //   /auth/me returned 5xx (or never reached the server) -> the deployment is failing. Telling
  //   the user their account lacks access is a false statement about an access decision that was
  //   never made, and it sends them to ask an administrator for an invitation that would not help.
  //
  //   /auth/me returned 401/403 -> the credential is genuinely not accepted: expired, or an
  //   identity this deployment has not granted access to. Only THIS is an access answer.
  //
  // Presence of the token string alone is never proof of a live session; only a resolved identity
  // is (REQ-1266/REQ-1267).
  const unresolved = authEnabled && !!token && !loading && !userId;
  const serverFailed =
    unresolved &&
    identityErrorStatus !== null &&
    identityErrorStatus !== 401 &&
    identityErrorStatus !== 403;
  // REQ-1289: a credential this deployment will not accept is not a destination. The token is
  // stale — expired, or from before this deployment knew this identity — and nothing the user can
  // do on a "no access" panel changes that, so drop it and put them back on sign-in where they can
  // authenticate or create an account. The old panel stranded them and told them to ask an
  // administrator who, on a deployment whose admin slot is still unclaimed, does not exist.
  const credentialRejected = unresolved && !serverFailed;
  useEffect(() => {
    if (!credentialRejected) return;
    // REQ-1326: this ends the session, so it clears what a sign-out clears. Dropping the token
    // alone left provisa_org/provisa_role and the persisted Apollo snapshot for the next identity.
    clearSessionState();
    onSessionExpired();
  }, [credentialRejected, onSessionExpired]);

  // REQ-1292: an unclaimed platform-admin slot outranks org onboarding. A browser that still holds
  // a valid credential would otherwise skip the sign-in page entirely — the only place the
  // first-login disclosure (REQ-1288) renders — and land on "create an organization" on a
  // deployment that has no administrator at all. Drop the credential and send them back through
  // sign-in, where they are told what claiming means before they pick a provider and claim it.
  // null = the answer is still in flight; nothing may be decided on it yet.
  const [slotUnclaimed, setSlotUnclaimed] = useState<boolean | null>(null);
  const [slotError, setSlotError] = useState<string | null>(null);
  useEffect(() => {
    if (!authEnabled || !userId) return;
    let cancelled = false;
    fetchBootstrapStatus()
      .then((unclaimed) => {
        if (!cancelled) setSlotUnclaimed(unclaimed);
      })
      .catch((e: unknown) => {
        if (!cancelled) setSlotError(e instanceof Error ? e.message : String(e));
      });
    return () => {
      cancelled = true;
    };
  }, [authEnabled, userId]);
  const firstLoginPending = authEnabled && !!userId && slotUnclaimed === true;
  useEffect(() => {
    if (!firstLoginPending) return;
    // REQ-1326: same as above — an unclaimed admin slot invalidates the whole session, not just
    // the credential. A stale provisa_org here rides on X-Org-Provisa into the claiming sign-in.
    clearSessionState();
    onSessionExpired();
  }, [firstLoginPending, onSessionExpired]);

  if (slotError) {
    return (
      <Box maw={480} mx="auto" my={80} data-testid="identity-unavailable">
        <Stack gap="md">
          <Title order={2}>{t("onboardGate.unavailableTitle")}</Title>
          <Text c="dimmed">{slotError}</Text>
          <Group>
            <Button data-testid="identity-unavailable-retry" onClick={() => void refresh()}>
              {t("onboardGate.retry")}
            </Button>
          </Group>
        </Stack>
      </Box>
    );
  }
  if (serverFailed) {
    return (
      <Box maw={480} mx="auto" my={80} data-testid="identity-unavailable">
        <Stack gap="md">
          <Title order={2}>{t("onboardGate.unavailableTitle")}</Title>
          <Text c="dimmed">{t("onboardGate.unavailableBody")}</Text>
          <Group>
            <Button data-testid="identity-unavailable-retry" onClick={() => void refresh()}>
              {t("onboardGate.retry")}
            </Button>
          </Group>
        </Stack>
      </Box>
    );
  }
  if (unresolved) {
    // The effect above has already cleared the token and asked App to re-render the sign-in page;
    // render nothing rather than flashing a panel the user cannot act on.
    return null;
  }
  if (firstLoginPending) {
    // The effect above cleared the credential and asked App to re-render sign-in, where the
    // first-login disclosure lives. Render nothing rather than flashing org onboarding.
    return null;
  }
  // The shell may not be mounted on an unsettled identity. Every branch below keys off `userId`,
  // and rendering children while /auth/me is still in flight mounts the whole app only to tear it
  // down the moment the answer arrives — taking with it anything the shell had already started.
  // REQ-1472: that remount is what killed a tour launched by ?tour=1; its provider was unmounted
  // mid-step and the query param that would have restarted it was already consumed.
  if (authEnabled && loading) {
    return (
      <div className="page">
        <PageLoading />
      </div>
    );
  }
  if (authEnabled && userId && slotUnclaimed === null) {
    // The admin-slot answer decides which screen this is; do not guess it.
    return (
      <div className="page">
        <PageLoading />
      </div>
    );
  }
  // REQ-1337: a control-plane principal (one holding the `cross_org` RIGHT) may hold zero org
  // memberships by design — the onboarding flow is for tenant users who must join or create an org.
  // The assigned role ids from /auth/me are resolved to the rights those roles carry; no role name
  // is tested here.
  const assignedRoleIds = new Set(assignments.map((a) => a.role_id));
  const canActCrossOrg = availableRoles.some(
    (r) => assignedRoleIds.has(r.id) && r.capabilities.includes("cross_org"),
  );
  if (
    authEnabled &&
    token &&
    !loading &&
    userId &&
    orgMemberships.length === 0 &&
    !canActCrossOrg
  ) {
    return (
      <Suspense
        fallback={
          <div className="page">
            <PageLoading />
          </div>
        }
      >
        <OnboardOrgPage />
      </Suspense>
    );
  }
  // REQ-1478: a membership the user did not create is explained here, above the shell, on the first
  // sign-in after it was granted.
  return (
    <>
      <JoinNotice />
      {children}
    </>
  );
}
