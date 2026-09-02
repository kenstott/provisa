// Copyright (c) 2026 Kenneth Stott
// Canary: d83ae114-81d4-4d5c-b79a-0412df654833
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { Alert, Button, Group, List, Modal, Stack, Text } from "@mantine/core";
import { useState } from "react";
import { useTranslation } from "react-i18next";
import { acknowledgeJoin, leaveOrg } from "../api/admin";
import { useAuth } from "../context/AuthContext";

// REQ-1478: memberships that need explaining. "created" is the user's own act and never announced;
// a null joined_via predates the column and explains nothing, so it is not announced either.
const ANNOUNCED = ["auto_join", "invite", "admin"];

// The two ways a membership can appear without the user asking for it. An invitee chose to accept,
// so they are told what happened but not offered the exit.
const UNASKED_FOR = ["auto_join", "admin"];

export function JoinNotice() {
  const { t } = useTranslation();
  const { orgMemberships, refresh } = useAuth();
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const pending = orgMemberships.find(
    (m) => m.acknowledged === false && ANNOUNCED.includes(m.joined_via ?? ""),
  );
  if (!pending) return null;

  // A per_visitor ephemeral membership (sandbox invite) is not a choice to join an org -- it is a
  // throwaway environment minted for one visitor. "You were added to Sandbox" would explain the
  // wrong thing, so the sandbox gets its own explanation: what the environment is, what may be
  // done in it, and when it goes away. Hardcoded rather than translated, like the sandbox banner
  // on LoginPage: the sandbox is served in English from cloud.provisa.dev.
  const isSandbox = Boolean(pending.env_name);

  async function acknowledge(orgId: string) {
    setBusy(true);
    try {
      await acknowledgeJoin(orgId);
      await refresh();
    } finally {
      setBusy(false);
    }
  }

  async function leave(orgId: string) {
    setBusy(true);
    setError(null);
    try {
      await leaveOrg(orgId);
      await refresh();
    } catch (e) {
      setError(e instanceof Error ? e.message : t("joinNotice.leaveError"));
      setBusy(false);
    }
  }

  return (
    <Modal
      opened
      onClose={() => {}}
      title={
        isSandbox
          ? "Welcome to the Provisa sandbox"
          : t("joinNotice.title", { org: pending.org_name })
      }
      centered
      closeOnClickOutside={false}
      closeOnEscape={false}
      withCloseButton={false}
      // Both this and the tour offer (TourAutoStart) open on the first screen after sign-in, and the
      // tour offer mounts later, so at equal depth it would cover this one. What the environment IS
      // has to be read before an offer to walk around in it, so this notice sits above it.
      zIndex={300}
      data-testid="join-notice-modal"
    >
      <Stack gap="md">
        {isSandbox ? (
          <Stack gap="sm" data-testid="join-notice-sandbox">
            <Text>
              You are on our shared starter instance, in an isolated environment of your own. The
              sample data is a private copy — nothing you do here is visible to another visitor, and
              nothing here touches anyone else's data.
            </Text>
            <List size="sm" spacing="xs">
              <List.Item>
                Almost every operation is open to you: register your own sources, model them, write
                policies and masking rules, and query the result in SQL, GraphQL, or Cypher.
              </List.Item>
              <List.Item>
                Held back are the org-wide controls — switching or managing environments, inviting
                people and conferring roles, org settings, and org-wide observability. Those pages
                stay visible, marked as part of the production system.
              </List.Item>
              <List.Item>
                Your environment is temporary. Signing out wipes it, and it is also reclaimed after
                a day of inactivity. Nothing you build here is meant to be kept.
              </List.Item>
              <List.Item>
                When you are ready for an organization that persists, upgrade to a Starter plan.
              </List.Item>
            </List>
          </Stack>
        ) : (
          <Text data-testid="join-notice-reason">
            {t(`joinNotice.${pending.joined_via}`, { org: pending.org_name })}
          </Text>
        )}
        {error && (
          <Alert color="red" data-testid="join-notice-error">
            {error}
          </Alert>
        )}
        <Group justify="flex-end">
          {!isSandbox && UNASKED_FOR.includes(pending.joined_via ?? "") && (
            <Button
              variant="subtle"
              color="red"
              loading={busy}
              onClick={() => leave(pending.org_id)}
              data-testid="join-notice-leave"
            >
              {t("joinNotice.leave")}
            </Button>
          )}
          <Button
            loading={busy}
            onClick={() => acknowledge(pending.org_id)}
            data-testid="join-notice-ack"
          >
            {isSandbox ? "Start exploring" : t("joinNotice.ack")}
          </Button>
        </Group>
      </Stack>
    </Modal>
  );
}
