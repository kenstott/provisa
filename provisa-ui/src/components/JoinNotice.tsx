// Copyright (c) 2026 Kenneth Stott
// Canary: d83ae114-81d4-4d5c-b79a-0412df654833
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { Alert, Button, Group, Modal, Stack, Text } from "@mantine/core";
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
    (m) =>
      m.acknowledged === false &&
      ANNOUNCED.includes(m.joined_via ?? "") &&
      // A per_visitor ephemeral membership (sandbox invite) was never a choice to join an org --
      // it's a throwaway environment minted for one visitor, so there is nothing to announce.
      !m.env_name,
  );
  if (!pending) return null;

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
      title={t("joinNotice.title", { org: pending.org_name })}
      centered
      closeOnClickOutside={false}
      closeOnEscape={false}
      withCloseButton={false}
      data-testid="join-notice-modal"
    >
      <Stack gap="md">
        <Text data-testid="join-notice-reason">
          {t(`joinNotice.${pending.joined_via}`, { org: pending.org_name })}
        </Text>
        {error && (
          <Alert color="red" data-testid="join-notice-error">
            {error}
          </Alert>
        )}
        <Group justify="flex-end">
          {UNASKED_FOR.includes(pending.joined_via ?? "") && (
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
            {t("joinNotice.ack")}
          </Button>
        </Group>
      </Stack>
    </Modal>
  );
}
