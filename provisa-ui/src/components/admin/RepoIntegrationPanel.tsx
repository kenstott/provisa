// Copyright (c) 2026 Kenneth Stott
// Canary: c4e70b1d-58a3-4f92-b6d7-1a0e93cf25b8
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useState } from "react";
import { Alert, Button, Checkbox, Group, Stack, Text, TextInput, Title } from "@mantine/core";
import { notifications } from "@mantine/notifications";
import { useTranslation } from "react-i18next";
import {
  createRepoRemote,
  fetchRepoIntegration,
  probeRepoRemote,
  saveRepoIntegration,
  type RemoteProbe,
} from "../../api/environments";

/**
 * REQ-1527: where the org's projection is mirrored, and where its status is reported.
 *
 * The remote is sent and stored VERBATIM — a secret reference such as
 * `https://${env:GIT_TOKEN}@github.com/acme/model.git` is what is kept and what is read back, and
 * it is resolved at push time and nowhere else. That is why the field is a plain text input rather
 * than a password one: what belongs in it is a reference, never a token, and masking it would
 * imply the opposite.
 */
export function RepoIntegrationPanel({ orgId }: { orgId: string }) {
  const { t } = useTranslation();
  const [remote, setRemote] = useState("");
  const [webhook, setWebhook] = useState("");
  const [loaded, setLoaded] = useState(false);
  const [saving, setSaving] = useState(false);
  // REQ-1537: what the last check found at the address in the field. Cleared the moment the field
  // changes — a verdict about an address the operator has since edited is worse than no verdict,
  // because "Create it" would then offer to create something other than what is on screen.
  const [probe, setProbe] = useState<RemoteProbe | null>(null);
  const [probing, setProbing] = useState(false);
  const [creating, setCreating] = useState(false);
  const [makePrivate, setMakePrivate] = useState(true);

  useEffect(() => {
    let live = true;
    fetchRepoIntegration(orgId)
      .then((it) => {
        if (!live) return;
        setRemote(it.remote ?? "");
        setWebhook(it.status_webhook ?? "");
        setLoaded(true);
      })
      .catch((err: Error) => notifications.show({ color: "red", message: err.message }));
    return () => {
      live = false;
    };
  }, [orgId]);

  async function save() {
    setSaving(true);
    try {
      // Both halves are written whole: an org that means to stop mirroring says so by clearing the
      // field, which is the null the server reads as "no remote".
      const it = await saveRepoIntegration(orgId, {
        remote: remote.trim() === "" ? null : remote.trim(),
        status_webhook: webhook.trim() === "" ? null : webhook.trim(),
      });
      setRemote(it.remote ?? "");
      setWebhook(it.status_webhook ?? "");
      notifications.show({ color: "green", message: t("environmentsTab.integrationSaved") });
      // The stored value is now the one on screen, so a verdict about the typed candidate still
      // describes it. Re-check it so a saved remote that does not exist yet says so immediately
      // rather than waiting for the first push to fail (REQ-1537).
      await check(it.remote ?? "");
    } catch (err) {
      notifications.show({ color: "red", message: (err as Error).message });
    } finally {
      setSaving(false);
    }
  }

  /**
   * REQ-1537: probe at CONFIGURE time, which is the only time the answer can change anything. A
   * push that discovers the repository is missing has already failed; a check while the field is
   * open turns the same discovery into a question with two good answers — fix the address, or ask
   * for the repository to be created.
   */
  async function check(candidate: string) {
    const address = candidate.trim();
    if (address === "") return;
    setProbing(true);
    setProbe(null);
    try {
      setProbe(await probeRepoRemote(orgId, address));
    } catch (err) {
      notifications.show({ color: "red", message: (err as Error).message });
    } finally {
      setProbing(false);
    }
  }

  /** Creating is the operator's answer to the probe, never Provisa's own move (REQ-1537). */
  async function create() {
    setCreating(true);
    try {
      const made = await createRepoRemote(orgId, remote.trim(), makePrivate);
      setProbe(made);
      notifications.show({
        color: "green",
        message: t("environmentsTab.probeCreated", { kind: made.kind, target: made.target }),
      });
    } catch (err) {
      notifications.show({ color: "red", message: (err as Error).message });
    } finally {
      setCreating(false);
    }
  }

  return (
    <Stack gap="sm" data-testid="repo-integration">
      <Title order={4}>{t("environmentsTab.integrationTitle")}</Title>
      <Alert color="blue" variant="light">
        {t("environmentsTab.integrationSecretHint")}
      </Alert>
      <TextInput
        label={t("environmentsTab.remoteLabel")}
        description={t("environmentsTab.remoteHelp")}
        placeholder="https://${env:GIT_TOKEN}@github.com/acme/model.git"
        value={remote}
        onChange={(e) => {
          setRemote(e.currentTarget.value);
          setProbe(null);
        }}
        data-testid="repo-remote-input"
        disabled={!loaded}
      />
      <Group gap="xs" align="center">
        <Button
          variant="default"
          onClick={() => check(remote)}
          loading={probing}
          disabled={!loaded || remote.trim() === ""}
          data-testid="repo-remote-probe"
        >
          {t("environmentsTab.probe")}
        </Button>
        {probing && (
          <Text size="sm" c="dimmed">
            {t("environmentsTab.probeChecking")}
          </Text>
        )}
      </Group>
      {probe?.exists && (
        <Alert color="green" variant="light" data-testid="repo-remote-found">
          {t("environmentsTab.probeFound", { kind: probe.kind, target: probe.target })}
        </Alert>
      )}
      {probe && !probe.exists && (
        <Alert color="yellow" variant="light" data-testid="repo-remote-missing">
          <Stack gap="xs">
            <Text size="sm">
              {t("environmentsTab.probeMissing", { target: probe.target, detail: probe.detail })}
            </Text>
            {probe.creatable ? (
              <Group gap="xs" align="center">
                <Button
                  size="xs"
                  onClick={create}
                  loading={creating}
                  data-testid="repo-remote-create"
                >
                  {t("environmentsTab.probeCreate")}
                </Button>
                <Checkbox
                  size="xs"
                  label={t("environmentsTab.probeCreatePrivate")}
                  checked={makePrivate}
                  onChange={(e) => setMakePrivate(e.currentTarget.checked)}
                  data-testid="repo-remote-create-private"
                />
              </Group>
            ) : (
              <Text size="sm" c="dimmed" data-testid="repo-remote-uncreatable">
                {t("environmentsTab.probeNotCreatable", { detail: probe.detail })}
              </Text>
            )}
            <Text size="xs" c="dimmed">
              {t("environmentsTab.probeHint")}
            </Text>
          </Stack>
        </Alert>
      )}
      <TextInput
        label={t("environmentsTab.webhookLabel")}
        description={t("environmentsTab.webhookHelp")}
        placeholder="https://ci.example.com/hooks/provisa"
        value={webhook}
        onChange={(e) => setWebhook(e.currentTarget.value)}
        data-testid="repo-webhook-input"
        disabled={!loaded}
      />
      <Group>
        <Button
          onClick={save}
          loading={saving}
          disabled={!loaded}
          data-testid="repo-integration-save"
        >
          {t("environmentsTab.save")}
        </Button>
        <Text size="sm" c="dimmed">
          {t("environmentsTab.integrationProjection")}
        </Text>
      </Group>
    </Stack>
  );
}
