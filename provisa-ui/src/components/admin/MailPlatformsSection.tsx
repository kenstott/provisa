// Copyright (c) 2026 Kenneth Stott
// Canary: 9338abaa-919a-4722-840e-a09ff09ef505
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1923: the mail platforms the organisation's sources connect to. For each platform an
// administrator enters the client the organisation registered with it, once; the address to
// register there is shown ready to copy. Nobody adding a source sees any of this.
//
// The client's secret never comes back: its field starts empty, and leaving it empty keeps the
// one already entered.

import { useCallback, useEffect, useState } from "react";
import {
  Alert,
  Badge,
  Button,
  Card,
  CopyButton,
  Group,
  PasswordInput,
  SimpleGrid,
  Stack,
  Text,
  TextInput,
  Title,
} from "@mantine/core";
import { useTranslation } from "react-i18next";
import {
  deleteMailPlatform,
  fetchMailPlatforms,
  putMailPlatform,
  type MailPlatforms,
} from "../../api/mailPlatforms";
import { useAuth } from "../../context/AuthContext";
import { draftOf, platformMissing, type Draft } from "./mailPlatformDraft";

export function MailPlatformsSection() {
  const { t } = useTranslation();
  const { activeOrgId } = useAuth();
  const [state, setState] = useState<MailPlatforms | null>(null);
  const [drafts, setDrafts] = useState<Record<string, Draft>>({});
  const [busy, setBusy] = useState("");
  const [said, setSaid] = useState<{ platform: string; text: string; bad: boolean } | null>(null);
  const [error, setError] = useState("");

  const load = useCallback(() => {
    if (!activeOrgId) return;
    fetchMailPlatforms(activeOrgId)
      .then((answered) => {
        setState(answered);
        setDrafts(Object.fromEntries(answered.platforms.map((p) => [p.platform, draftOf(p)])));
      })
      .catch((e: unknown) => setError(String(e)));
  }, [activeOrgId]);
  useEffect(load, [load]);

  if (error) {
    return (
      <Alert color="red" variant="light" data-testid="mail-platforms-error">
        {error}
      </Alert>
    );
  }
  if (!state || !activeOrgId) return null;

  const change = (platform: string, patch: Partial<Draft>) =>
    setDrafts((d) => ({ ...d, [platform]: { ...d[platform], ...patch } }));

  const run = async (platform: string, act: () => Promise<unknown>, done: string) => {
    setBusy(platform);
    setSaid(null);
    try {
      await act();
      setSaid({ platform, text: done, bad: false });
      load();
    } catch (e) {
      setSaid({ platform, text: String(e), bad: true });
    } finally {
      setBusy("");
    }
  };

  return (
    <Stack gap="md">
      {state.platforms.map((p) => {
        const draft = drafts[p.platform] ?? draftOf(p);
        const id = `mail-platform-${p.platform}`;
        return (
          <Card key={p.platform} withBorder data-testid={id}>
            <Group justify="space-between" mb="sm">
              <Title order={4}>{t(`mailPlatforms.platform.${p.platform}`)}</Title>
              <Badge color={p.configured ? "green" : "gray"} data-testid={`${id}-state`}>
                {p.configured ? t("mailPlatforms.connected") : t("mailPlatforms.notConnected")}
              </Badge>
            </Group>
            <Stack gap="sm">
              {state.redirect_address ? (
                <>
                  <Group align="flex-end" gap="sm">
                    <TextInput
                      label={t("mailPlatforms.redirectAddress")}
                      description={t(`mailPlatforms.redirectHelp.${p.platform}`)}
                      value={state.redirect_address}
                      readOnly
                      style={{ flex: 1 }}
                      data-testid={`${id}-redirect`}
                    />
                    <CopyButton value={state.redirect_address}>
                      {({ copied, copy }) => (
                        <Button variant="default" onClick={copy} data-testid={`${id}-copy`}>
                          {copied ? t("mailPlatforms.copied") : t("mailPlatforms.copy")}
                        </Button>
                      )}
                    </CopyButton>
                  </Group>
                  <Text size="xs" c="dimmed" data-testid={`${id}-same-address`}>
                    {t("mailPlatforms.sameAddressNote", {
                      origin: new URL(state.redirect_address).origin,
                    })}
                  </Text>
                </>
              ) : (
                <Alert color="yellow" variant="light" data-testid={`${id}-no-redirect`}>
                  {t("mailPlatforms.redirectUnknown")}
                </Alert>
              )}
              <SimpleGrid cols={{ base: 1, sm: 2 }} spacing="sm">
                <TextInput
                  label={t("mailPlatforms.clientId")}
                  required
                  value={draft.client_id}
                  onChange={(e) => change(p.platform, { client_id: e.currentTarget.value })}
                  data-testid={`${id}-client-id`}
                />
                <PasswordInput
                  label={t("mailPlatforms.clientSecret")}
                  description={p.configured ? t("mailPlatforms.clientSecretKeep") : undefined}
                  required={!p.configured}
                  value={draft.client_secret}
                  onChange={(e) => change(p.platform, { client_secret: e.currentTarget.value })}
                  data-testid={`${id}-client-secret`}
                />
                {p.settings_fields.map((field) => (
                  <TextInput
                    key={field}
                    label={t(`mailPlatforms.setting.${field}`)}
                    description={t(`mailPlatforms.settingHelp.${field}`)}
                    required
                    value={draft.settings[field] ?? ""}
                    onChange={(e) =>
                      change(p.platform, {
                        settings: { ...draft.settings, [field]: e.currentTarget.value },
                      })
                    }
                    data-testid={`${id}-setting-${field}`}
                  />
                ))}
              </SimpleGrid>
              <Group gap="sm">
                <Button
                  onClick={() =>
                    run(
                      p.platform,
                      () =>
                        putMailPlatform(activeOrgId, p.platform, {
                          client_id: draft.client_id.trim(),
                          ...(draft.client_secret ? { client_secret: draft.client_secret } : {}),
                          settings: draft.settings,
                        }),
                      t("mailPlatforms.saved"),
                    )
                  }
                  loading={busy === p.platform}
                  disabled={platformMissing(p, draft).length > 0}
                  data-testid={`${id}-save`}
                >
                  {t("mailPlatforms.save")}
                </Button>
                {p.configured && (
                  <Button
                    variant="default"
                    color="red"
                    onClick={() =>
                      run(
                        p.platform,
                        () => deleteMailPlatform(activeOrgId, p.platform),
                        t("mailPlatforms.removed"),
                      )
                    }
                    disabled={busy === p.platform}
                    data-testid={`${id}-remove`}
                  >
                    {t("mailPlatforms.remove")}
                  </Button>
                )}
                {said?.platform === p.platform && (
                  <Text size="sm" c={said.bad ? "red" : "green"} data-testid={`${id}-said`}>
                    {said.text}
                  </Text>
                )}
              </Group>
            </Stack>
          </Card>
        );
      })}
    </Stack>
  );
}
