// Copyright (c) 2026 Kenneth Stott
// Canary: 2e58c0a7-91d4-4b36-8fa1-6d73b90c5e18
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  Alert,
  Badge,
  Button,
  Checkbox,
  Group,
  Loader,
  NumberInput,
  Select,
  Stack,
  Table,
  Text,
  TextInput,
  Title,
} from "@mantine/core";
import { Check, TriangleAlert } from "lucide-react";
import {
  checkMetadataExport,
  fetchMetadataExport,
  publishMetadataExport,
  setMetadataExport,
  type MetadataExportState,
  type MetadataExportUpdate,
  type PublishOutcome,
} from "../../api/metadataExport";

// REQ-1074: configure and operate the per-org metadata export target.
//
// Credentials are entered here and never read back (REQ-1074): the server reports each as
// set/not-set, so an empty field means "leave the stored one alone" rather than "no secret". A
// field is only sent when the administrator typed in it, which is what makes editing the
// endpoint safe for the token.

/** Which credential fields each auth mode uses, so the form asks for exactly those. */
const ENTRA_MODE = "entra";
const BASIC_MODE = "basic";

const AUTH_MODES = [
  { value: "api_key", label: "API key" },
  { value: "bearer", label: "Bearer token" },
  { value: BASIC_MODE, label: "Username and password (Apache Atlas)" },
  { value: ENTRA_MODE, label: "Microsoft Entra (Purview)" },
];

type SecretField = "api_key" | "token" | "entra_client_secret";

export function MetadataExportTab() {
  const { t } = useTranslation();
  const [s, setS] = useState<MetadataExportState | null>(null);
  // Typed credentials, kept apart from `s.config` because the config carries only set/not-set.
  const [secrets, setSecrets] = useState<Partial<Record<SecretField, string>>>({});
  const [busy, setBusy] = useState("");
  const [msg, setMsg] = useState("");
  const [error, setError] = useState("");
  const [health, setHealth] = useState<{ ok: boolean; error?: string } | null>(null);
  const [publish, setPublish] = useState<PublishOutcome | null>(null);

  useEffect(() => {
    fetchMetadataExport()
      .then((state) => {
        setS(state);
        setPublish(state.last_publish);
      })
      .catch((e) => setError(String(e)));
  }, []);

  const patch = (p: Partial<MetadataExportState["config"]>) =>
    setS((prev) => (prev ? { ...prev, config: { ...prev.config, ...p } } : prev));

  const save = async () => {
    if (!s) return;
    setBusy("save");
    setMsg("");
    setError("");
    try {
      const body: MetadataExportUpdate = {
        enabled: s.config.enabled,
        provider: s.config.provider,
        endpoint: s.config.endpoint,
        auth_mode: s.config.auth_mode,
        username: s.config.username,
        entra_tenant_id: s.config.entra_tenant_id,
        entra_client_id: s.config.entra_client_id,
        reconcile_cron: s.config.reconcile_cron,
        timeout_seconds: s.config.timeout_seconds,
        ...secrets,
      };
      await setMetadataExport(body);
      // Re-read rather than assume: the set/not-set flags now reflect what was persisted, and a
      // typed secret is dropped from local state so it is not re-sent on the next save.
      const fresh = await fetchMetadataExport();
      setS(fresh);
      setSecrets({});
      setMsg(t("metadataExportTab.saved"));
    } catch (e) {
      setError(String(e));
    } finally {
      setBusy("");
    }
  };

  const runHealth = async () => {
    setBusy("health");
    setError("");
    setHealth(null);
    try {
      setHealth(await checkMetadataExport());
    } catch (e) {
      setError(String(e));
    } finally {
      setBusy("");
    }
  };

  const runPublish = async () => {
    setBusy("publish");
    setError("");
    try {
      setPublish(await publishMetadataExport());
    } catch (e) {
      setError(String(e));
    } finally {
      setBusy("");
    }
  };

  if (error && !s)
    return (
      <Alert color="red" icon={<TriangleAlert size={16} />} data-testid="metadata-export-error">
        {error}
      </Alert>
    );

  if (!s)
    return (
      <Group gap="xs">
        <Loader size="sm" />
        <Text>{t("metadataExportTab.loading")}</Text>
      </Group>
    );

  if (!s.entitled)
    return (
      <Alert
        color="yellow"
        icon={<TriangleAlert size={16} />}
        data-testid="metadata-export-not-entitled"
        title={t("metadataExportTab.notEntitledTitle")}
      >
        {t("metadataExportTab.notEntitled", { tier: s.required_tier })}
      </Alert>
    );

  const c = s.config;
  const secretField = (field: SecretField, label: string, isSet: boolean) => (
    <TextInput
      label={label}
      type="password"
      data-testid={`metadata-export-${field.replace(/_/g, "-")}`}
      placeholder={isSet ? t("metadataExportTab.secretSet") : t("metadataExportTab.secretUnset")}
      description={t("metadataExportTab.secretHelp")}
      value={secrets[field] ?? ""}
      onChange={(e) => setSecrets({ ...secrets, [field]: e.currentTarget.value })}
    />
  );

  return (
    <Stack maw={860} gap="md">
      <Text c="dimmed" size="sm">
        {t("metadataExportTab.intro")}
      </Text>

      <Checkbox
        label={t("metadataExportTab.enabled")}
        data-testid="metadata-export-enabled"
        checked={c.enabled}
        onChange={(e) => patch({ enabled: e.currentTarget.checked })}
      />
      <Select
        label={t("metadataExportTab.provider")}
        data-testid="metadata-export-provider"
        data={s.providers.map((p) => ({ value: p, label: p }))}
        value={c.provider || null}
        onChange={(v) => patch({ provider: v ?? "" })}
      />
      <TextInput
        label={t("metadataExportTab.endpoint")}
        data-testid="metadata-export-endpoint"
        value={c.endpoint}
        onChange={(e) => patch({ endpoint: e.currentTarget.value })}
      />
      <Select
        label={t("metadataExportTab.authMode")}
        data-testid="metadata-export-auth-mode"
        data={AUTH_MODES}
        value={c.auth_mode}
        onChange={(v) => patch({ auth_mode: v ?? "api_key" })}
      />

      {c.auth_mode === ENTRA_MODE ? (
        <>
          <TextInput
            label={t("metadataExportTab.entraTenantId")}
            data-testid="metadata-export-entra-tenant-id"
            value={c.entra_tenant_id}
            onChange={(e) => patch({ entra_tenant_id: e.currentTarget.value })}
          />
          <TextInput
            label={t("metadataExportTab.entraClientId")}
            data-testid="metadata-export-entra-client-id"
            value={c.entra_client_id}
            onChange={(e) => patch({ entra_client_id: e.currentTarget.value })}
          />
          {secretField(
            "entra_client_secret",
            t("metadataExportTab.entraClientSecret"),
            c.entra_client_secret_set,
          )}
        </>
      ) : c.auth_mode === BASIC_MODE ? (
        <>
          <TextInput
            label={t("metadataExportTab.username")}
            data-testid="metadata-export-username"
            value={c.username}
            onChange={(e) => patch({ username: e.currentTarget.value })}
          />
          {/* The password rides in the same `token` field the bearer mode uses — one stored
              secret per config, read by whichever mode is selected. */}
          {secretField("token", t("metadataExportTab.password"), c.token_set)}
        </>
      ) : (
        secretField(
          c.auth_mode === "bearer" ? "token" : "api_key",
          c.auth_mode === "bearer" ? t("metadataExportTab.token") : t("metadataExportTab.apiKey"),
          c.auth_mode === "bearer" ? c.token_set : c.api_key_set,
        )
      )}

      <TextInput
        label={t("metadataExportTab.reconcileCron")}
        description={t("metadataExportTab.reconcileCronHelp")}
        data-testid="metadata-export-reconcile-cron"
        value={c.reconcile_cron}
        onChange={(e) => patch({ reconcile_cron: e.currentTarget.value })}
      />
      <NumberInput
        label={t("metadataExportTab.timeout")}
        data-testid="metadata-export-timeout"
        value={c.timeout_seconds}
        onChange={(v) => patch({ timeout_seconds: Number(v) || 0 })}
      />

      <Group>
        <Button onClick={save} loading={busy === "save"} data-testid="metadata-export-save">
          {t("metadataExportTab.save")}
        </Button>
        <Button
          variant="default"
          onClick={runHealth}
          loading={busy === "health"}
          data-testid="metadata-export-health"
        >
          {t("metadataExportTab.testConnection")}
        </Button>
        <Button
          variant="default"
          onClick={runPublish}
          loading={busy === "publish"}
          data-testid="metadata-export-publish"
        >
          {t("metadataExportTab.publishNow")}
        </Button>
      </Group>

      {msg && (
        <Alert color="green" icon={<Check size={16} />} data-testid="metadata-export-saved">
          {msg}
        </Alert>
      )}
      {error && (
        <Alert color="red" icon={<TriangleAlert size={16} />} data-testid="metadata-export-error">
          {error}
        </Alert>
      )}
      {health && (
        <Alert
          color={health.ok ? "green" : "red"}
          icon={health.ok ? <Check size={16} /> : <TriangleAlert size={16} />}
          data-testid="metadata-export-health-result"
        >
          {health.ok ? t("metadataExportTab.healthOk") : health.error}
        </Alert>
      )}

      {publish && (
        <Stack gap="xs" data-testid="metadata-export-last-publish">
          <Group gap="xs">
            <Title order={5}>{t("metadataExportTab.lastPublishHeading")}</Title>
            <Badge color={publish.ok ? "green" : "red"}>
              {publish.ok
                ? t("metadataExportTab.publishComplete", { count: publish.total_published })
                : t("metadataExportTab.publishPartial", {
                    count: publish.total_published,
                    failed: publish.errors.length,
                  })}
            </Badge>
          </Group>
          {publish.errors.length > 0 && (
            <Table data-testid="metadata-export-publish-errors">
              <Table.Thead>
                <Table.Tr>
                  <Table.Th>{t("metadataExportTab.errorAsset")}</Table.Th>
                  <Table.Th>{t("metadataExportTab.errorMessage")}</Table.Th>
                </Table.Tr>
              </Table.Thead>
              <Table.Tbody>
                {publish.errors.map((e) => (
                  <Table.Tr key={`${e.asset}:${e.message}`}>
                    <Table.Td>{e.asset}</Table.Td>
                    <Table.Td>{e.message}</Table.Td>
                  </Table.Tr>
                ))}
              </Table.Tbody>
            </Table>
          )}
        </Stack>
      )}
    </Stack>
  );
}
