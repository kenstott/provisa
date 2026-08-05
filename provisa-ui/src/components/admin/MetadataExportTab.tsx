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
  checkMetadataEgress,
  fetchMetadataEgress,
  publishMetadataEgress,
  setMetadataEgress,
  type MetadataEgressState,
  type MetadataEgressUpdate,
  type PublishOutcome,
} from "../../api/metadataEgress";

// REQ-1074: configure and operate the per-org metadata egress target.
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

export function MetadataEgressTab() {
  const { t } = useTranslation();
  const [s, setS] = useState<MetadataEgressState | null>(null);
  // Typed credentials, kept apart from `s.config` because the config carries only set/not-set.
  const [secrets, setSecrets] = useState<Partial<Record<SecretField, string>>>({});
  const [busy, setBusy] = useState("");
  const [msg, setMsg] = useState("");
  const [error, setError] = useState("");
  const [health, setHealth] = useState<{ ok: boolean; error?: string } | null>(null);
  const [publish, setPublish] = useState<PublishOutcome | null>(null);

  useEffect(() => {
    fetchMetadataEgress()
      .then((state) => {
        setS(state);
        setPublish(state.last_publish);
      })
      .catch((e) => setError(String(e)));
  }, []);

  const patch = (p: Partial<MetadataEgressState["config"]>) =>
    setS((prev) => (prev ? { ...prev, config: { ...prev.config, ...p } } : prev));

  const save = async () => {
    if (!s) return;
    setBusy("save");
    setMsg("");
    setError("");
    try {
      const body: MetadataEgressUpdate = {
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
      await setMetadataEgress(body);
      // Re-read rather than assume: the set/not-set flags now reflect what was persisted, and a
      // typed secret is dropped from local state so it is not re-sent on the next save.
      const fresh = await fetchMetadataEgress();
      setS(fresh);
      setSecrets({});
      setMsg(t("metadataEgressTab.saved"));
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
      setHealth(await checkMetadataEgress());
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
      setPublish(await publishMetadataEgress());
    } catch (e) {
      setError(String(e));
    } finally {
      setBusy("");
    }
  };

  if (error && !s)
    return (
      <Alert color="red" icon={<TriangleAlert size={16} />} data-testid="metadata-egress-error">
        {error}
      </Alert>
    );

  if (!s)
    return (
      <Group gap="xs">
        <Loader size="sm" />
        <Text>{t("metadataEgressTab.loading")}</Text>
      </Group>
    );

  if (!s.entitled)
    return (
      <Alert
        color="yellow"
        icon={<TriangleAlert size={16} />}
        data-testid="metadata-egress-not-entitled"
        title={t("metadataEgressTab.notEntitledTitle")}
      >
        {t("metadataEgressTab.notEntitled", { tier: s.required_tier })}
      </Alert>
    );

  const c = s.config;
  const secretField = (field: SecretField, label: string, isSet: boolean) => (
    <TextInput
      label={label}
      type="password"
      data-testid={`metadata-egress-${field.replace(/_/g, "-")}`}
      placeholder={isSet ? t("metadataEgressTab.secretSet") : t("metadataEgressTab.secretUnset")}
      description={t("metadataEgressTab.secretHelp")}
      value={secrets[field] ?? ""}
      onChange={(e) => setSecrets({ ...secrets, [field]: e.currentTarget.value })}
    />
  );

  return (
    <Stack maw={860} gap="md">
      <Title order={4}>{t("metadataEgressTab.heading")}</Title>
      <Text c="dimmed" size="sm">
        {t("metadataEgressTab.intro")}
      </Text>

      <Checkbox
        label={t("metadataEgressTab.enabled")}
        data-testid="metadata-egress-enabled"
        checked={c.enabled}
        onChange={(e) => patch({ enabled: e.currentTarget.checked })}
      />
      <Select
        label={t("metadataEgressTab.provider")}
        data-testid="metadata-egress-provider"
        data={s.providers.map((p) => ({ value: p, label: p }))}
        value={c.provider || null}
        onChange={(v) => patch({ provider: v ?? "" })}
      />
      <TextInput
        label={t("metadataEgressTab.endpoint")}
        data-testid="metadata-egress-endpoint"
        value={c.endpoint}
        onChange={(e) => patch({ endpoint: e.currentTarget.value })}
      />
      <Select
        label={t("metadataEgressTab.authMode")}
        data-testid="metadata-egress-auth-mode"
        data={AUTH_MODES}
        value={c.auth_mode}
        onChange={(v) => patch({ auth_mode: v ?? "api_key" })}
      />

      {c.auth_mode === ENTRA_MODE ? (
        <>
          <TextInput
            label={t("metadataEgressTab.entraTenantId")}
            data-testid="metadata-egress-entra-tenant-id"
            value={c.entra_tenant_id}
            onChange={(e) => patch({ entra_tenant_id: e.currentTarget.value })}
          />
          <TextInput
            label={t("metadataEgressTab.entraClientId")}
            data-testid="metadata-egress-entra-client-id"
            value={c.entra_client_id}
            onChange={(e) => patch({ entra_client_id: e.currentTarget.value })}
          />
          {secretField(
            "entra_client_secret",
            t("metadataEgressTab.entraClientSecret"),
            c.entra_client_secret_set,
          )}
        </>
      ) : c.auth_mode === BASIC_MODE ? (
        <>
          <TextInput
            label={t("metadataEgressTab.username")}
            data-testid="metadata-egress-username"
            value={c.username}
            onChange={(e) => patch({ username: e.currentTarget.value })}
          />
          {/* The password rides in the same `token` field the bearer mode uses — one stored
              secret per config, read by whichever mode is selected. */}
          {secretField("token", t("metadataEgressTab.password"), c.token_set)}
        </>
      ) : (
        secretField(
          c.auth_mode === "bearer" ? "token" : "api_key",
          c.auth_mode === "bearer" ? t("metadataEgressTab.token") : t("metadataEgressTab.apiKey"),
          c.auth_mode === "bearer" ? c.token_set : c.api_key_set,
        )
      )}

      <TextInput
        label={t("metadataEgressTab.reconcileCron")}
        description={t("metadataEgressTab.reconcileCronHelp")}
        data-testid="metadata-egress-reconcile-cron"
        value={c.reconcile_cron}
        onChange={(e) => patch({ reconcile_cron: e.currentTarget.value })}
      />
      <NumberInput
        label={t("metadataEgressTab.timeout")}
        data-testid="metadata-egress-timeout"
        value={c.timeout_seconds}
        onChange={(v) => patch({ timeout_seconds: Number(v) || 0 })}
      />

      <Group>
        <Button onClick={save} loading={busy === "save"} data-testid="metadata-egress-save">
          {t("metadataEgressTab.save")}
        </Button>
        <Button
          variant="default"
          onClick={runHealth}
          loading={busy === "health"}
          data-testid="metadata-egress-health"
        >
          {t("metadataEgressTab.testConnection")}
        </Button>
        <Button
          variant="default"
          onClick={runPublish}
          loading={busy === "publish"}
          data-testid="metadata-egress-publish"
        >
          {t("metadataEgressTab.publishNow")}
        </Button>
      </Group>

      {msg && (
        <Alert color="green" icon={<Check size={16} />} data-testid="metadata-egress-saved">
          {msg}
        </Alert>
      )}
      {error && (
        <Alert color="red" icon={<TriangleAlert size={16} />} data-testid="metadata-egress-error">
          {error}
        </Alert>
      )}
      {health && (
        <Alert
          color={health.ok ? "green" : "red"}
          icon={health.ok ? <Check size={16} /> : <TriangleAlert size={16} />}
          data-testid="metadata-egress-health-result"
        >
          {health.ok ? t("metadataEgressTab.healthOk") : health.error}
        </Alert>
      )}

      {publish && (
        <Stack gap="xs" data-testid="metadata-egress-last-publish">
          <Group gap="xs">
            <Title order={5}>{t("metadataEgressTab.lastPublishHeading")}</Title>
            <Badge color={publish.ok ? "green" : "red"}>
              {publish.ok
                ? t("metadataEgressTab.publishComplete", { count: publish.total_published })
                : t("metadataEgressTab.publishPartial", {
                    count: publish.total_published,
                    failed: publish.errors.length,
                  })}
            </Badge>
          </Group>
          {publish.errors.length > 0 && (
            <Table data-testid="metadata-egress-publish-errors">
              <Table.Thead>
                <Table.Tr>
                  <Table.Th>{t("metadataEgressTab.errorAsset")}</Table.Th>
                  <Table.Th>{t("metadataEgressTab.errorMessage")}</Table.Th>
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
