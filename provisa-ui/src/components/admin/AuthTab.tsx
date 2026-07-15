// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  Alert,
  Button,
  Checkbox,
  Group,
  PasswordInput,
  Select,
  Stack,
  Text,
  TextInput,
  Title,
} from "@mantine/core";
import { Check } from "lucide-react";
import { fetchAuthConfig, setAuthConfig, type AuthConfigState } from "../../api/admin";

// REQ-919: configure the authentication provider (firebase/keycloak/oauth/simple) + role settings.
// The provider binds at startup, so changes take effect on restart.
export function AuthTab() {
  const { t } = useTranslation();
  const [s, setS] = useState<AuthConfigState | null>(null);
  const [provider, setProvider] = useState("none");
  const [config, setConfig] = useState<Record<string, Record<string, string>>>({});
  const [common, setCommon] = useState<AuthConfigState["common"] | null>(null);
  const [saving, setSaving] = useState(false);
  const [msg, setMsg] = useState("");
  const [error, setError] = useState("");

  useEffect(() => {
    fetchAuthConfig()
      .then((a) => {
        setS(a);
        setProvider(a.provider);
        setCommon(a.common);
        const cfg: Record<string, Record<string, string>> = {};
        for (const [pk, vals] of Object.entries(a.config)) {
          cfg[pk] = {};
          for (const [k, v] of Object.entries(vals)) cfg[pk][k] = v == null ? "" : String(v);
        }
        setConfig(cfg);
      })
      .catch((e) => setError(String(e)));
  }, []);

  const current = useMemo(() => s?.providers.find((p) => p.key === provider), [s, provider]);

  const missingRequired = useMemo(
    () =>
      (current?.config_fields ?? []).some(
        (f) => f.required && !(config[provider]?.[f.config_key] ?? "").trim(),
      ),
    [current, config, provider],
  );

  const save = async () => {
    if (!common) return;
    setSaving(true);
    setMsg("");
    setError("");
    try {
      const providerConfig: Record<string, unknown> = {};
      for (const f of current?.config_fields ?? []) {
        const v = (config[provider]?.[f.config_key] ?? "").trim();
        if (v !== "") providerConfig[f.config_key] = v;
      }
      const res = await setAuthConfig({ provider, config: providerConfig, common });
      setMsg(res.restart_required ? t("authTab.savedRestartRequired") : t("authTab.saved"));
    } catch (e) {
      setError(String(e));
    } finally {
      setSaving(false);
    }
  };

  if (error && !s) return <Alert color="red">{error}</Alert>;
  if (!s || !common) return <Text>{t("authTab.loading")}</Text>;

  const setField = (key: string, value: string) =>
    setConfig((c) => ({ ...c, [provider]: { ...(c[provider] ?? {}), [key]: value } }));

  const providerOptions = s.providers.map((p) => ({
    value: p.key,
    label: p.key === s.provider ? `${p.label}${t("authTab.current")}` : p.label,
  }));

  return (
    <Stack gap="md" maw={720}>
      <Text c="dimmed" size="sm">
        {t("authTab.intro")}
      </Text>

      <Stack gap="sm">
        <Select
          label={t("authTab.providerLabel")}
          data={providerOptions}
          value={provider}
          onChange={(v) => v && setProvider(v)}
          allowDeselect={false}
          data-testid="auth-provider-select"
        />
        {current && (
          <Text c="dimmed" size="xs">
            {current.description}
          </Text>
        )}

        {(current?.config_fields ?? []).map((f) =>
          f.secret ? (
            <PasswordInput
              key={f.config_key}
              label={f.label}
              required={f.required}
              value={config[provider]?.[f.config_key] ?? ""}
              placeholder={f.placeholder}
              autoComplete="new-password"
              onChange={(e) => setField(f.config_key, e.currentTarget.value)}
            />
          ) : (
            <TextInput
              key={f.config_key}
              label={f.label}
              required={f.required}
              value={config[provider]?.[f.config_key] ?? ""}
              placeholder={f.placeholder}
              autoComplete="off"
              onChange={(e) => setField(f.config_key, e.currentTarget.value)}
            />
          ),
        )}

        {provider === "simple" && (
          <Checkbox
            label={t("authTab.allowSimpleAuth")}
            checked={common.allow_simple_auth}
            onChange={(e) => setCommon({ ...common, allow_simple_auth: e.currentTarget.checked })}
          />
        )}
      </Stack>

      <Title order={4}>{t("authTab.roles")}</Title>
      <Stack gap="sm">
        <TextInput
          label={t("authTab.defaultRole")}
          value={common.default_role}
          onChange={(e) => setCommon({ ...common, default_role: e.currentTarget.value })}
        />
        <Select
          label={t("authTab.assignmentsFrom")}
          data={[
            { value: "claims", label: t("authTab.assignmentsSourceClaims") },
            { value: "provisa", label: t("authTab.assignmentsSourceProvisa") },
          ]}
          value={common.assignments_source}
          onChange={(v) => v && setCommon({ ...common, assignments_source: v })}
          allowDeselect={false}
        />
        <Checkbox
          label={t("authTab.trustUpstream")}
          checked={common.trust_upstream}
          onChange={(e) => setCommon({ ...common, trust_upstream: e.currentTarget.checked })}
        />
      </Stack>

      <Alert color="yellow" variant="light">
        {s.restart_required_note}
      </Alert>

      <Group gap="sm" align="center">
        <Button
          onClick={save}
          disabled={saving || missingRequired}
          title={t("authTab.saveButton")}
          aria-label={t("authTab.saveButton")}
          loading={saving}
          leftSection={saving ? undefined : <Check size={14} />}
          data-testid="auth-save-button"
        >
          {t("authTab.saveButton")}
        </Button>
        {msg && (
          <Text c="green" size="sm">
            {msg}
          </Text>
        )}
        {error && (
          <Text c="red" size="sm">
            {error}
          </Text>
        )}
      </Group>
    </Stack>
  );
}
