// Copyright (c) 2026 Kenneth Stott
// Canary: 7a41f0c9-63de-4b52-8a7d-04c9d6b1e2f3
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { Alert, Button, Group, NumberInput, Radio, Stack, Text, TextInput } from "@mantine/core";
import { Check } from "lucide-react";
import { fetchOrgEngine, setOrgEngine, type OrgEngineMode, type OrgEngineState } from "../../api/admin";

// REQ-1412: the org administrator's engine lane — shared, isolated (Provisa-operated), or external
// (org-operated). Distinct from FederationEngineTab, which picks the engine KIND for the whole
// deployment and is gated on platform_settings.
export function OrgEngineTab() {
  const { t } = useTranslation();
  const [state, setState] = useState<OrgEngineState | null>(null);
  const [mode, setMode] = useState<OrgEngineMode>("shared");
  const [host, setHost] = useState("");
  const [port, setPort] = useState("");
  const [saving, setSaving] = useState(false);
  const [msg, setMsg] = useState("");
  const [error, setError] = useState("");

  useEffect(() => {
    fetchOrgEngine()
      .then((s) => {
        setState(s);
        setMode(s.mode);
        setHost(s.external_host ?? "");
        setPort(s.external_port == null ? "" : String(s.external_port));
      })
      .catch((e) => setError(String(e)));
  }, []);

  const externalIncomplete = mode === "external" && (!host.trim() || !port.trim());

  const save = async () => {
    if (!state) return;
    setSaving(true);
    setMsg("");
    setError("");
    try {
      await setOrgEngine({
        mode,
        external_host: mode === "external" ? host.trim() : null,
        external_port: mode === "external" ? Number(port) : null,
      });
      setMsg(t("orgEngineTab.saved"));
      setState({ ...state, mode });
    } catch (e) {
      setError(String(e));
    } finally {
      setSaving(false);
    }
  };

  if (error && !state) return <Alert color="red">{error}</Alert>;
  if (!state) return <Text>{t("orgEngineTab.loading")}</Text>;

  return (
    <Stack gap="md" maw={720}>
      <Text c="dimmed" size="sm">
        {t("orgEngineTab.intro", { engine: state.engine_name })}
      </Text>

      <Radio.Group
        label={t("orgEngineTab.modeLabel")}
        value={mode}
        onChange={(v) => {
          setMode(v as OrgEngineMode);
          setMsg("");
        }}
        data-testid="org-engine-mode"
      >
        <Stack gap="sm" mt="xs">
          <Radio
            value="shared"
            label={t("orgEngineTab.modeShared") + (state.mode === "shared" ? t("orgEngineTab.currentSuffix") : "")}
            description={t("orgEngineTab.modeSharedHelp")}
            data-testid="org-engine-mode-shared"
          />
          <Radio
            value="isolated"
            disabled={!state.isolated_available}
            label={
              t("orgEngineTab.modeIsolated") +
              (state.mode === "isolated" ? t("orgEngineTab.currentSuffix") : "")
            }
            description={
              state.isolated_available
                ? t("orgEngineTab.modeIsolatedHelp")
                : t("orgEngineTab.modeIsolatedUnavailable")
            }
            data-testid="org-engine-mode-isolated"
          />
          <Radio
            value="external"
            label={
              t("orgEngineTab.modeExternal") +
              (state.mode === "external" ? t("orgEngineTab.currentSuffix") : "")
            }
            description={t("orgEngineTab.modeExternalHelp", { engine: state.engine_name })}
            data-testid="org-engine-mode-external"
          />
        </Stack>
      </Radio.Group>

      {mode === "external" && (
        <Stack gap="sm">
          <TextInput
            label={t("orgEngineTab.hostLabel")}
            required
            placeholder={t("orgEngineTab.hostPlaceholder")}
            value={host}
            onChange={(e) => setHost(e.currentTarget.value)}
            data-testid="org-engine-host"
          />
          <NumberInput
            label={t("orgEngineTab.portLabel")}
            required
            value={port === "" ? "" : Number(port)}
            onChange={(v) => setPort(String(v ?? ""))}
            data-testid="org-engine-port"
          />
        </Stack>
      )}

      <Alert color="yellow" variant="light">
        {t("orgEngineTab.applyNote")}
      </Alert>

      <Group gap="sm" align="center">
        <Button
          onClick={save}
          disabled={saving || externalIncomplete}
          title={t("orgEngineTab.saveButton")}
          aria-label={t("orgEngineTab.saveButton")}
          loading={saving}
          leftSection={saving ? undefined : <Check size={14} />}
          data-testid="org-engine-save-button"
        >
          {t("orgEngineTab.saveButton")}
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
