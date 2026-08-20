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
import {
  Alert,
  Button,
  Group,
  NumberInput,
  Radio,
  Select,
  Stack,
  Text,
  TextInput,
} from "@mantine/core";
import { Check } from "lucide-react";
import { Link } from "react-router-dom";
import {
  fetchOrgEngine,
  setOrgEngine,
  type OrgEngineMode,
  type OrgEngineState,
} from "../../api/admin";

/**
 * REQ-1512: on a hosted deployment the engine is REPORTED, not chosen.
 *
 * The lane and the size come from the plan (REQ-1510), so there is no lane selector, no engine-kind
 * list and no external endpoint form here — absent rather than shown disabled, because an
 * organization-operated engine is an enterprise arrangement sold on no hosted plan and is not
 * something a hosted administrator is invited to consider and then refused. Billing is named
 * instead, that being where the plan which decides all of this is changed.
 */
function HostedEngineReport({ state }: { state: OrgEngineState }) {
  const { t } = useTranslation();
  const laneKey =
    state.mode === "isolated"
      ? "orgEngineTab.hostedLaneIsolated"
      : state.mode === "external"
        ? "orgEngineTab.hostedLaneExternal"
        : "orgEngineTab.hostedLaneShared";
  const engineState = state.isolated_engine?.state ?? null;
  const stateText =
    engineState === null
      ? null
      : engineState === "ready" || engineState === "running"
        ? t("orgEngineTab.hostedStateReady")
        : engineState === "starting"
          ? t("orgEngineTab.hostedStateStarting")
          : engineState === "stopped" || engineState === "exited"
            ? t("orgEngineTab.hostedStateStopped")
            : engineState === "absent"
              ? t("orgEngineTab.hostedStateAbsent")
              : t("orgEngineTab.hostedStateOther", { state: engineState });

  return (
    <Stack gap="md" maw={720} data-testid="org-engine-hosted">
      <Text c="dimmed" size="sm">
        {t("orgEngineTab.hostedIntro")}
      </Text>
      <Stack gap="xs">
        <Text fw={600}>{t("orgEngineTab.hostedLane")}</Text>
        <Text size="sm" data-testid="org-engine-hosted-lane">
          {t(laneKey)}
        </Text>
        {state.plan && (
          <Text size="sm" data-testid="org-engine-hosted-plan">
            {t("orgEngineTab.hostedPlan", { plan: state.plan })}
          </Text>
        )}
        {state.engine_size && (
          <Text size="sm" data-testid="org-engine-hosted-size">
            {t("orgEngineTab.hostedSize", {
              machine: state.engine_size.machine_type,
              vcpu: state.engine_size.vcpu,
              memory: state.engine_size.memory_gib,
            })}
          </Text>
        )}
        {stateText && (
          <Text size="sm" data-testid="org-engine-hosted-state">
            {stateText}
          </Text>
        )}
      </Stack>
      <Alert color="blue" variant="light" data-testid="org-engine-hosted-change">
        <Link to="/admin/billing">{t("orgEngineTab.hostedChange")}</Link>
      </Alert>
    </Stack>
  );
}

// REQ-1412: the org administrator's engine lane — shared, isolated (Provisa-operated), or external
// (org-operated). Distinct from FederationEngineTab, which picks the engine KIND for the whole
// deployment and is gated on platform_settings.
export function OrgEngineTab() {
  const { t } = useTranslation();
  const [state, setState] = useState<OrgEngineState | null>(null);
  const [mode, setMode] = useState<OrgEngineMode>("shared");
  const [kind, setKind] = useState<string | null>(null);
  const [host, setHost] = useState("");
  const [port, setPort] = useState("");
  const [url, setUrl] = useState("");
  const [saving, setSaving] = useState(false);
  const [msg, setMsg] = useState("");
  const [error, setError] = useState("");

  useEffect(() => {
    fetchOrgEngine()
      .then((s) => {
        setState(s);
        setMode(s.mode);
        setKind(s.engine_kind);
        setHost(s.external_host ?? "");
        setPort(s.external_port == null ? "" : String(s.external_port));
      })
      .catch((e) => setError(String(e)));
  }, []);

  // REQ-1418: which address the org has to supply comes from the CHOSEN KIND, reported by the
  // server alongside the kind list — the tab never decides it from the shape of what was typed.
  const addressing = state?.external_kinds.find((k) => k.key === kind)?.addressing ?? null;
  const externalIncomplete =
    mode === "external" &&
    (!kind ||
      (addressing === "endpoint"
        ? !host.trim() || !port.trim()
        : !url.trim() && !state?.external_url_set));

  const save = async () => {
    if (!state) return;
    setSaving(true);
    setMsg("");
    setError("");
    try {
      const external = mode === "external";
      await setOrgEngine({
        mode,
        engine_kind: external ? kind : null,
        external_host: external && addressing === "endpoint" ? host.trim() : null,
        external_port: external && addressing === "endpoint" ? Number(port) : null,
        // Omitted when blank: an unchanged DSN is not re-sent, and the server keeps the stored one.
        external_url: external && addressing === "url" && url.trim() ? url.trim() : null,
      });
      setMsg(t("orgEngineTab.saved"));
      setState({
        ...state,
        mode,
        engine_kind: external ? kind : null,
        external_url_set: external && addressing === "url" ? true : false,
      });
      setUrl("");
    } catch (e) {
      setError(String(e));
    } finally {
      setSaving(false);
    }
  };

  if (error && !state) return <Alert color="red">{error}</Alert>;
  if (!state) return <Text>{t("orgEngineTab.loading")}</Text>;
  // REQ-1512: a plan decides the lane on a hosted deployment, so the controls below are not offered
  // there at all. They stay on self-hosted and enterprise deployments, where the lane is genuinely
  // the organization's own choice.
  if (state.plan_derived) return <HostedEngineReport state={state} />;

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
            label={
              t("orgEngineTab.modeShared") +
              (state.mode === "shared" ? t("orgEngineTab.currentSuffix") : "")
            }
            description={t("orgEngineTab.modeSharedHelp")}
            data-testid="org-engine-mode-shared"
          />
          <Radio
            value="isolated"
            disabled={!state.isolated_available || !state.isolated_entitled}
            label={
              t("orgEngineTab.modeIsolated") +
              (state.mode === "isolated" ? t("orgEngineTab.currentSuffix") : "")
            }
            description={
              !state.isolated_available
                ? t("orgEngineTab.modeIsolatedUnavailable")
                : state.isolated_entitled
                  ? t("orgEngineTab.modeIsolatedHelp")
                  : t("orgEngineTab.modeIsolatedNotEntitled")
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
          <Select
            label={t("orgEngineTab.kindLabel")}
            description={t("orgEngineTab.kindHelp")}
            required
            value={kind}
            onChange={setKind}
            data={state.external_kinds.map((k) => ({ value: k.key, label: k.label }))}
            data-testid="org-engine-kind"
          />
          {kind && (
            <Text c="dimmed" size="xs">
              {state.external_kinds.find((k) => k.key === kind)?.description}
            </Text>
          )}
          {addressing === "endpoint" && (
            <>
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
            </>
          )}
          {addressing === "url" && (
            <TextInput
              label={t("orgEngineTab.urlLabel")}
              required={!state.external_url_set}
              description={
                state.external_url_set ? t("orgEngineTab.urlStored") : t("orgEngineTab.urlHelp")
              }
              placeholder={t("orgEngineTab.urlPlaceholder")}
              value={url}
              onChange={(e) => setUrl(e.currentTarget.value)}
              data-testid="org-engine-url"
            />
          )}
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
