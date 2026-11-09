// Copyright (c) 2026 Kenneth Stott
// Canary: 96627e23-9cac-4900-b017-3da1bd61c697
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1412, REQ-1327: setting an org's engine lane -- shared, isolated (Provisa-operated) or
// external (org-operated) -- is the platform administrator's, on the Platform page. The org is
// picked here and named on the route, because the platform role is stripped in a tenant org
// (REQ-1297), so its administrator is never acting in the org it sets the lane for. The org reads
// the result on OrgEngineTab. Distinct from FederationEngineTab, which picks the engine KIND for
// the whole deployment.

import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { Alert, NumberInput, Radio, Select, Stack, Text, TextInput } from "@mantine/core";
import { TriangleAlert } from "lucide-react";
import {
  fetchPlatformOrgEngine,
  setPlatformOrgEngine,
  type OrgEngineMode,
  type OrgEngineState,
} from "../../api/admin";
import { fetchOrgGrants, problemOf, type OrgGrantSummary } from "../../api/regions";
import { FieldGroup, FormField, FormFooter, FormLayout } from "../form/FormLayout";
import { PageLoading } from "../PageLoading";
import { HostedEngineReport } from "./OrgEngineTab";

/** The Platform page's engine section: pick the org, then set the lane it runs on. */
export function PlatformOrgEngines() {
  const { t } = useTranslation();
  const [orgs, setOrgs] = useState<OrgGrantSummary[] | null>(null);
  const [orgId, setOrgId] = useState<string | null>(null);
  const [error, setError] = useState("");

  useEffect(() => {
    fetchOrgGrants()
      .then(setOrgs)
      .catch((e: unknown) => setError(problemOf(e).text));
  }, []);

  if (error) {
    return (
      <Alert color="red" icon={<TriangleAlert size={16} />} data-testid="platform-engines-error">
        {error}
      </Alert>
    );
  }
  if (orgs === null) return <PageLoading message={t("platformRegions.loadingOrgs")} />;
  return (
    <Stack gap="sm" data-testid="platform-engines">
      <Text c="dimmed" size="sm" maw={820}>
        {t("orgEngineTab.platformIntro")}
      </Text>
      <FormLayout data-testid="platform-engine-org-form">
        <FormField size="medium">
          <Select
            label={t("platformRegions.org")}
            placeholder={t("platformRegions.pickOrg")}
            data={orgs.map((o) => o.id)}
            value={orgId}
            onChange={setOrgId}
            searchable
            comboboxProps={{ withinPortal: true }}
            data-testid="platform-engine-org-pick"
          />
        </FormField>
      </FormLayout>
      {orgId !== null && <PlatformOrgEngine key={orgId} orgId={orgId} />}
    </Stack>
  );
}

export function PlatformOrgEngine({ orgId }: { orgId: string }) {
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
    fetchPlatformOrgEngine(orgId)
      .then((s) => {
        setState(s);
        setMode(s.mode);
        setKind(s.engine_kind);
        setHost(s.external_host ?? "");
        setPort(s.external_port == null ? "" : String(s.external_port));
      })
      .catch((e) => setError(String(e)));
  }, [orgId]);

  // REQ-1418: which address the org has to supply comes from the CHOSEN KIND, reported by the
  // server alongside the kind list — the form never decides it from the shape of what was typed.
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
      await setPlatformOrgEngine(orgId, {
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
  // there at all. No Billing link: that page is the acting org's, not the one picked here.
  if (state.plan_derived) return <HostedEngineReport state={state} billingLink={false} />;

  return (
    <Stack gap="md">
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
        <FormLayout>
          <FormField help={t("orgEngineTab.kindHelp")} size="medium">
            <Select
              label={t("orgEngineTab.kindLabel")}
              required
              value={kind}
              onChange={setKind}
              data={state.external_kinds.map((k) => ({ value: k.key, label: k.label }))}
              data-testid="org-engine-kind"
            />
          </FormField>
          {kind && (
            <FormField size="full">
              <Text c="dimmed" size="xs">
                {state.external_kinds.find((k) => k.key === kind)?.description}
              </Text>
            </FormField>
          )}
          {addressing === "endpoint" && (
            <>
              <FieldGroup>
                <FormField size="medium">
                  <TextInput
                    label={t("orgEngineTab.hostLabel")}
                    required
                    placeholder={t("orgEngineTab.hostPlaceholder")}
                    value={host}
                    onChange={(e) => setHost(e.currentTarget.value)}
                    data-testid="org-engine-host"
                  />
                </FormField>
                <FormField size="short">
                  <NumberInput
                    label={t("orgEngineTab.portLabel")}
                    required
                    value={port === "" ? "" : Number(port)}
                    onChange={(v) => setPort(String(v ?? ""))}
                    data-testid="org-engine-port"
                  />
                </FormField>
              </FieldGroup>
            </>
          )}
          {addressing === "url" && (
            <FormField
              hint={
                state.external_url_set ? t("orgEngineTab.urlStored") : t("orgEngineTab.urlHelp")
              }
              size="long"
            >
              <TextInput
                label={t("orgEngineTab.urlLabel")}
                required={!state.external_url_set}
                placeholder={t("orgEngineTab.urlPlaceholder")}
                value={url}
                onChange={(e) => setUrl(e.currentTarget.value)}
                data-testid="org-engine-url"
              />
            </FormField>
          )}
        </FormLayout>
      )}

      <Alert color="yellow" variant="light">
        {t("orgEngineTab.applyNote")}
      </Alert>

      <FormFooter
        onSave={save}
        saving={saving}
        saveProps={{ "data-testid": "org-engine-save-button", disabled: externalIncomplete }}
      >
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
      </FormFooter>
    </Stack>
  );
}
