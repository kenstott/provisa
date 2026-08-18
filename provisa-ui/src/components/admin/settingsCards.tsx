// Copyright (c) 2026 Kenneth Stott
// Canary: 2f7c4a91-6b30-4e58-9d12-8a0e5c31b7d4
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * REQ-1349: the settings cards that used to sit together under Admin → Overview, each moved to the
 * tab whose subject it actually is — redirect and the response TTL to Cache, the naming conventions
 * and the domain mode to Domains, sampling / CDC / GraphQL-remote to Federation, the configuration
 * file to Maintenance.
 *
 * A card is self-contained: it reads settings itself and saves only its own blocks, so an org
 * administrator saving `redirect` never sends a deployment-wide block the server would refuse. The
 * deployment-wide cards render nothing at all without `features.platform_settings`, which is the
 * same right the server checks on the write.
 */

import { useCallback, useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { useSettingsBlocks } from "./useSettingsBlocks";
import { usePanelState } from "../../hooks/usePanelState";
import { Check } from "lucide-react";
import {
  Accordion,
  Alert,
  Button,
  Card,
  Checkbox,
  FileButton,
  Group,
  Modal,
  NumberInput,
  Select,
  SimpleGrid,
  Stack,
  Text,
  TextInput,
  Title,
} from "@mantine/core";
import {
  downloadConfig,
  downloadConfigPatch,
  fetchConfigDiff,
  fetchSettings,
  setDomainPolicy,
  uploadConfig,
} from "../../api/admin";
import type { PlatformSettings } from "../../api/admin";
import { ConfigDiffView } from "./ConfigDiffView";

const FORMAT_OPTIONS = ["parquet", "orc", "json", "ndjson", "csv", "arrow"];

const CONVENTIONS = ["snake", "hasura_graphql", "apollo_graphql"] as const;

export function SaveRow({ save, saving, msg }: { save: () => void; saving: boolean; msg: string }) {
  const { t } = useTranslation();
  return (
    <Group gap="sm" align="center" mt="sm">
      <Button variant="filled" leftSection={<Check size={14} />} onClick={save} loading={saving}>
        {t("adminPage.saveSettings")}
      </Button>
      {msg && <Text fz="sm">{msg}</Text>}
    </Group>
  );
}

/** Presigned-URL redirect for large results (org-scoped). Cache → Redirect. */
export function RedirectSettingsCard() {
  const { t } = useTranslation();
  const { settings, setSettings, save, saving, msg } = useSettingsBlocks(["redirect"]);
  if (!settings) return null;
  const update = (key: string, value: unknown) =>
    setSettings({ ...settings, redirect: { ...settings.redirect, [key]: value } });

  return (
    <Card withBorder padding="md" data-testid="redirect-settings">
      <Title order={4} mb="sm">
        {t("adminPage.redirect")}
      </Title>
      <Stack gap="sm">
        <Checkbox
          label={t("adminPage.enabled")}
          description={t("adminPage.enabledHint")}
          checked={settings.redirect.enabled}
          onChange={(e) => update("enabled", e.currentTarget.checked)}
        />
        <NumberInput
          label={t("adminPage.defaultThreshold")}
          description={t("adminPage.defaultThresholdHint")}
          value={settings.redirect.threshold}
          onChange={(v) => update("threshold", typeof v === "number" ? v : 0)}
        />
        <Select
          label={t("adminPage.defaultFormat")}
          description={t("adminPage.defaultFormatHint")}
          data={FORMAT_OPTIONS}
          value={settings.redirect.default_format}
          onChange={(v) => v && update("default_format", v)}
          allowDeselect={false}
        />
        <NumberInput
          label={t("adminPage.presignedUrlTtl")}
          description={t("adminPage.presignedUrlTtlHint")}
          value={settings.redirect.ttl}
          onChange={(v) => update("ttl", typeof v === "number" ? v : 0)}
        />
      </Stack>
      <SaveRow save={save} saving={saving} msg={msg} />
    </Card>
  );
}

/** GraphQL/SQL naming conventions + the domain prefix (deployment-wide). Domains tab. */
export function NamingConventionsCard() {
  const { t } = useTranslation();
  const { settings, setSettings, save, saving, msg } = useSettingsBlocks(["naming"]);
  // The naming module those three fields configure is process-global, so they are the deployment's
  // and the payload omits them entirely for anyone else.
  if (!settings?.features?.platform_settings || settings.naming.convention === undefined) {
    return null;
  }
  const conventionData = CONVENTIONS.map((value) => ({
    value,
    label: t(
      value === "snake"
        ? "adminPage.namingConventionSnake"
        : value === "hasura_graphql"
          ? "adminPage.namingConventionHasura"
          : "adminPage.namingConventionApollo",
    ),
  }));

  return (
    <Card withBorder padding="md" data-testid="naming-settings">
      <Title order={4} mb="sm">
        {t("adminPage.naming")}
      </Title>
      <Stack gap="sm">
        <Checkbox
          label={t("adminPage.domainPrefix")}
          description={t("adminPage.domainPrefixHint")}
          checked={settings.naming.domain_prefix ?? false}
          onChange={(e) =>
            setSettings({
              ...settings,
              naming: { ...settings.naming, domain_prefix: e.currentTarget.checked },
            })
          }
        />
        <Select
          label={t("adminPage.namingConvention")}
          description={t("adminPage.namingConventionHint")}
          data={conventionData}
          value={settings.naming.convention}
          onChange={(v) =>
            v && setSettings({ ...settings, naming: { ...settings.naming, convention: v } })
          }
          allowDeselect={false}
        />
        <Select
          label={t("adminPage.sqlNamingConvention")}
          description={t("adminPage.sqlNamingConventionHint")}
          data={conventionData}
          value={settings.naming.sql_convention}
          onChange={(v) =>
            v && setSettings({ ...settings, naming: { ...settings.naming, sql_convention: v } })
          }
          allowDeselect={false}
        />
      </Stack>
      <SaveRow save={save} saving={saving} msg={msg} />
    </Card>
  );
}

/**
 * Whether this org namespaces its tables by domain (org-scoped, destructive).
 *
 * Applying it resets THIS org's catalog, so it goes through /admin/domain-policy behind a typed
 * confirmation rather than the ordinary save.
 */
export function DomainModeCard({ onApplied }: { onApplied?: () => void }) {
  const { t } = useTranslation();
  const [useDomains, setUseDomains] = useState<boolean | null>(false);
  // Not editable: in single-domain mode the name reaches no label, no name and no access check
  // -- it is only the string every row is stored under -- so it is round-tripped, never shown.
  const [defaultDomain, setDefaultDomain] = useState("default");
  const [modalOpen, setModalOpen] = useState(false);
  const [confirmText, setConfirmText] = useState("");
  const [applying, setApplying] = useState(false);
  const [appliedMsg, setAppliedMsg] = useState("");
  const [error, setError] = useState("");

  useEffect(() => {
    fetchSettings().then((s) => {
      setUseDomains(s.naming.use_domains);
      setDefaultDomain(s.naming.default_domain);
    });
  }, []);

  const apply = useCallback(async () => {
    setApplying(true);
    setError("");
    setAppliedMsg("");
    try {
      await setDomainPolicy({ use_domains: useDomains, default_domain: defaultDomain });
      setModalOpen(false);
      setConfirmText("");
      setAppliedMsg(t("adminPage.policyApplied"));
      const s = await fetchSettings();
      setUseDomains(s.naming.use_domains);
      setDefaultDomain(s.naming.default_domain);
      onApplied?.();
    } catch (err) {
      setError(err instanceof Error ? err.message : t("adminPage.policyApplyFailed"));
    } finally {
      setApplying(false);
    }
  }, [useDomains, defaultDomain, t, onApplied]);

  return (
    <>
      <Card withBorder padding="md" data-testid="domain-mode-settings">
        <Title order={4} mb="sm">
          {t("adminPage.domainMode")}
        </Title>
        <Stack gap="sm">
          <Select
            label={t("adminPage.domainMode")}
            description={t("adminPage.domainModeHint")}
            // Two states, because there are only two: every table carries a domain, or none does.
            // A deployment that never set the policy is already in the second one -- it stores
            // every registration under a single implicit domain -- so it is shown as Single rather
            // than as a third, unset choice.
            data={[
              { value: "single", label: t("adminPage.domainModeSingle") },
              { value: "namespaced", label: t("adminPage.domainModeNamespaced") },
            ]}
            value={useDomains === true ? "namespaced" : "single"}
            onChange={(v) => setUseDomains(v === "namespaced")}
            allowDeselect={false}
          />
          <Group gap="sm" align="center">
            <Button
              variant="default"
              data-testid="apply-domain-policy"
              onClick={() => {
                setError("");
                setConfirmText("");
                setModalOpen(true);
              }}
            >
              {t("adminPage.applyDomainPolicy")}
            </Button>
            {appliedMsg && <Text fz="sm">{appliedMsg}</Text>}
          </Group>
        </Stack>
      </Card>

      <Modal
        opened={modalOpen}
        onClose={() => {
          setModalOpen(false);
          setConfirmText("");
          setError("");
        }}
        title={t("adminPage.policyModalTitle")}
        centered
        closeOnClickOutside={!applying}
        closeOnEscape={!applying}
        data-testid="domain-policy-modal"
      >
        <Stack gap="md">
          <Alert color="red" variant="filled">
            {t("adminPage.policyModalWarning")}
          </Alert>
          <TextInput
            label={t("adminPage.policyConfirmLabel")}
            data-testid="domain-policy-confirm-input"
            value={confirmText}
            onChange={(e) => setConfirmText(e.currentTarget.value)}
          />
          {error && (
            <Alert color="red" variant="light">
              {error}
            </Alert>
          )}
          <Group justify="flex-end">
            <Button
              variant="default"
              onClick={() => {
                setModalOpen(false);
                setConfirmText("");
                setError("");
              }}
              disabled={applying}
            >
              {t("adminPage.policyCancel")}
            </Button>
            <Button
              color="red"
              data-testid="domain-policy-confirm-btn"
              disabled={confirmText !== "RESET" || applying}
              onClick={apply}
              loading={applying}
            >
              {applying ? t("adminPage.applying") : t("adminPage.policyResetApply")}
            </Button>
          </Group>
        </Stack>
      </Modal>
    </>
  );
}

/** Sampling, CDC and the GraphQL-remote ceilings (deployment-wide). Federation tab. */
export function FederationSettingsCards() {
  const { t } = useTranslation();
  const [panel, setPanel] = usePanelState("federation-settings");
  const { settings, setSettings, save, saving, msg } = useSettingsBlocks([
    "sampling",
    "cdc",
    "graphql_remote",
  ]);
  if (!settings?.features?.platform_settings) return null;
  const { sampling, cdc, graphql_remote: remote } = settings;
  if (!sampling || !cdc || !remote) return null;

  return (
    /* The admin section panel shape: separated accordion, Title order={4} control. */
    <Accordion
      variant="separated"
      value={panel}
      onChange={setPanel}
      data-testid="federation-settings"
    >
      <Accordion.Item value="settings">
        <Accordion.Control>
          <Title order={4}>{t("adminPage.settingsPanel")}</Title>
        </Accordion.Control>
        <Accordion.Panel>
          <Stack gap="md">
            <SimpleGrid cols={{ base: 1, md: 2 }} spacing="lg">
              <Card withBorder padding="md">
                <Title order={4} mb="sm">
                  {t("adminPage.sampling")}
                </Title>
                <NumberInput
                  label={t("adminPage.defaultSampleSize")}
                  description={t("adminPage.defaultSampleSizeHint")}
                  value={sampling.default_sample_size}
                  onChange={(v) =>
                    setSettings({
                      ...settings,
                      sampling: { default_sample_size: typeof v === "number" ? v : 0 },
                    })
                  }
                />
              </Card>

              <Card withBorder padding="md">
                <Title order={4} mb="sm">
                  {t("adminPage.cdc")}
                </Title>
                <TextInput
                  label={t("adminPage.consumerGroupId")}
                  placeholder={t("adminPage.consumerGroupIdPlaceholder")}
                  description={t("adminPage.consumerGroupIdHint")}
                  value={cdc.consumer_group_id}
                  onChange={(e) =>
                    setSettings({ ...settings, cdc: { consumer_group_id: e.currentTarget.value } })
                  }
                />
              </Card>

              <Card withBorder padding="md">
                <Title order={4} mb="sm">
                  {t("adminPage.graphqlRemote")}
                </Title>
                <Stack gap="sm">
                  <NumberInput
                    label={t("adminPage.maxObjectDepth")}
                    description={t("adminPage.maxObjectDepthHint")}
                    min={1}
                    value={remote.max_object_depth}
                    onChange={(v) =>
                      setSettings({
                        ...settings,
                        graphql_remote: {
                          ...remote,
                          max_object_depth: typeof v === "number" ? v : 0,
                        },
                      })
                    }
                  />
                  <NumberInput
                    label={t("adminPage.maxListDepth")}
                    description={t("adminPage.maxListDepthHint")}
                    min={1}
                    value={remote.max_list_depth}
                    onChange={(v) =>
                      setSettings({
                        ...settings,
                        graphql_remote: {
                          ...remote,
                          max_list_depth: typeof v === "number" ? v : 0,
                        },
                      })
                    }
                  />
                  <NumberInput
                    label={t("adminPage.maxListItems")}
                    description={t("adminPage.maxListItemsHint")}
                    min={1}
                    value={remote.max_list_items}
                    onChange={(v) =>
                      setSettings({
                        ...settings,
                        graphql_remote: {
                          ...remote,
                          max_list_items: typeof v === "number" ? v : 0,
                        },
                      })
                    }
                  />
                </Stack>
              </Card>
            </SimpleGrid>
            <SaveRow save={save} saving={saving} msg={msg} />
          </Stack>
        </Accordion.Panel>
      </Accordion.Item>
    </Accordion>
  );
}

/** Download / upload / diff of the deployment's configuration file. Maintenance tab. */
export function ConfigFileSection() {
  const { t } = useTranslation();
  const [settings, setSettings] = useState<PlatformSettings | null>(null);
  // null = the diff view is closed.
  const [diffOriginal, setDiffOriginal] = useState<string | null>(null);
  const [diffCurrent, setDiffCurrent] = useState("");
  const [revisedConfig, setRevisedConfig] = useState("");
  const [uploading, setUploading] = useState(false);
  const [uploadMsg, setUploadMsg] = useState("");
  const fileInputRef = useRef<() => void>(null);

  useEffect(() => {
    fetchSettings().then(setSettings);
  }, []);

  const handleDownload = async () => {
    const yaml = await downloadConfig();
    const blob = new Blob([yaml], { type: "application/x-yaml" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "provisa.yaml";
    a.click();
    URL.revokeObjectURL(url);
  };

  const handleViewConfig = async () => {
    if (diffOriginal !== null) {
      setDiffOriginal(null);
      return;
    }
    // Both sides normalized identically server-side, so the diff surfaces only real admin changes
    // (e.g. a created MV) rather than section/key reordering noise.
    const { original, current } = await fetchConfigDiff();
    setDiffOriginal(original);
    setDiffCurrent(current);
    setRevisedConfig(current);
  };

  const handleApplyRevised = async () => {
    setUploading(true);
    setUploadMsg("");
    const result = await uploadConfig(revisedConfig);
    setUploadMsg(result.message);
    setUploading(false);
  };

  const handleDownloadPatch = async () => {
    const patch = await downloadConfigPatch(revisedConfig);
    if (!patch) {
      setUploadMsg(t("adminPage.downloadPatchEmpty"));
      return;
    }
    const blob = new Blob([patch], { type: "text/x-patch" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "provisa.config.patch";
    a.click();
    URL.revokeObjectURL(url);
  };

  const handleFileChange = async (file: File | null) => {
    if (!file) return;
    setUploading(true);
    setUploadMsg("");
    const text = await file.text();
    const result = await uploadConfig(text);
    setUploadMsg(result.message);
    setUploading(false);
  };

  // The file IS the deployment's configuration, so it is the deployment administrator's surface.
  if (!settings?.features?.platform_settings) return null;

  return (
    <Stack gap="sm" data-testid="config-file-section">
      <Title order={3}>{t("adminPage.configurationFile")}</Title>
      <Group gap="sm" align="center">
        <Button variant="default" onClick={handleDownload}>
          {t("adminPage.download")}
        </Button>
        {settings.features?.live_config_export && (
          <Button variant="default" onClick={handleViewConfig}>
            {diffOriginal !== null ? t("adminPage.hideDiff") : t("adminPage.viewDiff")}
          </Button>
        )}
        <FileButton onChange={handleFileChange} accept=".yaml,.yml" resetRef={fileInputRef}>
          {(props) => (
            <Button {...props} loading={uploading}>
              {uploading ? t("adminPage.uploading") : t("adminPage.upload")}
            </Button>
          )}
        </FileButton>
        {uploadMsg && <Text fz="sm">{uploadMsg}</Text>}
      </Group>

      {diffOriginal !== null && (
        <>
          <Stack gap={4}>
            <Text fz="sm">
              <Text span fw={700}>
                {t("adminPage.diffLegendBaselineLabel")}
              </Text>{" "}
              {t("adminPage.diffLegendBaselineDesc")}
            </Text>
            <Text fz="sm">
              <Text span fw={700}>
                {t("adminPage.diffLegendCurrentLabel")}
              </Text>{" "}
              {t("adminPage.diffLegendCurrentDesc")}
            </Text>
          </Stack>
          <ConfigDiffView
            original={diffOriginal}
            current={diffCurrent}
            onCurrentChange={setRevisedConfig}
          />
          <Group gap="sm" mt="sm">
            <Button
              variant="default"
              onClick={handleDownloadPatch}
              disabled={revisedConfig === diffOriginal}
              title={t("adminPage.downloadPatchTitle")}
            >
              {t("adminPage.downloadPatch")}
            </Button>
            <Button
              onClick={handleApplyRevised}
              disabled={uploading || revisedConfig === diffOriginal}
              loading={uploading}
            >
              {uploading ? t("adminPage.applying") : t("adminPage.applyRevised")}
            </Button>
          </Group>
        </>
      )}
    </Stack>
  );
}
