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
 * deployment-wide cards render nothing at all without `features.deployment_settings`, which is the
 * same right the server checks on the write.
 */

import { namingConventionLabel, useNamingConventions } from "../../hooks/useNamingConventions";
import { useCallback, useEffect, useRef, useState } from "react";
import { useTranslation } from "react-i18next";
import { useSettingsBlocks } from "./useSettingsBlocks";
import { CollapsibleItem, Collapsibles } from "../collapsible/Collapsible";
import {
  Alert,
  Button,
  Card,
  Checkbox,
  FileButton,
  Group,
  MultiSelect,
  NumberInput,
  Select,
  Stack,
  Text,
  TextInput,
  Title,
} from "@mantine/core";
import {
  MODEL_SECTIONS,
  downloadConfig,
  downloadConfigPatch,
  exportModelConfig,
  fetchConfigDiff,
  fetchSettings,
  setDomainPolicy,
  updateSettings,
  uploadConfig,
} from "../../api/admin";
import type { PlatformSettings } from "../../api/admin";
import { ConfigDiffView } from "./ConfigDiffView";
import { FormChecks, FormField, FormFooter, FormLayout } from "../form/FormLayout";
import { Tip } from "../Tip";

const FORMAT_OPTIONS = ["parquet", "orc", "json", "ndjson", "csv", "arrow"];

export function SaveRow({ save, saving, msg }: { save: () => void; saving: boolean; msg: string }) {
  return (
    <FormFooter onSave={save} saving={saving} style={{ marginTop: "0.75rem" }}>
      {msg && <Text fz="sm">{msg}</Text>}
    </FormFooter>
  );
}

const TRANSPORTS = [
  "graphql",
  "rest",
  "jsonapi",
  "sql_http",
  "cypher_http",
  "pgwire",
  "flight",
  "bolt",
  "grpc",
  "mcp",
  "scheduler", // REQ-2001: a scheduled task's run
] as const;

/** Transports whose shipped value differs from the default, with the reason shown on the row. */
const TRANSPORT_HELP = { flight: true, pgwire: true } as const;

/** Positive seconds, or null when the text is not one. */
function parseSeconds(text: string): number | null {
  const trimmed = text.trim();
  if (trimmed === "") return null;
  const n = Number(trimmed);
  return Number.isFinite(n) && n > 0 ? n : null;
}

/**
 * REQ-1905: the request timeout (default) and one value per transport. An empty transport value
 * means it uses the default; the row says so and shows the effective value. A server 400 names its
 * field in the error `params.field` and is shown on that field.
 */
export function ServerLimitsCard() {
  const { t } = useTranslation();
  const [settings, setSettings] = useState<PlatformSettings | null>(null);
  const [loadError, setLoadError] = useState("");
  const [forbidden, setForbidden] = useState(false);
  // Rows cleared in this session whose value in force is still a number: the server's shipped value.
  const [shipped, setShipped] = useState<Record<string, number>>({});
  const [def, setDef] = useState("");
  const [rowLimit, setRowLimit] = useState("");
  const [per, setPer] = useState<Record<string, string>>({});
  const [errors, setErrors] = useState<Record<string, string>>({});
  const [saving, setSaving] = useState(false);
  const [msg, setMsg] = useState("");

  useEffect(() => {
    fetchSettings()
      .then((s) => {
        setSettings(s);
        if (!s.features?.deployment_settings) return;
        const limits = s.limits;
        if (!limits || !limits.request_timeouts || limits.request_timeout === undefined) {
          setLoadError(t("adminPage.requestTimeoutMissing"));
          return;
        }
        setDef(String(limits.request_timeout));
        setRowLimit(String(limits.default_row_limit));
        setPer(
          Object.fromEntries(
            TRANSPORTS.map((k) => [
              k,
              limits.request_timeouts[k] == null ? "" : String(limits.request_timeouts[k]),
            ]),
          ),
        );
      })
      .catch((e: unknown) => {
        // The platform blocks are platform_admin only: a 403 is a state, not a failure.
        if ((e as { status?: number }).status === 403) setForbidden(true);
        else setLoadError(e instanceof Error ? e.message : String(e));
      });
  }, [t]);

  if (forbidden) {
    return (
      <Alert color="gray" data-testid="limit-forbidden">
        {t("adminPage.setting.forbidden")}
      </Alert>
    );
  }
  if (!settings?.features?.deployment_settings && !loadError) return null;
  if (loadError) {
    return (
      <Alert color="red" data-testid="limit-settings-error">
        {loadError}
      </Alert>
    );
  }

  const effective = parseSeconds(def);
  const save = async () => {
    const next: Record<string, string> = {};
    const defaultSeconds = parseSeconds(def);
    if (defaultSeconds === null) next.default = t("adminPage.requestTimeoutInvalid");
    const rows = Number(rowLimit.trim());
    if (rowLimit.trim() === "" || !Number.isInteger(rows) || rows <= 0) {
      next.rowLimit = t("adminPage.rowLimitInvalid");
    }
    const timeouts: Record<string, number | null> = {};
    for (const k of TRANSPORTS) {
      const text = per[k] ?? "";
      if (text.trim() === "") {
        timeouts[k] = null;
        continue;
      }
      const seconds = parseSeconds(text);
      if (seconds === null) next[k] = t("adminPage.requestTimeoutInvalid");
      else timeouts[k] = seconds;
    }
    setErrors(next);
    setMsg("");
    if (Object.keys(next).length) return;
    setSaving(true);
    try {
      const result = await updateSettings({
        limits: {
          default_row_limit: rows,
          request_timeout: defaultSeconds as number,
          request_timeouts: timeouts,
        },
      });
      setMsg(
        result.updated.length
          ? t("adminPage.settingsUpdated", { fields: result.updated.join(", ") })
          : t("adminPage.settingsNoChanges"),
      );
      // Clearing a row removes its stored value; the API then reports the value in force. When that
      // is still a number, it is the shipped value, not something the operator stored.
      const fresh = (await fetchSettings()).limits?.request_timeouts;
      if (fresh) {
        const next: Record<string, number> = {};
        for (const k of TRANSPORTS) {
          const inForce = fresh[k];
          if (timeouts[k] === null && inForce !== null && inForce !== undefined) next[k] = inForce;
        }
        setShipped(next);
        setPer(
          Object.fromEntries(
            TRANSPORTS.map((k) => [k, timeouts[k] === null ? "" : String(timeouts[k])]),
          ),
        );
      }
    } catch (e: unknown) {
      const message = e instanceof Error ? e.message : String(e);
      const field = (e as { params?: { field?: unknown } }).params?.field;
      const named =
        typeof field === "string"
          ? field.endsWith("request_timeout")
            ? "default"
            : TRANSPORTS.find((k) => field.endsWith(`request_timeouts.${k}`))
          : undefined;
      const rowField = typeof field === "string" && field.endsWith("default_row_limit");
      if (rowField) setErrors({ rowLimit: message });
      else if (named) setErrors({ [named]: message });
      else setMsg(message);
    } finally {
      setSaving(false);
    }
  };

  return (
    <Card withBorder padding="md" data-testid="limit-settings">
      <Title order={4} mb="xs">
        {t("adminPage.serverLimits")}
      </Title>
      <Text fz="sm" c="dimmed" mb="sm">
        {t("adminPage.serverLimitsHint")}
      </Text>
      <Stack gap="sm">
        <FormLayout>
          <FormField help={t("adminPage.rowLimitHint")} size="short">
            <TextInput
              label={t("adminPage.rowLimit")}
              value={rowLimit}
              onChange={(e) => setRowLimit(e.currentTarget.value)}
              error={
                errors.rowLimit && (
                  <span data-testid="limit-default-row-limit-error">{errors.rowLimit}</span>
                )
              }
              data-testid="limit-default-row-limit"
            />
          </FormField>
          <FormField help={t("adminPage.requestTimeoutDefaultHint")} size="short">
            <TextInput
              label={t("adminPage.requestTimeoutDefault")}
              value={def}
              onChange={(e) => setDef(e.currentTarget.value)}
              error={
                errors.default && (
                  <span data-testid="limit-request-timeout-error">{errors.default}</span>
                )
              }
              data-testid="limit-request-timeout"
            />
          </FormField>
        </FormLayout>
        <Collapsibles id="admin.limits">
          <CollapsibleItem
            value="per-transport"
            defaultOpen={false}
            // A timeout the save would refuse keeps the panel open.
            forceOpen={TRANSPORTS.some((k) => Boolean(errors[k]))}
            controlTestId="limit-timeouts-toggle"
            title={t("settings.limits.request_timeouts.label")}
          >
            <Text fz="xs" c="dimmed" mb="xs">
              {t("settings.limits.request_timeouts.help")}
            </Text>
            <FormLayout data-testid="limit-timeouts-grid">
              {TRANSPORTS.map((k) => (
                <FormField size="short" key={k}>
                  <TextInput
                    label={t(`adminPage.transport.${k}`)}
                    value={per[k] ?? ""}
                    onChange={(e) => setPer({ ...per, [k]: e.currentTarget.value })}
                    error={
                      errors[k] && <span data-testid={`limit-timeout-${k}-error`}>{errors[k]}</span>
                    }
                    data-testid={`limit-timeout-${k}`}
                  />
                  <div className="form-status">
                    {k in TRANSPORT_HELP && (
                      <span data-testid={`limit-timeout-${k}-help`}>
                        {t(`adminPage.transportHelp.${k}`)}{" "}
                      </span>
                    )}
                    {(per[k] ?? "").trim() === "" && shipped[k] !== undefined && (
                      <span data-testid={`limit-timeout-${k}-shipped`}>
                        {t("adminPage.requestTimeoutUsesShipped", { seconds: shipped[k] })}
                      </span>
                    )}
                    {(per[k] ?? "").trim() === "" &&
                      shipped[k] === undefined &&
                      effective !== null && (
                        <span data-testid={`limit-timeout-${k}-effective`}>
                          {t("adminPage.requestTimeoutUsesDefault", { seconds: effective })}
                        </span>
                      )}
                  </div>
                </FormField>
              ))}
            </FormLayout>
          </CollapsibleItem>
        </Collapsibles>
      </Stack>
      <SaveRow save={save} saving={saving} msg={msg} />
    </Card>
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
      <FormLayout>
        <FormField help={t("adminPage.defaultThresholdHint")} size="short">
          <NumberInput
            label={t("adminPage.defaultThreshold")}
            value={settings.redirect.threshold}
            onChange={(v) => update("threshold", typeof v === "number" ? v : 0)}
          />
        </FormField>
        <FormField hint={t("adminPage.defaultFormatHint")} size="medium">
          <Select
            label={t("adminPage.defaultFormat")}
            data={FORMAT_OPTIONS}
            value={settings.redirect.default_format}
            onChange={(v) => v && update("default_format", v)}
            allowDeselect={false}
          />
        </FormField>
        <FormField help={t("adminPage.presignedUrlTtlHint")} size="short">
          <NumberInput
            label={t("adminPage.presignedUrlTtl")}
            value={settings.redirect.ttl}
            onChange={(v) => update("ttl", typeof v === "number" ? v : 0)}
          />
        </FormField>
        <FormChecks>
          <FormField help={t("adminPage.enabledHint")} size="short">
            <Checkbox
              label={t("adminPage.enabled")}
              checked={settings.redirect.enabled}
              onChange={(e) => update("enabled", e.currentTarget.checked)}
            />
          </FormField>
        </FormChecks>
      </FormLayout>
      <SaveRow save={save} saving={saving} msg={msg} />
    </Card>
  );
}

/** GraphQL/SQL naming conventions + the domain prefix (deployment-wide). Domains tab. */
export function NamingConventionsCard() {
  const { t } = useTranslation();
  const { settings, setSettings, save, saving, msg } = useSettingsBlocks(["naming"]);
  const conventions = useNamingConventions();
  // The naming module those three fields configure is process-global, so they are the deployment's
  // and the payload omits them entirely for anyone else.
  if (!settings?.features?.deployment_settings || settings.naming.convention === undefined) {
    return null;
  }
  // REQ-416: the server's conventions, the one list the admin offers.
  const conventionData = conventions.map((value) => ({
    value,
    label: namingConventionLabel(t, value),
  }));

  return (
    <Card withBorder padding="md" data-testid="naming-settings">
      <Title order={4} mb="sm">
        {t("adminPage.naming")}
      </Title>
      <FormLayout>
        <FormField help={t("adminPage.namingConventionHint")} size="medium">
          <Select
            label={t("adminPage.namingConvention")}
            data={conventionData}
            value={settings.naming.convention}
            onChange={(v) =>
              v && setSettings({ ...settings, naming: { ...settings.naming, convention: v } })
            }
            allowDeselect={false}
          />
        </FormField>
        <FormField help={t("adminPage.sqlNamingConventionHint")} size="medium">
          <Select
            label={t("adminPage.sqlNamingConvention")}
            data={conventionData}
            value={settings.naming.sql_convention}
            onChange={(v) =>
              v && setSettings({ ...settings, naming: { ...settings.naming, sql_convention: v } })
            }
            allowDeselect={false}
          />
        </FormField>
        <FormChecks>
          <FormField help={t("adminPage.domainPrefixHint")} size="long">
            <Checkbox
              label={t("adminPage.domainPrefix")}
              checked={settings.naming.domain_prefix ?? false}
              onChange={(e) =>
                setSettings({
                  ...settings,
                  naming: { ...settings.naming, domain_prefix: e.currentTarget.checked },
                })
              }
            />
          </FormField>
        </FormChecks>
      </FormLayout>
      <SaveRow save={save} saving={saving} msg={msg} />
    </Card>
  );
}

/**
 * Whether this org namespaces its tables by domain (org-scoped).
 *
 * Every registered table's domain is bound to the policy, so /admin/domain-policy refuses the
 * switch while the org has sources, tables or domains of its own (REQ-1919) and removes nothing.
 * The refusal, which counts what exists, is shown under the button.
 */
export function DomainModeCard({ onApplied }: { onApplied?: () => void }) {
  const { t } = useTranslation();
  const [useDomains, setUseDomains] = useState<boolean | null>(false);
  // Not editable: in single-domain mode the name reaches no label, no name and no access check
  // -- it is only the string every row is stored under -- so it is round-tripped, never shown.
  const [defaultDomain, setDefaultDomain] = useState("default");
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
        <FormLayout>
          <FormField help={t("adminPage.domainModeHint")} size="medium">
            <Select
              label={t("adminPage.domainMode")}
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
          </FormField>
          <FormField size="full">
            <Group gap="sm" align="center">
              <Button
                variant="default"
                data-testid="apply-domain-policy"
                onClick={apply}
                loading={applying}
              >
                {t("adminPage.applyDomainPolicy")}
              </Button>
              {appliedMsg && <Text fz="sm">{appliedMsg}</Text>}
            </Group>
          </FormField>
          {error && (
            <FormField size="full">
              <Alert color="red" variant="light" data-testid="domain-policy-refused">
                {error}
              </Alert>
            </FormField>
          )}
        </FormLayout>
      </Card>
    </>
  );
}

/** Sampling, CDC and the GraphQL-remote ceilings (deployment-wide). Federation tab. */
export function FederationSettingsCards() {
  const { t } = useTranslation();
  const { settings, setSettings, save, saving, msg } = useSettingsBlocks([
    "sampling",
    "cdc",
    "graphql_remote",
  ]);
  if (!settings?.features?.deployment_settings) return null;
  const { sampling, cdc, graphql_remote: remote } = settings;
  if (!sampling || !cdc || !remote) return null;

  return (
    <Collapsibles id="admin.federation-settings" testId="federation-settings">
      <CollapsibleItem value="settings" defaultOpen={false} title={t("adminPage.settingsPanel")}>
        <Stack gap="md">
          <Card withBorder padding="md">
            <Title order={4} mb="sm">
              {t("adminPage.sampling")}
            </Title>
            <FormLayout>
              <FormField help={t("adminPage.defaultSampleSizeHint")} size="short">
                <NumberInput
                  label={t("adminPage.defaultSampleSize")}
                  value={sampling.default_sample_size}
                  onChange={(v) =>
                    setSettings({
                      ...settings,
                      sampling: { default_sample_size: typeof v === "number" ? v : 0 },
                    })
                  }
                />
              </FormField>
            </FormLayout>
          </Card>

          <Card withBorder padding="md">
            <Title order={4} mb="sm">
              {t("adminPage.cdc")}
            </Title>
            <FormLayout>
              <FormField help={t("adminPage.consumerGroupIdHint")} size="medium">
                <TextInput
                  label={t("adminPage.consumerGroupId")}
                  placeholder={t("adminPage.consumerGroupIdPlaceholder")}
                  value={cdc.consumer_group_id}
                  onChange={(e) =>
                    setSettings({
                      ...settings,
                      cdc: { consumer_group_id: e.currentTarget.value },
                    })
                  }
                />
              </FormField>
            </FormLayout>
          </Card>

          <Card withBorder padding="md">
            <Title order={4} mb="sm">
              {t("adminPage.graphqlRemote")}
            </Title>
            <FormLayout>
              <FormField help={t("adminPage.maxObjectDepthHint")} size="short">
                <NumberInput
                  label={t("adminPage.maxObjectDepth")}
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
              </FormField>
              <FormField help={t("adminPage.maxListDepthHint")} size="short">
                <NumberInput
                  label={t("adminPage.maxListDepth")}
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
              </FormField>
              <FormField help={t("adminPage.maxListItemsHint")} size="short">
                <NumberInput
                  label={t("adminPage.maxListItems")}
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
              </FormField>
              <FormField help={t("adminPage.maxRowsHint")} size="short">
                <NumberInput
                  label={t("adminPage.maxRows")}
                  min={1}
                  value={remote.max_rows}
                  onChange={(v) =>
                    setSettings({
                      ...settings,
                      graphql_remote: {
                        ...remote,
                        max_rows: typeof v === "number" ? v : 0,
                      },
                    })
                  }
                />
              </FormField>
            </FormLayout>
          </Card>
          <ServerLimitsCard />
          <SaveRow save={save} saving={saving} msg={msg} />
        </Stack>
      </CollapsibleItem>
    </Collapsibles>
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
  // REQ-1919: the model sections an export carries; none chosen = the whole model.
  const [exportSections, setExportSections] = useState<string[]>([]);
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

  const handleExportModel = async () => {
    const yaml = await exportModelConfig(exportSections);
    const blob = new Blob([yaml], { type: "application/x-yaml" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = "provisa.model.yaml";
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
    try {
      const result = await uploadConfig(revisedConfig);
      setUploadMsg(result.message);
    } catch (e) {
      setUploadMsg(e instanceof Error ? e.message : String(e));
    } finally {
      setUploading(false);
    }
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
    try {
      const result = await uploadConfig(await file.text());
      setUploadMsg(result.message);
    } catch (e) {
      setUploadMsg(e instanceof Error ? e.message : String(e));
    } finally {
      setUploading(false);
    }
  };

  // The file IS the deployment's configuration, so it is the deployment administrator's surface.
  if (!settings?.features?.deployment_settings) return null;

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
      <Text fz="sm" c="dimmed">
        {t("adminPage.uploadAppliesOnce")}
      </Text>
      <Group gap="sm" align="flex-end">
        <FormLayout>
          <FormField size="long">
            <MultiSelect
              label={t("adminPage.exportSections")}
              placeholder={
                exportSections.length === 0 ? t("adminPage.exportWholeModel") : undefined
              }
              data={[...MODEL_SECTIONS]}
              value={exportSections}
              onChange={setExportSections}
              clearable
              searchable
              w={420}
              data-testid="export-sections"
            />
          </FormField>
        </FormLayout>
        <Button variant="default" onClick={handleExportModel} data-testid="export-model">
          {t("adminPage.exportModel")}
        </Button>
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
            <Tip text={t("adminPage.downloadPatchTitle")}>
              <Button
                variant="default"
                onClick={handleDownloadPatch}
                disabled={revisedConfig === diffOriginal}
              >
                {t("adminPage.downloadPatch")}
              </Button>
            </Tip>
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
