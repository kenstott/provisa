// Copyright (c) 2026 Kenneth Stott
// Canary: b8b1d1a3-e713-464e-8e0a-c8bc5b43544d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState, useEffect, useRef, useCallback } from "react";
import { useLocation } from "react-router-dom";
import { useTranslation } from "react-i18next";
import { Trash2, Check } from "lucide-react";
import {
  ActionIcon,
  Alert,
  Button,
  Card,
  Checkbox,
  FileButton,
  Group,
  Modal,
  NumberInput,
  Pagination,
  Select,
  SimpleGrid,
  Stack,
  Table,
  Text,
  TextInput,
  Title,
} from "@mantine/core";
import {
  useDomains,
  useTables,
  useRelationships,
  useSources,
  useRLSRules,
  useRoles,
  useCreateDomain,
  useDeleteDomain,
} from "../hooks/useAdminQueries";
import {
  downloadConfig,
  fetchConfigDiff,
  downloadConfigPatch,
  uploadConfig,
  fetchSettings,
  updateSettings,
  setDomainPolicy,
} from "../api/admin";
import type { PlatformSettings } from "../api/admin";
import { useAuth } from "../context/AuthContext";
import { domainGqlAlias } from "../types/admin";
import { CacheManager } from "../components/admin/CacheManager";
import { SystemHealth } from "../components/admin/SystemHealth";
import { ScheduledTasks } from "../components/admin/ScheduledTasks";
import { ObservabilityTab } from "../components/admin/ObservabilityTab";
import { FederationEngineTab } from "../components/admin/FederationEngineTab";
import { McpServerTab } from "../components/admin/McpServerTab";
import { EncryptionTab } from "../components/admin/EncryptionTab";
import { AuthTab } from "../components/admin/AuthTab";
import { LocalUsersTab } from "../components/admin/LocalUsersTab";
import { OrgsTab } from "../components/admin/OrgsTab";
import { ConfigDiffView } from "../components/admin/ConfigDiffView";

const FORMAT_OPTIONS = ["parquet", "orc", "json", "ndjson", "csv", "arrow"];

const ROUTE_TO_SECTION: Record<string, string> = {
  "/admin/overview": "Overview",
  "/admin/domains": "Domains",
  "/admin/cache": "Cache",
  "/admin/scheduled-tasks": "Scheduler",
  "/admin/federation-engine": "Federation",
  "/admin/encryption": "Encryption",
  "/admin/auth": "Authentication",
  "/admin/system-health": "Health",
  "/admin/observability": "Observability",
  "/admin/mcp-server": "MCP Server",
  "/admin/local-users": "Local Users",
  "/admin/orgs": "Orgs",
};

/** Admin overview page — dashboard, config management, platform settings. */
export function AdminPage() {
  const { t } = useTranslation();
  const location = useLocation();
  const activeTab = ROUTE_TO_SECTION[location.pathname] ?? "Overview";
  const { capabilities } = useAuth();
  const isSuperAdmin = capabilities.includes("superadmin") || capabilities.includes("admin");
  const [stats, setStats] = useState<Record<string, number>>({});
  const [newDomainId, setNewDomainId] = useState("");
  const [newDomainDesc, setNewDomainDesc] = useState("");
  const [newDomainAlias, setNewDomainAlias] = useState("");
  const [domainMsg, setDomainMsg] = useState("");
  const [loading, setLoading] = useState(true);
  // Config diff: original (on-disk) vs current (live state), with the edited/reverted current tracked
  // for apply. null diffOriginal = diff view closed.
  const [diffOriginal, setDiffOriginal] = useState<string | null>(null);
  const [diffCurrent, setDiffCurrent] = useState<string>("");
  const [revisedConfig, setRevisedConfig] = useState<string>("");
  const [uploading, setUploading] = useState(false);
  const [uploadMsg, setUploadMsg] = useState("");
  const [settings, setSettings] = useState<PlatformSettings | null>(null);
  const [settingsSaving, setSettingsSaving] = useState(false);
  const [settingsMsg, setSettingsMsg] = useState("");
  // Domain policy controls (NOT saved via updateSettings — destructive, applied separately)
  const [policyUseDomains, setPolicyUseDomains] = useState<boolean | null>(false);
  const [policyDefaultDomain, setPolicyDefaultDomain] = useState("default");
  const [policyModalOpen, setPolicyModalOpen] = useState(false);
  const [policyConfirmText, setPolicyConfirmText] = useState("");
  const [policyApplying, setPolicyApplying] = useState(false);
  const [policyMsg, setPolicyMsg] = useState("");
  const [policyError, setPolicyError] = useState("");
  const fileInputRef = useRef<() => void>(null);
  const [allDomains, setAllDomains] = useState<string[]>([]);

  // Pagination state
  const [domainPage, setDomainPage] = useState(0);
  const PAGE_SIZE = 50;

  // Apollo hooks for cache-and-network queries and mutations
  const { sources, loading: sourcesLoading } = useSources();
  const { domains, loading: domainsLoading, refetch: refetchDomains } = useDomains();
  const { tables, loading: tablesLoading } = useTables();
  const { relationships, loading: relsLoading } = useRelationships();
  const { rlsRules, loading: rlsLoading } = useRLSRules();
  const { roles } = useRoles();
  const { createDomain } = useCreateDomain();
  const { deleteDomain } = useDeleteDomain();
  const allRoles = roles.map((r) => r.id);

  // Update state and stats when hook data arrives
  useEffect(() => {
    const loading = sourcesLoading || domainsLoading || tablesLoading || relsLoading || rlsLoading;
    /* eslint-disable-next-line react-hooks/set-state-in-effect --
       derived state synced from multiple Apollo query results (documented useState+useEffect derived pattern) */
    setLoading(loading);

    if (!loading) {
      // A view is a registered table with view_sql; materialized ones additionally have materialize.
      const viewTables = tables.filter((t) => t.viewSql != null);
      setStats({
        Sources: sources.length,
        Domains: domains.length,
        Tables: tables.length,
        Views: viewTables.length,
        "Materialized Views": viewTables.filter((t) => t.materialize).length,
        Relationships: relationships.length,
        Roles: allRoles.length,
        "RLS Rules": rlsRules.length,
      });
      setAllDomains(domains.filter((d) => d.id !== "").map((d) => d.id));
    }
  }, [
    sources,
    domains,
    tables,
    relationships,
    rlsRules,
    rlsLoading,
    domainsLoading,
    tablesLoading,
    relsLoading,
    sourcesLoading,
    allRoles.length,
  ]);

  // Platform settings (REST); per-tab data is loaded by each tab component.
  useEffect(() => {
    fetchSettings().then((s) => {
      setSettings(s);
      setPolicyUseDomains(s.naming.use_domains);
      setPolicyDefaultDomain(s.naming.default_domain);
    });
  }, []);

  const domainsEnabled = settings?.naming.use_domains !== false;

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

  const saveSettings = useCallback(async () => {
    if (!settings) return;
    setSettingsSaving(true);
    setSettingsMsg("");
    // Domain policy (use_domains/default_domain) is applied separately via the
    // destructive /admin/domain-policy endpoint — never through the normal save.
    const { use_domains: _ud, default_domain: _dd, ...naming } = settings.naming;
    const payload = { ...settings, naming } as unknown as Partial<PlatformSettings>;
    const result = await updateSettings(payload);
    const base = result.updated.length
      ? t("adminPage.settingsUpdated", { fields: result.updated.join(", ") })
      : t("adminPage.settingsNoChanges");
    setSettingsMsg(
      result.restart_required ? t("adminPage.settingsRestartRequired", { base }) : base,
    );
    setSettingsSaving(false);
  }, [settings, t]);

  const applyDomainPolicy = useCallback(async () => {
    setPolicyApplying(true);
    setPolicyError("");
    setPolicyMsg("");
    try {
      const result = await setDomainPolicy({
        use_domains: policyUseDomains,
        default_domain: policyDefaultDomain,
      });
      setPolicyModalOpen(false);
      setPolicyConfirmText("");
      setPolicyMsg(t("adminPage.policyApplied", { backup: result.backup }));
      const s = await fetchSettings();
      setSettings(s);
      setPolicyUseDomains(s.naming.use_domains);
      setPolicyDefaultDomain(s.naming.default_domain);
    } catch (err) {
      setPolicyError(err instanceof Error ? err.message : t("adminPage.policyApplyFailed"));
    } finally {
      setPolicyApplying(false);
    }
  }, [policyUseDomains, policyDefaultDomain, t]);

  const updateRedirect = (key: string, value: unknown) => {
    if (!settings) return;
    setSettings({
      ...settings,
      redirect: { ...settings.redirect, [key]: value },
    });
  };

  const handleAddDomain = async () => {
    if (!newDomainId.trim()) return;
    await createDomain(newDomainId.trim(), newDomainDesc.trim(), newDomainAlias.trim() || null);
    await refetchDomains();
    setNewDomainId("");
    setNewDomainDesc("");
    setNewDomainAlias("");
    setDomainMsg(t("adminPage.domainAdded", { id: newDomainId.trim() }));
  };

  const handleDeleteDomain = async (id: string) => {
    await deleteDomain(id);
    await refetchDomains();
    setDomainMsg(t("adminPage.domainDeleted", { id }));
  };

  if (loading) return <div className="page">{t("adminPage.loading")}</div>;

  return (
    <div className="page">
      <Title order={2} mb="md">
        {t("adminPage.title")}
        {activeTab !== "Overview" ? ` — ${activeTab}` : ""}
      </Title>

      <Stack gap="lg">
        {activeTab === "Overview" && (
          <>
            <SimpleGrid cols={{ base: 2, sm: 4 }} spacing="md">
              {Object.entries(stats).map(([label, count]) => (
                <Card key={label} withBorder padding="md" data-testid={`stat-card-${label}`}>
                  <Text fz={28} fw={700}>
                    {count}
                  </Text>
                  <Text c="dimmed" fz="sm">
                    {label}
                  </Text>
                </Card>
              ))}
            </SimpleGrid>

            <Title order={3}>{t("adminPage.platformSettings")}</Title>
            {settings && (
              <SimpleGrid cols={{ base: 1, md: 2 }} spacing="lg">
                <Card withBorder padding="md">
                  <Title order={4} mb="sm">
                    {t("adminPage.redirect")}
                  </Title>
                  <Stack gap="sm">
                    <Checkbox
                      label={t("adminPage.enabled")}
                      checked={settings.redirect.enabled}
                      onChange={(e) => updateRedirect("enabled", e.currentTarget.checked)}
                    />
                    <NumberInput
                      label={t("adminPage.defaultThreshold")}
                      value={settings.redirect.threshold}
                      onChange={(v) => updateRedirect("threshold", typeof v === "number" ? v : 0)}
                    />
                    <Select
                      label={t("adminPage.defaultFormat")}
                      data={FORMAT_OPTIONS}
                      value={settings.redirect.default_format}
                      onChange={(v) => v && updateRedirect("default_format", v)}
                      allowDeselect={false}
                    />
                    <NumberInput
                      label={t("adminPage.presignedUrlTtl")}
                      value={settings.redirect.ttl}
                      onChange={(v) => updateRedirect("ttl", typeof v === "number" ? v : 0)}
                    />
                  </Stack>
                </Card>

                <Card withBorder padding="md">
                  <Title order={4} mb="sm">
                    {t("adminPage.naming")}
                  </Title>
                  <Stack gap="sm">
                    <Checkbox
                      label={t("adminPage.domainPrefix")}
                      checked={settings.naming.domain_prefix}
                      onChange={(e) =>
                        setSettings({
                          ...settings,
                          naming: { ...settings.naming, domain_prefix: e.currentTarget.checked },
                        })
                      }
                    />
                    <Select
                      label={t("adminPage.namingConvention")}
                      data={[
                        { value: "none", label: t("adminPage.namingConventionNone") },
                        { value: "snake_case", label: t("adminPage.namingConventionSnake") },
                        { value: "camelCase", label: t("adminPage.namingConventionCamel") },
                        { value: "PascalCase", label: t("adminPage.namingConventionPascal") },
                      ]}
                      value={settings.naming.convention || "snake_case"}
                      onChange={(v) =>
                        v &&
                        setSettings({
                          ...settings,
                          naming: { ...settings.naming, convention: v },
                        })
                      }
                      allowDeselect={false}
                    />
                    <Select
                      label={t("adminPage.domainMode")}
                      data={[
                        { value: "legacy", label: t("adminPage.domainModeLegacy") },
                        { value: "single", label: t("adminPage.domainModeSingle") },
                        { value: "namespaced", label: t("adminPage.domainModeNamespaced") },
                      ]}
                      value={
                        policyUseDomains === null
                          ? "legacy"
                          : policyUseDomains
                            ? "namespaced"
                            : "single"
                      }
                      onChange={(v) =>
                        setPolicyUseDomains(v === "legacy" ? null : v === "namespaced")
                      }
                      allowDeselect={false}
                    />
                    <TextInput
                      label={t("adminPage.defaultDomain")}
                      value={policyDefaultDomain}
                      disabled={policyUseDomains !== false}
                      onChange={(e) => setPolicyDefaultDomain(e.currentTarget.value)}
                    />
                    <Group gap="sm" align="center">
                      <Button
                        variant="default"
                        data-testid="apply-domain-policy"
                        onClick={() => {
                          setPolicyError("");
                          setPolicyConfirmText("");
                          setPolicyModalOpen(true);
                        }}
                      >
                        {t("adminPage.applyDomainPolicy")}
                      </Button>
                      {policyMsg && <Text fz="sm">{policyMsg}</Text>}
                    </Group>
                  </Stack>
                </Card>

                <Card withBorder padding="md">
                  <Title order={4} mb="sm">
                    {t("adminPage.sampling")}
                  </Title>
                  <NumberInput
                    label={t("adminPage.defaultSampleSize")}
                    value={settings.sampling.default_sample_size}
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
                    value={settings.cdc.consumer_group_id}
                    onChange={(e) =>
                      setSettings({
                        ...settings,
                        cdc: { consumer_group_id: e.currentTarget.value },
                      })
                    }
                    description={t("adminPage.consumerGroupIdHint")}
                  />
                </Card>

                <Card withBorder padding="md">
                  <Title order={4} mb="sm">
                    {t("adminPage.cache")}
                  </Title>
                  <NumberInput
                    label={t("adminPage.defaultTtl")}
                    value={settings.cache.default_ttl}
                    onChange={(v) =>
                      setSettings({
                        ...settings,
                        cache: { default_ttl: typeof v === "number" ? v : 0 },
                      })
                    }
                  />
                </Card>

                <Group gap="sm" align="center">
                  <ActionIcon
                    variant="filled"
                    size="lg"
                    aria-label={t("adminPage.saveSettings")}
                    onClick={saveSettings}
                    loading={settingsSaving}
                  >
                    <Check size={14} />
                  </ActionIcon>
                  {settingsMsg && <Text fz="sm">{settingsMsg}</Text>}
                </Group>
              </SimpleGrid>
            )}

            <Title order={3}>{t("adminPage.configurationFile")}</Title>
            <Group gap="sm" align="center">
              <Button variant="default" onClick={handleDownload}>
                {t("adminPage.download")}
              </Button>
              {settings?.features?.live_config_export && (
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
          </>
        )}

        {activeTab === "Domains" && domainsEnabled && (
          <>
            {domainMsg && (
              <Alert color="green" variant="light">
                {domainMsg}
              </Alert>
            )}
            {(() => {
              const IMPLICIT_DOMAIN_IDS = new Set(["", "meta", "ops"]);
              const userDomains = domains.filter((d) => !IMPLICIT_DOMAIN_IDS.has(d.id));
              const totalPages = Math.max(1, Math.ceil(userDomains.length / PAGE_SIZE));
              const paged = userDomains.slice(domainPage * PAGE_SIZE, (domainPage + 1) * PAGE_SIZE);
              return (
                <Stack gap="sm">
                  <Table.ScrollContainer minWidth={480}>
                    <Table striped highlightOnHover withTableBorder verticalSpacing="xs">
                      <Table.Thead>
                        <Table.Tr>
                          <Table.Th>{t("adminPage.colId")}</Table.Th>
                          <Table.Th>{t("adminPage.colDescription")}</Table.Th>
                          <Table.Th>{t("adminPage.colGqlAlias")}</Table.Th>
                          <Table.Th>{t("adminPage.colActions")}</Table.Th>
                        </Table.Tr>
                      </Table.Thead>
                      <Table.Tbody>
                        {userDomains.length === 0 && (
                          <Table.Tr>
                            <Table.Td colSpan={4} ta="center" c="dimmed">
                              {t("adminPage.noDomains")}
                            </Table.Td>
                          </Table.Tr>
                        )}
                        {paged.map((d) => (
                          <Table.Tr key={d.id}>
                            <Table.Td>{d.id}</Table.Td>
                            <Table.Td>{d.description || "—"}</Table.Td>
                            <Table.Td>
                              <Text c="dimmed" ff="monospace" fz="sm">
                                {domainGqlAlias(d)}
                              </Text>
                            </Table.Td>
                            <Table.Td>
                              <ActionIcon
                                variant="subtle"
                                color="red"
                                aria-label={t("adminPage.deleteDomain", { id: d.id })}
                                onClick={() => handleDeleteDomain(d.id)}
                              >
                                <Trash2 size={14} />
                              </ActionIcon>
                            </Table.Td>
                          </Table.Tr>
                        ))}
                      </Table.Tbody>
                    </Table>
                  </Table.ScrollContainer>
                  {totalPages > 1 && (
                    <Group justify="flex-end">
                      <Pagination
                        total={totalPages}
                        value={domainPage + 1}
                        onChange={(p) => setDomainPage(p - 1)}
                        size="sm"
                      />
                    </Group>
                  )}
                </Stack>
              );
            })()}
            <Group gap="sm" align="flex-end">
              <TextInput
                data-testid="new-domain-id"
                value={newDomainId}
                onChange={(e) => setNewDomainId(e.currentTarget.value)}
                placeholder={t("adminPage.domainIdPlaceholder")}
                w={160}
              />
              <TextInput
                data-testid="new-domain-desc"
                value={newDomainDesc}
                onChange={(e) => setNewDomainDesc(e.currentTarget.value)}
                placeholder={t("adminPage.domainDescPlaceholder")}
                style={{ flex: 1 }}
              />
              <TextInput
                data-testid="new-domain-alias"
                value={newDomainAlias}
                onChange={(e) => setNewDomainAlias(e.currentTarget.value)}
                placeholder={
                  newDomainId.trim()
                    ? t("adminPage.domainAliasPlaceholderDefault", {
                        alias: domainGqlAlias({ id: newDomainId.trim(), description: "" }),
                      })
                    : t("adminPage.domainAliasPlaceholder")
                }
                w={180}
              />
              <Button onClick={handleAddDomain} disabled={!newDomainId.trim()}>
                {t("adminPage.addDomain")}
              </Button>
            </Group>
          </>
        )}
        {activeTab === "Cache" && <CacheManager />}
        {activeTab === "Scheduler" && <ScheduledTasks />}
        {activeTab === "Federation" && <FederationEngineTab />}
        {activeTab === "Encryption" && <EncryptionTab />}
        {activeTab === "Authentication" && <AuthTab />}
        {activeTab === "Health" && <SystemHealth />}
        {activeTab === "Observability" && settings && (
          <ObservabilityTab settings={settings} setSettings={setSettings} />
        )}
        {activeTab === "MCP Server" && <McpServerTab />}
        {activeTab === "Local Users" && (
          <LocalUsersTab allRoles={allRoles} allDomains={allDomains} />
        )}
        {activeTab === "Orgs" && isSuperAdmin && <OrgsTab />}
      </Stack>

      <Modal
        opened={policyModalOpen}
        onClose={() => {
          setPolicyModalOpen(false);
          setPolicyConfirmText("");
          setPolicyError("");
        }}
        title={t("adminPage.policyModalTitle")}
        centered
        closeOnClickOutside={!policyApplying}
        closeOnEscape={!policyApplying}
        data-testid="domain-policy-modal"
      >
        <Stack gap="md">
          <Alert color="red" variant="filled">
            {t("adminPage.policyModalWarning")}
          </Alert>
          <TextInput
            label={t("adminPage.policyConfirmLabel")}
            data-testid="domain-policy-confirm-input"
            value={policyConfirmText}
            onChange={(e) => setPolicyConfirmText(e.currentTarget.value)}
          />
          {policyError && (
            <Alert color="red" variant="light">
              {policyError}
            </Alert>
          )}
          <Group justify="flex-end">
            <Button
              variant="default"
              onClick={() => {
                setPolicyModalOpen(false);
                setPolicyConfirmText("");
                setPolicyError("");
              }}
              disabled={policyApplying}
            >
              {t("adminPage.policyCancel")}
            </Button>
            <Button
              color="red"
              data-testid="domain-policy-confirm-btn"
              disabled={policyConfirmText !== "RESET" || policyApplying}
              onClick={applyDomainPolicy}
              loading={policyApplying}
            >
              {policyApplying ? t("adminPage.applying") : t("adminPage.policyResetApply")}
            </Button>
          </Group>
        </Stack>
      </Modal>
    </div>
  );
}
