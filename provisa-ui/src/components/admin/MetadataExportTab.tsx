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
  Paper,
  Select,
  Stack,
  Table,
  Text,
  TextInput,
  Title,
} from "@mantine/core";
import { Check, Trash2, TriangleAlert } from "lucide-react";
import { fetchCatalogs, type CatalogConnection } from "../../api/catalogs";
import {
  checkMetadataExport,
  fetchMetadataExport,
  publishMetadataExport,
  setMetadataExport,
  type HealthOutcome,
  type MetadataExportState,
  type MetadataExportTarget,
  type PublishOutcome,
} from "../../api/metadataExport";
import { useRoles } from "../../hooks/useAdminQueries";
import { exportProviderName } from "../../lib/exportProviders";
import { FormChecks, FormField, FormFooter, FormLayout } from "../form/FormLayout";

// REQ-1074, REQ-2118: configure and operate the org's metadata export targets -- every catalog it
// publishes to, two of one provider among them. REQ-2122: a target names a catalog of the Catalogs
// panel, which holds its address and credentials, and keeps only the export's own settings for it.

/** A DataZone catalog's export names the project that owns its data products. */
const DATAZONE = "datazone";
/** A Unity Catalog catalog's export may also publish each product as a Delta Share. */
const UNITY_CATALOG = "unity_catalog";

const blankTarget = (): MetadataExportTarget => ({
  catalog: "",
  datazone_project_id: "",
  delta_share: false,
  role: null,
});

export function MetadataExportTab() {
  const { t } = useTranslation();
  const { roles } = useRoles();
  const [s, setS] = useState<MetadataExportState | null>(null);
  const [rows, setRows] = useState<MetadataExportTarget[]>([]);
  const [catalogs, setCatalogs] = useState<CatalogConnection[]>([]);
  const [busy, setBusy] = useState("");
  const [msg, setMsg] = useState("");
  const [error, setError] = useState("");
  const [health, setHealth] = useState<HealthOutcome | null>(null);
  const [publish, setPublish] = useState<PublishOutcome | null>(null);

  const load = (state: MetadataExportState) => {
    setS(state);
    setRows(state.config.targets);
  };

  useEffect(() => {
    Promise.all([fetchMetadataExport(), fetchCatalogs()])
      .then(([state, kept]) => {
        load(state);
        setCatalogs(kept.catalogs);
        setPublish(state.last_publish);
      })
      .catch((e) => setError(String(e)));
  }, []);

  const patch = (p: Partial<MetadataExportState["config"]>) =>
    setS((prev) => (prev ? { ...prev, config: { ...prev.config, ...p } } : prev));
  const patchRow = (i: number, p: Partial<MetadataExportTarget>) =>
    setRows((prev) => prev.map((r, j) => (j === i ? { ...r, ...p } : r)));

  const save = async () => {
    if (!s) return;
    setBusy("save");
    setMsg("");
    setError("");
    try {
      await setMetadataExport({
        enabled: s.config.enabled,
        reconcile_cron: s.config.reconcile_cron,
        targets: rows,
      });
      load(await fetchMetadataExport());
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
        data-tour="metadata-export"
        title={t("metadataExportTab.notEntitledTitle")}
      >
        {t("metadataExportTab.notEntitled", { tier: s.required_tier })}
      </Alert>
    );

  const c = s.config;
  const betaProviders = new Set(s.beta_providers);

  const targetForm = (target: MetadataExportTarget, i: number) => {
    // A target's test ids carry its place in the list, so two of one provider stay apart.
    const tid = (name: string) => `metadata-export-${name}-${i}`;
    const provider = catalogs.find((k) => k.id === target.catalog)?.provider ?? "";
    return (
      <Paper key={i} withBorder p="sm" data-testid={tid("target")}>
        <Group justify="space-between" mb="xs">
          <Title order={6}>
            {target.catalog || t("metadataExportTab.newTarget")}
            {provider ? ` · ${exportProviderName(provider, provider, t, betaProviders)}` : ""}
          </Title>
          <Button
            variant="subtle"
            color="red"
            size="compact-sm"
            leftSection={<Trash2 size={14} />}
            data-testid={tid("remove")}
            onClick={() => setRows((prev) => prev.filter((_, j) => j !== i))}
          >
            {t("metadataExportTab.removeTarget")}
          </Button>
        </Group>
        <FormLayout>
          <FormField help={t("metadataExportTab.catalogHelp")} size="medium">
            <Select
              label={t("metadataExportTab.catalog")}
              data-testid={tid("catalog")}
              data={catalogs.map((k) => ({
                value: k.id,
                label: `${k.id} · ${exportProviderName(k.provider, k.provider, t, betaProviders)}`,
              }))}
              value={target.catalog || null}
              onChange={(v) => patchRow(i, { catalog: v ?? "" })}
            />
          </FormField>
          {/* REQ-2152: a catalog is sent exactly what the role it is published as reads. */}
          <FormField help={t("metadataExportTab.roleHelp")} size="medium">
            <Select
              label={t("metadataExportTab.role")}
              data-testid={tid("role")}
              data={roles.map((r) => r.id)}
              value={target.role}
              onChange={(v) => patchRow(i, { role: v })}
              required
            />
          </FormField>
          {provider === DATAZONE && (
            <FormField help={t("metadataExportTab.datazoneProjectIdHelp")} size="medium">
              <TextInput
                label={t("metadataExportTab.datazoneProjectId")}
                data-testid={tid("datazone-project-id")}
                value={target.datazone_project_id}
                onChange={(e) => patchRow(i, { datazone_project_id: e.currentTarget.value })}
              />
            </FormField>
          )}
          {provider === UNITY_CATALOG && (
            <FormChecks>
              <FormField help={t("metadataExportTab.deltaShareHelp")} size="long">
                <Checkbox
                  label={t("metadataExportTab.deltaShare")}
                  data-testid={tid("delta-share")}
                  checked={target.delta_share}
                  onChange={(e) => patchRow(i, { delta_share: e.currentTarget.checked })}
                />
              </FormField>
            </FormChecks>
          )}
        </FormLayout>
      </Paper>
    );
  };

  return (
    // The page's root, entitled as not (the alert above carries it too): the tour points at it.
    <div data-tour="metadata-export">
      <FormLayout>
        <FormField size="full">
          <Text c="dimmed" size="sm">
            {t("metadataExportTab.intro")}
          </Text>
        </FormField>

        <FormChecks>
          <FormField size="long">
            <Checkbox
              label={t("metadataExportTab.enabled")}
              data-testid="metadata-export-enabled"
              checked={c.enabled}
              onChange={(e) => patch({ enabled: e.currentTarget.checked })}
            />
          </FormField>
        </FormChecks>
        <FormField help={t("metadataExportTab.reconcileCronHelp")} size="medium">
          <TextInput
            label={t("metadataExportTab.reconcileCron")}
            data-testid="metadata-export-reconcile-cron"
            value={c.reconcile_cron}
            onChange={(e) => patch({ reconcile_cron: e.currentTarget.value })}
          />
        </FormField>

        <FormField size="full">
          <Stack gap="sm" data-testid="metadata-export-targets">
            <Group justify="space-between">
              <Title order={5}>{t("metadataExportTab.targetsHeading")}</Title>
              <Button
                variant="default"
                size="compact-sm"
                data-testid="metadata-export-add-target"
                onClick={() => setRows((prev) => [...prev, blankTarget()])}
              >
                {t("metadataExportTab.addTarget")}
              </Button>
            </Group>
            {rows.length === 0 && (
              <Text c="dimmed" size="sm" data-testid="metadata-export-no-targets">
                {t("metadataExportTab.noTargets")}
              </Text>
            )}
            {rows.map(targetForm)}
          </Stack>
        </FormField>

        <FormFooter
          onSave={save}
          saving={busy === "save"}
          saveProps={{ "data-testid": "metadata-export-save" }}
        >
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
        </FormFooter>

        {msg && (
          <FormField size="full">
            <Alert color="green" icon={<Check size={16} />} data-testid="metadata-export-saved">
              {msg}
            </Alert>
          </FormField>
        )}
        {error && (
          <FormField size="full">
            <Alert
              color="red"
              icon={<TriangleAlert size={16} />}
              data-testid="metadata-export-error"
            >
              {error}
            </Alert>
          </FormField>
        )}
        {health && (
          <FormField size="full">
            <Stack gap="xs" data-testid="metadata-export-health-result">
              {health.targets.map((h) => (
                <Alert
                  key={h.id}
                  color={h.ok ? "green" : "red"}
                  icon={h.ok ? <Check size={16} /> : <TriangleAlert size={16} />}
                  title={h.id}
                  data-testid={`metadata-export-health-${h.id}`}
                >
                  {h.ok ? t("metadataExportTab.healthOk") : h.error}
                </Alert>
              ))}
            </Stack>
          </FormField>
        )}

        {publish && (
          <FormField size="full">
            <Stack gap="xs" data-testid="metadata-export-last-publish">
              <Title order={5}>{t("metadataExportTab.lastPublishHeading")}</Title>
              {publish.targets.map((outcome) => (
                <Stack
                  gap={4}
                  key={outcome.id}
                  data-testid={`metadata-export-publish-${outcome.id}`}
                >
                  <Group gap="xs">
                    <Text fw={600} size="sm">
                      {outcome.id}
                    </Text>
                    <Badge color={outcome.ok ? "green" : "red"}>
                      {outcome.ok
                        ? t("metadataExportTab.publishComplete", {
                            count: outcome.total_published,
                          })
                        : t("metadataExportTab.publishPartial", {
                            count: outcome.total_published,
                            failed: outcome.errors.length,
                          })}
                    </Badge>
                  </Group>
                  {outcome.errors.length > 0 && (
                    <Table data-testid="metadata-export-publish-errors">
                      <Table.Thead>
                        <Table.Tr>
                          <Table.Th>{t("metadataExportTab.errorAsset")}</Table.Th>
                          <Table.Th>{t("metadataExportTab.errorMessage")}</Table.Th>
                        </Table.Tr>
                      </Table.Thead>
                      <Table.Tbody>
                        {outcome.errors.map((e) => (
                          <Table.Tr key={`${e.asset}:${e.message}`}>
                            <Table.Td>{e.asset}</Table.Td>
                            <Table.Td>{e.message}</Table.Td>
                          </Table.Tr>
                        ))}
                      </Table.Tbody>
                    </Table>
                  )}
                </Stack>
              ))}
            </Stack>
          </FormField>
        )}
      </FormLayout>
    </div>
  );
}
