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
  Paper,
  Select,
  Stack,
  Table,
  Text,
  TextInput,
  Title,
} from "@mantine/core";
import { Check, Trash2, TriangleAlert } from "lucide-react";
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
import { FormChecks, FormField, FormFooter, FormLayout } from "../form/FormLayout";

// REQ-1074, REQ-2118: configure and operate the org's metadata export targets -- every catalog it
// publishes to, two of one provider among them.
//
// Credentials are entered here and never read back (REQ-1074): the server reports each as
// set/not-set, so an empty field means "leave the stored one alone" rather than "no secret". A
// field is only sent when the administrator typed in it, which is what makes editing a target's
// endpoint safe for its token.

/** Which credential fields each auth mode uses, so the form asks for exactly those. */
const ENTRA_MODE = "entra";
const BASIC_MODE = "basic";
const GOOGLE_MODE = "google_service_account";

const AUTH_MODES = [
  { value: "api_key", label: "API key" },
  { value: "bearer", label: "Bearer token" },
  { value: BASIC_MODE, label: "Username and password (Apache Atlas)" },
  { value: ENTRA_MODE, label: "Microsoft Entra (Purview)" },
  { value: GOOGLE_MODE, label: "Google service account (Dataplex)" },
];

type SecretField =
  | "api_key"
  | "token"
  | "entra_client_secret"
  | "google_service_account_json"
  | "aws_access_key_id"
  | "aws_secret_access_key";

/** A DataZone target signs AWS requests with its own keys: it has no endpoint or auth mode. */
const DATAZONE = "datazone";

/** A target being edited, with the credentials typed for it since the last save. */
interface Row {
  target: MetadataExportTarget;
  secrets: Partial<Record<SecretField, string>>;
}

const blankTarget = (): MetadataExportTarget => ({
  id: "",
  provider: "",
  endpoint: "",
  auth_mode: "api_key",
  username: "",
  entra_tenant_id: "",
  entra_client_id: "",
  gcp_project: "",
  gcp_location: "",
  aws_region: "",
  datazone_domain_id: "",
  datazone_project_id: "",
  timeout_seconds: 30,
  api_key_set: false,
  token_set: false,
  entra_client_secret_set: false,
  google_service_account_json_set: false,
  aws_access_key_id_set: false,
  aws_secret_access_key_set: false,
});

export function MetadataExportTab() {
  const { t } = useTranslation();
  const [s, setS] = useState<MetadataExportState | null>(null);
  const [rows, setRows] = useState<Row[]>([]);
  const [busy, setBusy] = useState("");
  const [msg, setMsg] = useState("");
  const [error, setError] = useState("");
  const [health, setHealth] = useState<HealthOutcome | null>(null);
  const [publish, setPublish] = useState<PublishOutcome | null>(null);

  const load = (state: MetadataExportState) => {
    setS(state);
    setRows(state.config.targets.map((target) => ({ target, secrets: {} })));
  };

  useEffect(() => {
    fetchMetadataExport()
      .then((state) => {
        load(state);
        setPublish(state.last_publish);
      })
      .catch((e) => setError(String(e)));
  }, []);

  const patch = (p: Partial<MetadataExportState["config"]>) =>
    setS((prev) => (prev ? { ...prev, config: { ...prev.config, ...p } } : prev));
  const patchRow = (i: number, p: Partial<MetadataExportTarget>) =>
    setRows((prev) => prev.map((r, j) => (j === i ? { ...r, target: { ...r.target, ...p } } : r)));
  const typeSecret = (i: number, field: SecretField, value: string) =>
    setRows((prev) =>
      prev.map((r, j) => (j === i ? { ...r, secrets: { ...r.secrets, [field]: value } } : r)),
    );

  const save = async () => {
    if (!s) return;
    setBusy("save");
    setMsg("");
    setError("");
    try {
      await setMetadataExport({
        enabled: s.config.enabled,
        reconcile_cron: s.config.reconcile_cron,
        targets: rows.map(({ target, secrets }) => ({
          id: target.id,
          provider: target.provider,
          endpoint: target.endpoint,
          auth_mode: target.auth_mode,
          username: target.username,
          entra_tenant_id: target.entra_tenant_id,
          entra_client_id: target.entra_client_id,
          gcp_project: target.gcp_project,
          gcp_location: target.gcp_location,
          aws_region: target.aws_region,
          datazone_domain_id: target.datazone_domain_id,
          datazone_project_id: target.datazone_project_id,
          timeout_seconds: target.timeout_seconds,
          ...secrets,
        })),
      });
      // Re-read rather than assume: the set/not-set flags now reflect what was persisted, and a
      // typed secret is dropped from local state so it is not re-sent on the next save.
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

  const targetForm = ({ target, secrets }: Row, i: number) => {
    // A target's test ids carry its place in the list, so two of one provider stay apart.
    const tid = (name: string) => `metadata-export-${name}-${i}`;
    const secretField = (field: SecretField, label: string, isSet: boolean) => (
      <FormField help={t("metadataExportTab.secretHelp")} size="medium">
        <TextInput
          label={label}
          type="password"
          data-testid={tid(field.replace(/_/g, "-"))}
          placeholder={
            isSet ? t("metadataExportTab.secretSet") : t("metadataExportTab.secretUnset")
          }
          value={secrets[field] ?? ""}
          onChange={(e) => typeSecret(i, field, e.currentTarget.value)}
        />
      </FormField>
    );
    return (
      <Paper key={i} withBorder p="sm" data-testid={tid("target")}>
        <Group justify="space-between" mb="xs">
          <Title order={6}>
            {target.id || t("metadataExportTab.newTarget")}
            {target.provider ? ` · ${target.provider}` : ""}
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
          <FormField help={t("metadataExportTab.targetIdHelp")} size="medium">
            <TextInput
              label={t("metadataExportTab.targetId")}
              data-testid={tid("id")}
              value={target.id}
              onChange={(e) => patchRow(i, { id: e.currentTarget.value })}
            />
          </FormField>
          <FormField size="medium">
            <Select
              label={t("metadataExportTab.provider")}
              data-testid={tid("provider")}
              data={s.providers.map((p) => ({ value: p, label: p }))}
              value={target.provider || null}
              onChange={(v) => patchRow(i, { provider: v ?? "" })}
            />
          </FormField>
          {target.provider === DATAZONE ? (
            <>
              <FormField help={t("metadataExportTab.awsRegionHelp")} size="short">
                <TextInput
                  label={t("metadataExportTab.awsRegion")}
                  data-testid={tid("aws-region")}
                  value={target.aws_region}
                  onChange={(e) => patchRow(i, { aws_region: e.currentTarget.value })}
                />
              </FormField>
              <FormField help={t("metadataExportTab.datazoneDomainIdHelp")} size="medium">
                <TextInput
                  label={t("metadataExportTab.datazoneDomainId")}
                  data-testid={tid("datazone-domain-id")}
                  value={target.datazone_domain_id}
                  onChange={(e) => patchRow(i, { datazone_domain_id: e.currentTarget.value })}
                />
              </FormField>
              <FormField help={t("metadataExportTab.datazoneProjectIdHelp")} size="medium">
                <TextInput
                  label={t("metadataExportTab.datazoneProjectId")}
                  data-testid={tid("datazone-project-id")}
                  value={target.datazone_project_id}
                  onChange={(e) => patchRow(i, { datazone_project_id: e.currentTarget.value })}
                />
              </FormField>
              {secretField(
                "aws_access_key_id",
                t("metadataExportTab.awsAccessKeyId"),
                target.aws_access_key_id_set,
              )}
              {secretField(
                "aws_secret_access_key",
                t("metadataExportTab.awsSecretAccessKey"),
                target.aws_secret_access_key_set,
              )}
            </>
          ) : (
            <>
              <FormField size="long">
                <TextInput
                  label={t("metadataExportTab.endpoint")}
                  data-testid={tid("endpoint")}
                  value={target.endpoint}
                  onChange={(e) => patchRow(i, { endpoint: e.currentTarget.value })}
                />
              </FormField>
              <FormField size="medium">
                <Select
                  label={t("metadataExportTab.authMode")}
                  data-testid={tid("auth-mode")}
                  data={AUTH_MODES}
                  value={target.auth_mode}
                  onChange={(v) => patchRow(i, { auth_mode: v ?? "api_key" })}
                />
              </FormField>
              {target.provider === "dataplex" ? (
                <>
                  <FormField help={t("metadataExportTab.gcpProjectHelp")} size="medium">
                    <TextInput
                      label={t("metadataExportTab.gcpProject")}
                      data-testid={tid("gcp-project")}
                      value={target.gcp_project}
                      onChange={(e) => patchRow(i, { gcp_project: e.currentTarget.value })}
                    />
                  </FormField>
                  <FormField help={t("metadataExportTab.gcpLocationHelp")} size="short">
                    <TextInput
                      label={t("metadataExportTab.gcpLocation")}
                      data-testid={tid("gcp-location")}
                      value={target.gcp_location}
                      onChange={(e) => patchRow(i, { gcp_location: e.currentTarget.value })}
                    />
                  </FormField>
                </>
              ) : null}
              {target.auth_mode === GOOGLE_MODE ? (
                secretField(
                  "google_service_account_json",
                  t("metadataExportTab.googleServiceAccountJson"),
                  target.google_service_account_json_set,
                )
              ) : target.auth_mode === ENTRA_MODE ? (
                <>
                  <FormField size="medium">
                    <TextInput
                      label={t("metadataExportTab.entraTenantId")}
                      data-testid={tid("entra-tenant-id")}
                      value={target.entra_tenant_id}
                      onChange={(e) => patchRow(i, { entra_tenant_id: e.currentTarget.value })}
                    />
                  </FormField>
                  <FormField size="medium">
                    <TextInput
                      label={t("metadataExportTab.entraClientId")}
                      data-testid={tid("entra-client-id")}
                      value={target.entra_client_id}
                      onChange={(e) => patchRow(i, { entra_client_id: e.currentTarget.value })}
                    />
                  </FormField>
                  {secretField(
                    "entra_client_secret",
                    t("metadataExportTab.entraClientSecret"),
                    target.entra_client_secret_set,
                  )}
                </>
              ) : target.auth_mode === BASIC_MODE ? (
                <>
                  <FormField size="medium">
                    <TextInput
                      label={t("metadataExportTab.username")}
                      data-testid={tid("username")}
                      value={target.username}
                      onChange={(e) => patchRow(i, { username: e.currentTarget.value })}
                    />
                  </FormField>
                  {/* The password rides in the same `token` field the bearer mode uses — one stored
                  secret per target, read by whichever mode is selected. */}
                  {secretField("token", t("metadataExportTab.password"), target.token_set)}
                </>
              ) : (
                secretField(
                  target.auth_mode === "bearer" ? "token" : "api_key",
                  target.auth_mode === "bearer"
                    ? t("metadataExportTab.token")
                    : t("metadataExportTab.apiKey"),
                  target.auth_mode === "bearer" ? target.token_set : target.api_key_set,
                )
              )}
            </>
          )}
          <FormField size="short">
            <NumberInput
              label={t("metadataExportTab.timeout")}
              data-testid={tid("timeout")}
              value={target.timeout_seconds}
              onChange={(v) => patchRow(i, { timeout_seconds: Number(v) || 0 })}
            />
          </FormField>
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
                onClick={() => setRows((prev) => [...prev, { target: blankTarget(), secrets: {} }])}
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
