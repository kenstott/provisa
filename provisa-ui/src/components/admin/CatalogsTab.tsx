// Copyright (c) 2026 Kenneth Stott
// Canary: 9a3c5e17-2d84-4f60-b1e9-7c06d4a2f853
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2122: the catalogs an organization works with, each connected once: its provider, address,
// credentials and the domains that work with it. The metadata export names a catalog for each
// target, and a table's description is taken from, refreshed from and locked to its domain's
// catalogs through the same connection.
//
// Credentials are entered here and never read back: the server reports each as set/not-set, so an
// empty field means "leave the stored one alone" rather than "no secret". A field is only sent
// when the administrator typed in it, which is what makes editing a catalog's address safe for its
// token.

import { useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  Alert,
  Button,
  Group,
  Loader,
  MultiSelect,
  NumberInput,
  Paper,
  Select,
  Stack,
  Text,
  TextInput,
  Title,
} from "@mantine/core";
import { Check, Trash2, TriangleAlert } from "lucide-react";
import {
  checkCatalog,
  fetchCatalogs,
  setCatalogs,
  type CatalogConnection,
  type CatalogHealth,
  type CatalogSecretField,
  type CatalogsState,
} from "../../api/catalogs";
import { useDomains } from "../../hooks/useAdminQueries";
import { CatalogImportModal } from "./CatalogImportModal";
import { FormField, FormFooter, FormLayout } from "../form/FormLayout";

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

/** A DataZone catalog signs AWS requests with its own keys: it has no endpoint or auth mode. */
const DATAZONE = "datazone";
/** Unity Catalog and Snowflake Horizon work through the engine itself: no endpoint either. */
const ENGINE_NATIVE = new Set(["unity_catalog", "snowflake_horizon"]);

/** A catalog being edited, with the credentials typed for it since the last save. */
interface Row {
  catalog: CatalogConnection;
  secrets: Partial<Record<CatalogSecretField, string>>;
}

const blankCatalog = (): CatalogConnection => ({
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
  timeout_seconds: 30,
  domains: [],
  import_cron: "",
  api_key_set: false,
  token_set: false,
  entra_client_secret_set: false,
  google_service_account_json_set: false,
  aws_access_key_id_set: false,
  aws_secret_access_key_set: false,
});

export function CatalogsTab() {
  const { t } = useTranslation();
  const { domains } = useDomains();
  const [s, setS] = useState<CatalogsState | null>(null);
  const [rows, setRows] = useState<Row[]>([]);
  const [busy, setBusy] = useState("");
  const [msg, setMsg] = useState("");
  const [error, setError] = useState("");
  const [health, setHealth] = useState<Record<number, CatalogHealth>>({});
  const [importing, setImporting] = useState<string | null>(null);

  const load = (state: CatalogsState) => {
    setS(state);
    setRows(state.catalogs.map((catalog) => ({ catalog, secrets: {} })));
    setHealth({});
  };

  useEffect(() => {
    fetchCatalogs()
      .then(load)
      .catch((e) => setError(String(e)));
  }, []);

  const patchRow = (i: number, p: Partial<CatalogConnection>) =>
    setRows((prev) =>
      prev.map((r, j) => (j === i ? { ...r, catalog: { ...r.catalog, ...p } } : r)),
    );
  const typeSecret = (i: number, field: CatalogSecretField, value: string) =>
    setRows((prev) =>
      prev.map((r, j) => (j === i ? { ...r, secrets: { ...r.secrets, [field]: value } } : r)),
    );

  const save = async () => {
    setBusy("save");
    setMsg("");
    setError("");
    try {
      await setCatalogs(
        rows.map(({ catalog, secrets }) => ({
          id: catalog.id,
          provider: catalog.provider,
          endpoint: catalog.endpoint,
          auth_mode: catalog.auth_mode,
          username: catalog.username,
          entra_tenant_id: catalog.entra_tenant_id,
          entra_client_id: catalog.entra_client_id,
          gcp_project: catalog.gcp_project,
          gcp_location: catalog.gcp_location,
          aws_region: catalog.aws_region,
          datazone_domain_id: catalog.datazone_domain_id,
          timeout_seconds: catalog.timeout_seconds,
          domains: catalog.domains,
          import_cron: catalog.import_cron,
          ...secrets,
        })),
      );
      // Re-read rather than assume: the set/not-set flags now reflect what was persisted, and a
      // typed secret is dropped from local state so it is not re-sent on the next save.
      load(await fetchCatalogs());
      setMsg(t("catalogsTab.saved"));
    } catch (e) {
      setError(String(e));
    } finally {
      setBusy("");
    }
  };

  const test = async (i: number, id: string) => {
    setBusy(`health-${i}`);
    setError("");
    try {
      const result = await checkCatalog(id);
      setHealth((prev) => ({ ...prev, [i]: result }));
    } catch (e) {
      setError(String(e));
    } finally {
      setBusy("");
    }
  };

  if (error && !s)
    return (
      <Alert color="red" icon={<TriangleAlert size={16} />} data-testid="catalogs-error">
        {error}
      </Alert>
    );

  if (!s)
    return (
      <Group gap="xs">
        <Loader size="sm" />
        <Text>{t("catalogsTab.loading")}</Text>
      </Group>
    );

  const catalogForm = ({ catalog, secrets }: Row, i: number) => {
    // A catalog's test ids carry its place in the list, so two of one provider stay apart.
    const tid = (name: string) => `catalog-${name}-${i}`;
    const kept = s.catalogs.some((c) => c.id === catalog.id);
    const readable = s.readable.includes(catalog.provider);
    const secretField = (field: CatalogSecretField, label: string, isSet: boolean) => (
      <FormField help={t("catalogsTab.secretHelp")} size="medium">
        <TextInput
          label={label}
          type="password"
          data-testid={tid(field.replace(/_/g, "-"))}
          placeholder={isSet ? t("catalogsTab.secretSet") : t("catalogsTab.secretUnset")}
          value={secrets[field] ?? ""}
          onChange={(e) => typeSecret(i, field, e.currentTarget.value)}
        />
      </FormField>
    );
    const result = health[i];
    return (
      <Paper key={i} withBorder p="sm" data-testid={tid("entry")}>
        <Group justify="space-between" mb="xs">
          <Title order={6}>
            {catalog.id || t("catalogsTab.newCatalog")}
            {catalog.provider ? ` · ${catalog.provider}` : ""}
          </Title>
          <Group gap="xs">
            {/* The test reads the saved catalog, so it is offered once the catalog is kept. */}
            <Button
              variant="default"
              size="compact-sm"
              disabled={!kept}
              loading={busy === `health-${i}`}
              data-testid={tid("test")}
              onClick={() => test(i, catalog.id)}
            >
              {t("catalogsTab.testConnection")}
            </Button>
            {readable && (
              <Button
                variant="default"
                size="compact-sm"
                disabled={!kept}
                data-testid={tid("import")}
                onClick={() => setImporting(catalog.id)}
              >
                {t("catalogsTab.import")}
              </Button>
            )}
            <Button
              variant="subtle"
              color="red"
              size="compact-sm"
              leftSection={<Trash2 size={14} />}
              data-testid={tid("remove")}
              onClick={() => setRows((prev) => prev.filter((_, j) => j !== i))}
            >
              {t("catalogsTab.removeCatalog")}
            </Button>
          </Group>
        </Group>
        <FormLayout>
          <FormField help={t("catalogsTab.catalogIdHelp")} size="medium">
            <TextInput
              label={t("catalogsTab.catalogId")}
              data-testid={tid("id")}
              value={catalog.id}
              onChange={(e) => patchRow(i, { id: e.currentTarget.value })}
            />
          </FormField>
          <FormField size="medium">
            <Select
              label={t("catalogsTab.provider")}
              data-testid={tid("provider")}
              data={s.providers.map((p) => ({ value: p, label: p }))}
              value={catalog.provider || null}
              onChange={(v) => patchRow(i, { provider: v ?? "" })}
            />
          </FormField>
          <FormField help={t("catalogsTab.domainsHelp")} size="long">
            <MultiSelect
              label={t("catalogsTab.domains")}
              data-testid={tid("domains")}
              data={domains.map((d) => ({ value: d.id, label: d.id }))}
              value={catalog.domains}
              onChange={(v) => patchRow(i, { domains: v })}
              searchable
            />
          </FormField>
          {ENGINE_NATIVE.has(catalog.provider) ? null : catalog.provider === DATAZONE ? (
            <>
              <FormField help={t("catalogsTab.awsRegionHelp")} size="short">
                <TextInput
                  label={t("catalogsTab.awsRegion")}
                  data-testid={tid("aws-region")}
                  value={catalog.aws_region}
                  onChange={(e) => patchRow(i, { aws_region: e.currentTarget.value })}
                />
              </FormField>
              <FormField help={t("catalogsTab.datazoneDomainIdHelp")} size="medium">
                <TextInput
                  label={t("catalogsTab.datazoneDomainId")}
                  data-testid={tid("datazone-domain-id")}
                  value={catalog.datazone_domain_id}
                  onChange={(e) => patchRow(i, { datazone_domain_id: e.currentTarget.value })}
                />
              </FormField>
              {secretField(
                "aws_access_key_id",
                t("catalogsTab.awsAccessKeyId"),
                catalog.aws_access_key_id_set,
              )}
              {secretField(
                "aws_secret_access_key",
                t("catalogsTab.awsSecretAccessKey"),
                catalog.aws_secret_access_key_set,
              )}
            </>
          ) : (
            <>
              <FormField size="long">
                <TextInput
                  label={t("catalogsTab.endpoint")}
                  data-testid={tid("endpoint")}
                  value={catalog.endpoint}
                  onChange={(e) => patchRow(i, { endpoint: e.currentTarget.value })}
                />
              </FormField>
              <FormField size="medium">
                <Select
                  label={t("catalogsTab.authMode")}
                  data-testid={tid("auth-mode")}
                  data={AUTH_MODES}
                  value={catalog.auth_mode}
                  onChange={(v) => patchRow(i, { auth_mode: v ?? "api_key" })}
                />
              </FormField>
              {catalog.provider === "dataplex" ? (
                <>
                  <FormField help={t("catalogsTab.gcpProjectHelp")} size="medium">
                    <TextInput
                      label={t("catalogsTab.gcpProject")}
                      data-testid={tid("gcp-project")}
                      value={catalog.gcp_project}
                      onChange={(e) => patchRow(i, { gcp_project: e.currentTarget.value })}
                    />
                  </FormField>
                  <FormField help={t("catalogsTab.gcpLocationHelp")} size="short">
                    <TextInput
                      label={t("catalogsTab.gcpLocation")}
                      data-testid={tid("gcp-location")}
                      value={catalog.gcp_location}
                      onChange={(e) => patchRow(i, { gcp_location: e.currentTarget.value })}
                    />
                  </FormField>
                </>
              ) : null}
              {catalog.auth_mode === GOOGLE_MODE ? (
                secretField(
                  "google_service_account_json",
                  t("catalogsTab.googleServiceAccountJson"),
                  catalog.google_service_account_json_set,
                )
              ) : catalog.auth_mode === ENTRA_MODE ? (
                <>
                  <FormField size="medium">
                    <TextInput
                      label={t("catalogsTab.entraTenantId")}
                      data-testid={tid("entra-tenant-id")}
                      value={catalog.entra_tenant_id}
                      onChange={(e) => patchRow(i, { entra_tenant_id: e.currentTarget.value })}
                    />
                  </FormField>
                  <FormField size="medium">
                    <TextInput
                      label={t("catalogsTab.entraClientId")}
                      data-testid={tid("entra-client-id")}
                      value={catalog.entra_client_id}
                      onChange={(e) => patchRow(i, { entra_client_id: e.currentTarget.value })}
                    />
                  </FormField>
                  {secretField(
                    "entra_client_secret",
                    t("catalogsTab.entraClientSecret"),
                    catalog.entra_client_secret_set,
                  )}
                </>
              ) : catalog.auth_mode === BASIC_MODE ? (
                <>
                  <FormField size="medium">
                    <TextInput
                      label={t("catalogsTab.username")}
                      data-testid={tid("username")}
                      value={catalog.username}
                      onChange={(e) => patchRow(i, { username: e.currentTarget.value })}
                    />
                  </FormField>
                  {/* The password rides in the same `token` field the bearer mode uses — one stored
                  secret per catalog, read by whichever mode is selected. */}
                  {secretField("token", t("catalogsTab.password"), catalog.token_set)}
                </>
              ) : (
                secretField(
                  catalog.auth_mode === "bearer" ? "token" : "api_key",
                  catalog.auth_mode === "bearer" ? t("catalogsTab.token") : t("catalogsTab.apiKey"),
                  catalog.auth_mode === "bearer" ? catalog.token_set : catalog.api_key_set,
                )
              )}
            </>
          )}
          {readable && (
            <FormField help={t("catalogsTab.importCronHelp")} size="medium">
              <TextInput
                label={t("catalogsTab.importCron")}
                data-testid={tid("import-cron")}
                value={catalog.import_cron}
                onChange={(e) => patchRow(i, { import_cron: e.currentTarget.value })}
              />
            </FormField>
          )}
          <FormField size="short">
            <NumberInput
              label={t("catalogsTab.timeout")}
              data-testid={tid("timeout")}
              value={catalog.timeout_seconds}
              onChange={(v) => patchRow(i, { timeout_seconds: Number(v) || 0 })}
            />
          </FormField>
          {result && (
            <FormField size="full">
              <Alert
                color={result.ok ? "green" : "red"}
                icon={result.ok ? <Check size={16} /> : <TriangleAlert size={16} />}
                data-testid={tid("health")}
              >
                {result.ok ? t("catalogsTab.healthOk") : result.error}
              </Alert>
            </FormField>
          )}
        </FormLayout>
      </Paper>
    );
  };

  return (
    <div data-tour="catalogs">
      <FormLayout>
        <FormField size="full">
          <Text c="dimmed" size="sm">
            {t("catalogsTab.intro")}
          </Text>
        </FormField>
        <FormField size="full">
          <Stack gap="sm" data-testid="catalogs-list">
            <Group justify="space-between">
              <Title order={5}>{t("catalogsTab.heading")}</Title>
              <Button
                variant="default"
                size="compact-sm"
                data-testid="catalogs-add"
                onClick={() =>
                  setRows((prev) => [...prev, { catalog: blankCatalog(), secrets: {} }])
                }
              >
                {t("catalogsTab.addCatalog")}
              </Button>
            </Group>
            {rows.length === 0 && (
              <Text c="dimmed" size="sm" data-testid="catalogs-none">
                {t("catalogsTab.noCatalogs")}
              </Text>
            )}
            {rows.map(catalogForm)}
          </Stack>
        </FormField>
        <CatalogImportModal catalog={importing} onClose={() => setImporting(null)} />
        <FormFooter
          onSave={save}
          saving={busy === "save"}
          saveProps={{ "data-testid": "catalogs-save" }}
        />
        {msg && (
          <FormField size="full">
            <Alert color="green" icon={<Check size={16} />} data-testid="catalogs-saved">
              {msg}
            </Alert>
          </FormField>
        )}
        {error && (
          <FormField size="full">
            <Alert color="red" icon={<TriangleAlert size={16} />} data-testid="catalogs-error">
              {error}
            </Alert>
          </FormField>
        )}
      </FormLayout>
    </div>
  );
}
