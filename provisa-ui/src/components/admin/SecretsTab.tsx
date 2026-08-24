// Copyright (c) 2026 Kenneth Stott
// Canary: fdbadaed-2513-42aa-8781-07cb8a032249
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect, useMemo, useState, type ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { missingRequired, secretPlaceholder, seedFields } from "./secretFields";
import {
  Accordion,
  Alert,
  Badge,
  Button,
  Card,
  Code,
  Divider,
  Group,
  Modal,
  PasswordInput,
  Radio,
  Stack,
  Table,
  Text,
  TextInput,
  Tooltip,
} from "@mantine/core";
import { Copy, Plus } from "lucide-react";
import { useAuth } from "../../context/AuthContext";
import { useCapability } from "../../hooks/useCapability";
import { usePanelState } from "../../hooks/usePanelState";
import { ConfirmDialog } from "../ConfirmDialog";
import { OrgEncryptionTab } from "./OrgEncryptionTab";
import {
  fetchSecrets,
  putSecret,
  deleteSecret,
  fetchSecretsService,
  setSecretsService,
  type Secret,
  type SecretsState,
  type SecretsServiceState,
  type Vault,
} from "../../api/secrets";

/**
 * REQ-1557, REQ-1558: the org's secrets — NAMES GO IN, VALUES NEVER COME BACK OUT.
 *
 * There is no "show" button here and there is no endpoint behind one: the list says what exists,
 * what it is for, who last set it, and the reference to paste. Somebody who has lost a value
 * REPLACES it, which is the same act that created it, so the form is the same form.
 */
/**
 * REQ-1557, REQ-1558: which secrets service the DEPLOYMENT is wired to.
 *
 * Every backend the build knows is listed, installed or not. A row whose client library is
 * missing is greyed out and says which one — hiding it would leave an operator unsure whether
 * Provisa speaks to their secrets manager at all, when the only thing missing is a pip install.
 */
function SecretsServicePanel() {
  const { t } = useTranslation();
  const [s, setS] = useState<SecretsServiceState | null>(null);
  const [choice, setChoice] = useState("");
  const [config, setConfig] = useState<Record<string, Record<string, string>>>({});
  const [saving, setSaving] = useState(false);
  const [msg, setMsg] = useState("");
  const [error, setError] = useState("");

  const load = () =>
    fetchSecretsService()
      .then((state) => {
        setS(state);
        setChoice(state.provider);
        // REQ-1575: seeded from what the server sent, which is every field EXCEPT the secret ones
        // (the Vault token). A secret field starts empty and is only sent if it is typed into.
        setConfig(
          Object.fromEntries(
            state.providers.map((p) => [p.key, seedFields(p.config_fields, state.config[p.key])]),
          ),
        );
      })
      .catch((e) => setError(String(e)));

  useEffect(() => {
    load();
  }, []);

  const selected = useMemo(() => s?.providers.find((p) => p.key === choice), [s, choice]);
  // A stored secret satisfies its required field without being retyped (REQ-1575).
  const incomplete = missingRequired(
    selected?.config_fields ?? [],
    config[choice],
    s?.secret_set?.[choice],
  );

  const setField = (key: string, value: string) =>
    setConfig((c) => ({ ...c, [choice]: { ...(c[choice] ?? {}), [key]: value } }));

  const save = async () => {
    setSaving(true);
    setMsg("");
    setError("");
    try {
      const res = await setSecretsService({ provider: choice, config: config[choice] ?? {} });
      setMsg(t("secretsTab.serviceSaved", { provider: res.provider }));
      load();
    } catch (e) {
      setError(String(e));
    } finally {
      setSaving(false);
    }
  };

  if (error && !s) return <Alert color="red">{error}</Alert>;
  if (!s) return <Text>{t("secretsTab.loading")}</Text>;

  return (
    <Stack gap="sm" maw={720} data-testid="secrets-service">
      <Text c="dimmed" fz="sm">
        {t("secretsTab.serviceIntro")}
      </Text>

      {error && <Alert color="red">{error}</Alert>}
      {msg && <Alert color="green">{msg}</Alert>}

      <Radio.Group value={choice} onChange={setChoice}>
        <Stack gap="xs">
          {s.providers.map((p) => (
            <Card
              key={p.key}
              withBorder
              padding="sm"
              data-testid={`secrets-provider-${p.key}`}
              data-unavailable={p.available ? undefined : "true"}
              style={{ opacity: p.available ? 1 : 0.5 }}
            >
              <Radio
                value={p.key}
                disabled={!p.available}
                label={
                  <Group gap="xs">
                    <Text fw={500} c={p.available ? undefined : "dimmed"}>
                      {p.label}
                    </Text>
                    {p.key === s.provider && (
                      <Badge color="green" variant="light">
                        {t("secretsTab.serviceInUse")}
                      </Badge>
                    )}
                    {!p.available && p.requires && (
                      <Text c="dimmed" fz="xs" data-testid={`secrets-provider-requires-${p.key}`}>
                        {t("secretsTab.serviceRequires", { library: p.requires })}
                      </Text>
                    )}
                  </Group>
                }
                description={p.description}
              />
            </Card>
          ))}
        </Stack>
      </Radio.Group>

      {(selected?.config_fields ?? []).map((f) => (
        <TextInput
          key={f.config_key}
          label={f.label}
          required={f.required}
          placeholder={
            secretPlaceholder(f, s.secret_set?.[choice], {
              set: t("secretsTab.secretOnFile"),
              unset: t("secretsTab.secretNotSet"),
            }) ?? f.placeholder
          }
          // Never a password field: the value typed here is a ${env:...} REFERENCE to the
          // credential, not the credential (REQ-1557).
          value={config[choice]?.[f.config_key] ?? ""}
          onChange={(e) => setField(f.config_key, e.currentTarget.value)}
          data-testid={`secrets-service-field-${f.config_key}`}
        />
      ))}

      <Group>
        <Button
          onClick={save}
          loading={saving}
          disabled={incomplete || choice === ""}
          data-testid="secrets-service-save"
        >
          {t("secretsTab.serviceSave")}
        </Button>
      </Group>
    </Stack>
  );
}

interface VaultProps {
  /** Which vault this surface is of. There is no third state and no "all" (REQ-1560). */
  vault: Vault;
  /** What sits above the list — the service chooser on the org surface, nothing on the personal one. */
  header?: ReactNode;
}

/**
 * One vault's names (REQ-1560). The org surface and the personal surface are the same list, the
 * same form and the same "replace rather than reveal" rule; what differs is WHOSE, and that is
 * carried by `vault` into the URL rather than by a user id the browser could change.
 */
function SecretsVault({ vault, header }: VaultProps) {
  const { t } = useTranslation();
  const { activeOrgId } = useAuth();
  const [state, setState] = useState<SecretsState | null>(null);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");
  // The name being replaced, "" for a new secret, null when the form is closed. A replacement
  // fixes the name, because the name is the identity and changing it would create a second secret.
  const [editing, setEditing] = useState<string | null>(null);
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [value, setValue] = useState("");
  const [saving, setSaving] = useState(false);

  const load = () => {
    if (!activeOrgId) return;
    fetchSecrets(activeOrgId, vault)
      .then(setState)
      .catch((e) => setError(String(e)));
  };

  useEffect(load, [activeOrgId, vault]);

  const open = (secret: Secret | null) => {
    setEditing(secret ? secret.name : "");
    setName(secret ? secret.name : "");
    setDescription(secret?.description ?? "");
    setValue("");
    setError("");
    setMessage("");
  };

  const save = async () => {
    if (!activeOrgId) return;
    setSaving(true);
    setError("");
    try {
      await putSecret(activeOrgId, vault, name, { value, description: description || null });
      setMessage(t(editing ? "secretsTab.replaced" : "secretsTab.created", { name }));
      setEditing(null);
      setValue("");
      load();
    } catch (e) {
      setError(String(e));
    } finally {
      setSaving(false);
    }
  };

  const remove = async (secret: Secret) => {
    if (!activeOrgId) return;
    setError("");
    try {
      await deleteSecret(activeOrgId, vault, secret.name);
      setMessage(t("secretsTab.deleted", { name: secret.name }));
      load();
    } catch (e) {
      setError(String(e));
    }
  };

  if (error && !state) return <Alert color="red">{error}</Alert>;
  if (!state) return <Text>{t("secretsTab.loading")}</Text>;

  const writable = state.provider.writable;

  return (
    <Stack gap="md" data-testid={vault === "org" ? "secrets-tab" : "my-secrets-tab"}>
      {header}
      {header && <Divider />}
      <Text c="dimmed" fz="sm">
        {t(vault === "org" ? "secretsTab.intro" : "secretsTab.introPersonal")}
      </Text>
      <Alert color={writable ? "blue" : "yellow"} title={state.provider.label}>
        {writable
          ? t("secretsTab.providerBuiltIn")
          : t("secretsTab.providerCentral", { provider: state.provider.label })}
      </Alert>

      {error && <Alert color="red">{error}</Alert>}
      {message && <Alert color="green">{message}</Alert>}

      {state.secrets.length === 0 ? (
        <Text c="dimmed">{t("secretsTab.empty")}</Text>
      ) : (
        <Table striped withTableBorder data-testid="secrets-table">
          <Table.Thead>
            <Table.Tr>
              <Table.Th>{t("secretsTab.columnName")}</Table.Th>
              <Table.Th>{t("secretsTab.columnDescription")}</Table.Th>
              <Table.Th>{t("secretsTab.columnUpdated")}</Table.Th>
              <Table.Th>{t("secretsTab.columnReference")}</Table.Th>
              <Table.Th />
            </Table.Tr>
          </Table.Thead>
          <Table.Tbody>
            {state.secrets.map((s) => (
              <Table.Tr key={s.name} data-testid={`secret-row-${s.name}`}>
                <Table.Td>
                  <Code>{s.name}</Code>
                </Table.Td>
                <Table.Td>{s.description ?? ""}</Table.Td>
                <Table.Td>
                  <Text fz="sm">
                    {s.updated_at ? new Date(s.updated_at).toLocaleString() : t("secretsTab.never")}
                  </Text>
                  {s.updated_by && (
                    <Text fz="xs" c="dimmed">
                      {t("secretsTab.updatedBy", { actor: s.updated_by })}
                    </Text>
                  )}
                </Table.Td>
                <Table.Td>
                  <Group gap="xs">
                    <Code>{s.reference}</Code>
                    <Tooltip label={t("secretsTab.copyReference")}>
                      <Button
                        variant="subtle"
                        size="compact-xs"
                        aria-label={t("secretsTab.copyReference")}
                        onClick={() => navigator.clipboard?.writeText(s.reference)}
                      >
                        <Copy size={14} />
                      </Button>
                    </Tooltip>
                  </Group>
                </Table.Td>
                <Table.Td>
                  {writable && (
                    <Group gap="xs" justify="flex-end">
                      <Button
                        variant="default"
                        size="compact-sm"
                        data-testid={`secret-replace-${s.name}`}
                        onClick={() => open(s)}
                      >
                        {t("secretsTab.replaceAction")}
                      </Button>
                      <ConfirmDialog
                        title={t("secretsTab.deleteAction")}
                        consequence={t("secretsTab.deleteConfirm", { name: s.name })}
                        onConfirm={() => remove(s)}
                      >
                        {(openConfirm) => (
                          <Button
                            color="red"
                            variant="light"
                            size="compact-sm"
                            data-testid={`secret-delete-${s.name}`}
                            onClick={openConfirm}
                          >
                            {t("secretsTab.deleteAction")}
                          </Button>
                        )}
                      </ConfirmDialog>
                    </Group>
                  )}
                </Table.Td>
              </Table.Tr>
            ))}
          </Table.Tbody>
        </Table>
      )}

      {writable && (
        <Group>
          <Button
            leftSection={<Plus size={16} />}
            data-testid="secrets-add"
            onClick={() => open(null)}
          >
            {t("secretsTab.add")}
          </Button>
        </Group>
      )}

      <Modal
        opened={editing !== null}
        onClose={() => setEditing(null)}
        title={editing ? t("secretsTab.replaceTitle", { name: editing }) : t("secretsTab.addTitle")}
        centered
      >
        <Stack gap="sm">
          <TextInput
            label={t("secretsTab.nameLabel")}
            placeholder={t("secretsTab.namePlaceholder")}
            description={t("secretsTab.nameHelp")}
            value={name}
            // A replacement keeps its name: the name is the identity of the secret being replaced.
            disabled={!!editing}
            onChange={(e) => setName(e.currentTarget.value)}
          />
          <TextInput
            label={t("secretsTab.descriptionLabel")}
            placeholder={t("secretsTab.descriptionPlaceholder")}
            value={description}
            onChange={(e) => setDescription(e.currentTarget.value)}
          />
          <PasswordInput
            label={t("secretsTab.valueLabel")}
            description={t("secretsTab.valueHelp")}
            value={value}
            onChange={(e) => setValue(e.currentTarget.value)}
          />
          <Group justify="flex-end">
            <Button variant="default" onClick={() => setEditing(null)} disabled={saving}>
              {t("secretsTab.cancel")}
            </Button>
            <Button
              onClick={save}
              loading={saving}
              data-testid="secret-submit"
              disabled={!name.trim() || !value}
            >
              {editing ? t("secretsTab.replace") : t("secretsTab.add")}
            </Button>
          </Group>
        </Stack>
      </Modal>
    </Stack>
  );
}

/**
 * ORG SECRETS (REQ-1560): the shared vault, plus the deployment's choice of secrets service for
 * whoever runs the deployment.
 *
 * The org half is gated STRICTLY on `org_settings` -- `capabilities.includes` rather than
 * `hasCapability` -- because the platform wildcard must not answer for it (REQ-1361). A platform
 * admin holds `admin`, which satisfies every other gate in the UI; here it would have shown them
 * the list of names an org keeps, which is itself a statement about what that org connects to. The
 * server refuses the same call, so all the wildcard bought was a page that 403s.
 */
export function SecretsTab() {
  const { t } = useTranslation();
  const { capabilities } = useAuth();
  const mayChooseService = useCapability("platform_settings");
  const mayHoldSecrets = capabilities.includes("org_settings");
  const [servicePanel, setServicePanel] = usePanelState("secretsService");
  const [keyPanel, setKeyPanel] = usePanelState("orgKey");

  // Collapsed by default: choosing the backend happens once for the deployment, reading the names
  // happens every day, so the page opens on the names. The choice is remembered per browser.
  const service = mayChooseService ? (
    <Accordion
      value={servicePanel}
      onChange={setServicePanel}
      variant="contained"
      data-testid="secrets-service-panel"
    >
      <Accordion.Item value="service">
        <Accordion.Control data-testid="secrets-service-toggle">
          {t("secretsTab.serviceTitle")}
        </Accordion.Control>
        {/* Mounted only when open, so a collapsed panel does not read the deployment config. */}
        <Accordion.Panel>{servicePanel === "service" && <SecretsServicePanel />}</Accordion.Panel>
      </Accordion.Item>
    </Accordion>
  ) : null;

  // REQ-1574: the org's key sits above the org's secrets, because it is the thing they are wrapped
  // under -- the same right, org_settings, and the same page. Collapsed by default for the reason
  // the service panel is: setting the key happens once, reading the names happens every day.
  const orgKey = mayHoldSecrets ? (
    <Accordion
      value={keyPanel}
      onChange={setKeyPanel}
      variant="contained"
      data-testid="org-key-panel"
    >
      <Accordion.Item value="orgKey">
        <Accordion.Control data-testid="org-key-toggle">
          {t("orgEncryptionTab.panelTitle")}
        </Accordion.Control>
        {/* Mounted only when open, so a collapsed panel does not read the key's state. */}
        <Accordion.Panel>{keyPanel === "orgKey" && <OrgEncryptionTab />}</Accordion.Panel>
      </Accordion.Item>
    </Accordion>
  ) : null;

  const header = (
    <>
      {orgKey}
      {service}
    </>
  );

  if (!mayHoldSecrets) return <Stack data-testid="secrets-tab">{service}</Stack>;
  return <SecretsVault vault="org" header={header} />;
}

/**
 * YOUR SECRETS (REQ-1560): the caller's own vault, in the org they are acting in.
 *
 * No capability is consulted, here or on the server. Holding a credential of your own is not a
 * privilege an administrator grants, and the reason another developer cannot use yours is not a
 * check -- it is that no request can name it.
 */
export function MySecretsTab() {
  return <SecretsVault vault="user" />;
}
