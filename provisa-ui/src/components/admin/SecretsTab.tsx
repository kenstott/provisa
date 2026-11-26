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
  Alert,
  Badge,
  Button,
  Card,
  Code,
  Group,
  List,
  PasswordInput,
  Radio,
  Stack,
  Table,
  Text,
  TextInput,
} from "@mantine/core";
import { Copy, KeyRound, X } from "lucide-react";
import { useAuth } from "../../context/AuthContext";
import { PlatformRequired } from "./PlatformRequired";
import { isForbidden } from "./isForbidden";
import { CollapsibleItem, Collapsibles } from "../collapsible/Collapsible";
import { OrgEncryptionTab } from "./OrgEncryptionTab";
import { RedactionTab } from "./RedactionTab";
import { OrgFilesTab } from "./OrgFilesTab";
import {
  fetchSecrets,
  putSecret,
  deleteSecret,
  fetchSecretsService,
  setSecretsService,
  SecretStillReferenced,
  type Secret,
  type SecretReference,
  type SecretsState,
  type SecretsServiceState,
  type Vault,
} from "../../api/secrets";
import { ListRow } from "../list/ListTable";
import { SortGroupTable } from "../list/SortGroupTable";
import { FormField, FormFooter, FormIdentifiers, FormLayout } from "../form/FormLayout";
import { Tip } from "../Tip";
import { PageTitle } from "../PageTitle";
import { DetailFields } from "../detail/DetailFields";
import { PanelDelete } from "../list/PanelDelete";
import { PanelActions, SplitPane } from "../list/SplitPane";
import { useRememberedDetail } from "../list/useDetailParam";
import { useUrlFilter } from "../list/useUrlFilter";
import { useRequiredFields } from "../form/useRequiredFields";
import { matchesQuery } from "../../lib/searchItems";
import { FilterInput } from "./FilterInput";

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
export function SecretsServicePanel() {
  const { t } = useTranslation();
  const [s, setS] = useState<SecretsServiceState | null>(null);
  const [choice, setChoice] = useState("");
  const [config, setConfig] = useState<Record<string, Record<string, string>>>({});
  const [saving, setSaving] = useState(false);
  const [msg, setMsg] = useState("");
  const [error, setError] = useState("");
  const [forbidden, setForbidden] = useState(false);

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
      .catch((e) => (isForbidden(e) ? setForbidden(true) : setError(String(e))));

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

  if (forbidden) return <PlatformRequired />;
  if (error && !s) return <Alert color="red">{error}</Alert>;
  if (!s) return <Text>{t("secretsTab.loading")}</Text>;

  return (
    <Stack gap="sm" data-testid="secrets-service">
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

      <FormLayout>
        {(selected?.config_fields ?? []).map((f) => (
          <FormField size="medium" key={f.config_key}>
            <TextInput
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
          </FormField>
        ))}
      </FormLayout>

      <FormFooter
        onSave={save}
        saving={saving}
        saveProps={{ "data-testid": "secrets-service-save", disabled: incomplete || choice === "" }}
      />
    </Stack>
  );
}

interface VaultProps {
  /** Which vault this surface is of. There is no third state and no "all" (REQ-1560). */
  vault: Vault;
  /** The page's title, the Admin breadcrumb. */
  title: string;
  /** What sits above the list — the org's key and files on the org surface, nothing on the personal one. */
  header?: ReactNode;
}

/** The form open in the panel: a new secret, or the replacement of the one open. */
type SecretForm = "create" | "replace" | null;

/**
 * One vault's names (REQ-1560). The org surface and the personal surface are the same list, the
 * same form and the same "replace rather than reveal" rule; what differs is WHOSE, and that is
 * carried by `vault` into the URL rather than by a user id the browser could change.
 *
 * REQ-2277: the object management anatomy -- the header (title, filter, Add), the list, and a row
 * opening the secret's detail panel (?secret=<name>), where it is replaced or deleted.
 */
function SecretsVault({ vault, title, header }: VaultProps) {
  const { t } = useTranslation();
  const { activeOrgId } = useAuth();
  const [state, setState] = useState<SecretsState | null>(null);
  const [error, setError] = useState("");
  // REQ-1918: what still names a secret whose delete was refused.
  const [stillNamedBy, setStillNamedBy] = useState<SecretReference[]>([]);
  const [message, setMessage] = useState("");
  const [search, setSearch] = useUrlFilter();
  const detail = useRememberedDetail(
    vault === "org" ? "secrets" : "mySecrets",
    state === null ? null : state.secrets.map((s) => s.name),
  );
  // A replacement fixes the name, because the name is the identity and changing it would create a
  // second secret.
  const [form, setForm] = useState<SecretForm>(null);
  const [name, setName] = useState("");
  const [description, setDescription] = useState("");
  const [value, setValue] = useState("");
  const [saving, setSaving] = useState(false);
  const [problem, setProblem] = useState<string | null>(null);
  const required = useRequiredFields(form === "create" ? { name, value } : { value });

  const load = () => {
    if (!activeOrgId) return Promise.resolve();
    return fetchSecrets(activeOrgId, vault)
      .then(setState)
      .catch((e) => setError(String(e)));
  };

  useEffect(() => {
    void load();
    // eslint-disable-next-line react-hooks/exhaustive-deps -- read again only for another vault
  }, [activeOrgId, vault]);

  const openForm = (secret: Secret | null) => {
    setForm(secret ? "replace" : "create");
    setName(secret ? secret.name : "");
    setDescription(secret?.description ?? "");
    setValue("");
    setProblem(null);
    setMessage("");
  };
  const closeForm = () => {
    setForm(null);
    setProblem(null);
  };

  const save = async () => {
    if (!activeOrgId || !required.validate()) return;
    setSaving(true);
    setProblem(null);
    try {
      await putSecret(activeOrgId, vault, name, { value, description: description || null });
      setMessage(t(form === "replace" ? "secretsTab.replaced" : "secretsTab.created", { name }));
      setForm(null);
      setValue("");
      await load();
      detail.open(name);
    } catch (e) {
      setProblem(String(e));
    } finally {
      setSaving(false);
    }
  };

  const remove = async (secret: Secret): Promise<boolean> => {
    if (!activeOrgId) return false;
    setError("");
    setStillNamedBy([]);
    try {
      await deleteSecret(activeOrgId, vault, secret.name);
    } catch (e) {
      if (e instanceof SecretStillReferenced) {
        // REQ-1918: refused, and nothing was removed. Each value that still names it is listed
        // so the person knows what to change before deleting again.
        setError(e.message);
        setStillNamedBy(e.references);
        return false;
      }
      throw e;
    }
    setMessage(t("secretsTab.deleted", { name: secret.name }));
    await load();
    detail.close({ only: secret.name });
    return true;
  };

  // The header stands while the vault loads or is refused, so the page keeps its name.
  const pageHeader = (
    <div className="page-header">
      <PageTitle icon={KeyRound}>{title}</PageTitle>
      <FilterInput
        value={search}
        onChange={setSearch}
        placeholder={t("secretsTab.filterPlaceholder")}
        testId="secrets-filter"
      />
      <div className="page-actions">
        {state !== null && state.provider.writable && (
          <Button
            variant={form === "create" ? "outline" : "filled"}
            data-testid="secrets-add"
            aria-label={form === "create" ? t("secretsTab.closeForm") : undefined}
            onClick={() => (form === "create" ? closeForm() : openForm(null))}
          >
            {form === "create" ? <X size={14} /> : t("secretsTab.add")}
          </Button>
        )}
      </div>
    </div>
  );
  if (error && !state)
    return (
      <>
        {pageHeader}
        <Alert color="red">{error}</Alert>
      </>
    );
  if (!state)
    return (
      <>
        {pageHeader}
        <Text>{t("secretsTab.loading")}</Text>
      </>
    );

  const writable = state.provider.writable;
  const open = state.secrets.find((s) => s.name === detail.id) ?? null;
  const shown = state.secrets.filter((s) =>
    matchesQuery([s.name, s.description ?? "", s.reference], search),
  );

  const formView = (
    <FormLayout data-testid="secret-form">
      <FormIdentifiers>
        <FormField hint={t("secretsTab.nameHelp")} size="long">
          <TextInput
            label={t("secretsTab.nameLabel")}
            placeholder={t("secretsTab.namePlaceholder")}
            value={name}
            // A replacement keeps its name: the name is the identity of the secret being replaced.
            disabled={form === "replace"}
            onChange={(e) => setName(e.currentTarget.value)}
            {...(form === "create" ? required.field("name") : {})}
          />
        </FormField>
      </FormIdentifiers>
      <FormField size="long">
        <TextInput
          label={t("secretsTab.descriptionLabel")}
          placeholder={t("secretsTab.descriptionPlaceholder")}
          value={description}
          onChange={(e) => setDescription(e.currentTarget.value)}
        />
      </FormField>
      <FormField hint={t("secretsTab.valueHelp")} size="long">
        <PasswordInput
          label={t("secretsTab.valueLabel")}
          value={value}
          onChange={(e) => setValue(e.currentTarget.value)}
          {...required.field("value")}
        />
      </FormField>
      <FormFooter
        panel
        onSave={save}
        saving={saving}
        onCancel={closeForm}
        problem={problem ?? undefined}
        saveProps={{ "data-testid": "secret-submit" }}
      />
    </FormLayout>
  );

  const readView = (s: Secret) => (
    <Stack gap="sm" data-testid={`secret-detail-${s.name}`}>
      {writable && (
        <PanelActions>
          <fieldset className="panel-action-set">
            <Group gap="xs">
              <Button data-testid={`secret-replace-${s.name}`} onClick={() => openForm(s)}>
                {t("secretsTab.replaceAction")}
              </Button>
              <PanelDelete
                itemKey={s.name}
                title={t("secretsTab.deleteAction")}
                consequence={t(
                  vault === "org" ? "secretsTab.deleteConfirmOrg" : "secretsTab.deleteConfirm",
                  { name: s.name },
                )}
                ariaLabel={t("secretsTab.deleteAction")}
                confirmLabel={t("common.delete")}
                testId={`secret-delete-${s.name}`}
                onDelete={() => remove(s)}
              />
            </Group>
          </fieldset>
        </PanelActions>
      )}
      <DetailFields
        testId={`secret-read-${s.name}`}
        fields={[
          { label: t("secretsTab.nameLabel"), value: <Code>{s.name}</Code> },
          { label: t("secretsTab.descriptionLabel"), value: s.description ?? "" },
          {
            label: t("secretsTab.columnReference"),
            value: (
              <Group gap="xs">
                <Code>{s.reference}</Code>
                <Tip text={t("secretsTab.copyReference")} named>
                  <Button
                    variant="subtle"
                    size="compact-xs"
                    aria-label={t("secretsTab.copyReference")}
                    onClick={() => navigator.clipboard?.writeText(s.reference)}
                  >
                    <Copy size={14} />
                  </Button>
                </Tip>
              </Group>
            ),
          },
          {
            label: t("secretsTab.columnUpdated"),
            value: s.updated_at
              ? `${new Date(s.updated_at).toLocaleString()}${
                  s.updated_by ? ` ${t("secretsTab.updatedBy", { actor: s.updated_by })}` : ""
                }`
              : t("secretsTab.never"),
          },
        ]}
      />
    </Stack>
  );

  const panelTitle =
    form === "create"
      ? t("secretsTab.addTitle")
      : form === "replace" && open !== null
        ? t("secretsTab.replaceTitle", { name: open.name })
        : (open?.name ?? "");
  const panelBody = form !== null ? formView : open !== null ? readView(open) : null;

  return (
    <>
      {pageHeader}
      <Stack gap="md" data-testid={vault === "org" ? "secrets-tab" : "my-secrets-tab"}>
        {header}
        <Text c="dimmed" fz="sm">
          {t(vault === "org" ? "secretsTab.intro" : "secretsTab.introPersonal")}
        </Text>
        <Alert color={writable ? "blue" : "yellow"} title={state.provider.label}>
          {writable
            ? t("secretsTab.providerBuiltIn")
            : t("secretsTab.providerCentral", { provider: state.provider.label })}
        </Alert>

        {error && (
          <Alert color="red">
            {error}
            {stillNamedBy.length > 0 && (
              <List size="sm" mt="xs" data-testid="secret-references">
                {stillNamedBy.map((r) => (
                  <List.Item key={`${r.environment}/${r.kind}/${String(r.id)}/${r.column}`}>
                    {r.environment === null
                      ? t("secretsTab.referenceInOrg", {
                          kind: r.kind,
                          name: r.name,
                          column: r.column,
                        })
                      : t("secretsTab.referenceIn", {
                          kind: r.kind,
                          name: r.name,
                          column: r.column,
                          environment: r.environment,
                        })}
                    {r.unreadable && ` ${t("secretsTab.referenceUnreadable")}`}
                  </List.Item>
                ))}
              </List>
            )}
          </Alert>
        )}
        {message && <Alert color="green">{message}</Alert>}

        <SplitPane
          testId="secrets-split"
          storageKey={`provisa.secrets.${vault}.detailWidth`}
          title={panelTitle}
          onClose={() => {
            closeForm();
            detail.close();
          }}
          // Escape closes a secret that is only being looked at; it never drops what is typed.
          closeOnEscape={form === null}
          labels={{
            panel: t("secretsTab.panelLabel"),
            resize: t("tablesPage.resizeDetail"),
            expand: t("tablesPage.expandDetail"),
            restore: t("tablesPage.restoreDetail"),
            close: t("tablesPage.closeDetail"),
            backToList: t("tablesPage.backToList"),
          }}
          list={
            <SortGroupTable
              testPrefix="secrets"
              testId="secrets-table"
              rows={shown}
              columns={[
                { key: "name", label: t("secretsTab.columnName"), sortValue: (s) => s.name },
                {
                  key: "description",
                  label: t("secretsTab.columnDescription"),
                  sortValue: (s) => s.description ?? "",
                },
                {
                  key: "updated",
                  label: t("secretsTab.columnUpdated"),
                  sortValue: (s) => s.updated_at ?? "",
                },
                {
                  key: "reference",
                  label: t("secretsTab.columnReference"),
                  sortValue: (s) => s.reference,
                },
              ]}
              headers={[
                { col: "name" },
                { col: "description" },
                { col: "updated" },
                { col: "reference" },
              ]}
              colSpan={4}
              rowKey={(s) => s.name}
              empty={t("secretsTab.empty")}
              render={(s) => (
                <ListRow
                  key={s.name}
                  testId={`secret-row-${s.name}`}
                  selected={form !== "create" && s.name === detail.id}
                  onClick={() => {
                    closeForm();
                    if (detail.id === s.name) detail.close();
                    else detail.open(s.name);
                  }}
                >
                  <Table.Td>
                    <Code>{s.name}</Code>
                  </Table.Td>
                  <Table.Td>{s.description ?? ""}</Table.Td>
                  <Table.Td>
                    <Text fz="sm">
                      {s.updated_at
                        ? new Date(s.updated_at).toLocaleString()
                        : t("secretsTab.never")}
                    </Text>
                    {s.updated_by && (
                      <Text fz="xs" c="dimmed">
                        {t("secretsTab.updatedBy", { actor: s.updated_by })}
                      </Text>
                    )}
                  </Table.Td>
                  <Table.Td>
                    <Code>{s.reference}</Code>
                  </Table.Td>
                </ListRow>
              )}
            />
          }
        >
          {panelBody}
        </SplitPane>
      </Stack>
    </>
  );
}

/**
 * ORG SECRETS (REQ-1560): the shared vault. The route asks `org_settings` alone (REQ-1361): a list
 * of the names an org keeps is itself a statement about what that org connects to, so platform
 * authority does not reach it. The deployment's secrets service is on the Security page (REQ-1327).
 */
export function SecretsTab({ title }: { title: string }) {
  const { t } = useTranslation();

  // REQ-1574: the org's key sits above the org's secrets, because it is the thing they are wrapped
  // under -- the same right, org_settings, and the same page. Collapsed by default: setting the key
  // happens once, reading the names happens every day.
  const header = (
    <Collapsibles id="admin.secrets.org" testId="org-key-panel">
      {/* Mounted only when open, so a collapsed panel does not read the key's state. */}
      <CollapsibleItem
        value="orgKey"
        defaultOpen={false}
        mountWhenOpen
        controlTestId="org-key-toggle"
        title={t("orgEncryptionTab.panelTitle")}
      >
        <OrgEncryptionTab />
      </CollapsibleItem>
      {/* REQ-1992: the org's file folder, encrypted under a key the org holds in its vault. */}
      <CollapsibleItem
        value="orgFiles"
        defaultOpen={false}
        mountWhenOpen
        controlTestId="org-files-toggle"
        title={t("orgFilesTab.panelTitle")}
      >
        <OrgFilesTab />
      </CollapsibleItem>
      {/* REQ-2087: the redaction rules, which hold the org's secrets out of telemetry and errors. */}
      <CollapsibleItem
        value="redaction"
        defaultOpen={false}
        mountWhenOpen
        controlTestId="redaction-toggle"
        title={t("redactionTab.panelTitle")}
      >
        <RedactionTab />
      </CollapsibleItem>
    </Collapsibles>
  );

  return <SecretsVault vault="org" title={title} header={header} />;
}

/**
 * YOUR SECRETS (REQ-1560): the caller's own vault, in the org they are acting in.
 *
 * No capability is consulted, here or on the server. Holding a credential of your own is not a
 * privilege an administrator grants, and the reason another developer cannot use yours is not a
 * check -- it is that no request can name it.
 */
export function MySecretsTab({ title }: { title: string }) {
  return <SecretsVault vault="user" title={title} />;
}
