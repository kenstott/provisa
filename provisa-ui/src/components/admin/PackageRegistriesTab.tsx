// Copyright (c) 2026 Kenneth Stott
// Canary: 1327210d-927b-4de4-b9ce-418c2e447d3a
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1984, REQ-1993: the package registries every Node and Python install of the organisation
// goes through, entered once by an organisation administrator: npm registries (an address, an
// optional scope, a credential; one may be the mirror), PyPI indexes (an address and a credential;
// one may be the default index) and Git hosts (a host and a credential). The public npm registry
// and the public PyPI need no entry.
//
// REQ-2200, REQ-2227: Go module proxies (an address and user:password) and Cargo registries (a
// sparse index URL and a token) likewise; one of each kind may be the default, in place of the
// public Go module proxy and crates.io.
//
// Every field keeps what was typed -- the value, or a reference to a secret the administrator
// created -- and shows it again; the credential behind the reveal control.
//
// REQ-2088: a credential may be user:password (npm, PyPI: an Artifactory user and its API key) or
// user:token (Git: GitLab, Bitbucket), and every entry may carry the PEM certificate authority its
// server's TLS certificate chains to, for a proxy or Git server behind a private CA.

import { useCallback, useEffect, useState } from "react";
import {
  ActionIcon,
  Alert,
  Button,
  Checkbox,
  Group,
  PasswordInput,
  Select,
  Stack,
  Table,
  Text,
  Textarea,
  TextInput,
} from "@mantine/core";
import { Package, Pencil, X } from "lucide-react";
import { useTranslation } from "react-i18next";
import {
  addPackageRegistry,
  deletePackageRegistry,
  fetchPackageRegistries,
  replacePackageRegistry,
  type PackageRegistry,
  type RegistryKind,
} from "../../api/packageRegistries";
import { useAuth } from "../../context/AuthContext";
import { PageTitle } from "../PageTitle";
import { Tip } from "../Tip";
import { FormChecks, FormField, FormFooter, FormLayout, FormRead } from "../form/FormLayout";
import { useRequiredFields } from "../form/useRequiredFields";
import { ListRow } from "../list/ListTable";
import { PanelDelete } from "../list/PanelDelete";
import { PanelDuplicate } from "../list/PanelDuplicate";
import { SortGroupTable } from "../list/SortGroupTable";
import { PanelActions, SplitPane } from "../list/SplitPane";
import { detailListOf, searchNarrowing, shownBy } from "../list/leftOut";
import { useRememberedDetail } from "../list/useDetailParam";
import { useUrlFilter } from "../list/useUrlFilter";
import { FilterInput } from "./FilterInput";
import { PageActions } from "../PageActions";
import { PageCommand } from "../PageCommand";

const KINDS: readonly RegistryKind[] = ["npm", "pypi", "goproxy", "cargo", "git"];

/** An entry as the form holds it. */
interface Draft {
  kind: RegistryKind;
  address: string;
  scope: string;
  credential: string;
  mirror: boolean;
  caBundle: string;
}

const draftOf = (r: PackageRegistry): Draft => ({
  kind: r.kind,
  address: r.address,
  scope: r.scope ?? "",
  credential: r.credential,
  mirror: r.mirror,
  caBundle: r.ca_bundle ?? "",
});

const blank = (kind: RegistryKind): Draft => ({
  kind,
  address: "",
  scope: "",
  credential: "",
  mirror: false,
  caBundle: "",
});

/** The form open in the panel: a new entry (blank or a copy), or the open entry's edit. */
type Mode = "create" | "edit" | null;

/** Each kind's own labels and placeholder; only npm has a scope, and Git hosts no mirror. */
const LABELS: Record<
  RegistryKind,
  {
    address: string;
    addressHelp: string | null;
    placeholder: string;
    credential: string;
    credentialHelp: string;
    mirror: string | null;
  }
> = {
  npm: {
    address: "packageRegistries.npmAddress",
    addressHelp: null,
    placeholder: "https://npm.example.com/",
    credential: "packageRegistries.npmCredential",
    credentialHelp: "packageRegistries.npmCredentialHelp",
    mirror: "packageRegistries.mirror",
  },
  pypi: {
    address: "packageRegistries.pypiAddress",
    addressHelp: null,
    placeholder: "https://pypi.example.com/simple/",
    credential: "packageRegistries.pypiCredential",
    credentialHelp: "packageRegistries.pypiCredentialHelp",
    mirror: "packageRegistries.pypiMirror",
  },
  // REQ-2200: a Go module proxy; its credential is the login the go command sends.
  goproxy: {
    address: "packageRegistries.goproxyAddress",
    addressHelp: null,
    placeholder: "https://goproxy.example.com",
    credential: "packageRegistries.goproxyCredential",
    credentialHelp: "packageRegistries.goproxyCredentialHelp",
    mirror: "packageRegistries.goproxyMirror",
  },
  // REQ-2227: a Cargo registry, by its sparse index URL; its credential is the token cargo sends.
  cargo: {
    address: "packageRegistries.cargoAddress",
    addressHelp: "packageRegistries.cargoAddressHelp",
    placeholder: "sparse+https://cargo.example.com/index/",
    credential: "packageRegistries.cargoCredential",
    credentialHelp: "packageRegistries.cargoCredentialHelp",
    mirror: "packageRegistries.cargoMirror",
  },
  git: {
    address: "packageRegistries.gitHost",
    addressHelp: "packageRegistries.gitHostHelp",
    placeholder: "git.example.com",
    credential: "packageRegistries.gitCredential",
    credentialHelp: "packageRegistries.gitCredentialHelp",
    mirror: null,
  },
};

/** REQ-2277: the object management anatomy -- the header (title, filter, Add), every registry of
 * every kind in one list (grouped by kind), and a row opening the registry's panel
 * (?registry=<id>), read, with Edit, Duplicate and Delete in its head. */
export function PackageRegistriesTab({ title }: { title: string }) {
  const { t } = useTranslation();
  const { activeOrgId } = useAuth();
  const [kept, setKept] = useState<PackageRegistry[] | null>(null);
  const [error, setError] = useState("");
  const [search, setSearch] = useUrlFilter();
  const kindLabel = (k: RegistryKind) => t(`packageRegistries.kind_${k}`);
  // Whether a registry is its kind's default, as its row says.
  const isDefault = (r: PackageRegistry) =>
    LABELS[r.kind].mirror !== null && r.mirror ? t("detailFields.yes") : t("detailFields.no");
  // REQ-2277 rule 9: a registry is open only while the search shows its row.
  const narrowings = [
    searchNarrowing<PackageRegistry>(
      t,
      search,
      () => setSearch(""),
      // Every text a registry's row shows.
      (r) => [kindLabel(r.kind), r.address, r.scope, isDefault(r)],
    ),
  ];
  const detail = useRememberedDetail(
    "packageRegistries",
    detailListOf(
      t,
      kept,
      (r) => r.id,
      (r) => r.address,
      narrowings,
    ),
  );
  const [mode, setMode] = useState<Mode>(null);
  const [draft, setDraft] = useState<Draft>(blank("npm"));
  const [saving, setSaving] = useState(false);
  const [problem, setProblem] = useState<string | null>(null);
  const required = useRequiredFields({ address: draft.address, credential: draft.credential });

  const load = useCallback(() => {
    if (!activeOrgId) return Promise.resolve();
    return fetchPackageRegistries(activeOrgId)
      .then(setKept)
      .catch((e: unknown) => setError(String(e)));
  }, [activeOrgId]);
  useEffect(() => {
    void load();
  }, [load]);

  if (error) {
    return (
      <Alert color="red" variant="light" data-testid="package-registries-error">
        {error}
      </Alert>
    );
  }
  if (!kept || !activeOrgId) return null;

  const open = kept.find((r) => r.id === detail.id) ?? null;
  const shown = shownBy(kept, narrowings);

  const change = (patch: Partial<Draft>) => setDraft((d) => ({ ...d, ...patch }));
  const openForm = (next: Mode, from: Draft) => {
    setDraft(from);
    setProblem(null);
    setMode(next);
  };
  const closeForm = () => {
    setMode(null);
    setProblem(null);
  };

  const save = async () => {
    if (!required.validate()) return;
    const body = {
      kind: draft.kind,
      address: draft.address.trim(),
      scope: draft.kind === "npm" ? draft.scope.trim() || null : null,
      credential: draft.credential,
      mirror: LABELS[draft.kind].mirror !== null && draft.mirror,
      ca_bundle: draft.caBundle.trim() || null,
    };
    setSaving(true);
    setProblem(null);
    try {
      const saved =
        mode === "edit" && open !== null
          ? await replacePackageRegistry(activeOrgId, open.id, body)
          : await addPackageRegistry(activeOrgId, body);
      await load();
      setMode(null);
      detail.open(saved.id);
    } catch (e) {
      setProblem(String(e));
    } finally {
      setSaving(false);
    }
  };

  const remove = async (r: PackageRegistry) => {
    await deletePackageRegistry(activeOrgId, r.id);
    await load();
    detail.close({ only: r.id });
  };

  const fields = (d: Draft, editable: boolean) => {
    const labels = LABELS[d.kind];
    return (
      <>
        <FormField size="medium" help={t(`packageRegistries.${d.kind}Help`)}>
          <Select
            label={t("packageRegistries.kind")}
            data={KINDS.map((k) => ({ value: k, label: kindLabel(k) }))}
            value={d.kind}
            allowDeselect={false}
            // A kept entry keeps its kind: another kind is another registry.
            disabled={mode === "edit"}
            onChange={(v) => v !== null && change({ kind: v as RegistryKind })}
            data-testid="package-registry-kind"
          />
        </FormField>
        <FormField size="long" help={labels.addressHelp ? t(labels.addressHelp) : undefined}>
          <TextInput
            label={t(labels.address)}
            placeholder={labels.placeholder}
            value={d.address}
            onChange={(e) => change({ address: e.currentTarget.value })}
            autoComplete="off"
            data-testid="package-registry-address"
            {...(editable ? required.field("address") : {})}
          />
        </FormField>
        {d.kind === "npm" && (
          <FormField size="short" help={t("packageRegistries.scopeHelp")}>
            <TextInput
              label={t("packageRegistries.scope")}
              placeholder="@example"
              value={d.scope}
              onChange={(e) => change({ scope: e.currentTarget.value })}
              autoComplete="off"
              data-testid="package-registry-scope"
            />
          </FormField>
        )}
        <FormField size="long" help={t(labels.credentialHelp)}>
          <PasswordInput
            label={t(labels.credential)}
            value={d.credential}
            onChange={(e) => change({ credential: e.currentTarget.value })}
            autoComplete="new-password"
            data-testid="package-registry-credential"
            {...(editable ? required.field("credential") : {})}
          />
        </FormField>
        <FormField size="long" help={t("packageRegistries.caBundleHelp")}>
          <Textarea
            label={t("packageRegistries.caBundle")}
            placeholder={"-----BEGIN CERTIFICATE-----"}
            value={d.caBundle}
            onChange={(e) => change({ caBundle: e.currentTarget.value })}
            autosize
            minRows={2}
            maxRows={8}
            styles={{ input: { fontFamily: "var(--mantine-font-family-monospace)" } }}
            data-testid="package-registry-ca-bundle"
          />
        </FormField>
        {labels.mirror !== null && (
          <FormChecks>
            <FormField size="short" help={t(`${labels.mirror}Help`)}>
              <Checkbox
                label={t(labels.mirror)}
                checked={d.mirror}
                onChange={(e) => change({ mirror: e.currentTarget.checked })}
                data-testid="package-registry-mirror"
              />
            </FormField>
          </FormChecks>
        )}
      </>
    );
  };

  const formView = (
    <FormLayout data-testid="package-registry-form">
      {fields(draft, true)}
      <FormFooter
        panel
        onSave={save}
        onCancel={closeForm}
        saving={saving}
        problem={problem ?? undefined}
        saveProps={{ "data-testid": "package-registry-save" }}
      />
    </FormLayout>
  );

  const readView = (r: PackageRegistry) => (
    <Stack gap="sm" data-testid={`package-registry-detail-${r.id}`}>
      <PanelActions>
        <fieldset className="panel-action-set">
          <Group gap="xs">
            <Tip text={t("packageRegistries.edit", { name: r.address })} named>
              <ActionIcon
                aria-label={t("packageRegistries.edit", { name: r.address })}
                data-testid={`package-registry-edit-${r.id}`}
                onClick={(e) => {
                  e.stopPropagation();
                  openForm("edit", draftOf(r));
                }}
              >
                <Pencil size={14} />
              </ActionIcon>
            </Tip>
            <PanelDuplicate
              name={r.address}
              onDuplicate={() => openForm("create", { ...draftOf(r), mirror: false })}
              testId={`package-registry-duplicate-${r.id}`}
            />
            <PanelDelete
              itemKey={r.id}
              title={t("packageRegistries.remove")}
              consequence={t("packageRegistries.removeConfirm", { name: r.address })}
              ariaLabel={t("packageRegistries.remove")}
              testId={`package-registry-remove-${r.id}`}
              onDelete={() => remove(r)}
            />
          </Group>
        </fieldset>
      </PanelActions>
      <FormRead testId={`package-registry-read-${r.id}`}>{fields(draftOf(r), false)}</FormRead>
    </Stack>
  );

  const panelTitle =
    mode === "create" ? t("packageRegistries.add") : open !== null ? open.address : "";
  const panelBody =
    mode === "create"
      ? formView
      : open !== null
        ? mode === "edit"
          ? formView
          : readView(open)
        : null;
  return (
    <>
      <div className="page-header">
        <PageTitle icon={Package}>{title}</PageTitle>
        <FilterInput
          value={search}
          onChange={setSearch}
          placeholder={t("packageRegistries.filterPlaceholder")}
          testId="package-registries-filter"
        />
        <PageActions>
          <PageCommand
            description={t("pageCommands.addPackageRegistry")}
            docs="package-registries/"
          >
            <Button
              variant={mode === "create" ? "outline" : "filled"}
              onClick={() => (mode === "create" ? closeForm() : openForm("create", blank("npm")))}
              aria-label={mode === "create" ? t("packageRegistries.closeForm") : undefined}
              data-testid="package-registries-add"
            >
              {mode === "create" ? <X size={14} /> : t("packageRegistries.add")}
            </Button>
          </PageCommand>
        </PageActions>
      </div>
      <Stack gap="md" data-testid="package-registries">
        <Text size="sm" c="dimmed">
          {t("packageRegistries.help")}
        </Text>
        <SplitPane
          outOfList={detail.outOfList}
          testId="package-registries-split"
          storageKey="provisa.packageRegistries.detailWidth"
          title={panelTitle}
          onClose={() => {
            closeForm();
            detail.close();
          }}
          closeOnEscape={mode === null}
          labels={{
            panel: t("packageRegistries.panelLabel"),
            resize: t("tablesPage.resizeDetail"),
            expand: t("tablesPage.expandDetail"),
            restore: t("tablesPage.restoreDetail"),
            close: t("tablesPage.closeDetail"),
            backToList: t("tablesPage.backToList"),
          }}
          list={
            <SortGroupTable
              testPrefix="package-registries"
              testId="package-registries-list"
              emptyTestId="package-registries-empty"
              rows={shown}
              columns={[
                {
                  key: "kind",
                  label: t("packageRegistries.kind"),
                  sortValue: (r) => KINDS.indexOf(r.kind),
                  groupValue: (r) => kindLabel(r.kind),
                },
                {
                  key: "address",
                  label: t("packageRegistries.address"),
                  sortValue: (r) => r.address,
                },
                {
                  key: "scope",
                  label: t("packageRegistries.scope"),
                  sortValue: (r) => r.scope ?? "",
                },
                {
                  key: "default",
                  label: t("packageRegistries.defaultColumn"),
                  sortValue: isDefault,
                  groupValue: isDefault,
                },
              ]}
              headers={[{ col: "kind" }, { col: "address" }, { col: "scope" }, { col: "default" }]}
              colSpan={4}
              rowKey={(r) => r.id}
              // A list a filter emptied says so, never that none is kept.
              empty={
                kept.length === 0 ? t("packageRegistries.none") : t("packageRegistries.noMatch")
              }
              render={(r) => (
                <ListRow
                  key={r.id}
                  testId={`package-registry-${r.id}`}
                  selected={mode !== "create" && r.id === detail.id}
                  onClick={() => {
                    closeForm();
                    if (detail.id === r.id) detail.close();
                    else detail.open(r.id);
                  }}
                >
                  <Table.Td>{kindLabel(r.kind)}</Table.Td>
                  <Table.Td>{r.address}</Table.Td>
                  <Table.Td>{r.scope ?? ""}</Table.Td>
                  <Table.Td>{isDefault(r)}</Table.Td>
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
