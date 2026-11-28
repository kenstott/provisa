// Copyright (c) 2026 Kenneth Stott
// Canary: 42a83daa-1a22-4874-9059-f8ba460f2ccc
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2098: the container registries a code source's built image is pushed to, entered once by an
// organisation administrator beside its package registries: a registry and repository namespace,
// a user name and a credential, optionally the PEM certificate authority its TLS chains to. One
// may be the default a build pushes to. Every field keeps what was typed -- the value, or a
// reference to a secret -- and shows it again; the credential behind the reveal control.

import { useCallback, useEffect, useState } from "react";
import {
  ActionIcon,
  Alert,
  Button,
  Checkbox,
  Group,
  PasswordInput,
  Stack,
  Table,
  Text,
  Textarea,
  TextInput,
} from "@mantine/core";
import { Container, Pencil, X } from "lucide-react";
import { useTranslation } from "react-i18next";
import {
  addContainerRegistry,
  deleteContainerRegistry,
  fetchContainerRegistries,
  replaceContainerRegistry,
  type ContainerRegistry,
} from "../../api/containerRegistries";
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
import { useRememberedDetail } from "../list/useDetailParam";
import { detailListOf, searchNarrowing, shownBy } from "../list/leftOut";
import { useUrlFilter } from "../list/useUrlFilter";
import { FilterInput } from "./FilterInput";
import { PageActions } from "../PageActions";
import { PageCommand } from "../PageCommand";

/** An entry as the form holds it. */
interface Draft {
  address: string;
  username: string;
  credential: string;
  isDefault: boolean;
  caBundle: string;
}

const draftOf = (r: ContainerRegistry): Draft => ({
  address: r.address,
  username: r.username,
  credential: r.credential,
  isDefault: r.default,
  caBundle: r.ca_bundle ?? "",
});

const BLANK: Draft = { address: "", username: "", credential: "", isDefault: false, caBundle: "" };

/** The form open in the panel: a new entry (blank or a copy), or the open entry's edit. */
type Mode = "create" | "edit" | null;

/** REQ-2277: the object management anatomy -- the header (title, filter, Add), the list, and a row
 * opening the registry's panel (?registry=<id>), read, with Edit, Duplicate and Delete in its head. */
export function ContainerRegistriesTab({ title }: { title: string }) {
  const { t } = useTranslation();
  const { activeOrgId } = useAuth();
  const [kept, setKept] = useState<ContainerRegistry[] | null>(null);
  const [error, setError] = useState("");
  const [search, setSearch] = useUrlFilter();
  // REQ-2277 rule 9: a registry is open only while the search shows its row.
  const narrowings = [
    searchNarrowing<ContainerRegistry>(
      t,
      search,
      () => setSearch(""),
      // Every text a registry's row shows.
      (r) => [r.address, r.username, r.default ? t("detailFields.yes") : ""],
    ),
  ];
  const detail = useRememberedDetail(
    "containerRegistries",
    detailListOf(
      t,
      kept,
      (r) => r.id,
      (r) => r.address,
      narrowings,
    ),
  );
  const [mode, setMode] = useState<Mode>(null);
  const [draft, setDraft] = useState<Draft>(BLANK);
  const [saving, setSaving] = useState(false);
  const [problem, setProblem] = useState<string | null>(null);
  const required = useRequiredFields({
    address: draft.address,
    username: draft.username,
    credential: draft.credential,
  });

  const load = useCallback(() => {
    if (!activeOrgId) return Promise.resolve([] as ContainerRegistry[]);
    return fetchContainerRegistries(activeOrgId)
      .then((rows) => {
        setKept(rows);
        return rows;
      })
      .catch((e: unknown) => {
        setError(String(e));
        return [] as ContainerRegistry[];
      });
  }, [activeOrgId]);
  useEffect(() => {
    void load();
  }, [load]);

  if (error) {
    return (
      <Alert color="red" variant="light" data-testid="container-registries-error">
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
      address: draft.address.trim(),
      username: draft.username.trim(),
      credential: draft.credential,
      default: draft.isDefault,
      ca_bundle: draft.caBundle.trim() || null,
    };
    setSaving(true);
    setProblem(null);
    try {
      const saved =
        mode === "edit" && open !== null
          ? await replaceContainerRegistry(activeOrgId, open.id, body)
          : await addContainerRegistry(activeOrgId, body);
      await load();
      setMode(null);
      detail.open(saved.id);
    } catch (e) {
      setProblem(String(e));
    } finally {
      setSaving(false);
    }
  };

  const remove = async (r: ContainerRegistry) => {
    await deleteContainerRegistry(activeOrgId, r.id);
    await load();
    detail.close({ only: r.id });
  };

  const fields = (d: Draft, editable: boolean) => (
    <>
      <FormField size="long" help={t("containerRegistries.addressHelp")}>
        <TextInput
          label={t("containerRegistries.address")}
          placeholder="ghcr.io/example"
          value={d.address}
          onChange={(e) => change({ address: e.currentTarget.value })}
          autoComplete="off"
          data-testid="container-registry-address"
          {...(editable ? required.field("address") : {})}
        />
      </FormField>
      <FormField size="medium">
        <TextInput
          label={t("containerRegistries.username")}
          value={d.username}
          onChange={(e) => change({ username: e.currentTarget.value })}
          autoComplete="off"
          data-testid="container-registry-username"
          {...(editable ? required.field("username") : {})}
        />
      </FormField>
      <FormField size="long" help={t("containerRegistries.credentialHelp")}>
        <PasswordInput
          label={t("containerRegistries.credential")}
          value={d.credential}
          onChange={(e) => change({ credential: e.currentTarget.value })}
          autoComplete="new-password"
          data-testid="container-registry-credential"
          {...(editable ? required.field("credential") : {})}
        />
      </FormField>
      <FormField size="long" help={t("containerRegistries.caBundleHelp")}>
        <Textarea
          label={t("containerRegistries.caBundle")}
          placeholder={"-----BEGIN CERTIFICATE-----"}
          value={d.caBundle}
          onChange={(e) => change({ caBundle: e.currentTarget.value })}
          autosize
          minRows={2}
          maxRows={8}
          styles={{ input: { fontFamily: "var(--mantine-font-family-monospace)" } }}
          data-testid="container-registry-ca-bundle"
        />
      </FormField>
      <FormChecks>
        <FormField size="short" help={t("containerRegistries.defaultHelp")}>
          <Checkbox
            label={t("containerRegistries.default")}
            checked={d.isDefault}
            onChange={(e) => change({ isDefault: e.currentTarget.checked })}
            data-testid="container-registry-default"
          />
        </FormField>
      </FormChecks>
    </>
  );

  const formView = (
    <FormLayout data-testid="container-registry-form">
      {fields(draft, true)}
      <FormFooter
        panel
        onSave={save}
        onCancel={closeForm}
        saving={saving}
        problem={problem ?? undefined}
        saveProps={{ "data-testid": "container-registry-save" }}
      />
    </FormLayout>
  );

  const readView = (r: ContainerRegistry) => (
    <Stack gap="sm" data-testid={`container-registry-detail-${r.id}`}>
      <PanelActions>
        <fieldset className="panel-action-set">
          <Group gap="xs">
            <Tip text={t("containerRegistries.edit", { name: r.address })} named>
              <ActionIcon
                aria-label={t("containerRegistries.edit", { name: r.address })}
                data-testid={`container-registry-edit-${r.id}`}
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
              onDuplicate={() => openForm("create", { ...draftOf(r), isDefault: false })}
              testId={`container-registry-duplicate-${r.id}`}
            />
            <PanelDelete
              itemKey={r.id}
              title={t("containerRegistries.remove")}
              consequence={t("containerRegistries.removeConfirm", { name: r.address })}
              ariaLabel={t("containerRegistries.remove")}
              testId={`container-registry-remove-${r.id}`}
              onDelete={() => remove(r)}
            />
          </Group>
        </fieldset>
      </PanelActions>
      <FormRead testId={`container-registry-read-${r.id}`}>{fields(draftOf(r), false)}</FormRead>
    </Stack>
  );

  const panelTitle =
    mode === "create" ? t("containerRegistries.add") : open !== null ? open.address : "";
  const panelBody =
    mode === "create"
      ? formView
      : open !== null
        ? mode === "edit"
          ? formView
          : readView(open)
        : null;
  const yes = t("detailFields.yes");

  return (
    <>
      <div className="page-header">
        <PageTitle icon={Container}>{title}</PageTitle>
        <FilterInput
          value={search}
          onChange={setSearch}
          placeholder={t("containerRegistries.filterPlaceholder")}
          testId="container-registries-filter"
        />
        <PageActions>
          <PageCommand
            description={t("pageCommands.addContainerRegistry")}
            docs="code-sources/#deployment"
          >
            <Button
              variant={mode === "create" ? "outline" : "filled"}
              onClick={() => (mode === "create" ? closeForm() : openForm("create", BLANK))}
              aria-label={mode === "create" ? t("containerRegistries.closeForm") : undefined}
              data-testid="container-registries-add"
            >
              {mode === "create" ? <X size={14} /> : t("containerRegistries.add")}
            </Button>
          </PageCommand>
        </PageActions>
      </div>
      <Stack gap="md" data-testid="container-registries">
        <Text size="sm" c="dimmed">
          {t("containerRegistries.help")}
        </Text>
        <SplitPane
          outOfList={detail.outOfList}
          testId="container-registries-split"
          storageKey="provisa.containerRegistries.detailWidth"
          title={panelTitle}
          onClose={() => {
            closeForm();
            detail.close();
          }}
          closeOnEscape={mode === null}
          labels={{
            panel: t("containerRegistries.address"),
            resize: t("tablesPage.resizeDetail"),
            expand: t("tablesPage.expandDetail"),
            restore: t("tablesPage.restoreDetail"),
            close: t("tablesPage.closeDetail"),
            backToList: t("tablesPage.backToList"),
          }}
          list={
            <SortGroupTable
              testPrefix="container-registries"
              testId="container-registries-list"
              emptyTestId="container-registries-empty"
              rows={shown}
              columns={[
                {
                  key: "address",
                  label: t("containerRegistries.address"),
                  sortValue: (r) => r.address,
                },
                {
                  key: "username",
                  label: t("containerRegistries.username"),
                  sortValue: (r) => r.username,
                },
                {
                  key: "default",
                  label: t("containerRegistries.default"),
                  sortValue: (r) => (r.default ? 0 : 1),
                  groupValue: (r) => (r.default ? yes : t("detailFields.no")),
                },
              ]}
              headers={[{ col: "address" }, { col: "username" }, { col: "default" }]}
              colSpan={3}
              rowKey={(r) => r.id}
              // A list a filter emptied says so, never that none is kept.
              empty={
                kept.length === 0 ? t("containerRegistries.none") : t("containerRegistries.noMatch")
              }
              render={(r) => (
                <ListRow
                  key={r.id}
                  testId={`container-registry-${r.id}`}
                  selected={mode !== "create" && r.id === detail.id}
                  onClick={() => {
                    closeForm();
                    if (detail.id === r.id) detail.close();
                    else detail.open(r.id);
                  }}
                >
                  <Table.Td>{r.address}</Table.Td>
                  <Table.Td>{r.username}</Table.Td>
                  <Table.Td>{r.default ? yes : ""}</Table.Td>
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
