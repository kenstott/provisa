// Copyright (c) 2026 Kenneth Stott
// Canary: 3c9e1f74-5a28-4d06-b7e3-91f0a6c2d8b5
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1922: Admin > Regions -- the org's stores, and the platform regions it selects with the store
// it keeps each kind of data in there. Two lists and one detail panel. Every save sends the org's
// whole set, which the server validates as it validates a configuration and refuses naming the
// field; the panel says why and marks the field. A store URL is kept as entered -- a reference such
// as ${env:NAME} stays one -- and comes back without its password.
//
// An org that selects no region yet cannot keep a store on its own (the set would select none):
// a store added then is held on the page and saved with the org's first region.

import { useCallback, useEffect, useMemo, useState, type ReactNode } from "react";
import { useTranslation } from "react-i18next";
import {
  ActionIcon,
  Alert,
  Badge,
  Button,
  Group,
  Select,
  Stack,
  Table,
  Text,
  TextInput,
  Title,
} from "@mantine/core";
import { Pencil, TriangleAlert } from "lucide-react";
import {
  fetchOrgRegions,
  saveOrgRegions,
  RegionsRefused,
  STORE_ROLES,
  type OrgRegion,
  type OrgRegions,
  type Store,
  type StoreRole,
} from "../../api/regions";
import { DetailFields } from "../detail/DetailFields";
import { FormField, FormFooter, FormIdentifiers, FormLayout } from "../form/FormLayout";
import { ListEmpty, ListHead, ListItems, ListRow, ListTable } from "../list/ListTable";
import { PanelActions, SplitPane } from "../list/SplitPane";
import { PanelDelete } from "../list/PanelDelete";
import { useListSortGroup, type ListColumn } from "../list/useListSortGroup";
import { PageLoading } from "../PageLoading";
import { Tip } from "../Tip";

type Kind = "region" | "store";
/** What the panel shows: an item, or a new one (`id` null); `editing` for its form. */
interface Picked {
  kind: Kind;
  id: string | null;
  editing: boolean;
}

/** The refusal of a save, and the field it names (`regions.eu.engine`). */
interface Problem {
  text: string;
  field: string | null;
}

const problemOf = (e: unknown): Problem =>
  e instanceof RegionsRefused
    ? { text: e.message, field: e.field }
    : { text: e instanceof Error ? e.message : String(e), field: null };

const itemKey = (kind: Kind, id: string) => `${kind}:${id}`;

export function OrgRegionsTab() {
  const { t } = useTranslation();
  const [data, setData] = useState<OrgRegions | null>(null);
  const [error, setError] = useState("");
  const [picked, setPicked] = useState<Picked | null>(null);
  // Stores added while the org selects no region: saved with its first region.
  const [held, setHeld] = useState<Store[]>([]);

  const load = useCallback(() => {
    fetchOrgRegions()
      .then(setData)
      .catch((e: unknown) => setError(problemOf(e).text));
  }, []);
  useEffect(load, [load]);

  const stores = useMemo(() => [...(data?.stores ?? []), ...held], [data, held]);
  const regions = useMemo(() => data?.regions ?? [], [data]);

  const regionColumns: ListColumn<OrgRegion>[] = useMemo(
    () => [
      { key: "id", label: t("orgRegions.colRegion"), sortValue: (r) => r.id },
      ...STORE_ROLES.map((role) => ({
        key: role,
        label: t(`orgRegions.role.${role}`),
        sortValue: (r: OrgRegion) => r[role],
      })),
    ],
    [t],
  );
  const storeColumns: ListColumn<Store>[] = useMemo(
    () => [
      { key: "id", label: t("orgRegions.colStore"), sortValue: (s) => s.id },
      { key: "url", label: t("orgRegions.colUrl"), sortValue: (s) => s.url },
      { key: "kind", label: t("orgRegions.colKind"), sortValue: (s) => s.kind ?? "" },
    ],
    [t],
  );
  const regionSort = useListSortGroup(regions, regionColumns, "org-regions");
  const storeSort = useListSortGroup(stores, storeColumns, "org-stores");

  if (error) {
    return (
      <Alert color="red" icon={<TriangleAlert size={16} />} data-testid="org-regions-error">
        {error}
      </Alert>
    );
  }
  if (data === null) return <PageLoading message={t("orgRegions.loading")} />;
  if (data.platform_regions.length === 0) {
    return (
      <Text size="sm" c="dimmed" data-testid="org-regions-none-declared">
        {t("orgRegions.noPlatformRegions")}
      </Text>
    );
  }

  /** Save the set with this change; the page then shows what the server kept. */
  const commit = async (nextStores: Store[], nextRegions: OrgRegion[]) => {
    const saved = await saveOrgRegions(nextStores, nextRegions);
    setData({ ...data, ...saved });
    setHeld([]);
  };

  const saveRegion = async (region: OrgRegion, was: string | null) => {
    const others = regions.filter((r) => r.id !== (was ?? region.id));
    await commit(stores, [...others, region]);
    setPicked({ kind: "region", id: region.id, editing: false });
  };

  const saveStore = async (store: Store, was: string | null) => {
    const keep = (s: Store) => s.id !== (was ?? store.id);
    if (regions.length === 0) {
      // Nothing to save it with yet: held until the first region is.
      setHeld((h) => [...h.filter(keep), store]);
    } else {
      await commit([...stores.filter(keep), store], regions);
    }
    setPicked({ kind: "store", id: store.id, editing: false });
  };

  const removeRegion = async (id: string) => {
    await commit(
      stores,
      regions.filter((r) => r.id !== id),
    );
    setPicked(null);
  };

  const removeStore = async (id: string) => {
    if (held.some((s) => s.id === id)) {
      setHeld((h) => h.filter((s) => s.id !== id));
    } else {
      await commit(
        stores.filter((s) => s.id !== id),
        regions,
      );
    }
    setPicked(null);
  };

  const region = picked?.kind === "region" ? regions.find((r) => r.id === picked.id) : undefined;
  const store = picked?.kind === "store" ? stores.find((s) => s.id === picked.id) : undefined;
  const close = () => setPicked(null);

  let title = "";
  let panel: ReactNode = null;
  if (picked?.kind === "region" && (picked.editing || picked.id === null)) {
    title =
      picked.id === null
        ? t("orgRegions.addRegion")
        : t("orgRegions.editRegion", { id: picked.id });
    panel = (
      <RegionForm
        key={picked.id ?? "new"}
        region={region ?? null}
        free={data.platform_regions
          .map((p) => p.id)
          .filter((id) => id === picked.id || !regions.some((r) => r.id === id))}
        stores={stores.map((s) => s.id)}
        onSave={(r) => saveRegion(r, picked.id)}
        onCancel={() => (picked.id === null ? close() : setPicked({ ...picked, editing: false }))}
      />
    );
  } else if (picked?.kind === "store" && (picked.editing || picked.id === null)) {
    title =
      picked.id === null ? t("orgRegions.addStore") : t("orgRegions.editStore", { id: picked.id });
    panel = (
      <StoreForm
        key={picked.id ?? "new"}
        store={store ?? null}
        taken={stores.map((s) => s.id).filter((id) => id !== picked.id)}
        kinds={data.engine_kinds}
        onSave={(s) => saveStore(s, picked.id)}
        onCancel={() => (picked.id === null ? close() : setPicked({ ...picked, editing: false }))}
      />
    );
  } else if (region) {
    title = region.id;
    const address = data.platform_regions.find((p) => p.id === region.id)?.address ?? "";
    panel = (
      <ItemRead
        kind="region"
        id={region.id}
        onEdit={() => setPicked({ kind: "region", id: region.id, editing: true })}
        onDelete={() => removeRegion(region.id)}
        fields={[
          { label: t("orgRegions.colRegion"), value: region.id },
          { label: t("orgRegions.address"), value: address },
          ...STORE_ROLES.map((role) => ({
            label: t(`orgRegions.role.${role}`),
            value: region[role],
            testId: `org-region-read-${role}`,
          })),
        ]}
      />
    );
  } else if (store) {
    title = store.id;
    const usedBy = regions.filter((r) => STORE_ROLES.some((role) => r[role] === store.id));
    panel = (
      <ItemRead
        kind="store"
        id={store.id}
        onEdit={() => setPicked({ kind: "store", id: store.id, editing: true })}
        onDelete={() => removeStore(store.id)}
        fields={[
          { label: t("orgRegions.colStore"), value: store.id },
          { label: t("orgRegions.colUrl"), value: store.url, testId: "org-store-read-url" },
          { label: t("orgRegions.colKind"), value: store.kind ?? t("orgRegions.noKind") },
          {
            label: t("orgRegions.usedBy"),
            value: usedBy.length ? usedBy.map((r) => r.id).join(", ") : t("orgRegions.unused"),
          },
        ]}
      />
    );
  }

  const picks = (kind: Kind, id: string) => picked?.kind === kind && picked.id === id;

  return (
    <Stack gap="sm" data-testid="org-regions">
      <Group justify="space-between" wrap="nowrap" align="flex-start">
        <Text c="dimmed" size="sm" maw={820}>
          {t("orgRegions.intro")}
        </Text>
        <Group gap="xs" wrap="nowrap" data-toolbar>
          <Button
            size="xs"
            data-testid="org-regions-add-region"
            onClick={() => setPicked({ kind: "region", id: null, editing: true })}
          >
            {t("orgRegions.addRegion")}
          </Button>
          <Button
            size="xs"
            data-testid="org-regions-add-store"
            onClick={() => setPicked({ kind: "store", id: null, editing: true })}
          >
            {t("orgRegions.addStore")}
          </Button>
        </Group>
      </Group>
      {held.length > 0 && (
        <Alert color="yellow" data-testid="org-regions-held">
          {t("orgRegions.heldStores", { count: held.length })}
        </Alert>
      )}
      <SplitPane
        testId="org-regions-split"
        storageKey="provisa.orgRegions.detailWidth"
        title={title}
        onClose={close}
        closeOnEscape={!picked?.editing}
        labels={{
          panel: t("orgRegions.detailPanel"),
          resize: t("tablesPage.resizeDetail"),
          expand: t("tablesPage.expandDetail"),
          restore: t("tablesPage.restoreDetail"),
          close: t("tablesPage.closeDetail"),
          backToList: t("tablesPage.backToList"),
        }}
        list={
          <Stack gap="md">
            <Title order={5}>{t("orgRegions.regionsHeading")}</Title>
            <ListTable testId="org-regions-table">
              <ListHead
                sortGroup={regionSort}
                columns={[{ col: "id" }, ...STORE_ROLES.map((role) => ({ col: role }))]}
              />
              <Table.Tbody>
                {regions.length === 0 && (
                  <ListEmpty colSpan={7} testId="org-regions-empty">
                    {t("orgRegions.noRegions")}
                  </ListEmpty>
                )}
                <ListItems
                  state={regionSort}
                  colSpan={7}
                  rowKey={(r) => itemKey("region", r.id)}
                  render={(r) => (
                    <ListRow
                      key={r.id}
                      itemKey={itemKey("region", r.id)}
                      testId={`org-region-row-${r.id}`}
                      selected={picks("region", r.id)}
                      onClick={() =>
                        setPicked(
                          picks("region", r.id)
                            ? null
                            : { kind: "region", id: r.id, editing: false },
                        )
                      }
                    >
                      <Table.Td>
                        <Text size="sm" fw={600}>
                          {r.id}
                        </Text>
                      </Table.Td>
                      {STORE_ROLES.map((role) => (
                        <Table.Td key={role}>
                          <Text size="xs">{r[role]}</Text>
                        </Table.Td>
                      ))}
                    </ListRow>
                  )}
                />
              </Table.Tbody>
            </ListTable>
            <Title order={5}>{t("orgRegions.storesHeading")}</Title>
            <ListTable testId="org-stores-table">
              <ListHead
                sortGroup={storeSort}
                columns={[{ col: "id" }, { col: "url" }, { col: "kind" }]}
              />
              <Table.Tbody>
                {stores.length === 0 && (
                  <ListEmpty colSpan={3} testId="org-stores-empty">
                    {t("orgRegions.noStores")}
                  </ListEmpty>
                )}
                <ListItems
                  state={storeSort}
                  colSpan={3}
                  rowKey={(s) => itemKey("store", s.id)}
                  render={(s) => (
                    <ListRow
                      key={s.id}
                      itemKey={itemKey("store", s.id)}
                      testId={`org-store-row-${s.id}`}
                      selected={picks("store", s.id)}
                      onClick={() =>
                        setPicked(
                          picks("store", s.id) ? null : { kind: "store", id: s.id, editing: false },
                        )
                      }
                    >
                      <Table.Td>
                        <Group gap={6} wrap="nowrap">
                          <Text size="sm" fw={600}>
                            {s.id}
                          </Text>
                          {held.some((h) => h.id === s.id) && (
                            <Badge size="xs" color="yellow" variant="light">
                              {t("orgRegions.notSaved")}
                            </Badge>
                          )}
                        </Group>
                      </Table.Td>
                      <Table.Td>
                        <Text size="xs" ff="monospace" style={{ wordBreak: "break-all" }}>
                          {s.url}
                        </Text>
                      </Table.Td>
                      <Table.Td>
                        <Text size="xs">{s.kind ?? ""}</Text>
                      </Table.Td>
                    </ListRow>
                  )}
                />
              </Table.Tbody>
            </ListTable>
          </Stack>
        }
      >
        {panel}
      </SplitPane>
    </Stack>
  );
}

/** A region or a store as read: its fields, with Edit and Delete in the panel's head. */
function ItemRead({
  kind,
  id,
  fields,
  onEdit,
  onDelete,
}: {
  kind: Kind;
  id: string;
  fields: { label: string; value: ReactNode; testId?: string }[];
  onEdit: () => void;
  onDelete: () => Promise<void>;
}) {
  const { t } = useTranslation();
  return (
    <>
      <PanelActions>
        <fieldset className="panel-action-set">
          <Group gap="xs">
            <Tip text={t("orgRegions.edit")} named>
              <ActionIcon
                aria-label={t("orgRegions.edit")}
                data-testid={`org-${kind}-edit`}
                onClick={onEdit}
              >
                <Pencil size={14} />
              </ActionIcon>
            </Tip>
            <PanelDelete
              itemKey={itemKey(kind, id)}
              title={t(`orgRegions.delete.${kind}Title`, { id })}
              consequence={t(`orgRegions.delete.${kind}Consequence`, { id })}
              ariaLabel={t("orgRegions.remove")}
              testId={`org-${kind}-delete`}
              confirmTestId={`org-${kind}-delete-confirm`}
              onDelete={onDelete}
            />
          </Group>
        </fieldset>
      </PanelActions>
      <DetailFields testId={`org-${kind}-read-${id}`} fields={fields} />
    </>
  );
}

/** The field a refusal names, when it is one of this item's: `regions.eu.engine` → `engine`. */
const fieldOf = (
  problem: Problem | null,
  kind: "regions" | "stores",
  id: string,
): string | null => {
  const prefix = `${kind}.${id}.`;
  return problem?.field?.startsWith(prefix) ? problem.field.slice(prefix.length) : null;
};

function RegionForm({
  region,
  free,
  stores,
  onSave,
  onCancel,
}: {
  region: OrgRegion | null;
  /** The platform regions this one may be. */
  free: string[];
  stores: string[];
  onSave: (region: OrgRegion) => Promise<void>;
  onCancel: () => void;
}) {
  const { t } = useTranslation();
  const [draft, setDraft] = useState<OrgRegion>(
    region ?? { id: "", engine: "", replicas: "", views: "", cache: "", state: "", record: "" },
  );
  const [problem, setProblem] = useState<Problem | null>(null);
  const [saving, setSaving] = useState(false);
  const marked = fieldOf(problem, "regions", draft.id);
  const save = async () => {
    setProblem(null);
    setSaving(true);
    try {
      await onSave(draft);
    } catch (e) {
      setProblem(problemOf(e));
    } finally {
      setSaving(false);
    }
  };
  const blank = !draft.id || STORE_ROLES.some((role) => !draft[role]);
  const roleField = (role: StoreRole) => (
    <FormField key={role} size="medium" help={t(`orgRegions.roleHelp.${role}`)}>
      <Select
        label={t(`orgRegions.role.${role}`)}
        required
        data={stores}
        value={draft[role] || null}
        onChange={(v) => setDraft({ ...draft, [role]: v ?? "" })}
        error={marked === role ? true : undefined}
        comboboxProps={{ withinPortal: true }}
        data-testid={`org-region-${role}`}
      />
    </FormField>
  );
  return (
    <FormLayout data-testid="org-region-form">
      <FormIdentifiers>
        <FormField size="medium" help={t("orgRegions.regionHelp")}>
          <Select
            label={t("orgRegions.colRegion")}
            required
            data={free}
            value={draft.id || null}
            onChange={(v) => setDraft({ ...draft, id: v ?? "" })}
            disabled={region !== null}
            error={marked === "id" ? true : undefined}
            comboboxProps={{ withinPortal: true }}
            data-testid="org-region-id"
          />
        </FormField>
      </FormIdentifiers>
      {STORE_ROLES.map(roleField)}
      <FormFooter
        panel
        problem={problem?.text}
        saving={saving}
        onSave={save}
        onCancel={onCancel}
        saveProps={{ "data-testid": "org-region-save", disabled: blank }}
      />
    </FormLayout>
  );
}

function StoreForm({
  store,
  taken,
  kinds,
  onSave,
  onCancel,
}: {
  store: Store | null;
  taken: string[];
  kinds: string[];
  onSave: (store: Store) => Promise<void>;
  onCancel: () => void;
}) {
  const { t } = useTranslation();
  const [draft, setDraft] = useState<Store>(store ?? { id: "", url: "", kind: null });
  const [problem, setProblem] = useState<Problem | null>(null);
  const [saving, setSaving] = useState(false);
  const marked = fieldOf(problem, "stores", draft.id);
  const id = draft.id.trim();
  const save = async () => {
    setProblem(null);
    if (taken.includes(id)) {
      setProblem({ text: t("orgRegions.storeTaken", { id }), field: `stores.${id}.id` });
      return;
    }
    setSaving(true);
    try {
      // The URL as entered: a reference stays the reference.
      await onSave({ id, url: draft.url, kind: draft.kind });
    } catch (e) {
      setProblem(problemOf(e));
    } finally {
      setSaving(false);
    }
  };
  return (
    <FormLayout data-testid="org-store-form">
      <FormIdentifiers>
        <FormField size="medium" help={t("orgRegions.storeIdHelp")}>
          <TextInput
            label={t("orgRegions.colStore")}
            required
            value={draft.id}
            onChange={(e) => setDraft({ ...draft, id: e.currentTarget.value })}
            disabled={store !== null}
            error={marked === "id" ? true : undefined}
            autoComplete="off"
            data-testid="org-store-id"
          />
        </FormField>
      </FormIdentifiers>
      <FormField size="full" help={t("orgRegions.urlHelp")}>
        <TextInput
          label={t("orgRegions.colUrl")}
          required
          value={draft.url}
          onChange={(e) => setDraft({ ...draft, url: e.currentTarget.value })}
          error={marked === "url" ? true : undefined}
          autoComplete="off"
          styles={{ input: { fontFamily: "var(--mantine-font-family-monospace)" } }}
          data-testid="org-store-url"
        />
      </FormField>
      <FormField size="medium" help={t("orgRegions.kindHelp")}>
        <Select
          label={t("orgRegions.colKind")}
          data={kinds}
          value={draft.kind}
          onChange={(v) => setDraft({ ...draft, kind: v })}
          clearable
          error={marked === "kind" ? true : undefined}
          comboboxProps={{ withinPortal: true }}
          data-testid="org-store-kind"
        />
      </FormField>
      <FormFooter
        panel
        problem={problem?.text}
        saving={saving}
        onSave={save}
        onCancel={onCancel}
        saveProps={{ "data-testid": "org-store-save", disabled: !id || !draft.url.trim() }}
      />
    </FormLayout>
  );
}
