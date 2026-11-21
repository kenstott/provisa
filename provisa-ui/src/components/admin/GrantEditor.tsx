// Copyright (c) 2026 Kenneth Stott
// Canary: a85a352f-9810-4dcf-be9e-88cf35562b57
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1922: an org's grant -- the platform regions it is granted, with the store it keeps each kind
// of data in there -- as two lists and one detail panel. The platform administrator edits it (the
// Platform page, per org, and the default grant); the org administrator reads it (Admin > Regions).
// Every save sends the whole set, which the server validates as it validates a configuration and
// refuses naming the field; the panel says why and marks the field. A store URL is kept as entered
// -- a reference such as ${env:NAME} stays one -- and comes back without its password.
//
// A grant with no region yet cannot keep a store on its own (the set would select none): a store
// added then is held on the page and saved with the first region.

import { useMemo, useState, type ReactNode } from "react";
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
import { Pencil } from "lucide-react";
import {
  problemOf,
  STORE_ROLES,
  type Problem,
  type Grant,
  type GrantView,
  type OrgRegion,
  type Store,
  type StoreRole,
} from "../../api/regions";
import { DetailFields } from "../detail/DetailFields";
import { FormField, FormFooter, FormIdentifiers, FormLayout } from "../form/FormLayout";
import { ListEmpty, ListHead, ListItems, ListRow, ListTable } from "../list/ListTable";
import { PanelActions, SplitPane } from "../list/SplitPane";
import { PanelDelete } from "../list/PanelDelete";
import { PanelDuplicate } from "../list/PanelDuplicate";
import { isReference } from "../../pages/sources/sourceDuplicate";
import { useListSortGroup, type ListColumn } from "../list/useListSortGroup";
import { Tip } from "../Tip";
import { PurposeBadges, PurposesLegend } from "./RegionPurposes";

type Kind = "region" | "store";
/** What the panel shows: an item, or a new one (`id` null); `editing` for its form. */
interface Picked {
  kind: Kind;
  id: string | null;
  editing: boolean;
  /** With id null: the id of the item the new form was opened from by Duplicate. */
  seed?: string;
}

/** A store URL that holds a secret of its own: a password in its user info or its query. A
 * reference (`${provider:name}`) held whole is not one. */
const URL_SECRET = /\/\/[^/@\s]*:[^/@\s]*@|[?&;](password|passwd|pwd|secret|token|api_?key)=/i;
const urlHoldsSecret = (url: string) => !isReference(url) && URL_SECRET.test(url);

const itemKey = (kind: Kind, id: string) => `${kind}:${id}`;

export interface GrantEditorProps {
  /** The grant as held, with the platform's regions and the engine kinds a store may name. */
  data: GrantView;
  /** Save the whole set; ``confirmRemoval`` when the operator confirmed removing a region. Absent,
   * the grant is shown read-only. */
  onSave?: (grant: Grant, confirmRemoval: boolean) => Promise<Grant>;
  /** Where the detail panel's width is remembered. */
  storageKey: string;
}

export function GrantEditor({ data: given, onSave, storageKey }: GrantEditorProps) {
  const { t } = useTranslation();
  const [data, setData] = useState<GrantView>(given);
  const [picked, setPicked] = useState<Picked | null>(null);
  // Stores added while the grant has no region: saved with its first region.
  const [held, setHeld] = useState<Store[]>([]);
  const readOnly = onSave === undefined;

  const stores = useMemo(() => [...data.stores, ...held], [data, held]);
  const regions = useMemo(() => data.regions, [data]);
  const platformRegion = (id: string) => data.platform_regions.find((p) => p.id === id);

  const regionColumns: ListColumn<OrgRegion>[] = useMemo(
    () => [
      { key: "id", label: t("orgRegions.colRegion"), sortValue: (r) => r.id },
      {
        key: "purposes",
        label: t("orgRegions.colPurposes"),
        sortValue: (r) => (data.platform_regions.find((p) => p.id === r.id)?.purposes ?? []).join(),
      },
      ...STORE_ROLES.map((role) => ({
        key: role,
        label: t(`orgRegions.role.${role}`),
        sortValue: (r: OrgRegion) => r[role],
      })),
    ],
    [t, data.platform_regions],
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

  /** Save the set with this change; the page then shows what the server kept. */
  const commit = async (nextStores: Store[], nextRegions: OrgRegion[], confirm = false) => {
    if (onSave === undefined) return;
    const saved = await onSave({ stores: nextStores, regions: nextRegions }, confirm);
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
    // The delete dialog said whom removing the region affects: confirmed there.
    await commit(
      stores,
      regions.filter((r) => r.id !== id),
      true,
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
  if (!readOnly && picked?.kind === "region" && (picked.editing || picked.id === null)) {
    title =
      picked.id === null
        ? t("orgRegions.addRegion")
        : t("orgRegions.editRegion", { id: picked.id });
    panel = (
      <RegionForm
        key={picked.id ?? `new:${picked.seed ?? ""}`}
        region={region ?? null}
        seed={picked.id === null ? regions.find((r) => r.id === picked.seed) : undefined}
        free={data.platform_regions
          .map((p) => p.id)
          .filter((id) => id === picked.id || !regions.some((r) => r.id === id))}
        stores={stores.map((s) => s.id)}
        onSave={(r) => saveRegion(r, picked.id)}
        onCancel={() => (picked.id === null ? close() : setPicked({ ...picked, editing: false }))}
      />
    );
  } else if (!readOnly && picked?.kind === "store" && (picked.editing || picked.id === null)) {
    title =
      picked.id === null ? t("orgRegions.addStore") : t("orgRegions.editStore", { id: picked.id });
    panel = (
      <StoreForm
        key={picked.id ?? `new:${picked.seed ?? ""}`}
        store={store ?? null}
        seed={picked.id === null ? stores.find((x) => x.id === picked.seed) : undefined}
        taken={stores.map((s) => s.id).filter((id) => id !== picked.id)}
        kinds={data.engine_kinds}
        onSave={(s) => saveStore(s, picked.id)}
        onCancel={() => (picked.id === null ? close() : setPicked({ ...picked, editing: false }))}
      />
    );
  } else if (region) {
    title = region.id;
    const declared = platformRegion(region.id);
    panel = (
      <ItemRead
        kind="region"
        id={region.id}
        readOnly={readOnly}
        onEdit={() => setPicked({ kind: "region", id: region.id, editing: true })}
        onDuplicate={() => setPicked({ kind: "region", id: null, editing: true, seed: region.id })}
        // A grant holds each platform region once: with none left, there is nothing to copy to.
        canDuplicate={data.platform_regions.some((p) => !regions.some((r) => r.id === p.id))}
        onDelete={() => removeRegion(region.id)}
        fields={[
          { label: t("orgRegions.colRegion"), value: region.id },
          {
            label: t("orgRegions.description"),
            value: declared?.description ?? "",
            testId: "org-region-read-description",
          },
          {
            label: t("orgRegions.colPurposes"),
            value: <PurposeBadges purposes={declared?.purposes ?? []} />,
          },
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
        readOnly={readOnly}
        onEdit={() => setPicked({ kind: "store", id: store.id, editing: true })}
        onDuplicate={() => setPicked({ kind: "store", id: null, editing: true, seed: store.id })}
        canDuplicate
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
      {!readOnly && (
        <Group justify="flex-end" wrap="nowrap" align="flex-start">
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
      )}
      {held.length > 0 && (
        <Alert color="yellow" data-testid="org-regions-held">
          {t("orgRegions.heldStores", { count: held.length })}
        </Alert>
      )}
      <SplitPane
        testId="org-regions-split"
        storageKey={storageKey}
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
                columns={[
                  { col: "id" },
                  { col: "purposes" },
                  ...STORE_ROLES.map((role) => ({ col: role })),
                ]}
              />
              <Table.Tbody>
                {regions.length === 0 && (
                  <ListEmpty colSpan={8} testId="org-regions-empty">
                    {t("orgRegions.noRegions")}
                  </ListEmpty>
                )}
                <ListItems
                  state={regionSort}
                  colSpan={8}
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
                      <Table.Td>
                        <PurposeBadges purposes={platformRegion(r.id)?.purposes ?? []} />
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
            <PurposesLegend />
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
  readOnly,
  onEdit,
  onDuplicate,
  canDuplicate,
  onDelete,
}: {
  kind: Kind;
  id: string;
  fields: { label: string; value: ReactNode; testId?: string }[];
  readOnly: boolean;
  onEdit: () => void;
  /** Opens the new-item form filled from this one. */
  onDuplicate: () => void;
  canDuplicate: boolean;
  onDelete: () => Promise<void>;
}) {
  const { t } = useTranslation();
  return (
    <>
      {!readOnly && (
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
              <PanelDuplicate
                name={id}
                onDuplicate={onDuplicate}
                disabled={!canDuplicate}
                testId={`org-${kind}-duplicate`}
              />
              <PanelDelete
                itemKey={itemKey(kind, id)}
                title={t(`orgRegions.delete.${kind}Title`, { id })}
                consequence={t(`orgRegions.delete.${kind}Consequence`, { id })}
                ariaLabel={t("orgRegions.remove")}
                testId={`org-${kind}-delete`}
                confirmTestId={`org-${kind}-delete-dialog`}
                onDelete={onDelete}
              />
            </Group>
          </fieldset>
        </PanelActions>
      )}
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
  seed,
  free,
  stores,
  onSave,
  onCancel,
}: {
  region: OrgRegion | null;
  /** A duplicate's starting stores: the region it is made from; its own region is picked. */
  seed?: OrgRegion;
  /** The platform regions this one may be. */
  free: string[];
  stores: string[];
  onSave: (region: OrgRegion) => Promise<void>;
  onCancel: () => void;
}) {
  const { t } = useTranslation();
  const [draft, setDraft] = useState<OrgRegion>(
    region ??
      (seed
        ? { ...seed, id: "" }
        : { id: "", engine: "", replicas: "", views: "", cache: "", state: "", record: "" }),
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
  seed,
  taken,
  kinds,
  onSave,
  onCancel,
}: {
  store: Store | null;
  /** A duplicate's starting store: its kind and URL (unless the URL holds a secret), a new id. */
  seed?: Store;
  taken: string[];
  kinds: string[];
  onSave: (store: Store) => Promise<void>;
  onCancel: () => void;
}) {
  const { t } = useTranslation();
  // A URL that holds a password or a token of its own is not copied: it is entered again.
  const urlLeft = seed !== undefined && urlHoldsSecret(seed.url);
  const [draft, setDraft] = useState<Store>(
    store ??
      (seed
        ? { id: `${seed.id}-copy`, url: urlLeft ? "" : seed.url, kind: seed.kind }
        : { id: "", url: "", kind: null }),
  );
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
      <FormField size="long" help={t("orgRegions.urlHelp")}>
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
      {urlLeft && (
        <FormField size="full">
          <Alert
            color="yellow"
            variant="light"
            py={4}
            px="sm"
            data-testid="org-store-duplicate-note"
          >
            {t("sourcesPage.duplicateCredentialsNote", { source: seed.id })}
          </Alert>
        </FormField>
      )}
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
