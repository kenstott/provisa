// Copyright (c) 2026 Kenneth Stott
// Canary: 7e4a2c91-6d3b-4f58-8a07-c5b1e9d2f364
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1412: where the commercial plugin is installed, a further section sets each org's engine lane.
// REQ-1922: the Platform page, a platform administrator's. Four sections: the platform's regions
// (each an id, a description, the purposes it serves and its data addresses), the grant every new
// org receives, each org's grant (the regions it is granted and its stores in each), and the
// deployment's Redis and materialize-store connections (REQ-1327). A node
// reads the platform's regions before it opens any store, and binds an org's region lane when it
// builds the org, so every change here applies when the nodes restart; the page says so. Removing
// a region or a grant orgs hold lists the orgs it affects and is done only once confirmed.
//
// In a demo sandbox the platform's regions are shown read-only, with a note that they are shown to
// explain regions and cannot be changed there.

import { DestructiveConfirmButton } from "../form/DestructiveConfirmButton";
import { useCallback, useEffect, useMemo, useState, type ReactNode } from "react";
import { useTranslation } from "react-i18next";
import {
  ActionIcon,
  Alert,
  Button,
  Checkbox,
  Group,
  Select,
  Stack,
  Table,
  TagsInput,
  Tabs,
  Text,
  Textarea,
  TextInput,
} from "@mantine/core";
import { Info, Pencil, TriangleAlert } from "lucide-react";
import {
  fetchOrgGrant,
  fetchOrgGrants,
  fetchPlatformRegions,
  problemOf,
  REGION_PURPOSES,
  RegionsRefused,
  saveOrgGrant,
  savePlatformRegions,
  type Grant,
  type GrantView,
  type OrgGrantSummary,
  type PlatformRegion,
  type PlatformRegions,
  type RegionPurpose,
} from "../../api/regions";
import { useAuth } from "../../context/AuthContext";
import { DetailFields } from "../detail/DetailFields";
import { FormField, FormFooter, FormIdentifiers, FormLayout } from "../form/FormLayout";
import { ListEmpty, ListHead, ListItems, ListRow, ListTable } from "../list/ListTable";
import { PanelActions, SplitPane } from "../list/SplitPane";
import { PanelDelete } from "../list/PanelDelete";
import { PanelDuplicate } from "../list/PanelDuplicate";
import { useListSortGroup, type ListColumn } from "../list/useListSortGroup";
import { PageLoading } from "../PageLoading";
import { Tip } from "../Tip";
import { HotCacheStoreSettingsPanel, MaterializedSettingsPanel } from "./CacheStorageTab";
import { GrantEditor } from "./GrantEditor";
import { PlatformOrgEngines } from "./PlatformOrgEngine";
import { PurposeBadges, PurposesLegend } from "./RegionPurposes";

interface Picked {
  /** null: a new region. */
  id: string | null;
  editing: boolean;
  /** With id null: the region the new-region form was opened from by Duplicate. */
  seed?: PlatformRegion;
}

/** The id a duplicate of a region is offered under. A region's id is `[a-z][a-z0-9]{1,39}`: no
 * separator is accepted, so the suffix is joined to it. */
const duplicateRegionId = (id: string) => `${id.slice(0, 36)}copy`;

/** A removal the server refused because orgs hold the region: confirmed, it is made anyway. */
interface Affected {
  id: string;
  orgs: string[];
}

export function PlatformRegionsTab() {
  const { t } = useTranslation();
  const [data, setData] = useState<PlatformRegions | null>(null);
  const [error, setError] = useState("");
  // Saved since the page was opened: the nodes run on what they read until they restart.
  const [changed, setChanged] = useState(false);
  const [section, setSection] = useState<string | null>("regions");
  // REQ-1412: the engine lane is the commercial plugin's route, so it is offered only with it.
  const { billing } = useAuth();

  useEffect(() => {
    fetchPlatformRegions()
      .then(setData)
      .catch((e: unknown) => setError(problemOf(e).text));
  }, []);

  if (error) {
    return (
      <Alert color="red" icon={<TriangleAlert size={16} />} data-testid="platform-regions-error">
        {error}
      </Alert>
    );
  }
  if (data === null) return <PageLoading message={t("platformRegions.loading")} />;

  const saved = (next: PlatformRegions) => {
    setData(next);
    setChanged(true);
  };

  if (data.read_only) {
    return (
      <Stack gap="sm" data-testid="platform-regions">
        <Alert color="blue" icon={<Info size={16} />} data-testid="platform-regions-sandbox-note">
          {t("platformRegions.sandboxNote")}
        </Alert>
        <RegionsSection data={data} onSaved={saved} />
      </Stack>
    );
  }

  return (
    <Stack gap="sm" data-testid="platform-regions">
      {/* An engine lane applies at once (the org's runtime is rebuilt), so the note is not for it. */}
      {section !== "engines" && (
        <Alert
          color={changed ? "orange" : "blue"}
          icon={<Info size={16} />}
          data-testid="platform-regions-restart"
        >
          {changed ? t("platformRegions.restartPending") : t("platformRegions.restartNote")}
        </Alert>
      )}
      <Tabs value={section} onChange={setSection} keepMounted={false}>
        <Tabs.List mb="md">
          <Tabs.Tab value="regions" data-testid="platform-section-regions">
            {t("platformRegions.sectionRegions")}
          </Tabs.Tab>
          <Tabs.Tab value="default" data-testid="platform-section-default">
            {t("platformRegions.sectionDefaultGrant")}
          </Tabs.Tab>
          <Tabs.Tab value="orgs" data-testid="platform-section-orgs">
            {t("platformRegions.sectionOrgs")}
          </Tabs.Tab>
          <Tabs.Tab value="stores" data-testid="platform-section-stores">
            {t("platformRegions.sectionStores")}
          </Tabs.Tab>
          {billing && (
            <Tabs.Tab value="engines" data-testid="platform-section-engines">
              {t("platformRegions.sectionEngines")}
            </Tabs.Tab>
          )}
        </Tabs.List>
        <Tabs.Panel value="regions">
          <RegionsSection data={data} onSaved={saved} />
        </Tabs.Panel>
        <Tabs.Panel value="default">
          <Stack gap="sm">
            <Text c="dimmed" size="sm" maw={820}>
              {t("platformRegions.defaultGrantIntro")}
            </Text>
            <GrantEditor
              data={{
                platform_regions: data.regions,
                engine_kinds: data.engine_kinds,
                ...data.default_grant,
              }}
              storageKey="provisa.platformDefaultGrant.detailWidth"
              onSave={async (grant: Grant) => {
                const next = await savePlatformRegions(data.regions, grant, false);
                saved(next);
                return next.default_grant;
              }}
            />
          </Stack>
        </Tabs.Panel>
        <Tabs.Panel value="orgs">
          <OrgsSection onSaved={() => setChanged(true)} />
        </Tabs.Panel>
        <Tabs.Panel value="stores">
          <Stack gap="sm">
            <HotCacheStoreSettingsPanel />
            <MaterializedSettingsPanel />
          </Stack>
        </Tabs.Panel>
        {billing && (
          <Tabs.Panel value="engines">
            <PlatformOrgEngines />
          </Tabs.Panel>
        )}
      </Tabs>
    </Stack>
  );
}

/** The platform's regions: a list and its detail panel; read-only in a demo sandbox. */
function RegionsSection({
  data,
  onSaved,
}: {
  data: PlatformRegions;
  onSaved: (next: PlatformRegions) => void;
}) {
  const { t } = useTranslation();
  const [picked, setPicked] = useState<Picked | null>(null);
  const [affected, setAffected] = useState<Affected | null>(null);
  const regions = data.regions;
  const readOnly = data.read_only;

  const columns: ListColumn<PlatformRegion>[] = useMemo(
    () => [
      { key: "id", label: t("platformRegions.colRegion"), sortValue: (r) => r.id },
      {
        key: "description",
        label: t("platformRegions.colDescription"),
        sortValue: (r) => r.description,
      },
      {
        key: "purposes",
        label: t("platformRegions.colPurposes"),
        sortValue: (r) => r.purposes.join(),
      },
    ],
    [t],
  );
  const sortGroup = useListSortGroup(regions, columns, "platform-regions");

  const commit = async (next: PlatformRegion[], confirm: boolean) => {
    onSaved(await savePlatformRegions(next, data.default_grant, confirm));
  };
  const save = async (region: PlatformRegion, was: string | null) => {
    await commit([...regions.filter((r) => r.id !== (was ?? region.id)), region], false);
    setPicked({ id: region.id, editing: false });
  };
  const remove = async (id: string, confirm: boolean) => {
    try {
      await commit(
        regions.filter((r) => r.id !== id),
        confirm,
      );
    } catch (e) {
      // Orgs are granted it: the page lists them, and removes it only once that is confirmed.
      if (e instanceof RegionsRefused && e.affectedOrgs) {
        setAffected({ id, orgs: e.affectedOrgs });
        return;
      }
      throw e;
    }
    setAffected(null);
    setPicked(null);
  };

  const region = picked ? regions.find((r) => r.id === picked.id) : undefined;
  let title = "";
  let panel: ReactNode = null;
  if (!readOnly && picked && (picked.editing || picked.id === null)) {
    title =
      picked.id === null
        ? t("platformRegions.add")
        : t("platformRegions.editTitle", { id: picked.id });
    panel = (
      <PlatformRegionForm
        key={picked.id ?? `new:${picked.seed?.id ?? ""}`}
        region={region ?? null}
        seed={picked.id === null ? picked.seed : undefined}
        taken={regions.map((r) => r.id).filter((id) => id !== picked.id)}
        onSave={(r) => save(r, picked.id)}
        onCancel={() =>
          picked.id === null ? setPicked(null) : setPicked({ ...picked, editing: false })
        }
      />
    );
  } else if (region) {
    title = region.id;
    panel = (
      <>
        {!readOnly && (
          <PanelActions>
            <fieldset className="panel-action-set">
              <Group gap="xs">
                <Tip text={t("platformRegions.edit")} named>
                  <ActionIcon
                    aria-label={t("platformRegions.edit")}
                    data-testid="platform-region-edit"
                    onClick={() => setPicked({ id: region.id, editing: true })}
                  >
                    <Pencil size={14} />
                  </ActionIcon>
                </Tip>
                <PanelDuplicate
                  name={region.id}
                  onDuplicate={() => setPicked({ id: null, editing: true, seed: region })}
                  testId="platform-region-duplicate"
                />
                <PanelDelete
                  itemKey={region.id}
                  title={t("platformRegions.deleteTitle", { id: region.id })}
                  consequence={t("platformRegions.deleteConsequence", { id: region.id })}
                  ariaLabel={t("platformRegions.remove")}
                  testId="platform-region-delete"
                  confirmTestId="platform-region-delete-dialog"
                  onDelete={() => remove(region.id, false)}
                />
              </Group>
            </fieldset>
          </PanelActions>
        )}
        <DetailFields
          testId={`platform-region-read-${region.id}`}
          fields={[
            { label: t("platformRegions.colRegion"), value: region.id },
            {
              label: t("platformRegions.colDescription"),
              value: region.description,
              testId: "platform-region-read-description",
            },
            {
              label: t("platformRegions.colPurposes"),
              value: <PurposeBadges purposes={region.purposes} />,
            },
            {
              label: t("platformRegions.dataAddresses"),
              value: region.data_addresses.length
                ? region.data_addresses.join(", ")
                : t("platformRegions.noDataAddresses"),
              testId: "platform-region-read-data-addresses",
            },
          ]}
        />
      </>
    );
  }

  return (
    <Stack gap="sm">
      <Group justify="space-between" wrap="nowrap" align="flex-start">
        <Text c="dimmed" size="sm" maw={820}>
          {t("platformRegions.intro")}
        </Text>
        {!readOnly && (
          <Group gap="xs" wrap="nowrap" data-toolbar>
            <Button
              size="xs"
              data-testid="platform-regions-add"
              onClick={() => setPicked({ id: null, editing: true })}
            >
              {t("platformRegions.add")}
            </Button>
          </Group>
        )}
      </Group>
      {affected !== null && (
        <AffectedOrgs
          region={affected.id}
          orgs={affected.orgs}
          onConfirm={() => remove(affected.id, true)}
          onCancel={() => setAffected(null)}
        />
      )}
      <SplitPane
        testId="platform-regions-split"
        storageKey="provisa.platformRegions.detailWidth"
        title={title}
        onClose={() => setPicked(null)}
        closeOnEscape={!picked?.editing}
        labels={{
          panel: t("platformRegions.detailPanel"),
          resize: t("tablesPage.resizeDetail"),
          expand: t("tablesPage.expandDetail"),
          restore: t("tablesPage.restoreDetail"),
          close: t("tablesPage.closeDetail"),
          backToList: t("tablesPage.backToList"),
        }}
        list={
          <Stack gap="md">
            <ListTable testId="platform-regions-table">
              <ListHead
                sortGroup={sortGroup}
                columns={[{ col: "id" }, { col: "description" }, { col: "purposes" }]}
              />
              <Table.Tbody>
                {regions.length === 0 && (
                  <ListEmpty colSpan={3} testId="platform-regions-empty">
                    {t("platformRegions.none")}
                  </ListEmpty>
                )}
                <ListItems
                  state={sortGroup}
                  colSpan={3}
                  rowKey={(r) => r.id}
                  render={(r) => (
                    <ListRow
                      key={r.id}
                      itemKey={r.id}
                      testId={`platform-region-row-${r.id}`}
                      selected={picked?.id === r.id}
                      onClick={() =>
                        setPicked(picked?.id === r.id ? null : { id: r.id, editing: false })
                      }
                    >
                      <Table.Td>
                        <Text size="sm" fw={600}>
                          {r.id}
                        </Text>
                      </Table.Td>
                      <Table.Td>
                        <Text size="xs">{r.description}</Text>
                      </Table.Td>
                      <Table.Td>
                        <PurposeBadges purposes={r.purposes} />
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

/** Removing a region orgs are granted: the orgs, and a confirmation to remove it anyway. */
function AffectedOrgs({
  region,
  orgs,
  onConfirm,
  onCancel,
}: {
  region: string;
  orgs: string[];
  onConfirm: () => Promise<void>;
  onCancel: () => void;
}) {
  const { t } = useTranslation();
  const [working, setWorking] = useState(false);
  return (
    <Alert color="orange" icon={<TriangleAlert size={16} />} data-testid="platform-region-affected">
      <Stack gap="xs">
        <Text size="sm">
          {t("platformRegions.affectedOrgs", { id: region, orgs: orgs.join(", ") })}
        </Text>
        <Group gap="xs">
          <DestructiveConfirmButton
            loading={working}
            testId="platform-region-remove-anyway"
            onClick={() => {
              setWorking(true);
              onConfirm().finally(() => setWorking(false));
            }}
          >
            {t("platformRegions.removeAnyway")}
          </DestructiveConfirmButton>
          <Button size="xs" variant="default" onClick={onCancel}>
            {t("platformRegions.keep")}
          </Button>
        </Group>
      </Stack>
    </Alert>
  );
}

/** Each org's grant: pick the org, then its regions and stores as the grant editor shows them. */
function OrgsSection({ onSaved }: { onSaved: () => void }) {
  const { t } = useTranslation();
  const [orgs, setOrgs] = useState<OrgGrantSummary[] | null>(null);
  const [orgId, setOrgId] = useState<string | null>(null);
  const [grant, setGrant] = useState<GrantView | null>(null);
  const [error, setError] = useState("");

  const loadOrgs = useCallback(() => {
    fetchOrgGrants()
      .then(setOrgs)
      .catch((e: unknown) => setError(problemOf(e).text));
  }, []);
  useEffect(loadOrgs, [loadOrgs]);
  useEffect(() => {
    if (orgId === null) return;
    fetchOrgGrant(orgId)
      .then(setGrant)
      .catch((e: unknown) => setError(problemOf(e).text));
  }, [orgId]);

  if (error) {
    return (
      <Alert color="red" icon={<TriangleAlert size={16} />} data-testid="platform-orgs-error">
        {error}
      </Alert>
    );
  }
  if (orgs === null) return <PageLoading message={t("platformRegions.loadingOrgs")} />;
  return (
    <Stack gap="sm" data-testid="platform-orgs">
      <Text c="dimmed" size="sm" maw={820}>
        {t("platformRegions.orgsIntro")}
      </Text>
      <FormLayout data-testid="platform-org-form">
        <FormField size="medium">
          <Select
            label={t("platformRegions.org")}
            placeholder={t("platformRegions.pickOrg")}
            data={orgs.map((o) => ({
              value: o.id,
              label: o.regions.length
                ? t("platformRegions.orgGranted", { id: o.id, regions: o.regions.join(", ") })
                : t("platformRegions.orgNone", { id: o.id }),
            }))}
            value={orgId}
            onChange={(next) => {
              setGrant(null);
              setOrgId(next);
            }}
            searchable
            comboboxProps={{ withinPortal: true }}
            data-testid="platform-org-pick"
          />
        </FormField>
      </FormLayout>
      {orgId !== null && grant === null && (
        <PageLoading message={t("platformRegions.loadingGrant")} />
      )}
      {orgId !== null && grant !== null && (
        <GrantEditor
          key={orgId}
          data={grant}
          storageKey="provisa.platformOrgGrant.detailWidth"
          onSave={async (next, confirm) => {
            const kept = await saveOrgGrant(orgId, next, confirm);
            onSaved();
            loadOrgs();
            return kept;
          }}
        />
      )}
    </Stack>
  );
}

function PlatformRegionForm({
  region,
  seed,
  taken,
  onSave,
  onCancel,
}: {
  region: PlatformRegion | null;
  /** A duplicate's starting region: its description, purposes and data addresses, under a new id. */
  seed?: PlatformRegion;
  taken: string[];
  onSave: (region: PlatformRegion) => Promise<void>;
  onCancel: () => void;
}) {
  const { t } = useTranslation();
  const start = region ?? seed;
  const [id, setId] = useState(region ? region.id : seed ? duplicateRegionId(seed.id) : "");
  const [description, setDescription] = useState(start?.description ?? "");
  const [purposes, setPurposes] = useState<RegionPurpose[]>(start?.purposes ?? []);
  const [dataAddresses, setDataAddresses] = useState<string[]>(start?.data_addresses ?? []);
  const [problem, setProblem] = useState<{ text: string; field: string | null } | null>(null);
  const [saving, setSaving] = useState(false);
  const save = async () => {
    setProblem(null);
    if (taken.includes(id.trim())) {
      setProblem({ text: t("platformRegions.taken", { id: id.trim() }), field: "id" });
      return;
    }
    setSaving(true);
    try {
      await onSave({
        id: id.trim(),
        description: description.trim(),
        purposes,
        data_addresses: dataAddresses,
      });
    } catch (e) {
      // The refusal names the field: regions.<index>.<field>.
      setProblem(
        e instanceof RegionsRefused
          ? { text: e.message, field: e.field?.split(".").slice(2).join(".") || null }
          : { text: e instanceof Error ? e.message : String(e), field: null },
      );
    } finally {
      setSaving(false);
    }
  };
  return (
    <FormLayout data-testid="platform-region-form">
      <FormIdentifiers>
        <FormField size="medium" help={t("platformRegions.idHelp")}>
          <TextInput
            label={t("platformRegions.colRegion")}
            required
            value={id}
            onChange={(e) => setId(e.currentTarget.value)}
            disabled={region !== null}
            error={problem?.field === "id" ? true : undefined}
            autoComplete="off"
            data-testid="platform-region-id"
          />
        </FormField>
      </FormIdentifiers>
      <FormField size="long" help={t("platformRegions.descriptionHelp")}>
        <Textarea
          label={t("platformRegions.colDescription")}
          required
          autosize
          minRows={2}
          value={description}
          onChange={(e) => setDescription(e.currentTarget.value)}
          error={problem?.field === "description" ? true : undefined}
          data-testid="platform-region-description"
        />
      </FormField>
      <FormField size="long" help={t("platformRegions.purposesHelp")}>
        <Checkbox.Group
          label={t("platformRegions.colPurposes")}
          withAsterisk
          value={purposes}
          onChange={(v) => setPurposes(REGION_PURPOSES.filter((p) => v.includes(p)))}
          error={problem?.field === "purposes" ? true : undefined}
          data-testid="platform-region-purposes"
        >
          <Stack gap={6} mt={4}>
            {REGION_PURPOSES.map((p) => (
              <Checkbox
                key={p}
                value={p}
                label={
                  <Tip text={t(`regionPurposes.explain.${p}`)}>
                    <span>{t(`regionPurposes.name.${p}`)}</span>
                  </Tip>
                }
                data-testid={`platform-region-purpose-${p}`}
              />
            ))}
          </Stack>
        </Checkbox.Group>
      </FormField>
      <FormField size="long" help={t("platformRegions.dataAddressesHelp")}>
        <TagsInput
          label={t("platformRegions.dataAddresses")}
          value={dataAddresses}
          onChange={setDataAddresses}
          error={problem?.field === "data_addresses" ? true : undefined}
          data-testid="platform-region-data-addresses"
        />
      </FormField>
      <FormFooter
        panel
        problem={problem?.text}
        saving={saving}
        onSave={save}
        onCancel={onCancel}
        saveProps={{
          "data-testid": "platform-region-save",
          disabled: !id.trim() || !description.trim() || purposes.length === 0,
        }}
      />
    </FormLayout>
  );
}
