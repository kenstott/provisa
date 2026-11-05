// Copyright (c) 2026 Kenneth Stott
// Canary: 7e4a2c91-6d3b-4f58-8a07-c5b1e9d2f364
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1922: the Platform page -- the platform's regions, each an id and the address its nodes
// answer at, edited by a platform administrator. A node reads them before it opens any store, so a
// change takes effect when the nodes restart, and the page says so. An address is kept as entered:
// a reference such as ${env:NAME} stays one.

import { useCallback, useEffect, useMemo, useState, type ReactNode } from "react";
import { useTranslation } from "react-i18next";
import { ActionIcon, Alert, Button, Group, Stack, Table, Text, TextInput } from "@mantine/core";
import { Info, Pencil, TriangleAlert } from "lucide-react";
import {
  fetchPlatformRegions,
  savePlatformRegions,
  RegionsRefused,
  type PlatformRegion,
} from "../../api/regions";
import { DetailFields } from "../detail/DetailFields";
import { FormField, FormFooter, FormIdentifiers, FormLayout } from "../form/FormLayout";
import { ListEmpty, ListHead, ListItems, ListRow, ListTable } from "../list/ListTable";
import { PanelActions, SplitPane } from "../list/SplitPane";
import { PanelDelete } from "../list/PanelDelete";
import { useListSortGroup, type ListColumn } from "../list/useListSortGroup";
import { PageLoading } from "../PageLoading";
import { Tip } from "../Tip";

interface Picked {
  /** null: a new region. */
  id: string | null;
  editing: boolean;
}

export function PlatformRegionsTab() {
  const { t } = useTranslation();
  const [regions, setRegions] = useState<PlatformRegion[] | null>(null);
  const [error, setError] = useState("");
  const [picked, setPicked] = useState<Picked | null>(null);
  // Saved since the page was opened: the nodes run on the old list until they restart.
  const [changed, setChanged] = useState(false);

  const load = useCallback(() => {
    fetchPlatformRegions()
      .then(setRegions)
      .catch((e: unknown) => setError(e instanceof Error ? e.message : String(e)));
  }, []);
  useEffect(load, [load]);

  const columns: ListColumn<PlatformRegion>[] = useMemo(
    () => [
      { key: "id", label: t("platformRegions.colRegion"), sortValue: (r) => r.id },
      { key: "address", label: t("platformRegions.colAddress"), sortValue: (r) => r.address },
    ],
    [t],
  );
  const sortGroup = useListSortGroup(regions ?? [], columns, "platform-regions");

  if (error) {
    return (
      <Alert color="red" icon={<TriangleAlert size={16} />} data-testid="platform-regions-error">
        {error}
      </Alert>
    );
  }
  if (regions === null) return <PageLoading message={t("platformRegions.loading")} />;

  const commit = async (next: PlatformRegion[]) => {
    setRegions(await savePlatformRegions(next));
    setChanged(true);
  };
  const save = async (region: PlatformRegion, was: string | null) => {
    await commit([...regions.filter((r) => r.id !== (was ?? region.id)), region]);
    setPicked({ id: region.id, editing: false });
  };
  const remove = async (id: string) => {
    await commit(regions.filter((r) => r.id !== id));
    setPicked(null);
  };

  const region = picked ? regions.find((r) => r.id === picked.id) : undefined;
  let title = "";
  let panel: ReactNode = null;
  if (picked && (picked.editing || picked.id === null)) {
    title =
      picked.id === null
        ? t("platformRegions.add")
        : t("platformRegions.editTitle", { id: picked.id });
    panel = (
      <PlatformRegionForm
        key={picked.id ?? "new"}
        region={region ?? null}
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
              <PanelDelete
                itemKey={region.id}
                title={t("platformRegions.deleteTitle", { id: region.id })}
                consequence={t("platformRegions.deleteConsequence", { id: region.id })}
                ariaLabel={t("platformRegions.remove")}
                testId="platform-region-delete"
                confirmTestId="platform-region-delete-dialog"
                onDelete={() => remove(region.id)}
              />
            </Group>
          </fieldset>
        </PanelActions>
        <DetailFields
          testId={`platform-region-read-${region.id}`}
          fields={[
            { label: t("platformRegions.colRegion"), value: region.id },
            { label: t("platformRegions.colAddress"), value: region.address },
          ]}
        />
      </>
    );
  }

  return (
    <Stack gap="sm" data-testid="platform-regions">
      <Group justify="space-between" wrap="nowrap" align="flex-start">
        <Text c="dimmed" size="sm" maw={820}>
          {t("platformRegions.intro")}
        </Text>
        <Group gap="xs" wrap="nowrap" data-toolbar>
          <Button
            size="xs"
            data-testid="platform-regions-add"
            onClick={() => setPicked({ id: null, editing: true })}
          >
            {t("platformRegions.add")}
          </Button>
        </Group>
      </Group>
      <Alert
        color={changed ? "orange" : "blue"}
        icon={<Info size={16} />}
        data-testid="platform-regions-restart"
      >
        {changed ? t("platformRegions.restartPending") : t("platformRegions.restartNote")}
      </Alert>
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
          <ListTable testId="platform-regions-table">
            <ListHead sortGroup={sortGroup} columns={[{ col: "id" }, { col: "address" }]} />
            <Table.Tbody>
              {regions.length === 0 && (
                <ListEmpty colSpan={2} testId="platform-regions-empty">
                  {t("platformRegions.none")}
                </ListEmpty>
              )}
              <ListItems
                state={sortGroup}
                colSpan={2}
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
                      <Text size="xs" ff="monospace" style={{ wordBreak: "break-all" }}>
                        {r.address}
                      </Text>
                    </Table.Td>
                  </ListRow>
                )}
              />
            </Table.Tbody>
          </ListTable>
        }
      >
        {panel}
      </SplitPane>
    </Stack>
  );
}

function PlatformRegionForm({
  region,
  taken,
  onSave,
  onCancel,
}: {
  region: PlatformRegion | null;
  taken: string[];
  onSave: (region: PlatformRegion) => Promise<void>;
  onCancel: () => void;
}) {
  const { t } = useTranslation();
  const [id, setId] = useState(region?.id ?? "");
  const [address, setAddress] = useState(region?.address ?? "");
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
      // The address as entered: a reference stays the reference.
      await onSave({ id: id.trim(), address });
    } catch (e) {
      setProblem(
        e instanceof RegionsRefused
          ? { text: e.message, field: e.field?.endsWith(".address") ? "address" : "id" }
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
      <FormField size="long" help={t("platformRegions.addressHelp")}>
        <TextInput
          label={t("platformRegions.colAddress")}
          required
          value={address}
          onChange={(e) => setAddress(e.currentTarget.value)}
          error={problem?.field === "address" ? true : undefined}
          autoComplete="off"
          styles={{ input: { fontFamily: "var(--mantine-font-family-monospace)" } }}
          data-testid="platform-region-address"
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
          disabled: !id.trim() || !address.trim(),
        }}
      />
    </FormLayout>
  );
}
