// Copyright (c) 2026 Kenneth Stott
// Canary: 5e3a0c27-91f4-4d68-b2a1-7f0c9e4d6b13
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2247: the Syncing page. One list of every outside system kept in step with Provisa, under
// three headings by type (Data Catalog, Ontology, Custom); picking one opens it in the detail
// panel on the right, where it is read, edited, tested and, for a data catalog that exports,
// published now. The organisation-wide schedule of the full publish is shown once, above the list
// (REQ-1072). This page replaces the Catalogs panel and the Metadata Export screen (REQ-2122).
// REQ-2259: the list can be narrowed to the entries that publish chosen data products, from the
// record of every publish; the Data Products list links here with ?product=<id>.
// REQ-2277: the standard page anatomy -- the header (title, filter, add), the list, and the entry
// open in the panel named in the URL (?entry=<id>); the schedule is a setting, folded above the list.

import { useCallback, useEffect, useState } from "react";
import { useTranslation } from "react-i18next";
import { useSearchParams } from "react-router-dom";
import {
  Alert,
  Badge,
  Button,
  Group,
  Loader,
  Stack,
  Table,
  Text,
  TextInput,
  Title,
} from "@mantine/core";
import { Plus, RefreshCw, TriangleAlert } from "lucide-react";
import {
  blankEntry,
  checkEntry,
  duplicateEntry,
  entryUpdate,
  fetchSyncing,
  holdsCredential,
  publishEntry,
  runImport,
  setSyncing,
  SYNC_ENTRY_TYPES,
  type SyncEntry,
  type SyncEntryType,
  type SyncEntryUpdate,
  type SyncingState,
} from "../../../api/syncing";
import { useDomains } from "../../../hooks/useAdminQueries";
import { exportProviderName } from "../../../lib/exportProviders";
import { FormField, FormFooter, FormLayout } from "../../form/FormLayout";
import { ListRow } from "../../list/ListTable";
import { SortGroupTable } from "../../list/SortGroupTable";
import { SplitPane } from "../../list/SplitPane";
import { useRememberedDetail } from "../../list/useDetailParam";
import { detailListOf, filterNarrowing, searchNarrowing, shownBy } from "../../list/leftOut";
import { useUrlFilter } from "../../list/useUrlFilter";
import { CollapsibleItem, Collapsibles } from "../../collapsible/Collapsible";
import { PageTitle } from "../../PageTitle";
import { FilterInput } from "../FilterInput";
import { EntryPanel } from "./EntryPanel";
import { SyncProductFilter } from "./SyncProductFilter";
import type { Secrets } from "./EntryFields";

/** A new entry being made in the panel: empty from Add, or filled from the kept entry it
 *  duplicates. A kept entry open in the panel is the URL's ?entry=. */
type Creating = { from?: SyncEntry } | null;

export function SyncingTab() {
  const { t } = useTranslation();
  const { domains } = useDomains();
  const [s, setS] = useState<SyncingState | null>(null);
  const [error, setError] = useState("");
  const [creating, setCreating] = useState<Creating>(null);
  const [search, setSearch] = useUrlFilter();
  const [cron, setCron] = useState("");
  const [cronProblem, setCronProblem] = useState<string | null>(null);
  // REQ-2259: the data products the list is narrowed to, kept in the address so a link opens it.
  const [searchParams, setSearchParams] = useSearchParams();
  const productFilter = searchParams.getAll("product");
  const setProductFilter = (ids: string[]) =>
    setSearchParams(
      (prev) => {
        const next = new URLSearchParams(prev);
        next.delete("product");
        for (const id of ids) next.append("product", id);
        return next;
      },
      { replace: true },
    );

  // REQ-2277 rule 9: an entry is open only while the list -- narrowed to the products chosen and
  // by the search -- shows its row.
  const narrowings = [
    filterNarrowing<SyncEntry>(
      t,
      productFilter.length > 0,
      t("syncingTab.productFilterLabel"),
      (e) => e.products.some((p) => productFilter.includes(p.product_id)),
      () => setProductFilter([]),
    ),
    searchNarrowing<SyncEntry>(
      t,
      search,
      () => setSearch(""),
      (e) => [e.id, e.provider ?? "", ...e.domains],
    ),
  ];
  const detail = useRememberedDetail(
    "sync",
    detailListOf(
      t,
      s === null ? null : s.entries,
      (e) => e.id,
      (e) => e.id,
      narrowings,
    ),
  );
  const openEntry = (id: string) => {
    setCreating(null);
    detail.open(id);
  };
  const openNew = (from?: SyncEntry) => {
    detail.close();
    setCreating({ from });
  };
  const closePanel = () => {
    setCreating(null);
    detail.close();
  };

  const header = (
    <div className="page-header">
      <PageTitle icon={RefreshCw}>{t("syncingTab.title")}</PageTitle>
      <FilterInput
        value={search}
        onChange={setSearch}
        placeholder={t("syncingTab.filterPlaceholder")}
        testId="syncing-filter"
      />
      <div className="page-actions">
        {/* An entry is added to the list once it is read. */}
        {s !== null && (
          <Button
            leftSection={<Plus size={14} />}
            data-testid="syncing-add"
            onClick={() => openNew()}
          >
            {t("syncingTab.addEntry")}
          </Button>
        )}
      </div>
    </div>
  );

  const load = useCallback(async () => {
    const state = await fetchSyncing();
    setS(state);
    setCron(state.reconcile_cron);
    return state;
  }, []);

  useEffect(() => {
    const timer = window.setTimeout(() => {
      load().catch((e) => setError(String(e)));
    }, 0);
    return () => window.clearTimeout(timer);
  }, [load]);

  if (error && !s)
    return (
      <>
        {header}
        <Alert color="red" icon={<TriangleAlert size={16} />} data-testid="syncing-error">
          {error}
        </Alert>
      </>
    );

  if (!s)
    return (
      <>
        {header}
        <Group gap="xs">
          <Loader size="sm" />
          <Text>{t("syncingTab.loading")}</Text>
        </Group>
      </>
    );

  const betaProviders = new Set(s.beta_providers);
  const providerName = (p: string) => exportProviderName(p, p, t, betaProviders);
  const kept = (exceptId?: string): SyncEntryUpdate[] =>
    s.entries.filter((e) => e.id !== exceptId).map(entryUpdate);

  const saveEntry = async (draft: SyncEntry, secrets: Secrets, replacing?: string) => {
    const update: SyncEntryUpdate = { ...entryUpdate(draft), ...secrets };
    const others = kept(replacing);
    const at = replacing === undefined ? -1 : s.entries.findIndex((e) => e.id === replacing);
    const entries =
      at < 0 ? [...others, update] : [...others.slice(0, at), update, ...others.slice(at)];
    await setSyncing({ entries });
    await load();
    openEntry(draft.id);
  };

  const saveCron = async () => {
    setCronProblem(null);
    try {
      await setSyncing({ entries: kept(), reconcile_cron: cron });
      await load();
    } catch (e) {
      setCronProblem(e instanceof Error ? e.message : String(e));
    }
  };

  const selected = creating === null ? (s.entries.find((e) => e.id === detail.id) ?? null) : null;
  const fields = {
    providers: s.providers,
    providerName,
    publishableRoles: s.publishable_roles,
    domains: domains.map((d) => d.id),
  };

  // REQ-2259: every data product a recorded publish carried, and those the address names (a product
  // no publish carried yet is still shown chosen, with no entry under it); and whether an entry
  // publishes one of those the list is narrowed to.
  const productOptions = [
    ...new Set([
      ...s.entries.flatMap((e) => e.products.map((p) => p.product_id)),
      ...productFilter,
    ]),
  ].sort();
  const section = (type: SyncEntryType) => {
    const rows = shownBy(s.entries, narrowings).filter((e) => e.type === type);
    return (
      <Stack gap="xs" key={type} data-testid={`syncing-section-${type}`}>
        <Title order={5}>{t(`syncingTab.heading_${type}`)}</Title>
        <SortGroupTable
          testPrefix={`syncing-${type}`}
          minWidth={640}
          rows={rows}
          initialSortCol="name"
          columns={[
            { key: "name", label: t("syncingTab.name"), sortValue: (e) => e.id },
            {
              key: "direction",
              label: t("syncingTab.direction"),
              sortValue: (e) => e.direction,
              groupValue: (e) => t(`syncingTab.direction_${e.direction}`),
            },
            {
              key: "provider",
              label: t("syncingTab.provider"),
              sortValue: (e) => e.provider,
              groupValue: (e) => (e.provider ? providerName(e.provider) : ""),
            },
            { key: "domains", label: t("syncingTab.domains") },
            { key: "lastRun", label: t("syncingTab.lastRun") },
          ]}
          headers={[
            { col: "name" },
            { col: "direction" },
            { col: "provider" },
            { col: "domains" },
            { col: "lastRun" },
          ]}
          colSpan={5}
          empty={
            productFilter.length === 0
              ? t(`syncingTab.none_${type}`)
              : t("syncingTab.noneForProducts")
          }
          emptyTestId={`syncing-none-${type}`}
          rowKey={(e) => e.id}
          render={(e) => (
            <ListRow
              key={e.id}
              itemKey={e.id}
              testId={`syncing-row-${e.id}`}
              selected={selected?.id === e.id}
              onClick={() => (selected?.id === e.id ? closePanel() : openEntry(e.id))}
            >
              <Table.Td>{e.id}</Table.Td>
              <Table.Td>{t(`syncingTab.direction_${e.direction}`)}</Table.Td>
              <Table.Td>{e.provider ? providerName(e.provider) : "—"}</Table.Td>
              <Table.Td>{e.domains.length ? e.domains.join(", ") : "—"}</Table.Td>
              <Table.Td data-testid={`syncing-row-last-run-${e.id}`}>
                {e.last_run ? (
                  <Badge size="sm" variant="light" color={e.last_run.ok ? "green" : "red"}>
                    {e.last_run.ok
                      ? t("syncingTab.publishComplete", { count: e.last_run.total_published })
                      : t("syncingTab.publishPartial", {
                          count: e.last_run.total_published,
                          failed: e.last_run.errors.length,
                        })}
                  </Badge>
                ) : (
                  "—"
                )}
              </Table.Td>
            </ListRow>
          )}
        />
      </Stack>
    );
  };

  const panelTitle = creating !== null ? t("syncingTab.newEntry") : (selected?.id ?? "");
  const duplicated = creating?.from;

  return (
    <Stack gap="md" style={{ flex: 1, minHeight: 0 }} data-tour="syncing" data-testid="syncing">
      {header}
      <Text c="dimmed" size="sm">
        {t("syncingTab.intro")}
      </Text>
      {!s.entitled && (
        <Alert
          color="yellow"
          icon={<TriangleAlert size={16} />}
          data-testid="syncing-not-entitled"
          title={t("syncingTab.notEntitledTitle")}
        >
          {t("syncingTab.notEntitled", { tier: s.required_tier })}
        </Alert>
      )}
      <Collapsibles id="context.sync.settings" testId="syncing-settings">
        <CollapsibleItem value="schedule" defaultOpen={false} title={t("syncingTab.reconcileCron")}>
          <FormLayout data-testid="syncing-schedule">
            <FormField help={t("syncingTab.reconcileCronHelp")} size="medium">
              <TextInput
                label={t("syncingTab.reconcileCron")}
                data-testid="syncing-reconcile-cron"
                value={cron}
                disabled={!s.entitled}
                onChange={(e) => setCron(e.currentTarget.value)}
              />
            </FormField>
            <FormFooter
              problem={cronProblem ?? undefined}
              onSave={saveCron}
              saveProps={{
                "data-testid": "syncing-reconcile-save",
                disabled: !s.entitled || cron === s.reconcile_cron,
              }}
            />
          </FormLayout>
        </CollapsibleItem>
      </Collapsibles>
      <SyncProductFilter
        products={productOptions}
        value={productFilter}
        onChange={setProductFilter}
      />
      <SplitPane
        outOfList={detail.outOfList}
        testId="syncing-split"
        storageKey="provisa.syncing.detailWidth"
        title={panelTitle}
        onClose={closePanel}
        labels={{
          panel: t("syncingTab.panel"),
          resize: t("syncingTab.resizePanel"),
          expand: t("syncingTab.expandPanel"),
          restore: t("syncingTab.restorePanel"),
          close: t("syncingTab.closePanel"),
          backToList: t("syncingTab.backToList"),
        }}
        list={<Stack gap="lg">{SYNC_ENTRY_TYPES.map(section)}</Stack>}
      >
        {creating !== null ? (
          <EntryPanel
            // A duplicate opens a fresh form, never the state of an Add left open.
            key={duplicated ? `new:${duplicated.id}` : "new"}
            entry={duplicated ? duplicateEntry(duplicated) : blankEntry("data_catalog")}
            note={
              duplicated && holdsCredential(duplicated)
                ? t("syncingTab.duplicateCredentialsNote", { id: duplicated.id })
                : undefined
            }
            isNew
            entitled={s.entitled}
            fields={fields}
            onSave={(draft, secrets) => saveEntry(draft, secrets)}
            onCancelNew={closePanel}
            onDelete={async () => undefined}
            onTest={() => checkEntry("")}
            onPublish={async () => undefined}
            onRunImport={async () => undefined}
            onDuplicate={() => undefined}
          />
        ) : selected ? (
          <EntryPanel
            key={`${selected.id}:${JSON.stringify(selected.last_run)}`}
            entry={selected}
            isNew={false}
            entitled={s.entitled}
            fields={fields}
            onSave={(draft, secrets) => saveEntry(draft, secrets, selected.id)}
            onCancelNew={closePanel}
            onDelete={async () => {
              await setSyncing({ entries: kept(selected.id) });
              closePanel();
              await load();
            }}
            onTest={() => checkEntry(selected.id)}
            onPublish={async () => {
              const result = await publishEntry(selected.id);
              await load();
              return result.entries[0];
            }}
            onRunImport={async () => {
              // A failed run is recorded too, so the list is read again either way.
              try {
                await runImport(selected.id);
              } finally {
                await load();
              }
            }}
            onDuplicate={() => openNew(selected)}
          />
        ) : null}
      </SplitPane>
    </Stack>
  );
}
