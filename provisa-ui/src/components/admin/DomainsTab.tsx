// Copyright (c) 2026 Kenneth Stott
// Canary: 7bda817f-fc9f-4741-ac6d-1baad6e30944
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Admin › Domains: the org's domains, and the settings whose subject they are.

import { useCallback, useEffect, useMemo, useState } from "react";
import { useTranslation } from "react-i18next";
import {
  Alert,
  Button,
  Group,
  Pagination,
  Select,
  SimpleGrid,
  Stack,
  Table,
  Text,
  TextInput,
} from "@mantine/core";
import { FolderTree, X } from "lucide-react";
import { PeopleMultiPicker } from "../entitlements/PeoplePicker";
import {
  useCreateDomain,
  useDeleteDomain,
  useDeleteDomains,
  useDomains,
  useRoles,
} from "../../hooks/useAdminQueries";
import { useDependentsDialog } from "../../hooks/useDependentsDialog";
import { domainAliasMap, domainGqlAlias, type Domain } from "../../types/admin";
import { assignableDomainIds } from "../../lib/internalDomains";
import { matchesQuery } from "../../lib/searchItems";
import { OwnerResolutionIcon } from "../OwnerResolution";
import { PageTitle } from "../PageTitle";
import { CollapsibleItem, Collapsibles } from "../collapsible/Collapsible";
import { DetailFields } from "../detail/DetailFields";
import { FormField, FormFooter, FormIdentifiers, FormLayout } from "../form/FormLayout";
import { useRequiredFields } from "../form/useRequiredFields";
import { ListBulkBar } from "../list/ListBulkBar";
import { ListEmpty, ListHead, ListItems, ListRow, ListTable } from "../list/ListTable";
import { PanelDelete } from "../list/PanelDelete";
import { PanelActions, SplitPane } from "../list/SplitPane";
import { useListSelection } from "../list/listSelection";
import { useRememberedDetail } from "../list/useDetailParam";
import { pageItems, useListSortGroup, type ListColumn } from "../list/useListSortGroup";
import { useUrlFilter } from "../list/useUrlFilter";
import { FilterInput } from "./FilterInput";
import {
  ApolloSubgraphCard,
  DomainModeCard,
  NameClashPolicyCard,
  NamingConventionsCard,
  OrgProcessingCards,
} from "./settingsCards";

const PAGE_SIZE = 50;

/** REQ-2219 (amended 2026-11-24): what a domain's Steward field names, people and roles as
 * entered. */
const namedStewards = (d: Domain) => [...d.stewardField.people, ...d.stewardField.roles];

/** A domain's stewards as its row shows them: its field as entered, or with none of its own the
 * people it answers to (REQ-1363). */
const shownStewards = (d: Domain) =>
  namedStewards(d).length > 0 ? namedStewards(d) : d.effectiveStewards;

/**
 * REQ-2277: the object management anatomy -- the header (title, filter, Add), the domains listed,
 * and a row opening the domain's panel (?domain=<id>), where it is read and deleted. Adding one
 * opens its form in the same panel. REQ-1349: the org's domain mode is set here too, the tab whose
 * subject is domains, so a single-domain org (no list) can turn them back on.
 */
export function DomainsTab({ title, domainsEnabled }: { title: string; domainsEnabled: boolean }) {
  const { t } = useTranslation();
  // REQ-1918: a delete is refused while anything depends on the object; this lists them.
  const refusal = useDependentsDialog();
  const { domains, loading, refetch: refetchDomains } = useDomains();
  const { roles } = useRoles();
  const { createDomain } = useCreateDomain();
  const { deleteDomain } = useDeleteDomain();
  // REQ-2219 (amended 2026-11-23): the roles a Steward field may name, those holding domain_steward.
  const stewardRoleOptions = roles
    .filter((r) => r.effectiveCapabilities.includes("domain_steward"))
    .map((r) => r.id);

  const [adding, setAdding] = useState(false);
  const [newId, setNewId] = useState("");
  const [newDesc, setNewDesc] = useState("");
  const [newAlias, setNewAlias] = useState("");
  // REQ-1363: the domain the new one sits under; "" = none.
  const [newParent, setNewParent] = useState("");
  // REQ-609, REQ-2219 (amended 2026-11-23): the new domain's Steward field names people.
  const [newStewards, setNewStewards] = useState<string[]>([]);
  // ...and roles (every holder of each a steward).
  const [newStewardRoles, setNewStewardRoles] = useState<string[]>([]);
  // The last domain change's outcome: done (green), or the server's refusal or error (red).
  const [domainMsg, setDomainMsg] = useState<{ text: string; ok: boolean } | null>(null);
  const [saving, setSaving] = useState(false);
  const required = useRequiredFields({ id: newId });
  const onStewardSearchError = useCallback(
    (e: unknown) => setDomainMsg({ text: e instanceof Error ? e.message : String(e), ok: false }),
    [],
  );
  const [page, setPage] = useState(0);
  const [search, setSearch] = useUrlFilter();

  // REQ-471: each domain's alias as its root fields are named, every one its own.
  const aliases = useMemo(() => domainAliasMap(domains), [domains]);
  const userDomains = useMemo(() => {
    const assignable = new Set(assignableDomainIds(domains.map((d) => d.id)));
    return domains.filter((d) => assignable.has(d.id));
  }, [domains]);
  // REQ-1940: sort and group are the shared list mechanism.
  const shownDomains = useMemo(
    () =>
      userDomains.filter((d) =>
        matchesQuery([d.id, d.description, aliases[d.id], d.parentDomainId], search),
      ),
    [userDomains, aliases, search],
  );
  const columns: ListColumn<Domain>[] = [
    { key: "id", label: t("adminPage.colId"), sortValue: (d) => d.id },
    {
      key: "description",
      label: t("adminPage.colDescription"),
      sortValue: (d) => d.description ?? "",
    },
    { key: "alias", label: t("adminPage.colGqlAlias"), sortValue: (d) => aliases[d.id] },
    {
      key: "parent",
      label: t("adminPage.colParentDomain"),
      sortValue: (d) => d.parentDomainId ?? "",
      groupValue: (d) => d.parentDomainId ?? "",
    },
    {
      key: "steward",
      label: t("adminPage.colSteward"),
      sortValue: (d) => d.effectiveStewards.join(", "),
      groupValue: (d) => d.effectiveStewards.join(", "),
    },
  ];
  const sortGroup = useListSortGroup(shownDomains, columns, "domains");
  // REQ-2240: domains selected and deleted together.
  const shownIds = useMemo(() => shownDomains.map((d) => d.id), [shownDomains]);
  const selection = useListSelection(shownIds);
  const deleteDomains = useDeleteDomains();
  // `?domain=<id>` (a graph's or a refused delete's link, detailHref) opens that domain's panel.
  const detail = useRememberedDetail(
    "domains",
    loading || !domainsEnabled ? null : userDomains.map((d) => d.id),
  );
  const open = userDomains.find((d) => d.id === detail.id) ?? null;
  // A linked domain (a graph's or a refused delete's link) is shown on its page of the list and
  // brought into view.
  const linked = detail.id;
  const [seeded, setSeeded] = useState<string | null>(null);
  if (linked !== seeded && shownDomains.length > 0) {
    setSeeded(linked);
    const at = sortGroup.items.findIndex((item) => item.type === "row" && item.row.id === linked);
    if (at >= 0 && sortGroup.groupBy.length === 0) setPage(Math.floor(at / PAGE_SIZE));
  }
  useEffect(() => {
    if (linked === null) return;
    document
      .querySelector(`[data-testid="domain-row-${CSS.escape(linked)}"]`)
      ?.scrollIntoView({ block: "center" });
  }, [linked, page, shownDomains.length]);

  const closeAdd = () => {
    setAdding(false);
    setNewId("");
    setNewDesc("");
    setNewAlias("");
    setNewParent("");
    setNewStewards([]);
    setNewStewardRoles([]);
  };

  const handleAdd = async () => {
    if (!required.validate()) return;
    const id = newId.trim();
    setDomainMsg(null);
    setSaving(true);
    let result: Awaited<ReturnType<typeof createDomain>>;
    try {
      result = await createDomain(
        id,
        newDesc.trim(),
        newAlias.trim() || null,
        newParent || null,
        newStewards,
        newStewardRoles,
      );
    } catch (e) {
      setDomainMsg({ text: e instanceof Error ? e.message : String(e), ok: false });
      return;
    } finally {
      setSaving(false);
    }
    // A refusal (a reserved word, an id in use) is said, and the form stays open to correct it.
    if (!result.success) {
      setDomainMsg({ text: result.message, ok: false });
      return;
    }
    await refetchDomains();
    closeAdd();
    setDomainMsg({ text: t("adminPage.domainAdded", { id }), ok: true });
    detail.open(id);
  };

  const handleDelete = async (id: string): Promise<boolean> => {
    const result = await deleteDomain(id);
    if (refusal.refused(result, id)) return false;
    if (!result.success) throw new Error(result.message);
    await refetchDomains();
    detail.close({ only: id });
    setDomainMsg({ text: t("adminPage.domainDeleted", { id }), ok: true });
    return true;
  };

  const stewardsCell = (d: Domain) => (
    <Group gap={4} wrap="nowrap">
      <Text fz="sm" data-testid={`domain-stewards-${d.id}`}>
        {shownStewards(d).length > 0 ? shownStewards(d).join(", ") : "—"}
      </Text>
      {/* REQ-1363: no steward of its own -- it answers to an ancestor's. */}
      {namedStewards(d).length === 0 &&
        d.stewards.length === 0 &&
        d.effectiveStewards.length > 0 && (
          <Text fz="xs" c="dimmed" data-testid="steward-inherited">
            {t("adminPage.stewardInherited")}
          </Text>
        )}
      {shownStewards(d).length > 0 && (
        <OwnerResolutionIcon
          refs={shownStewards(d)}
          ariaLabel={t("adminPage.resolveSteward", { id: d.id })}
        />
      )}
    </Group>
  );

  const addForm = (
    <FormLayout data-testid="new-domain-form">
      <FormIdentifiers>
        <FormField size="medium">
          <TextInput
            label={t("adminPage.colId")}
            data-testid="new-domain-id"
            value={newId}
            onChange={(e) => setNewId(e.currentTarget.value)}
            placeholder={t("adminPage.domainIdPlaceholder")}
            {...required.field("id")}
          />
        </FormField>
      </FormIdentifiers>
      <FormField size="long">
        <TextInput
          label={t("adminPage.colDescription")}
          data-testid="new-domain-desc"
          value={newDesc}
          onChange={(e) => setNewDesc(e.currentTarget.value)}
        />
      </FormField>
      <FormField size="medium" hint={t("adminPage.parentDomainHint")}>
        <Select
          label={t("adminPage.colParentDomain")}
          data-testid="new-domain-parent"
          data={userDomains.map((d) => d.id)}
          value={newParent || null}
          onChange={(v) => setNewParent(v ?? "")}
          placeholder={t("adminPage.noParentDomain")}
          clearable
          searchable
        />
      </FormField>
      <FormField size="long" hint={t("adminPage.stewardsHint")}>
        <PeopleMultiPicker
          label={t("adminPage.colSteward")}
          value={newStewards}
          onChange={setNewStewards}
          roleOptions={stewardRoleOptions}
          roles={newStewardRoles}
          onRolesChange={setNewStewardRoles}
          onError={onStewardSearchError}
          testId="new-domain-stewards"
        />
      </FormField>
      <FormField size="medium">
        <TextInput
          label={t("adminPage.colGqlAlias")}
          data-testid="new-domain-alias"
          value={newAlias}
          onChange={(e) => setNewAlias(e.currentTarget.value)}
          placeholder={
            newId.trim()
              ? t("adminPage.domainAliasPlaceholderDefault", {
                  alias: domainGqlAlias({ id: newId.trim(), graphqlAlias: null }),
                })
              : undefined
          }
        />
      </FormField>
      <FormFooter
        panel
        onSave={() => void handleAdd()}
        onCancel={closeAdd}
        saving={saving}
        saveProps={{ "data-testid": "new-domain-save" }}
      />
    </FormLayout>
  );

  const domainView = (d: Domain) => (
    <Stack gap="sm" data-testid={`domain-detail-${d.id}`}>
      <PanelActions>
        <fieldset className="panel-action-set">
          <PanelDelete
            itemKey={d.id}
            title={t("adminPage.deleteDomainTitle", { id: d.id })}
            consequence={t("adminPage.deleteDomainConsequence")}
            ariaLabel={t("adminPage.deleteDomain", { id: d.id })}
            confirmLabel={t("common.delete")}
            testId={`domain-delete-${d.id}`}
            onDelete={() => handleDelete(d.id)}
          />
        </fieldset>
      </PanelActions>
      <DetailFields
        testId={`domain-read-${d.id}`}
        fields={[
          { label: t("adminPage.colId"), value: d.id },
          { label: t("adminPage.colDescription"), value: d.description },
          {
            label: t("adminPage.colGqlAlias"),
            value: (
              <Text ff="monospace" fz="sm" component="span">
                {aliases[d.id]}
              </Text>
            ),
          },
          { label: t("adminPage.colParentDomain"), value: d.parentDomainId },
          { label: t("adminPage.colSteward"), value: stewardsCell(d) },
        ]}
      />
    </Stack>
  );

  const totalPages = Math.max(1, Math.ceil(shownDomains.length / PAGE_SIZE));

  return (
    <>
      {refusal.dialog}
      <div className="page-header">
        <PageTitle icon={FolderTree}>{title}</PageTitle>
        {domainsEnabled && (
          <FilterInput
            value={search}
            onChange={(v) => {
              setSearch(v);
              setPage(0);
            }}
            placeholder={t("adminPage.domainsFilterPlaceholder")}
            testId="domains-filter"
          />
        )}
        <div className="page-actions">
          {domainsEnabled && (
            <Button
              variant={adding ? "outline" : "filled"}
              onClick={() => (adding ? closeAdd() : setAdding(true))}
              aria-label={adding ? t("adminPage.closeForm") : undefined}
              data-testid="new-domain-open"
            >
              {adding ? <X size={14} /> : t("adminPage.addDomain")}
            </Button>
          )}
        </div>
      </div>
      <Stack gap="lg">
        <Collapsibles id="admin.domains.settings" testId="domain-settings-panel">
          <CollapsibleItem
            value="settings"
            defaultOpen={false}
            title={t("adminPage.settingsPanel")}
          >
            <SimpleGrid cols={{ base: 1, md: 2 }} spacing="lg">
              <DomainModeCard onApplied={() => refetchDomains()} />
              <NamingConventionsCard />
              <OrgProcessingCards />
              <NameClashPolicyCard />
              <ApolloSubgraphCard />
            </SimpleGrid>
          </CollapsibleItem>
        </Collapsibles>
        {domainMsg && (
          <Alert color={domainMsg.ok ? "green" : "red"} variant="light" data-testid="domain-msg">
            {domainMsg.text}
          </Alert>
        )}
        {domainsEnabled && (
          <SplitPane
            testId="domains-split"
            storageKey="provisa.domains.detailWidth"
            title={adding ? t("adminPage.addDomain") : (open?.id ?? "")}
            onClose={() => {
              closeAdd();
              detail.close();
            }}
            closeOnEscape={!adding}
            labels={{
              panel: t("adminPage.domainPanel"),
              resize: t("tablesPage.resizeDetail"),
              expand: t("tablesPage.expandDetail"),
              restore: t("tablesPage.restoreDetail"),
              close: t("tablesPage.closeDetail"),
              backToList: t("tablesPage.backToList"),
            }}
            list={
              <Stack gap="sm">
                <ListBulkBar
                  selection={selection}
                  nameOf={(id) => id}
                  onDelete={deleteDomains}
                  testId="domains-bulk"
                />
                <ListTable minWidth={480} testId="domains-list" selection={selection}>
                  <ListHead
                    sortGroup={sortGroup}
                    columns={[
                      { col: "id" },
                      { col: "description" },
                      { col: "alias" },
                      { col: "parent" },
                      { col: "steward" },
                    ]}
                  />
                  <Table.Tbody>
                    {shownDomains.length === 0 && (
                      <ListEmpty colSpan={5}>{t("adminPage.noDomains")}</ListEmpty>
                    )}
                    <ListItems
                      state={sortGroup}
                      items={pageItems(sortGroup, page, PAGE_SIZE)}
                      colSpan={5}
                      rowKey={(d) => d.id}
                      render={(d) => (
                        <ListRow
                          selected={!adding && d.id === detail.id}
                          testId={`domain-row-${d.id}`}
                          onClick={() => {
                            closeAdd();
                            if (detail.id === d.id) detail.close();
                            else detail.open(d.id);
                          }}
                        >
                          <Table.Td>{d.id}</Table.Td>
                          <Table.Td>{d.description || "—"}</Table.Td>
                          <Table.Td>
                            <Text c="dimmed" ff="monospace" fz="sm">
                              {aliases[d.id]}
                            </Text>
                          </Table.Td>
                          <Table.Td>{d.parentDomainId || "—"}</Table.Td>
                          <Table.Td>{stewardsCell(d)}</Table.Td>
                        </ListRow>
                      )}
                    />
                  </Table.Tbody>
                </ListTable>
                {totalPages > 1 && sortGroup.groupBy.length === 0 && (
                  <Group justify="flex-end">
                    <Pagination
                      total={totalPages}
                      value={page + 1}
                      onChange={(p) => setPage(p - 1)}
                      size="sm"
                    />
                  </Group>
                )}
              </Stack>
            }
          >
            {adding ? addForm : open !== null ? domainView(open) : null}
          </SplitPane>
        )}
      </Stack>
    </>
  );
}
