// Copyright (c) 2026 Kenneth Stott
// Canary: b8b1d1a3-e713-464e-8e0a-c8bc5b43544d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useState, useEffect } from "react";
import { useLocation, useSearchParams } from "react-router-dom";
import { useTranslation } from "react-i18next";
import { Plus, Trash2 } from "lucide-react";
import {
  ActionIcon,
  Alert,
  Button,
  Card,
  Group,
  Pagination,
  Select,
  SimpleGrid,
  Stack,
  Table,
  Text,
  TextInput,
  Title,
} from "@mantine/core";
import {
  useDomains,
  useTables,
  useRelationships,
  useSources,
  useRoles,
  useCreateDomain,
  useDeleteDomain,
} from "../hooks/useAdminQueries";
import { useRLSRules } from "../hooks/useSecurityQueries";
import { fetchSettings } from "../api/admin";
import type { PlatformSettings } from "../api/admin";
import { useAuth } from "../context/AuthContext";
import { domainGqlAlias } from "../types/admin";
import { OwnerResolutionIcon } from "../components/OwnerResolution";
import { ConfirmDialog } from "../components/ConfirmDialog";
import { EnvironmentsTab } from "../components/admin/EnvironmentsTab";
import { CacheManager } from "../components/admin/CacheManager";
import { SystemHealth } from "../components/admin/SystemHealth";
import { ScheduledTasks } from "../components/admin/ScheduledTasks";
import { ObservabilityTab } from "../components/admin/ObservabilityTab";
import { FederationEngineTab } from "../components/admin/FederationEngineTab";
import { OrgEngineTab } from "../components/admin/OrgEngineTab";
import { MaintenanceTab } from "../components/admin/MaintenanceTab"; // REQ-1466
import { BillingTab } from "../components/admin/BillingTab"; // REQ-1469
import { McpServerTab } from "../components/admin/McpServerTab";
import { OrgsTab } from "../components/admin/OrgsTab";
import { AiModelsTab } from "../components/admin/AiModelsTab";
import { PackageRegistriesTab } from "../components/admin/PackageRegistriesTab";
import { ContainerRegistriesTab } from "../components/admin/ContainerRegistriesTab";
import { MetadataExportTab } from "../components/admin/MetadataExportTab";
import { ImportTab } from "../components/admin/ImportTab";
import { TagsTab } from "../components/admin/TagsTab";
import { ClassificationKindsTab } from "../components/admin/ClassificationKindsTab";
import { ReportsTab } from "../components/admin/ReportsTab";
import { GlossaryTab } from "../components/admin/GlossaryTab";
import { SecurityManager } from "../components/admin/SecurityManager";
import { SecretsTab, MySecretsTab } from "../components/admin/SecretsTab";
import { EmailTab } from "../components/admin/EmailTab";
import { SettingsCatalogPanel } from "../components/admin/SettingField";
import { DomainModeCard, NamingConventionsCard } from "../components/admin/settingsCards";
import { PageLoading } from "../components/PageLoading";
import { CollapsibleItem, Collapsibles } from "../components/collapsible/Collapsible";
import { useDependentsDialog } from "../hooks/useDependentsDialog";
import { ListTable, ListHead, ListRow, ListEmpty, ListItems } from "../components/list/ListTable";
import { useListSortGroup, pageItems, type ListColumn } from "../components/list/useListSortGroup";
import { FormField, FormFooter, FormIdentifiers, FormLayout } from "../components/form/FormLayout";
import { assignableDomainIds } from "../lib/internalDomains";

const ROUTE_TO_SECTION: Record<string, string> = {
  // Both routes open the merged dashboard; /admin/system-health keeps working as a deep link.
  "/admin/overview": "Dashboard",
  "/admin/domains": "Domains",
  "/admin/environments": "Environments",
  "/admin/cache": "Cache",
  "/admin/scheduled-tasks": "Triggers", // REQ-2054: every automatic run in one place
  "/admin/federation-engine": "Federation",
  "/admin/org-engine": "Org Engine", // REQ-1412: this org's engine lane
  "/admin/email": "Email", // REQ-1576: the mail transport platform messages go out through
  "/admin/maintenance": "Maintenance", // REQ-1466: the scheduled-downtime banner
  "/admin/deployment-settings": "Deployment settings", // REQ-1913: every operator setting
  "/admin/billing": "Billing", // REQ-1469: plan, current bill, next charge
  "/admin/system-health": "Dashboard",
  // REQ-1008: MCP status is read-only deployment health, so it sits on the dashboard rather than
  // owning a nav entry of its own; the old route still deep-links there.
  "/admin/mcp-server": "Dashboard",
  "/admin/observability": "Observability",
  "/admin/orgs": "Orgs",
  "/admin/ai-models": "AI Models",
  "/admin/package-registries": "Package Registries", // REQ-1984: what Node installs go through
  "/admin/container-registries": "Container Registries", // REQ-2098: where built images go
  "/admin/metadata-export": "Metadata Export",
  "/admin/import": "Import", // REQ-1483: Hasura v2 / DDN import
  "/admin/tags": "Tags",
  "/admin/classification-kinds": "Classification", // REQ-2147: the profiler's classification kinds
  "/admin/reports": "Reports", // REQ-1386: ops-domain management report viewer
  "/admin/ontology": "Glossary", // REQ-1387: business-glossary curation
  // Consolidated Security area — posture, encryption, auth, and local users as sub-tabs.
  // Legacy routes deep-link to the matching sub-tab.
  "/admin/security": "Security",
  "/admin/encryption": "Security",
  "/admin/auth": "Security",
  "/admin/local-users": "Security",
  // REQ-1560: one vault per surface, each named for whose it is.
  "/admin/secrets": "Org Secrets",
  "/admin/my-secrets": "Your Secrets",
};

// Which Security sub-tab a route opens on.
const SECURITY_SUBTAB: Record<string, "posture" | "encryption" | "authentication" | "localUsers"> =
  {
    "/admin/encryption": "encryption",
    "/admin/auth": "authentication",
    "/admin/local-users": "localUsers",
  };

/** Admin overview page — dashboard, config management, platform settings. */
export function AdminPage() {
  // REQ-1918: a delete is refused while anything depends on the object; this lists them.
  const refusal = useDependentsDialog();
  const { t } = useTranslation();
  const location = useLocation();
  const activeTab = ROUTE_TO_SECTION[location.pathname] ?? "Dashboard";
  const { capabilities } = useAuth();
  // REQ-1337: administering orgs other than the active one is the `cross_org` RIGHT. "platform_admin"
  // was never a capability — it was a role id folded in as a pseudo-right, which this rule forbids.
  const canAdministerAllOrgs = capabilities.includes("cross_org");
  // REQ-1361: /admin/secrets serves whichever half the caller may reach, and the heading says which.
  // Without a literal `org_settings` there is no org vault on the page -- only the DEPLOYMENT's
  // choice of secrets service -- and the platform wildcard does not stand in for the org right here.
  const heading =
    activeTab === "Org Secrets" && !capabilities.includes("org_settings")
      ? "Platform Secrets"
      : activeTab;
  const [stats, setStats] = useState<Record<string, number>>({});
  const [addingDomain, setAddingDomain] = useState(false);
  const [newDomainId, setNewDomainId] = useState("");
  const [newDomainDesc, setNewDomainDesc] = useState("");
  const [newDomainAlias, setNewDomainAlias] = useState("");
  // REQ-1363: the domain the new one sits under; "" = none.
  const [newDomainParent, setNewDomainParent] = useState("");
  // The last domain change's outcome: done (green), or the server's refusal or error (red).
  const [domainMsg, setDomainMsg] = useState<{ text: string; ok: boolean } | null>(null);
  const [loading, setLoading] = useState(true);
  // REQ-1349: the settings this page still reads. The cards that EDIT them moved to the tabs whose
  // subject they are (settingsCards.tsx); what is left is the domain mode, which decides whether
  // the Domains tab shows a domain catalog at all.
  const [settings, setSettings] = useState<PlatformSettings | null>(null);
  const [allDomains, setAllDomains] = useState<string[]>([]);

  // Pagination state
  const [domainPage, setDomainPage] = useState(0);
  const PAGE_SIZE = 50;

  // Apollo hooks for cache-and-network queries and mutations
  const { sources, loading: sourcesLoading } = useSources();
  const { domains, loading: domainsLoading, refetch: refetchDomains } = useDomains();
  const { tables, loading: tablesLoading } = useTables();
  const { relationships, loading: relsLoading } = useRelationships();
  const { rlsRules, loading: rlsLoading } = useRLSRules();
  const { roles } = useRoles();
  const { createDomain } = useCreateDomain();
  const { deleteDomain } = useDeleteDomain();
  const allRoles = roles.map((r) => r.id);

  // Update state and stats when hook data arrives
  useEffect(() => {
    const loading = sourcesLoading || domainsLoading || tablesLoading || relsLoading || rlsLoading;
    /* eslint-disable-next-line react-hooks/set-state-in-effect --
       derived state synced from multiple Apollo query results (documented useState+useEffect derived pattern) */
    setLoading(loading);

    if (!loading) {
      // A view is a registered table with view_sql; materialized ones additionally have materialize.
      const viewTables = tables.filter((t) => t.viewSql != null);
      setStats({
        Sources: sources.length,
        Domains: domains.length,
        Tables: tables.length,
        Views: viewTables.length,
        "Materialized Views": viewTables.filter((t) => t.materialize).length,
        Relationships: relationships.length,
        Roles: allRoles.length,
        "RLS Rules": rlsRules.length,
      });
      setAllDomains(domains.filter((d) => d.id !== "").map((d) => d.id));
    }
  }, [
    sources,
    domains,
    tables,
    relationships,
    rlsRules,
    rlsLoading,
    domainsLoading,
    tablesLoading,
    relsLoading,
    sourcesLoading,
    allRoles.length,
  ]);

  // Platform settings (REST); per-tab data is loaded by each tab component.
  useEffect(() => {
    fetchSettings().then(setSettings);
  }, []);

  const domainsEnabled = settings?.naming.use_domains !== false;

  // REQ-1940: sort and group are the shared list mechanism.
  const assignable = new Set(assignableDomainIds(domains.map((d) => d.id)));
  const userDomains = domains.filter((d) => assignable.has(d.id));
  const domainColumns: ListColumn<(typeof domains)[number]>[] = [
    { key: "id", label: t("adminPage.colId"), sortValue: (d) => d.id },
    {
      key: "description",
      label: t("adminPage.colDescription"),
      sortValue: (d) => d.description ?? "",
    },
    { key: "alias", label: t("adminPage.colGqlAlias"), sortValue: (d) => domainGqlAlias(d) },
    {
      key: "parent",
      label: t("adminPage.colParentDomain"),
      sortValue: (d) => d.parentDomainId ?? "",
      groupValue: (d) => d.parentDomainId ?? "",
    },
    {
      key: "steward",
      label: t("adminPage.colSteward"),
      sortValue: (d) => d.steward ?? "",
      groupValue: (d) => d.steward ?? "",
    },
  ];
  const domainSortGroup = useListSortGroup(userDomains, domainColumns, "domains");
  // `?domain=<id>` (a graph's domain link, lib/entityHref): that domain's row is marked, on the
  // page that holds it, and brought into view.
  const [searchParams] = useSearchParams();
  const linkedDomain = searchParams.get("domain");
  const [seededDomain, setSeededDomain] = useState<string | null>(null);
  if (linkedDomain !== seededDomain && userDomains.length > 0) {
    setSeededDomain(linkedDomain);
    const at = domainSortGroup.items.findIndex(
      (item) => item.type === "row" && item.row.id === linkedDomain,
    );
    if (at >= 0 && domainSortGroup.groupBy.length === 0) setDomainPage(Math.floor(at / PAGE_SIZE));
  }
  useEffect(() => {
    if (linkedDomain === null) return;
    document
      .querySelector(`[data-testid="domain-row-${CSS.escape(linkedDomain)}"]`)
      ?.scrollIntoView({ block: "center" });
    // Again once the rows are drawn: the list waits on the settings saying domains are on.
  }, [linkedDomain, domainPage, activeTab, settings, userDomains]);

  const closeAddDomain = () => {
    setAddingDomain(false);
    setNewDomainId("");
    setNewDomainDesc("");
    setNewDomainAlias("");
    setNewDomainParent("");
  };

  const handleAddDomain = async () => {
    if (!newDomainId.trim()) return;
    setDomainMsg(null);
    let result: Awaited<ReturnType<typeof createDomain>>;
    try {
      result = await createDomain(
        newDomainId.trim(),
        newDomainDesc.trim(),
        newDomainAlias.trim() || null,
        newDomainParent || null,
      );
    } catch (e) {
      setDomainMsg({ text: e instanceof Error ? e.message : String(e), ok: false });
      return;
    }
    // A refusal (a reserved word, an id in use) is said, and the form stays open to correct it.
    if (!result.success) {
      setDomainMsg({ text: result.message, ok: false });
      return;
    }
    await refetchDomains();
    closeAddDomain();
    setDomainMsg({ text: t("adminPage.domainAdded", { id: newDomainId.trim() }), ok: true });
  };

  const handleDeleteDomain = async (id: string) => {
    const result = await deleteDomain(id);
    if (refusal.refused(result, id)) return;
    await refetchDomains();
    setDomainMsg(
      result.success
        ? { text: t("adminPage.domainDeleted", { id }), ok: true }
        : { text: result.message, ok: false },
    );
  };

  if (loading) return <PageLoading message={t("adminPage.loading")} />;

  // Glossary's detail panel is meant to fill to the bottom of the viewport rather than
  // shrink-wrap its content, so its page and content Stack join the flex-height chain that
  // `main` already provides (the same `page-sticky-head` pattern the sticky-header list
  // pages use) instead of the plain block-flow the other admin tabs render into.
  const isGlossary = activeTab === "Glossary";

  return (
    <div className={isGlossary ? "page page-sticky-head" : "page"}>
      {activeTab !== "Glossary" && (
        <Title order={2} mb="md">
          {/* Reads as a breadcrumb: the area, then the section within it. */}
          {t("adminPage.title")} — {heading}
        </Title>
      )}

      {/* Anchor the tour waits on: step23 highlights the navbar link, which exists before this
          page has painted, so it gates on the admin content itself. */}
      <Stack
        gap="lg"
        data-tour="admin-content"
        style={isGlossary ? { flex: 1, minHeight: 0 } : undefined}
      >
        {activeTab === "Dashboard" && (
          <>
            <SimpleGrid cols={{ base: 2, sm: 4 }} spacing="md">
              {Object.entries(stats).map(([label, count]) => (
                <Card key={label} withBorder padding="md" data-testid={`stat-card-${label}`}>
                  <Text fz={28} fw={700}>
                    {count}
                  </Text>
                  <Text c="dimmed" fz="sm">
                    {label}
                  </Text>
                </Card>
              ))}
            </SimpleGrid>
            <div>
              <Title order={3} mb="sm">
                {t("systemHealth.title")}
              </Title>
              <SystemHealth />
            </div>
            <Collapsibles id="admin.dashboard.mcp-server" testId="mcp-server-panel">
              <CollapsibleItem value="mcp" defaultOpen={false} title={t("navBar.itemMcpServer")}>
                <McpServerTab />
              </CollapsibleItem>
            </Collapsibles>
          </>
        )}

        {activeTab === "Environments" && <EnvironmentsTab />}

        {activeTab === "Domains" && (
          <>
            {/* REQ-1349: the org's domain mode is set HERE, not on Overview — this is the tab whose
                subject is domains, and it is the only place a single-domain org can turn them back
                on, so the tab renders the card whether or not domains are in use. */}
            <Collapsibles id="admin.domains.settings" testId="domain-settings-panel">
              <CollapsibleItem
                value="settings"
                defaultOpen={false}
                title={t("adminPage.settingsPanel")}
              >
                <SimpleGrid cols={{ base: 1, md: 2 }} spacing="lg">
                  <DomainModeCard onApplied={() => refetchDomains()} />
                  <NamingConventionsCard />
                </SimpleGrid>
              </CollapsibleItem>
            </Collapsibles>
            {domainMsg && (
              <Alert
                color={domainMsg.ok ? "green" : "red"}
                variant="light"
                data-testid="domain-msg"
              >
                {domainMsg.text}
              </Alert>
            )}
            {/* The opener sits above the list it adds to and is hidden while its form is open. */}
            {domainsEnabled &&
              (addingDomain ? (
                <FormLayout data-testid="new-domain-form">
                  <FormIdentifiers>
                    <FormField size="medium">
                      <TextInput
                        label={t("adminPage.colId")}
                        data-testid="new-domain-id"
                        value={newDomainId}
                        onChange={(e) => setNewDomainId(e.currentTarget.value)}
                        placeholder={t("adminPage.domainIdPlaceholder")}
                      />
                    </FormField>
                  </FormIdentifiers>
                  <FormField size="long">
                    <TextInput
                      label={t("adminPage.colDescription")}
                      data-testid="new-domain-desc"
                      value={newDomainDesc}
                      onChange={(e) => setNewDomainDesc(e.currentTarget.value)}
                    />
                  </FormField>
                  <FormField size="medium" hint={t("adminPage.parentDomainHint")}>
                    <Select
                      label={t("adminPage.colParentDomain")}
                      data-testid="new-domain-parent"
                      data={userDomains.map((d) => d.id)}
                      value={newDomainParent || null}
                      onChange={(v) => setNewDomainParent(v ?? "")}
                      placeholder={t("adminPage.noParentDomain")}
                      clearable
                      searchable
                    />
                  </FormField>
                  <FormField size="medium">
                    <TextInput
                      label={t("adminPage.colGqlAlias")}
                      data-testid="new-domain-alias"
                      value={newDomainAlias}
                      onChange={(e) => setNewDomainAlias(e.currentTarget.value)}
                      placeholder={
                        newDomainId.trim()
                          ? t("adminPage.domainAliasPlaceholderDefault", {
                              alias: domainGqlAlias({ id: newDomainId.trim(), graphqlAlias: null }),
                            })
                          : undefined
                      }
                    />
                  </FormField>
                  <FormFooter
                    onSave={handleAddDomain}
                    onCancel={closeAddDomain}
                    saveProps={{ "data-testid": "new-domain-save", disabled: !newDomainId.trim() }}
                  />
                </FormLayout>
              ) : (
                <FormFooter>
                  <Button
                    leftSection={<Plus size={14} />}
                    onClick={() => setAddingDomain(true)}
                    data-testid="new-domain-open"
                  >
                    {t("adminPage.addDomain")}
                  </Button>
                </FormFooter>
              ))}
            {domainsEnabled &&
              (() => {
                const totalPages = Math.max(1, Math.ceil(userDomains.length / PAGE_SIZE));
                return (
                  <Stack gap="sm">
                    <ListTable minWidth={480} testId="domains-list">
                      <ListHead
                        sortGroup={domainSortGroup}
                        columns={[
                          { col: "id" },
                          { col: "description" },
                          { col: "alias" },
                          { col: "parent" },
                          { col: "steward" },
                          t("adminPage.colActions"),
                        ]}
                      />
                      <Table.Tbody>
                        {userDomains.length === 0 && (
                          <ListEmpty colSpan={6}>{t("adminPage.noDomains")}</ListEmpty>
                        )}
                        <ListItems
                          state={domainSortGroup}
                          items={pageItems(domainSortGroup, domainPage, PAGE_SIZE)}
                          colSpan={6}
                          rowKey={(d) => d.id}
                          render={(d) => (
                            <ListRow selected={d.id === linkedDomain} testId={`domain-row-${d.id}`}>
                              <Table.Td>{d.id}</Table.Td>
                              <Table.Td>{d.description || "—"}</Table.Td>
                              <Table.Td>
                                <Text c="dimmed" ff="monospace" fz="sm">
                                  {domainGqlAlias(d)}
                                </Text>
                              </Table.Td>
                              <Table.Td>{d.parentDomainId || "—"}</Table.Td>
                              <Table.Td>
                                <Group gap={4} wrap="nowrap">
                                  <Text fz="sm">{d.effectiveSteward || "—"}</Text>
                                  {/* REQ-1363: no steward of its own -- it answers to an ancestor's. */}
                                  {!d.steward && d.effectiveSteward && (
                                    <Text fz="xs" c="dimmed" data-testid="steward-inherited">
                                      {t("adminPage.stewardInherited")}
                                    </Text>
                                  )}
                                  {d.effectiveSteward && (
                                    <OwnerResolutionIcon
                                      refs={[d.effectiveSteward]}
                                      ariaLabel={t("adminPage.resolveSteward", { id: d.id })}
                                    />
                                  )}
                                </Group>
                              </Table.Td>
                              <Table.Td>
                                <ConfirmDialog
                                  title={t("adminPage.deleteDomainTitle", { id: d.id })}
                                  consequence={t("adminPage.deleteDomainConsequence")}
                                  onConfirm={() => handleDeleteDomain(d.id)}
                                >
                                  {(open) => (
                                    <ActionIcon
                                      variant="subtle"
                                      color="red"
                                      aria-label={t("adminPage.deleteDomain", { id: d.id })}
                                      onClick={open}
                                    >
                                      <Trash2 size={14} />
                                    </ActionIcon>
                                  )}
                                </ConfirmDialog>
                              </Table.Td>
                            </ListRow>
                          )}
                        />
                      </Table.Tbody>
                    </ListTable>
                    {totalPages > 1 && domainSortGroup.groupBy.length === 0 && (
                      <Group justify="flex-end">
                        <Pagination
                          total={totalPages}
                          value={domainPage + 1}
                          onChange={(p) => setDomainPage(p - 1)}
                          size="sm"
                        />
                      </Group>
                    )}
                  </Stack>
                );
              })()}
          </>
        )}
        {activeTab === "Cache" && <CacheManager />}
        {activeTab === "Triggers" && <ScheduledTasks />}
        {activeTab === "Federation" && <FederationEngineTab />}
        {activeTab === "Org Engine" && <OrgEngineTab />}
        {activeTab === "Email" && <EmailTab />}
        {activeTab === "Maintenance" && <MaintenanceTab />}
        {activeTab === "Deployment settings" && <SettingsCatalogPanel />}
        {activeTab === "Billing" && <BillingTab />}
        {activeTab === "Security" && (
          <SecurityManager
            allRoles={allRoles}
            allDomains={allDomains}
            initialTab={SECURITY_SUBTAB[location.pathname]}
          />
        )}
        {activeTab === "Org Secrets" && <SecretsTab />}
        {activeTab === "Your Secrets" && <MySecretsTab />}
        {activeTab === "Observability" && settings && (
          <ObservabilityTab settings={settings} setSettings={setSettings} />
        )}
        {activeTab === "Orgs" && canAdministerAllOrgs && <OrgsTab />}
        {activeTab === "AI Models" && <AiModelsTab />}
        {activeTab === "Package Registries" && <PackageRegistriesTab />}
        {activeTab === "Container Registries" && <ContainerRegistriesTab />}
        {activeTab === "Metadata Export" && <MetadataExportTab />}
        {activeTab === "Import" && <ImportTab />}
        {activeTab === "Tags" && <TagsTab />}
        {activeTab === "Classification" && <ClassificationKindsTab />}
        {activeTab === "Reports" && <ReportsTab />}
        {activeTab === "Glossary" && <GlossaryTab />}
      </Stack>
      {refusal.dialog}
    </div>
  );
}
