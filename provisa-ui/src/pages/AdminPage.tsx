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
import { useLocation } from "react-router-dom";
import { useTranslation } from "react-i18next";
import { Trash2 } from "lucide-react";
import {
  Accordion,
  ActionIcon,
  Alert,
  Button,
  Card,
  Group,
  Pagination,
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
  useRLSRules,
  useRoles,
  useCreateDomain,
  useDeleteDomain,
} from "../hooks/useAdminQueries";
import { fetchSettings } from "../api/admin";
import type { PlatformSettings } from "../api/admin";
import { useAuth } from "../context/AuthContext";
import { domainGqlAlias } from "../types/admin";
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
import { MetadataExportTab } from "../components/admin/MetadataExportTab";
import { ImportTab } from "../components/admin/ImportTab";
import { TagsTab } from "../components/admin/TagsTab";
import { ReportsTab } from "../components/admin/ReportsTab";
import { GlossaryTab } from "../components/admin/GlossaryTab";
import { SecurityManager } from "../components/admin/SecurityManager";
import { SecretsTab, MySecretsTab } from "../components/admin/SecretsTab";
import { DomainModeCard, NamingConventionsCard } from "../components/admin/settingsCards";
import { PageLoading } from "../components/PageLoading";
import { usePanelState } from "../hooks/usePanelState";

const ROUTE_TO_SECTION: Record<string, string> = {
  // Both routes open the merged dashboard; /admin/system-health keeps working as a deep link.
  "/admin/overview": "Dashboard",
  "/admin/domains": "Domains",
  "/admin/environments": "Environments",
  "/admin/cache": "Cache",
  "/admin/scheduled-tasks": "Scheduler",
  "/admin/federation-engine": "Federation",
  "/admin/org-engine": "Org Engine", // REQ-1412: this org's engine lane
  "/admin/maintenance": "Maintenance", // REQ-1466: the scheduled-downtime banner
  "/admin/billing": "Billing", // REQ-1469: plan, current bill, next charge
  "/admin/system-health": "Dashboard",
  // REQ-1008: MCP status is read-only deployment health, so it sits on the dashboard rather than
  // owning a nav entry of its own; the old route still deep-links there.
  "/admin/mcp-server": "Dashboard",
  "/admin/observability": "Observability",
  "/admin/orgs": "Orgs",
  "/admin/ai-models": "AI Models",
  "/admin/metadata-export": "Metadata Export",
  "/admin/import": "Import", // REQ-1483: Hasura v2 / DDN import
  "/admin/tags": "Tags",
  "/admin/reports": "Reports", // REQ-1386: ops-domain management report viewer
  "/admin/glossary": "Glossary", // REQ-1387: business-glossary curation
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
  const { t } = useTranslation();
  const [domainPanel, setDomainPanel] = usePanelState("domain-settings");
  const [mcpPanel, setMcpPanel] = usePanelState("mcp-server");
  const location = useLocation();
  const activeTab = ROUTE_TO_SECTION[location.pathname] ?? "Dashboard";
  const { capabilities } = useAuth();
  // REQ-1337: administering orgs other than the active one is the `cross_org` RIGHT. "platform_admin"
  // was never a capability — it was a role id folded in as a pseudo-right, which this rule forbids.
  const canAdministerAllOrgs = capabilities.includes("cross_org");
  const [stats, setStats] = useState<Record<string, number>>({});
  const [newDomainId, setNewDomainId] = useState("");
  const [newDomainDesc, setNewDomainDesc] = useState("");
  const [newDomainAlias, setNewDomainAlias] = useState("");
  const [domainMsg, setDomainMsg] = useState("");
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

  const handleAddDomain = async () => {
    if (!newDomainId.trim()) return;
    await createDomain(newDomainId.trim(), newDomainDesc.trim(), newDomainAlias.trim() || null);
    await refetchDomains();
    setNewDomainId("");
    setNewDomainDesc("");
    setNewDomainAlias("");
    setDomainMsg(t("adminPage.domainAdded", { id: newDomainId.trim() }));
  };

  const handleDeleteDomain = async (id: string) => {
    await deleteDomain(id);
    await refetchDomains();
    setDomainMsg(t("adminPage.domainDeleted", { id }));
  };

  if (loading) return <PageLoading message={t("adminPage.loading")} />;

  return (
    <div className="page">
      {activeTab !== "Glossary" && (
        <Title order={2} mb="md">
          {/* Reads as a breadcrumb: the area, then the section within it. */}
          {t("adminPage.title")} — {activeTab}
        </Title>
      )}

      {/* Anchor the tour waits on: step23 highlights the navbar link, which exists before this
          page has painted, so it gates on the admin content itself. */}
      <Stack gap="lg" data-tour="admin-content">
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
            <Accordion
              variant="separated"
              value={mcpPanel}
              onChange={setMcpPanel}
              data-testid="mcp-server-panel"
            >
              <Accordion.Item value="mcp">
                <Accordion.Control>
                  <Title order={4}>{t("navBar.itemMcpServer")}</Title>
                </Accordion.Control>
                <Accordion.Panel>
                  <McpServerTab />
                </Accordion.Panel>
              </Accordion.Item>
            </Accordion>
          </>
        )}

        {activeTab === "Environments" && <EnvironmentsTab />}

        {activeTab === "Domains" && (
          <>
            {/* REQ-1349: the org's domain mode is set HERE, not on Overview — this is the tab whose
                subject is domains, and it is the only place a single-domain org can turn them back
                on, so the tab renders the card whether or not domains are in use. */}
            {/* variant="separated" with a Title order={4} control is the admin section panel —
                the same shape TeamPage's sections use. */}
            <Accordion
              variant="separated"
              value={domainPanel}
              onChange={setDomainPanel}
              data-testid="domain-settings-panel"
            >
              <Accordion.Item value="settings">
                <Accordion.Control>
                  <Title order={4}>{t("adminPage.settingsPanel")}</Title>
                </Accordion.Control>
                <Accordion.Panel>
                  <SimpleGrid cols={{ base: 1, md: 2 }} spacing="lg">
                    <DomainModeCard onApplied={() => refetchDomains()} />
                    <NamingConventionsCard />
                  </SimpleGrid>
                </Accordion.Panel>
              </Accordion.Item>
            </Accordion>
            {domainMsg && (
              <Alert color="green" variant="light">
                {domainMsg}
              </Alert>
            )}
            {domainsEnabled &&
              (() => {
                const IMPLICIT_DOMAIN_IDS = new Set(["", "meta", "ops"]);
                const userDomains = domains.filter((d) => !IMPLICIT_DOMAIN_IDS.has(d.id));
                const totalPages = Math.max(1, Math.ceil(userDomains.length / PAGE_SIZE));
                const paged = userDomains.slice(
                  domainPage * PAGE_SIZE,
                  (domainPage + 1) * PAGE_SIZE,
                );
                return (
                  <Stack gap="sm">
                    <Table.ScrollContainer minWidth={480}>
                      <Table striped highlightOnHover withTableBorder verticalSpacing="xs">
                        <Table.Thead>
                          <Table.Tr>
                            <Table.Th>{t("adminPage.colId")}</Table.Th>
                            <Table.Th>{t("adminPage.colDescription")}</Table.Th>
                            <Table.Th>{t("adminPage.colGqlAlias")}</Table.Th>
                            <Table.Th>{t("adminPage.colActions")}</Table.Th>
                          </Table.Tr>
                        </Table.Thead>
                        <Table.Tbody>
                          {userDomains.length === 0 && (
                            <Table.Tr>
                              <Table.Td colSpan={4} ta="center" c="dimmed">
                                {t("adminPage.noDomains")}
                              </Table.Td>
                            </Table.Tr>
                          )}
                          {paged.map((d) => (
                            <Table.Tr key={d.id}>
                              <Table.Td>{d.id}</Table.Td>
                              <Table.Td>{d.description || "—"}</Table.Td>
                              <Table.Td>
                                <Text c="dimmed" ff="monospace" fz="sm">
                                  {domainGqlAlias(d)}
                                </Text>
                              </Table.Td>
                              <Table.Td>
                                <ActionIcon
                                  variant="subtle"
                                  color="red"
                                  aria-label={t("adminPage.deleteDomain", { id: d.id })}
                                  onClick={() => handleDeleteDomain(d.id)}
                                >
                                  <Trash2 size={14} />
                                </ActionIcon>
                              </Table.Td>
                            </Table.Tr>
                          ))}
                        </Table.Tbody>
                      </Table>
                    </Table.ScrollContainer>
                    {totalPages > 1 && (
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
            {domainsEnabled && (
              <Group gap="sm" align="flex-end">
                <TextInput
                  data-testid="new-domain-id"
                  value={newDomainId}
                  onChange={(e) => setNewDomainId(e.currentTarget.value)}
                  placeholder={t("adminPage.domainIdPlaceholder")}
                  w={160}
                />
                <TextInput
                  data-testid="new-domain-desc"
                  value={newDomainDesc}
                  onChange={(e) => setNewDomainDesc(e.currentTarget.value)}
                  placeholder={t("adminPage.domainDescPlaceholder")}
                  style={{ flex: 1 }}
                />
                <TextInput
                  data-testid="new-domain-alias"
                  value={newDomainAlias}
                  onChange={(e) => setNewDomainAlias(e.currentTarget.value)}
                  placeholder={
                    newDomainId.trim()
                      ? t("adminPage.domainAliasPlaceholderDefault", {
                          alias: domainGqlAlias({ id: newDomainId.trim(), description: "" }),
                        })
                      : t("adminPage.domainAliasPlaceholder")
                  }
                  w={180}
                />
                <Button onClick={handleAddDomain} disabled={!newDomainId.trim()}>
                  {t("adminPage.addDomain")}
                </Button>
              </Group>
            )}
          </>
        )}
        {activeTab === "Cache" && <CacheManager />}
        {activeTab === "Scheduler" && <ScheduledTasks />}
        {activeTab === "Federation" && <FederationEngineTab />}
        {activeTab === "Org Engine" && <OrgEngineTab />}
        {activeTab === "Maintenance" && <MaintenanceTab />}
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
        {activeTab === "Metadata Export" && <MetadataExportTab />}
        {activeTab === "Import" && <ImportTab />}
        {activeTab === "Tags" && <TagsTab />}
        {activeTab === "Reports" && <ReportsTab />}
        {activeTab === "Glossary" && <GlossaryTab />}
      </Stack>
    </div>
  );
}
