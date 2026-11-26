// Copyright (c) 2026 Kenneth Stott
// Canary: b8b1d1a3-e713-464e-8e0a-c8bc5b43544d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { PageTitle } from "../components/PageTitle";
import { useState, useEffect } from "react";
import { useLocation } from "react-router-dom";
import { useTranslation } from "react-i18next";
import { Settings } from "lucide-react";
import { Card, SimpleGrid, Stack, Text, Title } from "@mantine/core";
import {
  useDomains,
  useTables,
  useRelationships,
  useSources,
  useRoles,
} from "../hooks/useAdminQueries";
import { useRLSRules } from "../hooks/useSecurityQueries";
import { fetchSettings } from "../api/admin";
import type { PlatformSettings } from "../api/admin";
import { useAuth } from "../context/AuthContext";
import { DomainsTab } from "../components/admin/DomainsTab";
import { EnvironmentsTab } from "../components/admin/EnvironmentsTab";
import { CacheManager } from "../components/admin/CacheManager";
import { SystemHealth } from "../components/admin/SystemHealth";
import { ScheduledTasks } from "../components/admin/ScheduledTasks";
import { ObservabilityTab, PlatformObservabilityTab } from "../components/admin/ObservabilityTab";
import { FederationEngineTab } from "../components/admin/FederationEngineTab";
import { OrgEngineTab } from "../components/admin/OrgEngineTab";
import { MaintenanceTab } from "../components/admin/MaintenanceTab"; // REQ-1466
import { BillingTab } from "../components/admin/BillingTab"; // REQ-1469
import { McpServerTab } from "../components/admin/McpServerTab";
import { OrgsTab } from "../components/admin/OrgsTab";
import { AiModelsTab } from "../components/admin/AiModelsTab";
import { PackageRegistriesTab } from "../components/admin/PackageRegistriesTab";
import { ContainerRegistriesTab } from "../components/admin/ContainerRegistriesTab";
import { SparkClusterTab } from "../components/admin/SparkClusterTab";
import { ImportTab } from "../components/admin/ImportTab";
import { TagsTab } from "../components/admin/TagsTab";
import { ClassificationKindsTab } from "../components/admin/ClassificationKindsTab";
import { ReportsTab } from "../components/admin/ReportsTab";
import { SecurityManager } from "../components/admin/SecurityManager";
import { SecretsTab, MySecretsTab } from "../components/admin/SecretsTab";
import { EmailTab } from "../components/admin/EmailTab";
import { OrgRegionsTab } from "../components/admin/OrgRegionsTab"; // REQ-1922
import { PlatformRegionsTab } from "../components/admin/PlatformRegionsTab"; // REQ-1922
import { SettingsCatalogPanel } from "../components/admin/SettingField";
import { PageLoading } from "../components/PageLoading";
import { CollapsibleItem, Collapsibles } from "../components/collapsible/Collapsible";

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
  "/admin/platform": "Platform", // REQ-1922: the platform's regions
  "/admin/regions": "Regions", // REQ-1922: the org's stores and regions
  "/admin/billing": "Billing", // REQ-1469: plan, current bill, next charge
  "/admin/system-health": "Dashboard",
  // REQ-1008: MCP status is read-only deployment health, so it sits on the dashboard rather than
  // owning a nav entry of its own; the old route still deep-links there.
  "/admin/mcp-server": "Dashboard",
  "/admin/observability": "Observability",
  "/admin/platform-observability": "Platform Observability", // REQ-1327: the deployment's telemetry
  "/admin/orgs": "Orgs",
  "/admin/ai-models": "AI Models",
  "/admin/package-registries": "Package Registries", // REQ-1984: what Node installs go through
  "/admin/container-registries": "Container Registries", // REQ-2098: where built images go
  "/admin/spark-cluster": "Spark Cluster", // REQ-2225: where a Spark script runs by default
  "/admin/import": "Import", // REQ-1483: Hasura v2 / DDN import
  "/admin/tags": "Tags",
  "/admin/classification-kinds": "Classification", // REQ-2147: the profiler's classification kinds
  "/admin/reports": "Reports", // REQ-1386: ops-domain management report viewer
  // Consolidated Security area — posture, encryption, auth, and the secrets service as sub-tabs.
  // Legacy routes deep-link to the matching sub-tab.
  "/admin/security": "Security",
  "/admin/encryption": "Security",
  "/admin/auth": "Security",
  // REQ-1560: one vault per surface, each named for whose it is.
  "/admin/secrets": "Org Secrets",
  "/admin/my-secrets": "Your Secrets",
};

// Which Security sub-tab a route opens on.
const SECURITY_SUBTAB: Record<string, "encryption" | "authentication"> = {
  "/admin/encryption": "encryption",
  "/admin/auth": "authentication",
};

// REQ-2277: the tabs that are list pages draw their own header (title, filter, add).
const OWN_HEADER: ReadonlySet<string> = new Set([
  "Tags",
  "Classification",
  "Org Secrets",
  "Your Secrets",
  "Package Registries",
  "Container Registries",
  "Reports",
  "Orgs",
  "Triggers",
  "Domains",
  "Platform",
  "Environments",
]);

/** Admin overview page — dashboard, config management, platform settings. */
export function AdminPage() {
  const { t } = useTranslation();
  const location = useLocation();
  const activeTab = ROUTE_TO_SECTION[location.pathname] ?? "Dashboard";
  const { capabilities } = useAuth();
  // REQ-1337: administering orgs other than the active one is the `cross_org` RIGHT. "platform_admin"
  // was never a capability — it was a role id folded in as a pseudo-right, which this rule forbids.
  const canAdministerAllOrgs = capabilities.includes("cross_org");
  const [stats, setStats] = useState<Record<string, number>>({});
  const [loading, setLoading] = useState(true);
  // REQ-1349: the settings this page still reads. The cards that EDIT them moved to the tabs whose
  // subject they are (settingsCards.tsx); what is left is the domain mode, which decides whether
  // the Domains tab shows a domain catalog at all.
  const [settings, setSettings] = useState<PlatformSettings | null>(null);

  // Apollo hooks for cache-and-network queries and mutations
  const { sources, loading: sourcesLoading } = useSources();
  const { domains, loading: domainsLoading } = useDomains();
  const { tables, loading: tablesLoading } = useTables({ columns: false });
  const { relationships, loading: relsLoading } = useRelationships();
  const { rlsRules, loading: rlsLoading } = useRLSRules();
  const { roles } = useRoles();
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

  if (loading) return <PageLoading message={t("adminPage.loading")} />;
  const adminTitle = `${t("adminPage.title")} — ${activeTab}`;

  return (
    <div className="page">
      {/* Reads as a breadcrumb: the area, then the section within it. A tab that is a list page
          draws the header itself, with its filter and its add (REQ-2277). */}
      {!OWN_HEADER.has(activeTab) && (
        <div className="page-header">
          <PageTitle icon={Settings}>{adminTitle}</PageTitle>
        </div>
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
            <Collapsibles id="admin.dashboard.mcp-server" testId="mcp-server-panel">
              <CollapsibleItem value="mcp" defaultOpen={false} title={t("navBar.itemMcpServer")}>
                <McpServerTab />
              </CollapsibleItem>
            </Collapsibles>
          </>
        )}

        {activeTab === "Environments" && <EnvironmentsTab title={adminTitle} />}

        {activeTab === "Platform" && <PlatformRegionsTab title={adminTitle} />}

        {activeTab === "Regions" && <OrgRegionsTab />}

        {activeTab === "Domains" && (
          <DomainsTab title={adminTitle} domainsEnabled={domainsEnabled} />
        )}
        {activeTab === "Cache" && <CacheManager />}
        {activeTab === "Triggers" && <ScheduledTasks title={adminTitle} />}
        {activeTab === "Federation" && <FederationEngineTab />}
        {activeTab === "Org Engine" && <OrgEngineTab />}
        {activeTab === "Email" && <EmailTab />}
        {activeTab === "Maintenance" && <MaintenanceTab />}
        {activeTab === "Deployment settings" && <SettingsCatalogPanel />}
        {activeTab === "Billing" && <BillingTab />}
        {activeTab === "Security" && (
          <SecurityManager initialTab={SECURITY_SUBTAB[location.pathname]} />
        )}
        {activeTab === "Org Secrets" && <SecretsTab title={adminTitle} />}
        {activeTab === "Your Secrets" && <MySecretsTab title={adminTitle} />}
        {activeTab === "Observability" && <ObservabilityTab />}
        {activeTab === "Platform Observability" && settings && (
          <PlatformObservabilityTab settings={settings} setSettings={setSettings} />
        )}
        {activeTab === "Orgs" && canAdministerAllOrgs && <OrgsTab title={adminTitle} />}
        {activeTab === "AI Models" && <AiModelsTab />}
        {activeTab === "Package Registries" && <PackageRegistriesTab title={adminTitle} />}
        {activeTab === "Container Registries" && <ContainerRegistriesTab title={adminTitle} />}
        {activeTab === "Spark Cluster" && <SparkClusterTab />}
        {activeTab === "Import" && <ImportTab />}
        {activeTab === "Tags" && <TagsTab title={adminTitle} />}
        {activeTab === "Classification" && <ClassificationKindsTab title={adminTitle} />}
        {activeTab === "Reports" && <ReportsTab title={adminTitle} />}
      </Stack>
    </div>
  );
}
