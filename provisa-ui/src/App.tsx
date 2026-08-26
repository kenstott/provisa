// Copyright (c) 2026 Kenneth Stott
// Canary: 56f95443-fb45-4d9f-94bd-13ad316b8806
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { lazy, Suspense, useState, useCallback, useEffect } from "react";
import { BrowserRouter, Routes, Route, Navigate, useLocation } from "react-router-dom";
import { ApolloProvider } from "@apollo/client/react";
import { client } from "./apolloClient";
import { AuthProvider, useAuth } from "./context/AuthContext";
import { DomainFilterProvider } from "./context/DomainFilterContext";
import { SubnavExtraProvider } from "./context/SubnavExtraContext";
import { NavBar } from "./components/NavBar";
import { AdminRail } from "./components/AdminRail";
import { NAV_GROUPS, entryItem } from "./components/navGroups";
import { CapabilityGate } from "./components/CapabilityGate";
import { CredentialCheck } from "./components/CredentialCheck";
import { PageLoading } from "./components/PageLoading";
import { OnboardGate } from "./components/OnboardGate";
import { ServerUnavailableModal } from "./components/ServerUnavailableModal";
import { EngineWakingBanner } from "./components/EngineWakingBanner";
import { TitleTooltips } from "./components/TitleTooltips";
import { PlatformAdminWelcomeModal } from "./components/PlatformAdminWelcomeModal";
import { MaintenanceBanner } from "./components/MaintenanceBanner"; // REQ-1466
import { fetchSetupStatus } from "./api/setup";
import { TourProvider } from "./tour/useTour";
import { TourAutoStart } from "./tour/TourAutoStart";
import { prefetchPageChunksOnIdle } from "./pageChunks";
import { storedToken } from "./lib/sessionToken";
import "./App.css";

const SourcesPage = lazy(() =>
  import("./pages/SourcesPage").then((m) => ({ default: m.SourcesPage })),
);
const TablesPage = lazy(() =>
  import("./pages/TablesPage").then((m) => ({ default: m.TablesPage })),
);
const RelationshipsPage = lazy(() =>
  import("./pages/RelationshipsPage").then((m) => ({ default: m.RelationshipsPage })),
);
const SecurityRolesPage = lazy(() =>
  import("./pages/SecurityPage").then((m) => ({ default: m.SecurityRolesPage })),
);
const SecurityRlsPage = lazy(() =>
  import("./pages/SecurityPage").then((m) => ({ default: m.SecurityRlsPage })),
);
const QueryPage = lazy(() => import("./pages/QueryPage").then((m) => ({ default: m.QueryPage })));
const AdminPage = lazy(() => import("./pages/AdminPage").then((m) => ({ default: m.AdminPage })));
const CommandsPage = lazy(() =>
  import("./pages/CommandsPage").then((m) => ({ default: m.CommandsPage })),
);
const LineagePage = lazy(() =>
  import("./pages/LineagePage").then((m) => ({ default: m.LineagePage })),
); // REQ-1160/1161
const ViewsPage = lazy(() => import("./pages/ViewsPage").then((m) => ({ default: m.ViewsPage })));
const MetricsPage = lazy(() =>
  import("./pages/MetricsPage").then((m) => ({ default: m.MetricsPage })),
);
const RequestsPage = lazy(() =>
  import("./pages/RequestsPage").then((m) => ({ default: m.RequestsPage })),
);
const DocsPage = lazy(() => import("./pages/DocsPage").then((m) => ({ default: m.DocsPage })));
const OrgWelcomePage = lazy(() =>
  import("./pages/OrgWelcomePage").then((m) => ({ default: m.OrgWelcomePage })),
);
const LandingPage = lazy(() =>
  import("./pages/LandingPage").then((m) => ({ default: m.LandingPage })),
);
const GraphPage = lazy(() => import("./pages/GraphPage").then((m) => ({ default: m.GraphPage })));
const SqlPage = lazy(() => import("./pages/SqlPage").then((m) => ({ default: m.SqlPage })));
const SetupPage = lazy(() => import("./pages/SetupPage").then((m) => ({ default: m.SetupPage })));
const TeamPage = lazy(() => import("./pages/TeamPage").then((m) => ({ default: m.TeamPage })));
const SchemaExplorer = lazy(() =>
  import("./pages/SchemaExplorer").then((m) => ({ default: m.SchemaExplorer })),
);
const NlPage = lazy(() => import("./pages/NlPage").then((m) => ({ default: m.NlPage })));
const GrpcPage = lazy(() => import("./pages/GrpcPage").then((m) => ({ default: m.GrpcPage })));
const JsonApiPage = lazy(() =>
  import("./pages/JsonApiPage").then((m) => ({ default: m.JsonApiPage })),
);
const OpenApiPage = lazy(() =>
  import("./pages/OpenApiPage").then((m) => ({ default: m.OpenApiPage })),
);
const McpExplorePage = lazy(() =>
  import("./pages/McpExplorePage").then((m) => ({ default: m.McpExplorePage })),
);

function NotAuthorized() {
  return <div className="page">You do not have permission to view this page.</div>;
}

/**
 * REQ-1349: `/admin` resolves to the first admin section the caller's rights admit (honouring the
 * remembered subnav item when it is still permitted), so typing the bare URL lands exactly where
 * clicking the Admin tab does. A fixed redirect to /admin/overview needed `observability` and sent
 * everyone else to NotAuthorized, which replaces the whole page including the subnav — leaving no
 * tab to click. A caller holding no admin surface at all still gets NotAuthorized.
 */
function AdminEntry() {
  const { capabilities, loading, billing } = useAuth();
  const group = NAV_GROUPS.find((g) => g.id === "admin");
  // REQ-1430: no capabilities yet during bootstrap — resolving the entry now would deny a caller
  // whose rights simply have not arrived.
  if (loading) return <CredentialCheck />;
  const target = group ? entryItem(group, capabilities, billing) : undefined;
  if (!target) return <NotAuthorized />;
  return <Navigate to={target.to} replace />;
}

/** Redirects to /login when auth is enabled and no token is present. */
function RequireAuth({ children }: { children: React.ReactNode }) {
  const location = useLocation();
  const { authEnabled } = useAuth();
  const token = storedToken();
  if (authEnabled && !token && location.pathname !== "/login") {
    return <Navigate to="/login" replace />;
  }
  return <>{children}</>;
}

// REQ-1571: /login and /register are identity routes, not product routes. Someone following an
// invite link arrives with a live session more often than not, and rendering the sign-in form
// inside the app shell put the whole navigation — Sources, Tables, Org, Env, Role — above a form
// that has not established who they are yet. These two paths render the standalone landing card,
// centered, whether or not a token is present.
export function IdentityRoutes({
  onLoginSuccess,
  authDisabled,
  children,
}: {
  onLoginSuccess: (token: string) => void;
  authDisabled: boolean;
  children: React.ReactNode;
}) {
  const { pathname } = useLocation();
  if (pathname === "/login" || pathname === "/register") {
    return (
      <Suspense fallback={<PageLoading />}>
        <LandingPage onLoginSuccess={onLoginSuccess} authDisabled={authDisabled} />
      </Suspense>
    );
  }
  return <>{children}</>;
}

function App() {
  const [authVersion, setAuthVersion] = useState(0);
  const handleLoginSuccess = useCallback(() => {
    setAuthVersion((v) => v + 1);
  }, []);
  const [setupChecked, setSetupChecked] = useState(false);
  const [needsSetup, setNeedsSetup] = useState(false);
  const [demoMode, setDemoMode] = useState(false);
  const [authEnabled, setAuthEnabled] = useState(false);
  const [multitenancy, setMultitenancy] = useState(false);
  const [setupError, setSetupError] = useState<string | null>(null);

  useEffect(() => {
    fetchSetupStatus()
      .then(({ needs_setup, demo_mode, auth_enabled, multitenancy }) => {
        setNeedsSetup(needs_setup);
        setDemoMode(demo_mode);
        setAuthEnabled(auth_enabled);
        setMultitenancy(multitenancy);
        setSetupChecked(true);
      })
      .catch((err: unknown) => {
        setSetupError(err instanceof Error ? err.message : String(err));
      });
  }, []);

  // Prewarm all route chunks (incl. Monaco) once the app is idle, so the first navigation to
  // each Explore surface is instant instead of blocking on a lazy chunk load.
  useEffect(() => prefetchPageChunksOnIdle(), []);

  // Read on every render (handleLoginSuccess bumps authVersion → re-render → re-read) so the
  // public/shell split flips the moment a token is stored. Signed-out visitors get the
  // standalone LandingPage instead of the app chrome (REQ-1266).
  const token = storedToken();

  return (
    <BrowserRouter>
      <ApolloProvider client={client}>
        {/* REQ-1291: authVersion also reaches AuthProvider, so a sign-in refetches /auth/me.
            Without it the identity resolved at mount (unauthenticated) stood forever, and
            OnboardGate read the freshly stored token as a rejected credential. */}
        <AuthProvider
          authEnabled={authEnabled}
          authSettled={setupChecked}
          authVersion={authVersion}
          multitenancy={multitenancy}
        >
          {/* REQ-1514: outside every gate below — a server that is not answering is what breaks
              setup, sign-in and the app alike, and the notice must not depend on any of them. */}
          <ServerUnavailableModal />
          {/* REQ-1520: mounted above every route so a native `title` anywhere in the app is
              painted in the app's own styling rather than the browser's. */}
          <TitleTooltips />
          <EngineWakingBanner />
          {setupError ? (
            <div className="page">
              <p>Could not reach the Provisa API.</p>
              <p>{setupError}</p>
            </div>
          ) : !setupChecked ? (
            <div className="page">
              <PageLoading />
            </div>
          ) : needsSetup ? (
            <Suspense fallback={<PageLoading />}>
              <Routes>
                <Route
                  path="*"
                  element={
                    <SetupPage
                      onSetupComplete={() => {
                        setNeedsSetup(false);
                      }}
                    />
                  }
                />
              </Routes>
            </Suspense>
          ) : authEnabled && !token ? (
            <Suspense fallback={<PageLoading />}>
              <Routes>
                <Route
                  path="*"
                  element={
                    <LandingPage onLoginSuccess={handleLoginSuccess} authDisabled={!authEnabled} />
                  }
                />
              </Routes>
            </Suspense>
          ) : (
            <IdentityRoutes onLoginSuccess={handleLoginSuccess} authDisabled={!authEnabled}>
              <OnboardGate onSessionExpired={handleLoginSuccess}>
                <DomainFilterProvider>
                  <RequireAuth>
                    <TourProvider>
                      <SubnavExtraProvider>
                        <TourAutoStart demoMode={demoMode} />
                        {/* REQ-1294: the sign-in that claims the platform-admin slot lands here; this is
                    where the user is told what they now are and how to invite anyone else. */}
                        <PlatformAdminWelcomeModal />
                        {/* REQ-1466: above the nav, on every route — planned work that replaces the
                    engine cluster (REQ-1465) fails queries everywhere, so the notice cannot be
                    the property of any one page. */}
                        <MaintenanceBanner />
                        <NavBar />
                        {/* REQ-1559: the admin rail stands beside the page rather than above it, so
                    the row is the shell for both. It renders nothing off an admin route. */}
                        <div className="app-body">
                          <AdminRail />
                          <main>
                            <Suspense fallback={<PageLoading />}>
                              <Routes>
                                <Route path="/" element={<Navigate to="/query" replace />} />
                                <Route path="/setup" element={<Navigate to="/" replace />} />
                                <Route
                                  path="/sources"
                                  element={
                                    <CapabilityGate
                                      capability="source_registration"
                                      fallback={<NotAuthorized />}
                                    >
                                      <SourcesPage />
                                    </CapabilityGate>
                                  }
                                />
                                <Route
                                  path="/tables"
                                  element={
                                    <CapabilityGate
                                      capability="table_registration"
                                      fallback={<NotAuthorized />}
                                    >
                                      <TablesPage />
                                    </CapabilityGate>
                                  }
                                />
                                <Route
                                  path="/relationships"
                                  element={
                                    <CapabilityGate
                                      capability="create_relationship"
                                      fallback={<NotAuthorized />}
                                    >
                                      <RelationshipsPage />
                                    </CapabilityGate>
                                  }
                                />
                                <Route
                                  path="/team"
                                  element={
                                    <CapabilityGate
                                      capability="user_management"
                                      fallback={<NotAuthorized />}
                                    >
                                      <TeamPage />
                                    </CapabilityGate>
                                  }
                                />
                                <Route
                                  path="/security"
                                  element={<Navigate to="/security/roles" replace />}
                                />
                                <Route
                                  path="/security/roles"
                                  element={
                                    <CapabilityGate
                                      capability="access_config"
                                      fallback={<NotAuthorized />}
                                    >
                                      <SecurityRolesPage />
                                    </CapabilityGate>
                                  }
                                />
                                <Route
                                  path="/security/rls"
                                  element={
                                    <CapabilityGate
                                      capability="access_config"
                                      fallback={<NotAuthorized />}
                                    >
                                      <SecurityRlsPage />
                                    </CapabilityGate>
                                  }
                                />
                                <Route
                                  path="/query"
                                  element={
                                    <CapabilityGate
                                      capability="query_development"
                                      fallback={<NotAuthorized />}
                                    >
                                      <QueryPage />
                                    </CapabilityGate>
                                  }
                                />
                                <Route
                                  path="/schema"
                                  element={
                                    <CapabilityGate
                                      capability="query_development"
                                      fallback={<NotAuthorized />}
                                    >
                                      <Suspense fallback={<PageLoading />}>
                                        <SchemaExplorer />
                                      </Suspense>
                                    </CapabilityGate>
                                  }
                                />
                                <Route
                                  path="/graph"
                                  element={
                                    <CapabilityGate
                                      capability="query_development"
                                      fallback={<NotAuthorized />}
                                    >
                                      <GraphPage />
                                    </CapabilityGate>
                                  }
                                />
                                <Route
                                  path="/sql"
                                  element={
                                    <CapabilityGate
                                      capability="query_development"
                                      fallback={<NotAuthorized />}
                                    >
                                      <SqlPage />
                                    </CapabilityGate>
                                  }
                                />
                                <Route
                                  path="/nl"
                                  element={
                                    <CapabilityGate
                                      capability="query_development"
                                      fallback={<NotAuthorized />}
                                    >
                                      <NlPage />
                                    </CapabilityGate>
                                  }
                                />
                                <Route
                                  path="/grpc"
                                  element={
                                    <CapabilityGate
                                      capability="query_development"
                                      fallback={<NotAuthorized />}
                                    >
                                      <GrpcPage />
                                    </CapabilityGate>
                                  }
                                />
                                <Route
                                  path="/jsonapi"
                                  element={
                                    <CapabilityGate
                                      capability="query_development"
                                      fallback={<NotAuthorized />}
                                    >
                                      <JsonApiPage />
                                    </CapabilityGate>
                                  }
                                />
                                <Route
                                  path="/openapi"
                                  element={
                                    <CapabilityGate
                                      capability="query_development"
                                      fallback={<NotAuthorized />}
                                    >
                                      <OpenApiPage />
                                    </CapabilityGate>
                                  }
                                />
                                <Route
                                  path="/explore"
                                  element={
                                    <CapabilityGate
                                      capability="query_development"
                                      fallback={<NotAuthorized />}
                                    >
                                      <McpExplorePage />
                                    </CapabilityGate>
                                  }
                                />
                                <Route
                                  path="/views"
                                  element={
                                    <CapabilityGate
                                      capability="table_registration"
                                      fallback={<NotAuthorized />}
                                    >
                                      <ViewsPage />
                                    </CapabilityGate>
                                  }
                                />
                                <Route
                                  path="/metrics"
                                  element={
                                    <CapabilityGate
                                      capability="table_registration"
                                      fallback={<NotAuthorized />}
                                    >
                                      <MetricsPage />
                                    </CapabilityGate>
                                  }
                                />
                                <Route
                                  path="/commands"
                                  element={
                                    // Commands are model artifacts authored beside views and
                                    // metrics, and the endpoints behind them require no capability
                                    // of their own — so they are gated with the rest of the Model
                                    // group. `admin` is the platform wildcard, which no org role
                                    // carries (rights.py PLATFORM_BYPASS_CAPABILITIES).
                                    <CapabilityGate
                                      capability="table_registration"
                                      fallback={<NotAuthorized />}
                                    >
                                      <CommandsPage />
                                    </CapabilityGate>
                                  }
                                />
                                <Route
                                  path="/actions"
                                  element={<Navigate to="/commands" replace />}
                                />
                                <Route
                                  path="/lineage"
                                  element={
                                    // REQ-1160: lineage reads the view registry and analyzes a
                                    // statement -- a modeling surface, gated like the rest of the
                                    // Model group. `admin` is the platform wildcard (rights.py
                                    // PLATFORM_BYPASS_CAPABILITIES), which no org role holds, so
                                    // gating it there denied the page to every org_admin while the
                                    // endpoint itself requires nothing.
                                    <CapabilityGate
                                      capability="table_registration"
                                      fallback={<NotAuthorized />}
                                    >
                                      <LineagePage />
                                    </CapabilityGate>
                                  }
                                />

                                {/* REQ-1349: approvals are the acting org's own pending creation requests. */}
                                <Route
                                  path="/admin/requests"
                                  element={
                                    <CapabilityGate
                                      capability="org_settings"
                                      fallback={<NotAuthorized />}
                                    >
                                      <RequestsPage />
                                    </CapabilityGate>
                                  }
                                />
                                {/* Docs reader — ungated, available to every role (bundled + live fallback) */}
                                {/* REQ-1276: where an org create lands, on the org's own host. */}
                                <Route path="/welcome" element={<OrgWelcomePage />} />
                                <Route path="/docs" element={<DocsPage />} />
                                <Route path="/admin" element={<AdminEntry />} />
                                {/* REQ-1337: each admin route names the RIGHT it needs, never a role name. The
                      deployment-wide settings surfaces (federation engine, cache storage,
                      encryption/auth providers) require `platform_settings`; administering other
                      orgs requires `cross_org`. The seed decides which role carries each, so a
                      multitenant org_admin cannot reach these even by typing the URL.
                      REQ-1349 adds the two org-scoped rights: `org_settings` for surfaces whose
                      subject is the acting org (its AI/NL provider, domains, scheduled tasks,
                      approvals) and `observability` for read-only performance and health. */}
                                {(
                                  [
                                    ["/admin/overview", "observability"],
                                    ["/admin/domains", "org_settings"],
                                    // REQ-1573: environments have their own right — a developer
                                    // manages them while holding no org settings at all.
                                    ["/admin/environments", "environment_management"],
                                    ["/admin/secrets", "org_settings"], // REQ-1560: the org vault
                                    // REQ-1560: a person's own vault is not a grant — `usage` is the
                                    // right every seeded role carries, so every member reaches it.
                                    ["/admin/my-secrets", "usage"],
                                    ["/admin/cache", "org_settings"],
                                    ["/admin/scheduled-tasks", "org_settings"],
                                    ["/admin/federation-engine", "platform_settings"],
                                    ["/admin/org-engine", "org_settings"], // REQ-1412
                                    ["/admin/encryption", "platform_settings"],
                                    ["/admin/auth", "platform_settings"],
                                    ["/admin/system-health", "observability"],
                                    ["/admin/observability", "observability"],
                                    ["/admin/mcp-server", "admin"],
                                    ["/admin/local-users", "admin"],
                                    ["/admin/orgs", "cross_org"],
                                    ["/admin/ai-models", "org_settings"],
                                    ["/admin/metadata-export", "org_settings"],
                                    ["/admin/import", "org_settings"], // REQ-1483
                                    ["/admin/tags", "org_settings"],
                                    ["/admin/reports", "observability"], // REQ-1386
                                    // REQ-1590: the glossary has its own read right. Reading a term
                                    // is not administering the org, so an analyst opens it; the
                                    // curation controls inside are gated on `glossary_rw`.
                                    ["/admin/glossary", "glossary_read"], // REQ-1387
                                    ["/admin/security", "platform_settings"],
                                    ["/admin/email", "platform_settings"], // REQ-1576
                                    ["/admin/maintenance", "platform_settings"], // REQ-1466
                                    ["/admin/billing", "org_settings"], // REQ-1469
                                  ] as const
                                ).map(([path, capability]) => (
                                  <Route
                                    key={path}
                                    path={path}
                                    element={
                                      <CapabilityGate
                                        capability={capability}
                                        fallback={<NotAuthorized />}
                                      >
                                        <AdminPage />
                                      </CapabilityGate>
                                    }
                                  />
                                ))}
                              </Routes>
                            </Suspense>
                          </main>
                        </div>
                      </SubnavExtraProvider>
                    </TourProvider>
                  </RequireAuth>
                </DomainFilterProvider>
              </OnboardGate>
            </IdentityRoutes>
          )}
        </AuthProvider>
      </ApolloProvider>
    </BrowserRouter>
  );
}

export default App;
