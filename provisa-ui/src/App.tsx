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
import { BrowserRouter, Routes, Route, Navigate, useLocation, useSearchParams } from "react-router-dom";
import { ApolloProvider } from "@apollo/client/react";
import { client } from "./apolloClient";
import { AuthProvider } from "./context/AuthContext";
import { DomainFilterProvider } from "./context/DomainFilterContext";
import { NavBar } from "./components/NavBar";
import { CapabilityGate } from "./components/CapabilityGate";
import { fetchSetupStatus } from "./api/setup";
import { TourProvider, useTour, hasSeenTour } from "./tour/useTour";
import "./App.css";

const SourcesPage = lazy(() => import("./pages/SourcesPage").then((m) => ({ default: m.SourcesPage })));
const TablesPage = lazy(() => import("./pages/TablesPage").then((m) => ({ default: m.TablesPage })));
const RelationshipsPage = lazy(() => import("./pages/RelationshipsPage").then((m) => ({ default: m.RelationshipsPage })));
const SecurityRolesPage = lazy(() => import("./pages/SecurityPage").then((m) => ({ default: m.SecurityRolesPage })));
const SecurityRlsPage = lazy(() => import("./pages/SecurityPage").then((m) => ({ default: m.SecurityRlsPage })));
const QueryPage = lazy(() => import("./pages/QueryPage").then((m) => ({ default: m.QueryPage })));
const AdminPage = lazy(() => import("./pages/AdminPage").then((m) => ({ default: m.AdminPage })));
const CommandsPage = lazy(() => import("./pages/CommandsPage").then((m) => ({ default: m.CommandsPage })));
const LineagePage = lazy(() => import("./pages/LineagePage").then((m) => ({ default: m.LineagePage }))); // REQ-1160/1161
const ViewsPage = lazy(() => import("./pages/ViewsPage").then((m) => ({ default: m.ViewsPage })));
const RequestsPage = lazy(() => import("./pages/RequestsPage").then((m) => ({ default: m.RequestsPage })));
const DocsPage = lazy(() => import("./pages/DocsPage").then((m) => ({ default: m.DocsPage })));
const LoginPage = lazy(() => import("./pages/LoginPage").then((m) => ({ default: m.LoginPage })));
const GraphPage = lazy(() => import("./pages/GraphPage").then((m) => ({ default: m.GraphPage })));
const SqlPage = lazy(() => import("./pages/SqlPage").then((m) => ({ default: m.SqlPage })));
const SetupPage = lazy(() => import("./pages/SetupPage").then((m) => ({ default: m.SetupPage })));
const SchemaExplorer = lazy(() => import("./pages/SchemaExplorer").then((m) => ({ default: m.SchemaExplorer })));
const NlPage = lazy(() => import("./pages/NlPage").then((m) => ({ default: m.NlPage })));
const GrpcPage = lazy(() => import("./pages/GrpcPage").then((m) => ({ default: m.GrpcPage })));
const JsonApiPage = lazy(() => import("./pages/JsonApiPage").then((m) => ({ default: m.JsonApiPage })));
const OpenApiPage = lazy(() => import("./pages/OpenApiPage").then((m) => ({ default: m.OpenApiPage })));
const McpExplorePage = lazy(() =>
  import("./pages/McpExplorePage").then((m) => ({ default: m.McpExplorePage })),
);

// Route chunks are code-split (lazy above), so the first visit to each surface pays a chunk
// fetch/parse — worst on the Query/SQL/gRPC editors, which pull Monaco (multi-MB). Warm every
// page chunk on idle after first paint so navigation is instant. Vite dedupes modules by
// resolved id, so these glob loaders hit the SAME chunks as the lazy() imports above (and drag
// Monaco in via the editor pages). Lazy by default — the functions trigger the import when called.
const PAGE_CHUNK_LOADERS = import.meta.glob("./pages/*.tsx");

function prefetchPageChunksOnIdle(): () => void {
  const schedule =
    typeof window.requestIdleCallback === "function"
      ? window.requestIdleCallback
      : (cb: () => void) => window.setTimeout(cb, 200);
  const cancel =
    typeof window.cancelIdleCallback === "function" ? window.cancelIdleCallback : window.clearTimeout;
  const handle = schedule(() => {
    for (const load of Object.values(PAGE_CHUNK_LOADERS)) void load();
  });
  return () => cancel(handle as number);
}

const AUTH_ENABLED = import.meta.env.VITE_AUTH_ENABLED === "true";

function NotAuthorized() {
  return <div className="page">You do not have permission to view this page.</div>;
}

/**
 * Auto-launches the guided tour when the server is in demo mode (first visit
 * only) or when the URL carries `?tour=1`, which always starts it and then
 * strips the param so a refresh doesn't relaunch.
 */
function TourAutoStart({ demoMode }: { demoMode: boolean }) {
  const { startTour } = useTour();
  const [searchParams, setSearchParams] = useSearchParams();
  const tourParam = searchParams.get("tour");
  useEffect(() => {
    if (tourParam !== null) {
      setSearchParams(
        (p) => {
          const n = new URLSearchParams(p);
          n.delete("tour");
          return n;
        },
        { replace: true },
      );
      // ?tour=1 always starts fresh from the top.
      startTour({ restart: true });
      return;
    }
    if (demoMode && !hasSeenTour()) startTour({ restart: true });
  }, [tourParam, demoMode, startTour, setSearchParams]);
  return null;
}

/** Redirects to /login when auth is enabled and no token is present. */
function RequireAuth({ children }: { children: React.ReactNode }) {
  const location = useLocation();
  const token = localStorage.getItem("provisa_token");
  if (AUTH_ENABLED && !token && location.pathname !== "/login") {
    return <Navigate to="/login" replace />;
  }
  return <>{children}</>;
}

function App() {
  const [, setAuthVersion] = useState(0);
  const handleLoginSuccess = useCallback(() => {
    setAuthVersion((v) => v + 1);
  }, []);
  const [setupChecked, setSetupChecked] = useState(false);
  const [needsSetup, setNeedsSetup] = useState(false);
  const [demoMode, setDemoMode] = useState(false);
  const [setupError, setSetupError] = useState<string | null>(null);

  useEffect(() => {
    fetchSetupStatus()
      .then(({ needs_setup, demo_mode }) => {
        setNeedsSetup(needs_setup);
        setDemoMode(demo_mode);
        setSetupChecked(true);
      })
      .catch((err: unknown) => {
        setSetupError(err instanceof Error ? err.message : String(err));
      });
  }, []);

  // Prewarm all route chunks (incl. Monaco) once the app is idle, so the first navigation to
  // each Explore surface is instant instead of blocking on a lazy chunk load.
  useEffect(() => prefetchPageChunksOnIdle(), []);

  return (
    <BrowserRouter>
      <ApolloProvider client={client}>
        <AuthProvider>
          {setupError ? (
            <div className="page">
              <p>Could not reach the Provisa API.</p>
              <p>{setupError}</p>
            </div>
          ) : !setupChecked ? (
            <div className="page"><p>Loading...</p></div>
          ) : needsSetup ? (
            <Suspense fallback={<div className="page"><p>Loading...</p></div>}>
              <Routes>
                <Route path="*" element={
                  <SetupPage onSetupComplete={() => { setNeedsSetup(false); }} />
                } />
              </Routes>
            </Suspense>
          ) : (
            <DomainFilterProvider>
              <RequireAuth>
                <TourProvider>
                <TourAutoStart demoMode={demoMode} />
                <NavBar />
                <main>
                <Suspense fallback={<div className="page"><p>Loading...</p></div>}>
                <Routes>
                  <Route path="/" element={<Navigate to="/query" replace />} />
                  <Route
                    path="/login"
                    element={
                      <LoginPage
                        onLoginSuccess={handleLoginSuccess}
                        authDisabled={!AUTH_ENABLED}
                      />
                    }
                  />
                  <Route
                    path="/register"
                    element={
                      <LoginPage
                        onLoginSuccess={handleLoginSuccess}
                        authDisabled={!AUTH_ENABLED}
                      />
                    }
                  />
                  <Route path="/setup" element={<Navigate to="/" replace />} />
                  <Route
                    path="/sources"
                    element={
                      <CapabilityGate capability="source_registration" fallback={<NotAuthorized />}>
                        <SourcesPage />
                      </CapabilityGate>
                    }
                  />
                  <Route
                    path="/tables"
                    element={
                      <CapabilityGate capability="table_registration" fallback={<NotAuthorized />}>
                        <TablesPage />
                      </CapabilityGate>
                    }
                  />
                  <Route
                    path="/relationships"
                    element={
                      <CapabilityGate capability="create_relationship" fallback={<NotAuthorized />}>
                        <RelationshipsPage />
                      </CapabilityGate>
                    }
                  />
                  <Route path="/security" element={<Navigate to="/security/roles" replace />} />
                  <Route
                    path="/security/roles"
                    element={
                      <CapabilityGate capability="access_config" fallback={<NotAuthorized />}>
                        <SecurityRolesPage />
                      </CapabilityGate>
                    }
                  />
                  <Route
                    path="/security/rls"
                    element={
                      <CapabilityGate capability="access_config" fallback={<NotAuthorized />}>
                        <SecurityRlsPage />
                      </CapabilityGate>
                    }
                  />
                  <Route
                    path="/query"
                    element={
                      <CapabilityGate capability="query_development" fallback={<NotAuthorized />}>
                        <QueryPage />
                      </CapabilityGate>
                    }
                  />
                  <Route
                    path="/schema"
                    element={
                      <CapabilityGate capability="query_development" fallback={<NotAuthorized />}>
                        <Suspense fallback={<div className="page">Loading schema explorer...</div>}>
                          <SchemaExplorer />
                        </Suspense>
                      </CapabilityGate>
                    }
                  />
                  <Route
                    path="/graph"
                    element={
                      <CapabilityGate capability="query_development" fallback={<NotAuthorized />}>
                        <GraphPage />
                      </CapabilityGate>
                    }
                  />
                  <Route
                    path="/sql"
                    element={
                      <CapabilityGate capability="query_development" fallback={<NotAuthorized />}>
                        <SqlPage />
                      </CapabilityGate>
                    }
                  />
                  <Route
                    path="/nl"
                    element={
                      <CapabilityGate capability="query_development" fallback={<NotAuthorized />}>
                        <NlPage />
                      </CapabilityGate>
                    }
                  />
                  <Route
                    path="/grpc"
                    element={
                      <CapabilityGate capability="query_development" fallback={<NotAuthorized />}>
                        <GrpcPage />
                      </CapabilityGate>
                    }
                  />
                  <Route
                    path="/jsonapi"
                    element={
                      <CapabilityGate capability="query_development" fallback={<NotAuthorized />}>
                        <JsonApiPage />
                      </CapabilityGate>
                    }
                  />
                  <Route
                    path="/openapi"
                    element={
                      <CapabilityGate capability="query_development" fallback={<NotAuthorized />}>
                        <OpenApiPage />
                      </CapabilityGate>
                    }
                  />
                  <Route
                    path="/explore"
                    element={
                      <CapabilityGate capability="query_development" fallback={<NotAuthorized />}>
                        <McpExplorePage />
                      </CapabilityGate>
                    }
                  />
                  <Route
                    path="/views"
                    element={
                      <CapabilityGate capability="table_registration" fallback={<NotAuthorized />}>
                        <ViewsPage />
                      </CapabilityGate>
                    }
                  />
                  <Route
                    path="/commands"
                    element={
                      <CapabilityGate capability="admin" fallback={<NotAuthorized />}>
                        <CommandsPage />
                      </CapabilityGate>
                    }
                  />
                  <Route path="/actions" element={<Navigate to="/commands" replace />} />
                  <Route
                    path="/lineage"
                    element={
                      <CapabilityGate capability="admin" fallback={<NotAuthorized />}>
                        <LineagePage />
                      </CapabilityGate>
                    }
                  />

                  <Route
                    path="/admin/requests"
                    element={
                      <CapabilityGate capability="admin" fallback={<NotAuthorized />}>
                        <RequestsPage />
                      </CapabilityGate>
                    }
                  />
                  {/* Docs reader — ungated, available to every role (bundled + live fallback) */}
                  <Route path="/docs" element={<DocsPage />} />
                  <Route path="/admin" element={<Navigate to="/admin/overview" replace />} />
                  {[
                    "/admin/overview",
                    "/admin/domains",
                    "/admin/cache",
                    "/admin/scheduled-tasks",
                    "/admin/federation-engine",
                    "/admin/encryption",
                    "/admin/auth",
                    "/admin/system-health",
                    "/admin/observability",
                    "/admin/mcp-server",
                    "/admin/local-users",
                    "/admin/orgs",
                    "/admin/ai-models",
                    "/admin/security",
                  ].map((path) => (
                    <Route
                      key={path}
                      path={path}
                      element={
                        <CapabilityGate capability="admin" fallback={<NotAuthorized />}>
                          <AdminPage />
                        </CapabilityGate>
                      }
                    />
                  ))}
                </Routes>
                </Suspense>
              </main>
                </TourProvider>
              </RequireAuth>
            </DomainFilterProvider>
          )}
      </AuthProvider>
    </ApolloProvider>
    </BrowserRouter>
  );
}

export default App;
