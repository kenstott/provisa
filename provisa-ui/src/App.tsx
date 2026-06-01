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
import { AuthProvider } from "./context/AuthContext";
import { DomainFilterProvider } from "./context/DomainFilterContext";
import { NavBar } from "./components/NavBar";
import { CapabilityGate } from "./components/CapabilityGate";
import { fetchSetupStatus } from "./api/setup";
import "./App.css";

const SourcesPage = lazy(() => import("./pages/SourcesPage").then((m) => ({ default: m.SourcesPage })));
const TablesPage = lazy(() => import("./pages/TablesPage").then((m) => ({ default: m.TablesPage })));
const RelationshipsPage = lazy(() => import("./pages/RelationshipsPage").then((m) => ({ default: m.RelationshipsPage })));
const SecurityPage = lazy(() => import("./pages/SecurityPage").then((m) => ({ default: m.SecurityPage })));
const QueryPage = lazy(() => import("./pages/QueryPage").then((m) => ({ default: m.QueryPage })));
const AdminPage = lazy(() => import("./pages/AdminPage").then((m) => ({ default: m.AdminPage })));
const CommandsPage = lazy(() => import("./pages/CommandsPage").then((m) => ({ default: m.CommandsPage })));
const ViewsPage = lazy(() => import("./pages/ViewsPage").then((m) => ({ default: m.ViewsPage })));
const LoginPage = lazy(() => import("./pages/LoginPage").then((m) => ({ default: m.LoginPage })));
const GraphPage = lazy(() => import("./pages/GraphPage").then((m) => ({ default: m.GraphPage })));
const SqlPage = lazy(() => import("./pages/SqlPage").then((m) => ({ default: m.SqlPage })));
const SetupPage = lazy(() => import("./pages/SetupPage").then((m) => ({ default: m.SetupPage })));
const SchemaExplorer = lazy(() => import("./pages/SchemaExplorer").then((m) => ({ default: m.SchemaExplorer })));

const AUTH_ENABLED = import.meta.env.VITE_AUTH_ENABLED === "true";

function NotAuthorized() {
  return <div className="page">You do not have permission to view this page.</div>;
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

  useEffect(() => {
    fetchSetupStatus().then(({ needs_setup }) => {
      setNeedsSetup(needs_setup);
      setSetupChecked(true);
    });
  }, []);

  return (
    <BrowserRouter>
      <ApolloProvider client={client}>
        <AuthProvider>
          {!setupChecked ? (
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
                  <Route
                    path="/security"
                    element={
                      <CapabilityGate capability="access_config" fallback={<NotAuthorized />}>
                        <SecurityPage />
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
                  <Route path="/admin" element={<Navigate to="/admin/overview" replace />} />
                  {[
                    "/admin/overview",
                    "/admin/domains",
                    "/admin/materialized-views",
                    "/admin/cache",
                    "/admin/scheduled-tasks",
                    "/admin/system-health",
                    "/admin/observability",
                    "/admin/local-users",
                    "/admin/orgs",
                    "/admin/roles",
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
              </RequireAuth>
            </DomainFilterProvider>
          )}
      </AuthProvider>
    </ApolloProvider>
    </BrowserRouter>
  );
}

export default App;
