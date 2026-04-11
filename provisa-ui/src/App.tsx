// Copyright (c) 2026 Kenneth Stott
// Canary: 56f95443-fb45-4d9f-94bd-13ad316b8806
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { lazy, Suspense, useState, useCallback } from "react";
import { BrowserRouter, Routes, Route, Navigate, useLocation } from "react-router-dom";
import { AuthProvider } from "./context/AuthContext";
import { NavBar } from "./components/NavBar";
import { CapabilityGate } from "./components/CapabilityGate";
import { SourcesPage } from "./pages/SourcesPage";
import { TablesPage } from "./pages/TablesPage";
import { RelationshipsPage } from "./pages/RelationshipsPage";
import { SecurityPage } from "./pages/SecurityPage";
import { QueryPage } from "./pages/QueryPage";
import { ApprovalsPage } from "./pages/ApprovalsPage";
import { AdminPage } from "./pages/AdminPage";
import { ViewsPage } from "./pages/ViewsPage";
import { CommandsPage } from "./pages/CommandsPage";
import { LoginPage } from "./pages/LoginPage";
import { GraphPage } from "./pages/GraphPage";
import "./App.css";

// Lazy-load SchemaExplorer — graphql-voyager requires @mui/material and browser globals
const SchemaExplorer = lazy(() =>
  import("./pages/SchemaExplorer").then((m) => ({ default: m.SchemaExplorer }))
);

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

  return (
    <BrowserRouter>
      <AuthProvider>
        <RequireAuth>
          <NavBar />
          <main>
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
                  <CapabilityGate capability="relationship_registration" fallback={<NotAuthorized />}>
                    <RelationshipsPage />
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
                path="/security"
                element={
                  <CapabilityGate capability="security_config" fallback={<NotAuthorized />}>
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
                path="/approvals"
                element={
                  <CapabilityGate capability="query_approval" fallback={<NotAuthorized />}>
                    <ApprovalsPage />
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
                path="/admin"
                element={
                  <CapabilityGate capability="admin" fallback={<NotAuthorized />}>
                    <AdminPage />
                  </CapabilityGate>
                }
              />
            </Routes>
          </main>
        </RequireAuth>
      </AuthProvider>
    </BrowserRouter>
  );
}

export default App;
