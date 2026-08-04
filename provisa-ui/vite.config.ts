// Copyright (c) 2026 Kenneth Stott
// Canary: 4c7847ac-ac55-49f8-92ba-8cf2718d7c6a
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { defineConfig } from "vite";
import type { Plugin } from "vite";
import { Agent as HttpAgent, request as httpRequest } from "http";
import react from "@vitejs/plugin-react";
import istanbul from "vite-plugin-istanbul";
import path from "path";
import { graphqlLoader } from "./src/plugins/graphql-loader";
import _monacoEditorPluginModule from "vite-plugin-monaco-editor";
type MonacoPluginFactory = (...args: unknown[]) => Plugin;
const monacoEditorPlugin: MonacoPluginFactory =
  (_monacoEditorPluginModule as { default?: MonacoPluginFactory }).default ??
  (_monacoEditorPluginModule as unknown as MonacoPluginFactory);

// The in-app Docs reader iframes the bundled MkDocs site at /docs-site/ for its
// offline mode. MkDocs uses directory URLs (/docs-site/, /docs-site/security/),
// but the vite dev server has no directory-index resolution, so those requests
// fall through to the SPA fallback and render index.html instead of the docs.
// Rewrite trailing-slash /docs-site/ URLs to their index.html so vite's static
// handler serves the real page — mirroring ui_server.py's production behaviour.
function serveOfflineDocsSite(): Plugin {
  return {
    name: "serve-offline-docs-site",
    configureServer(server) {
      server.middlewares.use((req, _res, next) => {
        if (req.url?.startsWith("/docs-site/")) {
          const [p, q] = req.url.split("?");
          if (p.endsWith("/")) req.url = `${p}index.html${q ? `?${q}` : ""}`;
        }
        next();
      });
    },
  };
}

// REQ-1363: isolated e2e harness must not share ports with the interactive dev
// environment (start-ui.sh). Defaults preserve current dev behavior; Playwright's
// config overrides both via env vars to run on collision-proof ports.
const DEV_SERVER_PORT = Number(process.env.PROVISA_UI_PORT ?? 3000);
const BACKEND_TARGET = `http://127.0.0.1:${process.env.PROVISA_API_PORT ?? 8001}`;

// e2e only: the harness runs one backend process per Playwright worker (see playwright.config.ts
// — `state.schema_version` is process-global and the UI resets its Apollo store whenever it
// changes, so workers sharing a backend blank each other's queries mid-assertion). One Vite dev
// server still serves them all, so each proxied request has to be routed to the backend belonging
// to the worker that made it. e2e/coverage.ts stamps `x-e2e-worker` on every request via
// extraHTTPHeaders; this reads it back.
//
// Empty outside the harness, which leaves every proxy entry on BACKEND_TARGET exactly as before.
const E2E_BACKEND_PORTS = (process.env.PROVISA_E2E_BACKEND_PORTS ?? "")
  .split(",")
  .filter(Boolean);

// Same prefixes `server.proxy` below forwards to the backend. Kept as one list so the two cannot
// drift into disagreeing about what is an API call.
const BACKEND_PREFIXES = ["/data", "/admin", "/query", "/health", "/setup", "/auth/"];

/** Routes e2e API traffic to the calling worker's own backend.
 *
 * Installed as a plugin middleware rather than by picking a target inside `server.proxy`'s
 * `bypass`: vite stores the proxy options as `[proxy, {...opts}]` (a shallow copy) and hands
 * `bypass` the copy, so assigning `opts.target` there changes nothing — the ProxyServer was
 * constructed over the original object. A `configureServer` middleware runs before every internal
 * middleware including the proxy, so it can claim these requests outright.
 *
 * Requests with no `x-e2e-worker` header get 421 Misdirected Request rather than a default
 * backend: silently sending them to worker 0 is exactly the shared-backend condition this exists
 * to eliminate, and it would resurface as an unrelated assertion failure in some other spec.
 */
// keepAlive: false is required, not a tuning choice. Node's http.globalAgent pools sockets with
// keepAlive on (keepAliveMsecs 1000, timeout 5000) and uvicorn closes idle connections at its own
// 5 s --timeout-keep-alive. The two deadlines coincide, so a pooled socket the agent believes is
// reusable is being closed by the backend at the same instant the next request is written to it:
// the write fails with ECONNRESET and the error handler below turns it into a 502. In the browser
// that 502 lands on /admin/graphql, errors the Apollo query, and useTables() then returns an empty
// list for the rest of the page's life (cache-and-network never retries an errored query) — which
// is how a spec ends up staring at a table list or fact picker that never populates. A fresh
// connection per request cannot hit the race; the cost is irrelevant on loopback in a test harness.
const e2eProxyAgent = new HttpAgent({ keepAlive: false });

function e2eWorkerBackendRouter(): Plugin {
  return {
    name: "provisa-e2e-worker-backend-router",
    apply: "serve",
    configureServer(server) {
      if (E2E_BACKEND_PORTS.length === 0) return;
      server.middlewares.use((req, res, next) => {
        const url = req.url ?? "";
        if (!BACKEND_PREFIXES.some((p) => url.startsWith(p))) return next();
        // Page navigations are SPA routes — vite's own proxy bypass serves index.html for them.
        if (req.headers.accept?.includes("text/html")) return next();
        const header = req.headers["x-e2e-worker"];
        const port = typeof header === "string" ? E2E_BACKEND_PORTS[Number(header)] : undefined;
        if (!port) {
          res.writeHead(421, { "Content-Type": "text/plain" });
          res.end(
            `x-e2e-worker header ${JSON.stringify(header ?? null)} names no backend in ` +
              `${E2E_BACKEND_PORTS.join(",")}; every e2e request must carry it ` +
              `(provisa-ui/e2e/coverage.ts).`,
          );
          return;
        }
        const upstream = httpRequest(
          {
            host: "127.0.0.1",
            port: Number(port),
            method: req.method,
            path: url,
            agent: e2eProxyAgent,
            headers: { ...req.headers, host: `127.0.0.1:${port}`, connection: "close" },
          },
          (upstreamRes) => {
            res.writeHead(upstreamRes.statusCode ?? 502, upstreamRes.headers);
            upstreamRes.pipe(res);
          },
        );
        upstream.on("error", (err) => {
          if (!res.headersSent) res.writeHead(502, { "Content-Type": "text/plain" });
          res.end(`e2e worker ${header} backend :${port} — ${err.message}`);
        });
        req.pipe(upstream);
      });
    },
  };
}

export default defineConfig((config) => ({
  plugins: [
    e2eWorkerBackendRouter(),
    react(),
    serveOfflineDocsSite(),
    graphqlLoader(),
    monacoEditorPlugin({
      languageWorkers: ["editorWorkerService", "json"],
      customWorkers: [{ label: "graphql", entry: "monaco-graphql/esm/graphql.worker" }],
    }),
    ...(config.mode !== "production"
      ? [
          istanbul({
            include: "src/**/*",
            exclude: [
              "node_modules",
              "e2e/**",
              "src/plugins/graphiql-explorer-fork.cjs",
              "src/plugins/table-view.tsx",
            ],
            extension: [".ts", ".tsx"],
          }),
        ]
      : []),
  ],
  resolve: {
    alias: {
      "graphiql-explorer": path.resolve(__dirname, "src/plugins/graphiql-explorer-fork.cjs"),
      "@neo4j-cypher/codemirror/lib/cypher-state-definitions": path.resolve(
        __dirname,
        "node_modules/@neo4j-cypher/codemirror/lib/cypher-state-definitions.js",
      ),
    },
  },
  optimizeDeps: {
    // graphiql-explorer is aliased to src/plugins/graphiql-explorer-fork.cjs (CJS → needs pre-bundle)
    include: ["graphiql-explorer", "picomatch-browser", "lodash.includes", "lodash.find"],
    // rolldown (Vite 8) rejects internal ./lib/ sub-path imports not listed in package exports
    // Apollo Client v4 has issues with pre-bundling, exclude it
    exclude: ["@neo4j-cypher/codemirror", "@apollo/client"],
  },
  build: {
    chunkSizeWarningLimit: 6000,
    rollupOptions: {
      // REQ-1348: two HTML entries. `auth-relay.html` is the control plane's cross-subdomain
      // token endpoint — it must be emitted to the dist root so ui_server's static handler
      // serves it directly instead of falling through to the SPA's index.html. Naming any
      // input at all turns off Vite's implicit index.html entry, so index must be listed too.
      input: {
        index: path.resolve(__dirname, "index.html"),
        "auth-relay": path.resolve(__dirname, "auth-relay.html"),
      },
      output: {
        codeSplitting: true,
        manualChunks(id) {
          if (id.includes("node_modules/mermaid")) return "vendor-mermaid";
          if (id.includes("node_modules/firebase")) return "vendor-firebase";
          if (id.includes("node_modules/@mui") || id.includes("node_modules/@emotion"))
            return "vendor-mui";
          if (id.includes("node_modules/cytoscape")) return "vendor-cytoscape";
          if (
            id.includes("node_modules/monaco-editor") ||
            id.includes("node_modules/monaco-graphql") ||
            id.includes("node_modules/@uiw/react-codemirror") ||
            id.includes("node_modules/@codemirror")
          )
            return "vendor-monaco";
          if (id.includes("node_modules/@apollo")) return "vendor-apollo";
          if (
            id.includes("node_modules/react") ||
            id.includes("node_modules/react-dom") ||
            id.includes("node_modules/react-router-dom")
          )
            return "vendor-react";
        },
      },
    },
  },
  server: {
    port: DEV_SERVER_PORT,
    // Node's "localhost" default resolves to the IPv6 loopback ([::1]) first on this platform, but
    // Chromium's --host-resolver-rules below maps the e2e hostnames to the IPv4 loopback — bind
    // explicitly to it so those origins can actually connect.
    host: "127.0.0.1",
    // REQ-1348: cross-subdomain sign-in only exists between two hosts under one base domain, so it
    // cannot be exercised on `localhost` — the relay's `isSiblingOrigin` check needs a real base
    // domain to compare. The e2e maps `*.provisa.test` and `*.example.test` (the non-sibling case)
    // to 127.0.0.1 with Chromium's host-resolver-rules; Vite rejects a Host it was not told to
    // serve, so both zones are named here. `.test` is reserved (RFC 6761) and resolves nowhere.
    allowedHosts: [".provisa.test", ".example.test"],
    watch: {
      // macOS creates binary ._* AppleDouble files on exFAT volumes — exclude from watching
      ignored: ["**/._*"],
    },
    proxy: {
      "/data": BACKEND_TARGET,
      "/admin": {
        target: BACKEND_TARGET,
        bypass(req) {
          // Page navigations (Accept: text/html) are SPA routes — serve index.html
          if (req.headers.accept?.includes("text/html")) return "/index.html";
        },
      },
      "/query": {
        target: BACKEND_TARGET,
        bypass(req) {
          // Page navigations (Accept: text/html) are SPA routes — serve index.html
          if (req.headers.accept?.includes("text/html")) return "/index.html";
        },
      },
      "/health": BACKEND_TARGET,
      "/setup": BACKEND_TARGET,
      // REQ-1348: the trailing slash matters. Vite matches a proxy key as a path PREFIX, so a bare
      // "/auth" also captured "/auth-relay.html" — the control plane's cross-subdomain token
      // endpoint — and forwarded it to the API, which has no such route. Every auth endpoint lives
      // under /auth/ (auth_router's prefix + a path), so the relay is the only thing this excludes.
      "/auth/": BACKEND_TARGET,
    },
  },
}));
