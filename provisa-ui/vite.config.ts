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

export default defineConfig((config) => ({
  plugins: [
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
    port: 3000,
    watch: {
      // macOS creates binary ._* AppleDouble files on exFAT volumes — exclude from watching
      ignored: ["**/._*"],
    },
    proxy: {
      "/data": "http://127.0.0.1:8000",
      "/admin": {
        target: "http://127.0.0.1:8000",
        bypass(req) {
          // Page navigations (Accept: text/html) are SPA routes — serve index.html
          if (req.headers.accept?.includes("text/html")) return "/index.html";
        },
      },
      "/query": {
        target: "http://127.0.0.1:8000",
        bypass(req) {
          // Page navigations (Accept: text/html) are SPA routes — serve index.html
          if (req.headers.accept?.includes("text/html")) return "/index.html";
        },
      },
      "/health": "http://127.0.0.1:8000",
      "/setup": "http://127.0.0.1:8000",
      "/auth": "http://127.0.0.1:8000",
    },
  },
}));
