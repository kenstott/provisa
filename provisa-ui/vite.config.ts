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
import { parse } from "graphql";
import type { OperationDefinitionNode, FragmentDefinitionNode } from "graphql";
import istanbul from "vite-plugin-istanbul";
import path from "path";
import fs from "fs";
import _monacoEditorPluginModule from "vite-plugin-monaco-editor";
type MonacoPluginFactory = (...args: unknown[]) => Plugin;
const monacoEditorPlugin: MonacoPluginFactory =
  (_monacoEditorPluginModule as { default?: MonacoPluginFactory }).default ??
  (_monacoEditorPluginModule as unknown as MonacoPluginFactory);

const graphqlLoader = (): Plugin => {
  const cache = new Map<string, string>();

  return {
    name: "graphql-module-loader",
    enforce: "pre",

    resolveId(id: string, importer: string | undefined) {
      if (id.endsWith(".graphql") || id.endsWith(".gql")) {
        const resolved = importer
          ? path.resolve(path.dirname(importer), id)
          : path.resolve(process.cwd(), id);
        return resolved;
      }
      return undefined;
    },

    load(id: string) {
      if (id.endsWith(".graphql") || id.endsWith(".gql")) {
        if (cache.has(id)) return cache.get(id)!;

        const content = fs.readFileSync(id, "utf-8");
        const ast = parse(content);

        const lines: string[] = [
          `import { parse } from 'graphql';`,
          `const _source = \`${content.replace(/`/g, "\\`").replace(/\$/g, "\\$")}\`;`,
          `const _doc = parse(_source);`,
        ];

        for (const def of ast.definitions) {
          const named = def as OperationDefinitionNode | FragmentDefinitionNode;
          if (named.name?.value) {
            const name = named.name.value;
            lines.push(
              `export const ${name} = {`,
              `  ...(_doc),`,
              `  definitions: _doc.definitions.filter(`,
              `    d => !('name' in d) || d.name?.value === '${name}'`,
              `  )`,
              `};`,
            );
          }
        }

        lines.push(`export default _doc;`);
        const result = lines.join("\n");
        cache.set(id, result);
        return result;
      }
      return undefined;
    },
  };
};

export default defineConfig((config) => ({
  plugins: [
    react(),
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
      "/health": "http://127.0.0.1:8000",
      "/setup": "http://127.0.0.1:8000",
      "/auth": "http://127.0.0.1:8000",
    },
  },
}));
