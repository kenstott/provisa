// Copyright (c) 2026 Kenneth Stott
// Canary: 4c7847ac-ac55-49f8-92ba-8cf2718d7c6a
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { defineConfig, createLogger } from 'vite'
import react from '@vitejs/plugin-react'
import istanbul from 'vite-plugin-istanbul'
import path from 'path'
import fs from 'fs'
import _monacoEditorPluginModule from 'vite-plugin-monaco-editor'
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const monacoEditorPlugin: (...args: any[]) => any =
  (_monacoEditorPluginModule as any).default ?? _monacoEditorPluginModule

const logger = createLogger()
const origWarn = logger.warn.bind(logger)
logger.warn = (msg, opts) => {
  if (msg.includes('monaco-graphql') && msg.includes('sourcemap')) return
  origWarn(msg, opts)
}

export default defineConfig(({ mode }) => ({
  customLogger: logger,
  plugins: [
    {
      name: 'graphql-module-loader',
      resolveId(id) {
        if (id.endsWith('.graphql') || id.endsWith('.gql')) {
          return id;
        }
      },
      load(id) {
        if (id.endsWith('.graphql') || id.endsWith('.gql')) {
          const filePath = path.resolve(id);
          const content = fs.readFileSync(filePath, 'utf-8');
          const escaped = content.replace(/`/g, '\\`').replace(/\$/g, '\\$');

          // Extract operation names (query/mutation/fragment Name)
          const operations: string[] = [];
          const opRegex = /(?:query|mutation|fragment)\s+(\w+)/g;
          let match;
          const seen = new Set<string>();
          while ((match = opRegex.exec(content)) !== null) {
            if (!seen.has(match[1])) {
              operations.push(match[1]);
              seen.add(match[1]);
            }
          }

          let output = "import { parse } from 'graphql';\n";
          output += `const doc = parse(\`${escaped}\`);\n`;
          output += 'export default doc;\n';

          // Export each named operation
          operations.forEach(op => {
            output += `export const ${op} = doc;\n`;
          });

          return output;
        }
      }
    },
    react(),
    monacoEditorPlugin({
      languageWorkers: ['editorWorkerService', 'json'],
      customWorkers: [
        { label: 'graphql', entry: 'monaco-graphql/esm/graphql.worker' },
      ],
    }),
    ...(mode !== 'production'
      ? [
          istanbul({
            include: 'src/**/*',
            exclude: ['node_modules', 'e2e/**', 'src/plugins/graphiql-explorer-fork.cjs', 'src/plugins/table-view.tsx'],
            extension: ['.ts', '.tsx'],
          }),
        ]
      : []),
  ],
  resolve: {
    alias: {
      'graphiql-explorer': path.resolve(
        __dirname,
        'src/plugins/graphiql-explorer-fork.cjs'
      ),
      '@neo4j-cypher/codemirror/lib/cypher-state-definitions': path.resolve(
        __dirname,
        'node_modules/@neo4j-cypher/codemirror/lib/cypher-state-definitions.js'
      ),
    },
  },
  optimizeDeps: {
    // graphiql-explorer is aliased to src/plugins/graphiql-explorer-fork.cjs (CJS → needs pre-bundle)
    include: ['graphiql-explorer', 'picomatch-browser', 'lodash.includes', 'lodash.find'],
    // rolldown (Vite 8) rejects internal ./lib/ sub-path imports not listed in package exports
    // Apollo Client v4 has issues with pre-bundling, exclude it
    exclude: ['@neo4j-cypher/codemirror', '@apollo/client'],
  },
  build: {
    rollupOptions: {
      onwarn(warning, warn) {
        if (warning.code === 'SOURCEMAP_ERROR' && warning.message.includes('monaco-graphql')) return;
        warn(warning);
      },
    },
  },
  server: {
    port: 3000,
    watch: {
      // macOS creates binary ._* AppleDouble files on exFAT volumes — exclude from watching
      ignored: ['**\/._*'],
    },
    proxy: {
      '/data': 'http://127.0.0.1:8000',
      '/admin': {
        target: 'http://127.0.0.1:8000',
        bypass(req) {
          // Page navigations (Accept: text/html) are SPA routes — serve index.html
          if (req.headers.accept?.includes('text/html')) return '/index.html';
        },
      },
      '/health': 'http://127.0.0.1:8000',
      '/setup': 'http://127.0.0.1:8000',
      '/auth': 'http://127.0.0.1:8000',
    },
  },
}))
