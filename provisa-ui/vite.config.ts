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
import type { Plugin } from 'vite'
import react from '@vitejs/plugin-react'
import istanbul from 'vite-plugin-istanbul'
import path from 'path'
import _monacoEditorPluginModule from 'vite-plugin-monaco-editor'
import fs from 'fs'
// eslint-disable-next-line @typescript-eslint/no-explicit-any
const monacoEditorPlugin: (...args: any[]) => any =
  (_monacoEditorPluginModule as any).default ?? _monacoEditorPluginModule

function graphqlPlugin(): Plugin {
  return {
    name: 'graphql-loader',
    async resolveId(id, importer) {
      if (id.endsWith('.graphql')) {
        // Resolve relative path to absolute
        const resolvedPath = importer
          ? path.resolve(path.dirname(importer), id)
          : path.resolve(__dirname, id);
        // Mark as virtual module with \0 prefix
        return '\0' + resolvedPath;
      }
    },
    async load(id) {
      // Check for virtual module marker
      if (!id.startsWith('\0') || !id.slice(1).endsWith('.graphql')) return null;

      // Remove virtual marker to get actual file path
      const filePath = id.slice(1);

      try {
        if (!fs.existsSync(filePath)) {
          return null;
        }

        const code = fs.readFileSync(filePath, 'utf-8');

        // Extract operation names (query|mutation followed by name)
        const operations: string[] = [];
        const lines = code.split('\n');
        for (const line of lines) {
          const match = line.match(/^\s*(query|mutation)\s+(\w+)/);
          if (match) {
            operations.push(match[2]);
          }
        }

        // Escape backticks and dollar signs for template string
        const escaped = code
          .replace(/\\/g, '\\\\')  // Escape backslashes first
          .replace(/`/g, '\\`')     // Escape backticks
          .replace(/\$/g, '\\$');   // Escape dollar signs

        // Build output with proper escaping
        const lines_out: string[] = [
          "import { gql } from '@apollo/client';",
          '',
          `const doc = gql\`${escaped}\`;`,
          '',
        ];

        // Export each named operation
        for (const op of operations) {
          lines_out.push(`export const ${op} = doc;`);
        }

        lines_out.push('export default doc;');

        return lines_out.join('\n');
      } catch (error) {
        return null;
      }
    },
  };
}

const logger = createLogger()
const origWarn = logger.warn.bind(logger)
logger.warn = (msg, opts) => {
  if (msg.includes('monaco-graphql') && msg.includes('sourcemap')) return
  origWarn(msg, opts)
}

export default defineConfig(({ mode }) => ({
  customLogger: logger,
  plugins: [
    graphqlPlugin(),
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
