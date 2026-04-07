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
    },
  },
  optimizeDeps: {
    // graphiql-explorer is aliased to src/plugins/graphiql-explorer-fork.cjs (CJS → needs pre-bundle)
    include: ['graphiql-explorer', 'picomatch-browser'],
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
    proxy: {
      '/data': 'http://localhost:8001',
      '/admin': 'http://localhost:8001',
      '/health': 'http://localhost:8001',
    },
  },
}))
