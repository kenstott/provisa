// Copyright (c) 2026 Kenneth Stott
// Canary: bc6c6a9c-f45b-4e42-abb3-e0266cb692c4
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { defineConfig } from 'vitest/config'
import react from '@vitejs/plugin-react'
import path from 'path'

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: [
      {
        find: 'graphiql-explorer',
        replacement: path.resolve(__dirname, 'src/plugins/graphiql-explorer-fork.cjs'),
      },
      // Exact match only — sub-paths like monaco-editor/esm/... must remain resolvable
      {
        find: /^monaco-editor$/,
        replacement: path.resolve(__dirname, 'src/__mocks__/monaco-editor.ts'),
      },
    ],
  },
  test: {
    environment: 'jsdom',
    globals: true,
    setupFiles: ['./src/test-setup.ts'],
    exclude: ['e2e/**', 'node_modules/**'],
    pool: 'vmThreads',
    fileParallelism: false,
    coverage: {
      provider: 'v8',
      reporter: ['json'],
      reportsDirectory: '.nyc_output/vitest',
    },
  },
})
