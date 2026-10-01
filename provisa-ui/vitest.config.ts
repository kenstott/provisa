// Copyright (c) 2026 Kenneth Stott
// Canary: bc6c6a9c-f45b-4e42-abb3-e0266cb692c4
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react";
import path from "path";
import { graphqlLoader } from "./src/plugins/graphql-loader";

export default defineConfig({
  plugins: [graphqlLoader(), react()],
  resolve: {
    alias: [
      {
        find: "graphiql-explorer",
        replacement: path.resolve(__dirname, "src/plugins/graphiql-explorer-fork.cjs"),
      },
      // Exact match only — sub-paths like monaco-editor/esm/... must remain resolvable
      {
        find: /^monaco-editor$/,
        replacement: path.resolve(__dirname, "src/__mocks__/monaco-editor.ts"),
      },
    ],
  },
  test: {
    environment: "jsdom",
    globals: true,
    setupFiles: ["./src/test-setup.ts"],
    exclude: ["e2e/**", "node_modules/**"],
    // 'threads' (the default) gives each test file its own module registry. Under 'vmThreads' with
    // fileParallelism:false every file shared one registry, so a vi.mock in one file decided which
    // version of that module EVERY file got — mocks silently applied or failed to apply depending
    // on file order.
    pool: "threads",
    // Every per-test and per-wait budget here is wall clock. This suite runs on a host shared with
    // other test tiers (pytest, Docker stacks, Playwright), and at a load average of 40-50 on 12
    // cores the jsdom renders and userEvent sequences run several times slower: three full runs at
    // that load failed 46, 88 and 10 tests, all but a handful "Test timed out in 5000ms", with the
    // rest being 1 s waitFor budgets or work a timed-out test left running into the next one. The
    // budgets bound a hang, not a speed target, so they are sized for a loaded host; a real hang
    // still fails, just later. The waitFor/findBy budget is set in src/test-setup.ts.
    testTimeout: 60000,
    hookTimeout: 60000,
    coverage: {
      provider: "v8",
      reporter: ["json"],
      reportsDirectory: ".nyc_output/vitest",
    },
  },
});
