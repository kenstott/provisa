// Copyright (c) 2026 Kenneth Stott
// Canary: cca4a8cb-ed12-455e-9479-20704e2ff922
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import js from '@eslint/js'
import globals from 'globals'
import reactHooks from 'eslint-plugin-react-hooks'
import reactRefresh from 'eslint-plugin-react-refresh'
import tseslint from 'typescript-eslint'
import comments from '@eslint-community/eslint-plugin-eslint-comments/configs'
import { defineConfig, globalIgnores } from 'eslint/config'

export default defineConfig([
  globalIgnores(['dist']),
  {
    files: ['**/*.{ts,tsx}'],
    extends: [
      js.configs.recommended,
      tseslint.configs.recommended,
      reactHooks.configs.flat.recommended,
      reactRefresh.configs.vite,
      comments.recommended,
    ],
    languageOptions: {
      ecmaVersion: 2020,
      globals: globals.browser,
    },
    rules: {
      // Honor the codebase's `_`-prefix convention for intentionally-unused
      // bindings (omit-via-rest, deliberately-unused props/params).
      '@typescript-eslint/no-unused-vars': ['error', {
        argsIgnorePattern: '^_',
        varsIgnorePattern: '^_',
        caughtErrorsIgnorePattern: '^_',
        ignoreRestSiblings: true,
      }],
      // `eslint-enable` only closes a disable pair; the justification lives on the
      // matching `eslint-disable`, so it does not need its own description.
      '@eslint-community/eslint-comments/require-description': ['error', { ignore: ['eslint-enable'] }],
      // Allow file-top `/* eslint-disable rule -- reason */` (whole-file intentional
      // patterns, e.g. context Provider+hook modules) without a matching enable.
      '@eslint-community/eslint-comments/disable-enable-pair': ['error', { allowWholeFile: true }],
      // A file past 1000 lines is a design signal — subdivide into modules
      // (pure helpers, sub-components, hooks). See react-graphql SKILL.md.
      'max-lines': ['error', { max: 1000, skipBlankLines: true, skipComments: true }],
    },
  },
  // Grandfather list: files that predate the max-lines rule. Each entry is a
  // debt marker — subdivide the file, then delete its line here. When this list
  // is empty, max-lines is enforced everywhere with no exceptions.
  {
    files: [
      'src/pages/GraphFrame.tsx',
      'src/pages/SqlPage.tsx',
      'src/components/SqlModelingModal.tsx',
      'src/pages/SourcesPage.tsx',
      'src/pages/TablesPage.tsx',
      'src/pages/AdminPage.tsx',
      'src/pages/GraphPage.tsx',
      'src/pages/RelationshipsPage.tsx',
      'src/api/admin.ts',
      'src/pages/CommandsPage.tsx',
    ],
    rules: { 'max-lines': 'off' },
  },
])
