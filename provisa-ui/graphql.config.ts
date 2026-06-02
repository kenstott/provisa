// Copyright (c) 2026 Kenneth Stott
// Canary: 066221f2-c5a1-40e7-b2bb-0f14c6424ea4
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { IGraphQLConfig } from 'graphql-config';

const config: IGraphQLConfig = {
  schema: 'schema.graphql',
  documents: ['src/**/*.graphql'],
  extensions: {
    codegen: {
      generates: {
        'src/generated/graphql.ts': {
          preset: 'client',
          config: {
            useTypeImports: true,
          },
        },
      },
    },
  },
};

export default config;
