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
