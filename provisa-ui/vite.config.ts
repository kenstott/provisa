import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import istanbul from 'vite-plugin-istanbul'
import path from 'path'

export default defineConfig(({ mode }) => ({
  plugins: [
    react(),
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
  server: {
    port: 3000,
    proxy: {
      '/data': 'http://localhost:8001',
      '/admin': 'http://localhost:8001',
      '/health': 'http://localhost:8001',
    },
  },
}))
