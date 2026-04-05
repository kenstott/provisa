// Copyright (c) 2026 Kenneth Stott
// Canary: 00916f3a-8398-45ce-9f67-c6a98a0c948d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import './index.css'
import App from './App.tsx'
// ?worker imports must live in app source — rolldown cannot process them inside node_modules
import EditorWorker from 'monaco-editor/esm/vs/editor/editor.worker.js?worker'
import JsonWorker from 'monaco-editor/esm/vs/language/json/json.worker.js?worker'
import GraphQLWorker from 'monaco-graphql/esm/graphql.worker.js?worker'

globalThis.MonacoEnvironment = {
  getWorker(_workerId: string, label: string) {
    switch (label) {
      case 'json': return new JsonWorker()
      case 'graphql': return new GraphQLWorker()
      default: return new EditorWorker()
    }
  },
}

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <App />
  </StrictMode>,
)
