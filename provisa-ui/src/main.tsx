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
