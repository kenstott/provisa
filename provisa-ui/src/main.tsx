// Copyright (c) 2026 Kenneth Stott
// Canary: 00916f3a-8398-45ce-9f67-c6a98a0c948d
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { createRoot } from 'react-dom/client'
import './index.css'
import App from './App.tsx'

declare global {
  interface Window {
    __provisaHideSplash?: () => void
  }
}

createRoot(document.getElementById('root')!).render(
  <App />,
)

// Dismiss the pre-React convergence-splash once the app has mounted and painted its
// first frame. The splash enforces its own minimum display time + fade, so this only
// signals readiness.
requestAnimationFrame(() => requestAnimationFrame(() => window.__provisaHideSplash?.()))
