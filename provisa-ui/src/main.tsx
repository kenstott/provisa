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
// Mantine styles must load before app CSS so local overrides win the cascade.
import '@mantine/core/styles.css'
import '@mantine/notifications/styles.css'
import { MantineProvider, localStorageColorSchemeManager } from '@mantine/core'
import { Notifications } from '@mantine/notifications'
import { I18nextProvider } from 'react-i18next'
import { theme } from './theme/theme.ts'
import './theme/tokens.css'
import i18n from './i18n/index.ts'
import './index.css'
import App from './App.tsx'

declare global {
  interface Window {
    __provisaHideSplash?: () => void
  }
}

// Key must match the inline anti-flash script in index.html.
const colorSchemeManager = localStorageColorSchemeManager({
  key: 'provisa-color-scheme',
})

createRoot(document.getElementById('root')!).render(
  <MantineProvider
    theme={theme}
    defaultColorScheme="dark"
    colorSchemeManager={colorSchemeManager}
  >
    <I18nextProvider i18n={i18n}>
      <Notifications />
      <App />
    </I18nextProvider>
  </MantineProvider>,
)

// Dismiss the pre-React convergence-splash once the app has mounted and painted its
// first frame. The splash enforces its own minimum display time + fade, so this only
// signals readiness.
requestAnimationFrame(() => requestAnimationFrame(() => window.__provisaHideSplash?.()))
