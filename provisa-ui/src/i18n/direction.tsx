// Copyright (c) 2026 Kenneth Stott
// Canary: 7d4f2c1e-9b3a-4e8f-b6d5-2a1c8e7f4b90
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import { useEffect } from 'react'
import { useDirection } from '@mantine/core'
import { useTranslation } from 'react-i18next'

// RTL layout support: layout direction is a function of the active locale.
// Only the base subtag matters (he-IL → he). The set covers the RTL scripts
// i18next could resolve to; today only `he` ships a catalog.
const RTL_BASE_LNGS = new Set(['he', 'ar', 'fa', 'ur', 'yi'])

export function dirForLanguage(lng: string): 'rtl' | 'ltr' {
  const base = lng.toLowerCase().split('-')[0]
  return RTL_BASE_LNGS.has(base) ? 'rtl' : 'ltr'
}

// Keeps <html dir>/<html lang> and Mantine's direction context in sync with
// the active i18next language, including runtime switches. Must render inside
// both DirectionProvider and I18nextProvider.
export function DirectionSync() {
  const { setDirection } = useDirection()
  const { i18n } = useTranslation()

  useEffect(() => {
    const apply = (lng: string) => {
      setDirection(dirForLanguage(lng))
      document.documentElement.setAttribute('lang', lng)
    }
    apply(i18n.language)
    i18n.on('languageChanged', apply)
    return () => {
      i18n.off('languageChanged', apply)
    }
  }, [i18n, setDirection])

  return null
}
