// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import i18n from "i18next";
import { initReactI18next } from "react-i18next";
import LanguageDetector from "i18next-browser-languagedetector";
import en from "./locales/en.json";

// Frontend internationalization runtime (REQ-1012). English is the base
// catalog and the source of truth for keys. Core keys live in en.json; each
// migrated component owns a per-namespace file under ./locales/en/*.json which
// is glob-merged here — so parallel component migrations never edit a shared
// catalog. Each file's top-level keys become translation namespaces.
const perComponent = import.meta.glob<Record<string, unknown>>(
  "./locales/en/*.json",
  { eager: true, import: "default" },
);
const componentCatalog = Object.values(perComponent).reduce(
  (acc, mod) => ({ ...acc, ...mod }),
  {} as Record<string, unknown>,
);

export const defaultNS = "translation";
export const resources = {
  en: { translation: { ...en, ...componentCatalog } },
} as const;

void i18n
  .use(LanguageDetector)
  .use(initReactI18next)
  .init({
    resources,
    fallbackLng: "en",
    defaultNS,
    interpolation: {
      // React already escapes rendered values.
      escapeValue: false,
    },
    detection: {
      order: ["localStorage", "navigator"],
      caches: ["localStorage"],
      lookupLocalStorage: "provisa_lang",
    },
  });

export default i18n;
