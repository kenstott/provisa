// Copyright (c) 2026 Kenneth Stott
// Canary: 2b1bfab6-b77f-4a67-8292-dd1d065d66d9
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
import es from "./locales/es.json";
import fr from "./locales/fr.json";
import de from "./locales/de.json";
import nl from "./locales/nl.json";
import he from "./locales/he.json";
import ja from "./locales/ja.json";
import zh from "./locales/zh.json";
import ru from "./locales/ru.json";
import it from "./locales/it.json";
import pt from "./locales/pt.json";
import zhHK from "./locales/zh-HK.json";
import hi from "./locales/hi.json";

// Frontend internationalization runtime (REQ-1012). English is the base
// catalog and the source of truth for keys. Core keys live in en.json; each
// migrated component owns a per-namespace file under ./locales/en/*.json which
// is glob-merged here — so parallel component migrations never edit a shared
// catalog. Each file's top-level keys become translation namespaces.
const perComponent = import.meta.glob<Record<string, unknown>>("./locales/en/*.json", {
  eager: true,
  import: "default",
});
const componentCatalog = Object.values(perComponent).reduce(
  (acc, mod) => ({ ...acc, ...mod }),
  {} as Record<string, unknown>,
);

const perComponentEs = import.meta.glob<Record<string, unknown>>("./locales/es/*.json", {
  eager: true,
  import: "default",
});
const componentCatalogEs = Object.values(perComponentEs).reduce(
  (acc, mod) => ({ ...acc, ...mod }),
  {} as Record<string, unknown>,
);

const perComponentFr = import.meta.glob<Record<string, unknown>>("./locales/fr/*.json", {
  eager: true,
  import: "default",
});
const componentCatalogFr = Object.values(perComponentFr).reduce(
  (acc, mod) => ({ ...acc, ...mod }),
  {} as Record<string, unknown>,
);

const perComponentDe = import.meta.glob<Record<string, unknown>>("./locales/de/*.json", {
  eager: true,
  import: "default",
});
const componentCatalogDe = Object.values(perComponentDe).reduce(
  (acc, mod) => ({ ...acc, ...mod }),
  {} as Record<string, unknown>,
);

const perComponentNl = import.meta.glob<Record<string, unknown>>("./locales/nl/*.json", {
  eager: true,
  import: "default",
});
const componentCatalogNl = Object.values(perComponentNl).reduce(
  (acc, mod) => ({ ...acc, ...mod }),
  {} as Record<string, unknown>,
);

const perComponentHe = import.meta.glob<Record<string, unknown>>("./locales/he/*.json", {
  eager: true,
  import: "default",
});
const componentCatalogHe = Object.values(perComponentHe).reduce(
  (acc, mod) => ({ ...acc, ...mod }),
  {} as Record<string, unknown>,
);

const perComponentJa = import.meta.glob<Record<string, unknown>>("./locales/ja/*.json", {
  eager: true,
  import: "default",
});
const componentCatalogJa = Object.values(perComponentJa).reduce(
  (acc, mod) => ({ ...acc, ...mod }),
  {} as Record<string, unknown>,
);

const perComponentZh = import.meta.glob<Record<string, unknown>>("./locales/zh/*.json", {
  eager: true,
  import: "default",
});
const componentCatalogZh = Object.values(perComponentZh).reduce(
  (acc, mod) => ({ ...acc, ...mod }),
  {} as Record<string, unknown>,
);

const perComponentRu = import.meta.glob<Record<string, unknown>>("./locales/ru/*.json", {
  eager: true,
  import: "default",
});
const componentCatalogRu = Object.values(perComponentRu).reduce(
  (acc, mod) => ({ ...acc, ...mod }),
  {} as Record<string, unknown>,
);

const perComponentIt = import.meta.glob<Record<string, unknown>>("./locales/it/*.json", {
  eager: true,
  import: "default",
});
const componentCatalogIt = Object.values(perComponentIt).reduce(
  (acc, mod) => ({ ...acc, ...mod }),
  {} as Record<string, unknown>,
);

const perComponentPt = import.meta.glob<Record<string, unknown>>("./locales/pt/*.json", {
  eager: true,
  import: "default",
});
const componentCatalogPt = Object.values(perComponentPt).reduce(
  (acc, mod) => ({ ...acc, ...mod }),
  {} as Record<string, unknown>,
);

const perComponentZhHK = import.meta.glob<Record<string, unknown>>("./locales/zh-HK/*.json", {
  eager: true,
  import: "default",
});
const componentCatalogZhHK = Object.values(perComponentZhHK).reduce(
  (acc, mod) => ({ ...acc, ...mod }),
  {} as Record<string, unknown>,
);

const perComponentHi = import.meta.glob<Record<string, unknown>>("./locales/hi/*.json", {
  eager: true,
  import: "default",
});
const componentCatalogHi = Object.values(perComponentHi).reduce(
  (acc, mod) => ({ ...acc, ...mod }),
  {} as Record<string, unknown>,
);

export const defaultNS = "translation";
export const resources = {
  en: { translation: { ...en, ...componentCatalog } },
  es: { translation: { ...es, ...componentCatalogEs } },
  fr: { translation: { ...fr, ...componentCatalogFr } },
  de: { translation: { ...de, ...componentCatalogDe } },
  nl: { translation: { ...nl, ...componentCatalogNl } },
  he: { translation: { ...he, ...componentCatalogHe } },
  ja: { translation: { ...ja, ...componentCatalogJa } },
  zh: { translation: { ...zh, ...componentCatalogZh } },
  ru: { translation: { ...ru, ...componentCatalogRu } },
  it: { translation: { ...it, ...componentCatalogIt } },
  pt: { translation: { ...pt, ...componentCatalogPt } },
  "zh-HK": { translation: { ...zhHK, ...componentCatalogZhHK } },
  hi: { translation: { ...hi, ...componentCatalogHi } },
} as const;

void i18n
  .use(LanguageDetector)
  .use(initReactI18next)
  .init({
    resources,
    fallbackLng: "en",
    // Regional variants (es-MX, es-419, fr-CA) resolve to the base catalog.
    // "zh-HK" (Traditional, HK) must precede plain "zh" resolution: exact tag
    // wins; other zh-* variants fall back to the base Simplified catalog.
    supportedLngs: [
      "en",
      "es",
      "fr",
      "de",
      "nl",
      "he",
      "ja",
      "zh",
      "zh-HK",
      "ru",
      "it",
      "pt",
      "hi",
    ],
    nonExplicitSupportedLngs: true,
    defaultNS,
    interpolation: {
      // React already escapes rendered values.
      escapeValue: false,
    },
    detection: {
      order: ["navigator"],
      caches: [],
    },
  });

export default i18n;
