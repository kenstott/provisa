// Copyright (c) 2026 Kenneth Stott
// Canary: 2b1bfab6-b77f-4a67-8292-dd1d065d66d9
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import i18n, { type BackendModule, type ReadCallback } from "i18next";
import { initReactI18next } from "react-i18next";
import LanguageDetector from "i18next-browser-languagedetector";
import en from "./locales/en.json";

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

// Every other language's catalog is loaded only when it is the session's language (REQ-1342: the
// browser's, read once at start). Bundled eagerly, all thirteen were part of every page load: some
// 2,500 catalog modules ahead of the app's own, and 27 MB of JSON shipped to every user.
const coreCatalogs = import.meta.glob<Record<string, unknown>>(
  ["./locales/*.json", "!./locales/en.json"],
  { import: "default" },
);
const componentCatalogs = import.meta.glob<Record<string, unknown>>(
  ["./locales/*/*.json", "!./locales/en/*.json"],
  { import: "default" },
);

export const defaultNS = "translation";

// "zh-HK" (Traditional, HK) must precede plain "zh" resolution: exact tag wins; other zh-*
// variants fall back to the base Simplified catalog.
export const supportedLngs = [
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
] as const;

/** ``lng``'s whole catalog: its core file, then each component's file over it. */
export async function loadCatalog(lng: string): Promise<Record<string, unknown>> {
  if (lng === "en") return { ...en, ...componentCatalog };
  const core = coreCatalogs[`./locales/${lng}.json`];
  if (core === undefined) throw new Error(`no catalog for language ${lng}`);
  const components = Object.entries(componentCatalogs)
    .filter(([path]) => path.startsWith(`./locales/${lng}/`))
    .map(([, load]) => load());
  const parts = await Promise.all([core(), ...components]);
  return parts.reduce((acc, part) => ({ ...acc, ...part }), {} as Record<string, unknown>);
}

const catalogBackend: BackendModule = {
  type: "backend",
  init() {},
  read(lng: string, _ns: string, callback: ReadCallback) {
    loadCatalog(lng).then(
      (catalog) => callback(null, catalog as never),
      (err: unknown) => callback(err as Error, false),
    );
  },
};

/** Settles once the session's language has its catalog: rendering waits for it, so a reader of
 * another language never sees English first. */
export const i18nReady = i18n
  .use(LanguageDetector)
  .use(catalogBackend)
  .use(initReactI18next)
  .init({
    resources: { en: { [defaultNS]: { ...en, ...componentCatalog } } },
    // English is bundled; the session's language, if another, is read by catalogBackend.
    partialBundledLanguages: true,
    fallbackLng: "en",
    // Regional variants (es-MX, es-419, fr-CA) resolve to the base catalog.
    supportedLngs: [...supportedLngs],
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
