// Copyright (c) 2026 Kenneth Stott
// Canary: e3e04116-3d2c-4402-b5f9-6764a8eef9cd
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Requirements: REQ-1984

import type { CodeLanguage } from "./CodeEditorField";

const LANGUAGE_BY_EXTENSION: Record<string, CodeLanguage> = {
  py: "python",
  yaml: "yaml",
  yml: "yaml",
  json: "json",
  sql: "sql",
  ttl: "turtle",
};

/** A file's language, from its extension. The model repository holds only `.yaml` files
 * (provisa/core/env_files.py SUFFIX refuses any other), so an extension with no language here is
 * a defect upstream and is refused by name. */
export function languageForPath(path: string): CodeLanguage {
  const dot = path.lastIndexOf(".");
  const ext = dot < 0 ? "" : path.slice(dot + 1).toLowerCase();
  const lang = LANGUAGE_BY_EXTENSION[ext];
  if (lang === undefined)
    throw new Error(`${path}: no code language for extension ${JSON.stringify(ext)}`);
  return lang;
}
