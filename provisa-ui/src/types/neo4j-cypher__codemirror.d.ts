// Copyright (c) 2026 Kenneth Stott
// Canary: d35841cf-b777-4a6e-b017-ea45bc4e6695
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Shim for @neo4j-cypher/codemirror — package.json "exports" map does not
// expose a resolvable types entry under moduleResolution "bundler".
declare module "@neo4j-cypher/codemirror" {
  import type { Extension } from "@codemirror/state";
  export function cypherLanguage(): Extension;
}
