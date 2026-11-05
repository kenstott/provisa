// Copyright (c) 2026 Kenneth Stott
// Canary: 9c2f5a16-3d84-4e7b-a0c9-5b1e8d7f2a43
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The languages a command binding names a command in (REQ-2168), and the command's sections.

import type { LanguageBinding } from "../../api/actions";

export const BINDING_LANGUAGES = ["sql", "cypher", "graphql", "rest", "mcp", "sparql"] as const;
export type BindingLanguage = (typeof BINDING_LANGUAGES)[number];
export type LanguageBindings = Partial<Record<BindingLanguage, LanguageBinding>>;

export const COMMAND_SECTIONS = ["command", "bindings"] as const;
export type CommandSection = (typeof COMMAND_SECTIONS)[number];
