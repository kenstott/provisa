// Copyright (c) 2026 Kenneth Stott
// Canary: 3f8b1c2d-9e4a-4b7f-a6c5-8d2e1f9b3a5c
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/** Minimal vitest stub for monaco-editor. */
export const languages = {
  registerCompletionItemProvider: () => ({ dispose: () => {} }),
  CompletionItemKind: {
    EnumMember: 20,
    Keyword: 17,
    Text: 0,
    Method: 1,
    Function: 2,
    Constructor: 3,
    Field: 4,
    Variable: 5,
    Class: 6,
    Interface: 7,
    Module: 8,
    Property: 9,
    Unit: 10,
    Value: 11,
    Enum: 12,
    Color: 15,
    File: 16,
    Reference: 17,
    Folder: 18,
    TypeParameter: 24,
    Snippet: 25,
  },
};

export const editor = {
  create: () => ({ dispose: () => {}, onDidChangeModelContent: () => ({ dispose: () => {} }) }),
  createModel: () => null,
  setModelLanguage: () => {},
};

export const Uri = {
  parse: (s: string) => ({ toString: () => s }),
};

export default { languages, editor, Uri };
