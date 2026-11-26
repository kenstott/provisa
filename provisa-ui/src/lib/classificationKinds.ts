// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2147 (amended 2026-11-25): a classification kind is shipped, its words translated by its id,
// or the org's own custom kind (provisa/profiler/classify.py CUSTOM_PREFIX), named by the label the
// org gave it, in whatever language it wrote it.

export const CUSTOM_KIND_PREFIX = "custom.";

export function isCustomKind(id: string): boolean {
  return id.startsWith(CUSTOM_KIND_PREFIX);
}
