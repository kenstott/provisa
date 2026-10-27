// Copyright (c) 2026 Kenneth Stott
// Canary: 7a8e52e2-e02d-4809-9ea4-07c587919828
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// The lifecycle every governed object shares -- a glossary term (REQ-2144), a data product.
export type LifecycleStatus = "draft" | "live" | "deprecated" | "retired";
