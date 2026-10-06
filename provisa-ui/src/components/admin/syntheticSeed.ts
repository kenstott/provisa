// Copyright (c) 2026 Kenneth Stott
// Canary: e7c41b58-0a92-4d36-b3f8-2c6d9e1a5f07
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1939: the synthetic dataset a new environment may be seeded with (environments_router
// SyntheticSeedBody).

export interface SyntheticSeed {
  dataset: string;
  tables: string[];
  profile_env: string;
  scale: number;
  seed: number;
}

export const EMPTY_SEED: SyntheticSeed = {
  dataset: "",
  tables: [],
  profile_env: "prod",
  scale: 1,
  seed: 1,
};
