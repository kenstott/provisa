// Copyright (c) 2026 Kenneth Stott
// Canary: 0c57de91-2ef9-459b-9367-d1b230f62982
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// Which loader the demo-sandbox screen shows: chosen at random, the source injectable for tests.

export const SANDBOX_LOADERS = ["cube", "box", "gift", "pot", "plant", "score"] as const;
export type SandboxLoaderKind = (typeof SANDBOX_LOADERS)[number];

/** The page's title goes with its animation: what is on screen is what the title says is done. */
const TITLE_KEYS: Partial<Record<SandboxLoaderKind, string>> = {
  cube: "demoOpenPage.titleMaking",
  pot: "demoOpenPage.titleCooking",
  plant: "demoOpenPage.titleGrowing",
  score: "demoOpenPage.titleComposing",
};

/** The i18n key of the title shown over loader `kind`. */
export function loaderTitleKey(kind: SandboxLoaderKind): string {
  return TITLE_KEYS[kind] ?? "demoOpenPage.title";
}

/** One loader at random; `random` returns [0, 1) like Math.random. */
export function pickLoader(random: () => number = Math.random): SandboxLoaderKind {
  const i = Math.floor(random() * SANDBOX_LOADERS.length);
  return SANDBOX_LOADERS[Math.min(Math.max(i, 0), SANDBOX_LOADERS.length - 1)];
}
