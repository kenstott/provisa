// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * How a metadata-export provider is named to a user: "<name> (beta)" while its adapter is not
 * yet proven against a live instance. The flag is the provider class's own (`beta` on the
 * adapter); the admin API reports it as `beta_providers`, and copy outside the admin page reads
 * the same list from src/generated/exportProviders.json (scripts/gen_export_providers.py).
 */

import type { TFunction } from "i18next";

import generated from "../generated/exportProviders.json";

const BETA = new Set<string>(generated.beta);

/** ``name`` as a user sees provider ``id``: ``name`` (beta) while ``id`` is in ``beta``. */
export function exportProviderName(
  id: string,
  name: string,
  t: TFunction,
  beta: ReadonlySet<string> = BETA,
): string {
  return beta.has(id) ? t("metadataExportTab.betaProvider", { name }) : name;
}
