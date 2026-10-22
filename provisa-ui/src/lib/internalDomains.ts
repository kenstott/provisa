// Copyright (c) 2026 Kenneth Stott
// Canary: eb03db9f-cea2-4abe-8c0a-71a0d2c99d03
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

/**
 * The domains Provisa generates for itself: `meta` (the model's own metadata tables) and `ops`
 * (activity traces). They are read like any domain (the Domains menu, a role's domain access, a
 * filter) but no picker offers one as the domain to put something into: a table, view, model,
 * source, data product, command or glossary term.
 *
 * Not `Domain.isSystem`: that also marks the default domain of a single-domain org, which is where
 * everything of that org goes.
 */
export const INTERNAL_DOMAIN_IDS: readonly string[] = ["meta", "ops"];

export function isInternalDomain(domainId: string): boolean {
  return INTERNAL_DOMAIN_IDS.includes(domainId);
}

/** Whether something may be put into `domainId`: not an internal domain, not the unnamed "". */
export function isAssignableDomain(domainId: string): boolean {
  return domainId !== "" && !isInternalDomain(domainId);
}

/** The ids of `domainIds` something may be put into. */
export function assignableDomainIds(domainIds: readonly string[]): string[] {
  return domainIds.filter(isAssignableDomain);
}
