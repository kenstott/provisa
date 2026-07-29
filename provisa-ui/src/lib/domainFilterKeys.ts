// Copyright (c) 2026 Kenneth Stott
// Canary: 872371cd-9c23-4c01-96f9-69a5b000a6a7
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-1297: the persisted domain filter is SESSION- AND ORG-scoped state — its ids name the domains
// of one identity's one org. Kept in its own module because three places need the keys (the filter
// itself, lib/session.ts's SESSION_KEYS, and AuthContext's org switch) and AuthContext ↔
// DomainFilterContext already import each other.
//
// Left standing across a sign-out or an org switch, a domain the previous session had unchecked is in
// `known` but absent from `checked`, and mergeCheckedDomains keeps it unchecked — a freshly
// provisioned org whose meta and ops domains do exist server-side then renders with them missing,
// indistinguishable from a provisioning failure.
export const CHECKED_DOMAINS_KEY = "provisa.checkedDomains";
export const KNOWN_DOMAINS_KEY = "provisa.knownDomains";
