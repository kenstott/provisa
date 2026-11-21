// Copyright (c) 2026 Kenneth Stott
// Canary: ce497b3d-6e0f-4724-bd2b-d3fe2b07a0f9
// Canary: placeholder
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2253: an AI-assisted request carries the navbar filter's selection (useViewDomains; null
// asks for no narrowing) as repeated ``domains=`` parameters, so the server reads, sends to the
// model and writes nothing outside it. Repeated rather than comma-joined: the no-domain domain's
// id is the empty string.

/** ``params`` with the selection appended, one ``domains=`` each. */
export function withSelectedDomains(
  params: URLSearchParams,
  domains: ReadonlySet<string> | null,
): URLSearchParams {
  if (domains !== null) for (const d of domains) params.append("domains", d);
  return params;
}

/** The selection as a query string ("?domains=…"), or "" for none. */
export function selectedDomainsQuery(domains: ReadonlySet<string> | null): string {
  if (domains === null) return "";
  return `?${withSelectedDomains(new URLSearchParams(), domains).toString()}`;
}
