// Copyright (c) 2026 Kenneth Stott
// Canary: a488fc6f-6c44-4c21-80e1-5d4f60bdd2ac
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { CatalogSetting } from "../../api/admin";

/**
 * REQ-1913: the transport a network or TLS setting belongs to, from its key: `server.<t>_port`,
 * `server.<t>_compression` and `tls.<t>_cert` / `tls.<t>_key` belong to transport `<t>`; anything
 * else (the hostname, the HTTP certificate pair) is general.
 */
export function networkTransport(s: CatalogSetting): string {
  const m =
    /^server\.(\w+)_(?:port|compression)$/.exec(s.key) ?? /^tls\.(\w+)_(?:cert|key)$/.exec(s.key);
  return m ? m[1] : "general";
}

/** How a card's settings are grouped into panels, by card id; a card not listed is not grouped. */
export const CARD_GROUPS: Record<string, (s: CatalogSetting) => string> = {
  network: networkTransport,
};
