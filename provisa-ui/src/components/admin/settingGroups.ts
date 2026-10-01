// Copyright (c) 2026 Kenneth Stott
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

import type { CatalogSetting } from "../../api/admin";

/**
 * REQ-1913: the transport a network or TLS setting belongs to, from its key: `server.<t>_port` and
 * `tls.<t>_cert` / `tls.<t>_key` belong to transport `<t>`; anything else (the hostname, the HTTP
 * certificate pair) is general.
 */
export function networkTransport(s: CatalogSetting): string {
  const m = /^server\.(\w+)_port$/.exec(s.key) ?? /^tls\.(\w+)_(?:cert|key)$/.exec(s.key);
  return m ? m[1] : "general";
}

/** How a card's settings are grouped into panels, by card id; a card not listed is not grouped. */
export const CARD_GROUPS: Record<string, (s: CatalogSetting) => string> = {
  network: networkTransport,
};
