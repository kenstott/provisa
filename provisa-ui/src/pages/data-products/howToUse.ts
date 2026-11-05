// Copyright (c) 2026 Kenneth Stott
// Canary: e41c7b06-8a23-4d9f-b6e5-2c90f3a7d118
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// REQ-2158: what a contract's How to use reads from it. Everything here comes from the contract's
// standard elements, servers and roles: an exported contract carries no instructions.

import type { OdcsContract, OdcsRecord } from "../../api/dataProductContracts";

/** A server as it is read by host and port, or the location an API server gives instead. */
export function serverLocation(server: OdcsRecord): unknown {
  return server.host ? [server.host, server.port].filter(Boolean).join(":") : server.location;
}

/** The most specific URI a surface has for its table. A server's location already is one on the
 * HTTP surfaces (the table's own, or the description narrowed to it); the SQL listeners take the
 * schema on a connection URI. */
export function surfaceAddress(server: OdcsRecord): unknown {
  const at = `${String(server.host)}:${String(server.port)}`;
  const database = encodeURIComponent(String(server.database));
  const schema = encodeURIComponent(String(server.schema));
  if (server.server === "pgwire") {
    return `postgresql://${at}/${database}?options=-csearch_path%3D${schema}`;
  }
  if (server.server === "flight-sql") return `grpc://${at}?database=${database}&schema=${schema}`;
  return serverLocation(server);
}

/** Who grants access to a contract's table: its roles and their first-level approvers. */
export interface Grant {
  roles: string[];
  approvers: string[];
}

export function contractGrant(contract: OdcsContract): Grant {
  const declared = contract.roles ?? [];
  return {
    roles: declared.map((r) => String(r.role)),
    approvers: [
      ...new Set(
        declared.flatMap((r) =>
          typeof r.firstLevelApprovers === "string"
            ? r.firstLevelApprovers.split(",").map((a) => a.trim())
            : [],
        ),
      ),
    ].filter(Boolean),
  };
}

/** The viewer can read the table: it holds a role the contract names. */
export const canRead = (grant: Grant, held: ReadonlySet<string>) =>
  grant.roles.some((role) => held.has(role));
