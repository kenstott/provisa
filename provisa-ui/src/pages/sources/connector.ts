// Copyright (c) 2026 Kenneth Stott
// Canary: 4ea1b8d5-c273-4e96-a03c-8d61f9b27e40
//
// This source code is licensed under the Business Source License 1.1
// found in the LICENSE file in the root directory of this source tree.
//
// NOTICE: Use of this software for training artificial intelligence or
// machine learning models is strictly prohibited without explicit written
// permission from the copyright holder.

// A source made from a connector the organization offers: what its setup form holds, and the
// mapping that joins the host its operator runs. Everything else about the source -- its code hash,
// its manifest, how the host is reached afterwards -- the server fills in from the handshake.

import type { Connector } from "../../api/connectors";

/** How a connector is told from a built-in type in the source type picker. */
const PICKED = "connector:";

export const connectorPick = (name: string): string => `${PICKED}${name}`;

export function pickedConnector(value: string, connectors: Connector[]): Connector | null {
  if (!value.startsWith(PICKED)) return null;
  const name = value.slice(PICKED.length);
  return connectors.find((c) => c.name === name) ?? null;
}

/** The field a required value is kept under in the form. */
export const requiredField = (name: string): string => `connector_require_${name}`;

/** Which of the form's values are still missing, by the names the form labels them with. */
export function missingConnectorFields(
  fields: Record<string, string>,
  connector: Connector,
): string[] {
  const missing = ["connector_host", "connector_port", "connector_pairing_code"].filter(
    (k) => !fields[k],
  );
  for (const r of connector.requires) {
    if (!fields[requiredField(r.name)]) missing.push(requiredField(r.name));
  }
  return missing;
}

/** The mapping that joins the connector, consumed by the server when the source is created: the
 * pairing code does not outlive it. A secret is always named: the org secret of that name, or a
 * reference, never a value typed here. */
export function connectorMappingJson(fields: Record<string, string>, connector: Connector): string {
  const secrets: Record<string, string | null> = {};
  const settings: Record<string, string> = {};
  for (const r of connector.requires) {
    const given = fields[requiredField(r.name)] ?? "";
    if (r.secret) secrets[r.name] = given === r.name ? null : `\${secret:${given}}`;
    else settings[r.name] = given;
  }
  return JSON.stringify({
    connector_join: {
      connector: connector.name,
      host: fields.connector_host ?? "",
      port: Number(fields.connector_port),
      pairing_code: fields.connector_pairing_code ?? "",
    },
    secrets,
    settings,
  });
}
