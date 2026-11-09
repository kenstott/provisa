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

// Requirements: REQ-2221, REQ-2225

import type { Connector, ConnectorSetting } from "../../api/connectors";
import type { SourceFormState } from "./SourceFormFields";

/** Where each standard source setting a connector can set is kept in the form. */
const FORM_KEYS: Record<string, keyof SourceFormState> = {
  cache_enabled: "cacheEnabled",
  cache_ttl: "cacheTtl",
  replicate: "replicate",
  change_signal: "changeSignal",
  load_protected: "loadProtected",
  off_peak_window: "offPeakWindow",
  off_peak_tz: "offPeakTz",
  sentinel_path: "sentinelPath",
  max_live_concurrency: "maxLiveConcurrency",
  freshness_gate: "freshnessGate",
};

/** The numbers the form keeps as the text of their boxes. */
const TEXT_IN_FORM = new Set(["cache_ttl", "max_live_concurrency"]);

const inForm = (name: string, value: boolean | number | string) =>
  TEXT_IN_FORM.has(name) ? String(value) : value;

const isFixed = (s: ConnectorSetting) => s.fixed_value !== undefined;

function formValues(settings: ConnectorSetting[]): Partial<SourceFormState> {
  return Object.fromEntries(
    settings.map((s) => [
      FORM_KEYS[s.name],
      inForm(s.name, (isFixed(s) ? s.fixed_value : s.default_value) as boolean | number | string),
    ]),
  ) as Partial<SourceFormState>;
}

/** The form's values when a connector is picked: its defaults and its fixed values. */
export const connectorFormValues = (connector: Connector): Partial<SourceFormState> =>
  formValues(connector.settings);

/** The form's values the connector fixes. */
export const fixedFormValues = (connector: Connector): Partial<SourceFormState> =>
  formValues(connector.settings.filter(isFixed));

/** The form's values shown read-only; none for a source made from no connector. */
export const fixedFormKeys = (
  connector: Connector | null | undefined,
): ReadonlySet<keyof SourceFormState> =>
  new Set((connector?.settings ?? []).filter(isFixed).map((s) => FORM_KEYS[s.name]));

/** The settings with a default that the creator changed: the server applies every other default. */
export function connectorChose(form: SourceFormState, connector: Connector): string[] {
  return connector.settings
    .filter((s) => !isFixed(s))
    .filter((s) => form[FORM_KEYS[s.name]] !== inForm(s.name, s.default_value as never))
    .map((s) => s.name);
}

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

/** The setup form's fields when a connector is picked: each required value it sets. */
export function connectorFields(connector: Connector): Record<string, string> {
  const out: Record<string, string> = {};
  for (const r of connector.requires) {
    const value = r.fixed_value ?? r.default_value;
    if (value !== undefined) out[requiredField(r.name)] = value;
  }
  return out;
}

/** A saved connector source's fields, with what its connector fixes as the connector now says. */
export function heldFields(
  fields: Record<string, string>,
  connector: Connector,
): Record<string, string> {
  const settings = JSON.parse(fields.code_connector_settings || "{}") as Record<string, unknown>;
  for (const r of connector.requires) {
    if (r.fixed_value !== undefined) settings[r.name] = r.fixed_value;
  }
  return { ...fields, code_connector_settings: JSON.stringify(settings) };
}

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
 * reference, never a value typed here. `chose` is {@link connectorChose}. */
export function connectorMappingJson(
  fields: Record<string, string>,
  connector: Connector,
  chose: string[] = [],
): string {
  const secrets: Record<string, string | null> = {};
  const settings: Record<string, string> = {};
  for (const r of connector.requires) {
    const given = fields[requiredField(r.name)] ?? "";
    if (r.secret) secrets[r.name] = given === r.name ? null : `\${secret:${given}}`;
    else settings[r.name] = r.fixed_value ?? given;
  }
  return JSON.stringify({
    connector_join: {
      connector: connector.name,
      host: fields.connector_host ?? "",
      port: Number(fields.connector_port),
      pairing_code: fields.connector_pairing_code ?? "",
      ...(chose.length ? { chose } : {}),
    },
    secrets,
    settings,
  });
}
